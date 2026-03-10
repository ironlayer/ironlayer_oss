"""Webhook endpoints: GitHub push events and webhook config management."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, SettingsDep, TenantDep
from api.http_errors import not_found_404
from api.middleware.rbac import Permission, Role, require_permission
from api.services.github_webhook_service import GitHubWebhookService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class WebhookConfigCreate(BaseModel):
    """Request body for ``POST /webhooks/config``."""

    repo_url: str = Field(..., description="Repository clone or HTTPS URL.")
    branch: str = Field(default="main", description="Branch to listen on.")
    secret: str = Field(..., min_length=32, description="Webhook secret (min 32 chars, ~128-bit entropy).")
    auto_plan: bool = Field(default=True, description="Auto-generate plans on push.")
    auto_apply: bool = Field(default=False, description="Auto-apply generated plans.")


# ---------------------------------------------------------------------------
# HMAC validation helper
# ---------------------------------------------------------------------------


def _verify_webhook_hmac(
    body: bytes,
    signature_header: str,
    secret: str,
) -> bool:
    """Compute HMAC-SHA256 and perform constant-time comparison.

    Parameters
    ----------
    body:
        Raw request body bytes.
    signature_header:
        The full ``X-Hub-Signature-256`` value (``sha256=<hex>``).
    secret:
        Plaintext webhook secret for HMAC computation.

    Returns
    -------
    bool
        ``True`` if the computed digest matches the header value.
    """
    if not signature_header.startswith("sha256="):
        return False

    expected_sig = (
        "sha256="
        + hmac.new(
            secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()
    )

    return hmac.compare_digest(expected_sig, signature_header)


# ---------------------------------------------------------------------------
# GitHub webhook receiver
# ---------------------------------------------------------------------------


@router.post("/github")
async def github_webhook(request: Request) -> dict[str, Any]:
    """Receive GitHub webhook push events.

    Validates the ``X-Hub-Signature-256`` header via HMAC-SHA256
    computation and dispatches the event to the webhook service.
    This endpoint bypasses JWT auth (validated by HMAC signature
    instead).

    Only ``push`` events are processed; other event types return
    ``{"status": "ignored"}``.
    """
    event_type = request.headers.get("x-github-event", "")

    if event_type != "push":
        return {"status": "ignored", "event_type": event_type}

    # Read the raw body first -- needed for both HMAC verification and
    # JSON parsing.  The HMAC MUST be verified BEFORE any processing.
    body = await request.body()
    signature = request.headers.get("x-hub-signature-256", "")

    if not signature:
        raise HTTPException(status_code=400, detail="Missing X-Hub-Signature-256 header")

    if not signature.startswith("sha256="):
        raise HTTPException(status_code=400, detail="Invalid signature format")

    # Parse the event payload (minimal extraction for config lookup).
    try:
        event_data = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Extract repo_url and branch from the payload so we can look up
    # the matching webhook config (and its encrypted secret).
    repo = event_data.get("repository", {})
    repo_url = repo.get("clone_url", "") or repo.get("html_url", "")
    ref = event_data.get("ref", "")
    branch = ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ref

    # Look up the webhook configuration.  The search is across all
    # tenants because the webhook receiver authenticates via HMAC
    # rather than JWT.  Tenant isolation is ensured by per-config
    # secret validation -- a request can only succeed if it carries
    # the HMAC matching *that specific config's* secret.
    from api.dependencies import get_session_factory, get_settings

    session_factory = get_session_factory()
    async with session_factory() as session:
        from core_engine.state.tables import WebhookConfigTable
        from sqlalchemy import select

        result = await session.execute(
            select(WebhookConfigTable).where(
                WebhookConfigTable.provider == "github",
                WebhookConfigTable.repo_url == repo_url,
                WebhookConfigTable.branch == branch,
            )
        )
        config = result.scalar_one_or_none()

        if config is None:
            return {"status": "ignored", "reason": "no_matching_config"}

        # ---------------------------------------------------------------
        # HMAC-SHA256 signature verification (MUST happen before any
        # event processing, tenant context setting, or DB mutations).
        # ---------------------------------------------------------------
        secret_encrypted = getattr(config, "secret_encrypted", None)

        if secret_encrypted:
            # Decrypt the stored secret to compute the expected HMAC.
            from api.security import CredentialVault

            settings = get_settings(request)
            vault = CredentialVault(settings.credential_encryption_key.get_secret_value())
            try:
                plaintext_secret = vault.decrypt(secret_encrypted)
            except Exception:
                logger.error(
                    "Failed to decrypt webhook secret for config id=%s repo=%s; rejecting request.",
                    config.id,
                    repo_url,
                )
                raise HTTPException(
                    status_code=500,
                    detail="Webhook secret decryption failed",
                )

            if not _verify_webhook_hmac(body, signature, plaintext_secret):
                logger.warning(
                    "Webhook HMAC verification failed for repo=%s branch=%s config_id=%s tenant=%s",
                    repo_url,
                    branch,
                    config.id,
                    config.tenant_id,
                )
                raise HTTPException(
                    status_code=403,
                    detail="Signature verification failed",
                )
        else:
            # secret_encrypted is null — this config was created before
            # migration 023 or was not given a secret.  Without a secret
            # we cannot compute the expected HMAC so the request MUST be
            # rejected.  Operators must re-create the webhook config with
            # a secret or run migration 023 to backfill encrypted secrets.
            # Logging and proceeding without verification (the prior
            # behaviour) allowed unauthenticated webhook processing.
            logger.error(
                "Webhook config id=%s for repo=%s has no encrypted secret; "
                "HMAC cannot be verified — rejecting request to prevent "
                "unauthenticated webhook processing.",
                config.id,
                repo_url,
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    "Webhook signature cannot be verified: this webhook "
                    "configuration has no secret.  Update the webhook "
                    "configuration to include a secret."
                ),
            )

        # ---------------------------------------------------------------
        # M-6: Replay prevention — reject events with stale timestamps.
        # After HMAC verification succeeds we know the payload is
        # authentic.  Reject deliveries whose head commit timestamp is
        # older than 5 minutes to prevent captured requests from being
        # replayed later.
        # ---------------------------------------------------------------
        _REPLAY_WINDOW_SECONDS = 300  # 5 minutes
        head_commit = event_data.get("head_commit")
        if isinstance(head_commit, dict):
            commit_ts_str = head_commit.get("timestamp", "")
            if commit_ts_str:
                try:
                    commit_dt = datetime.fromisoformat(
                        commit_ts_str.replace("Z", "+00:00"),
                    )
                    age_seconds = (
                        datetime.now(timezone.utc) - commit_dt
                    ).total_seconds()
                    if age_seconds > _REPLAY_WINDOW_SECONDS:
                        logger.warning(
                            "Rejecting stale webhook: commit timestamp %s is %.0fs old "
                            "(max %ds) for repo=%s branch=%s",
                            commit_ts_str,
                            age_seconds,
                            _REPLAY_WINDOW_SECONDS,
                            repo_url,
                            branch,
                        )
                        raise HTTPException(
                            status_code=400,
                            detail="Webhook delivery too old — possible replay attack",
                        )
                except HTTPException:
                    raise
                except (ValueError, TypeError, OverflowError):
                    # Unparseable timestamp — proceed rather than blocking
                    # on unexpected format changes from GitHub.
                    pass

        # ---------------------------------------------------------------
        # Event processing (only reached after HMAC validation passes).
        # Activate RLS for the identified tenant before any DB mutations
        # so that all downstream queries are scoped correctly.
        # ---------------------------------------------------------------
        from core_engine.state.database import set_tenant_context

        await set_tenant_context(session, config.tenant_id)

        service = GitHubWebhookService(session, tenant_id=config.tenant_id)
        result_data = await service.handle_push_event(event_data)
        await session.commit()

    return result_data


# ---------------------------------------------------------------------------
# Webhook configuration management (ADMIN only)
# ---------------------------------------------------------------------------


@router.post("/config")
async def create_webhook_config(
    body: WebhookConfigCreate,
    session: SessionDep,
    tenant_id: TenantDep,
    settings: SettingsDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> dict[str, Any]:
    """Create a new webhook configuration for the authenticated tenant."""
    from api.security import CredentialVault
    vault = CredentialVault(settings.credential_encryption_key.get_secret_value())

    service = GitHubWebhookService(
        session,
        tenant_id=tenant_id,
        credential_vault=vault,
    )
    return await service.create_config(
        repo_url=body.repo_url,
        branch=body.branch,
        secret=body.secret,
        auto_plan=body.auto_plan,
        auto_apply=body.auto_apply,
    )


@router.get("/config")
async def list_webhook_configs(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> list[dict[str, Any]]:
    """List all webhook configurations for the authenticated tenant."""
    service = GitHubWebhookService(session, tenant_id=tenant_id)
    return await service.list_configs()


@router.delete("/config/{config_id}")
async def delete_webhook_config(
    config_id: int,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> dict[str, Any]:
    """Delete a webhook configuration by ID."""
    service = GitHubWebhookService(session, tenant_id=tenant_id)
    deleted = await service.delete_config(config_id)
    if not deleted:
        raise not_found_404("Webhook config", config_id)
    return {"deleted": True, "config_id": config_id}
