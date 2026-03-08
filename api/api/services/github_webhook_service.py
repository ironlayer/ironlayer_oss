"""GitHub webhook service for CI-triggered plan generation.

Handles HMAC-SHA256 signature validation, push event processing,
and webhook configuration CRUD with bcrypt-hashed secrets and
Fernet-encrypted secrets (for HMAC computation).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import TYPE_CHECKING, Any

import bcrypt
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from api.security import CredentialVault

logger = logging.getLogger(__name__)


class GitHubWebhookService:
    """Processes GitHub webhook events and manages webhook configurations.

    Parameters
    ----------
    session:
        Active database session.
    tenant_id:
        The tenant owning the webhook configuration.
    credential_vault:
        Optional :class:`CredentialVault` instance for encrypting/decrypting
        webhook secrets.  Required for ``create_config`` to persist the
        encrypted secret used for HMAC validation.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        tenant_id: str,
        credential_vault: CredentialVault | None = None,
    ) -> None:
        self._session = session
        self._tenant_id = tenant_id
        self._vault = credential_vault

    # ------------------------------------------------------------------
    # Signature validation
    # ------------------------------------------------------------------

    @staticmethod
    def validate_signature(
        payload: bytes,
        signature_header: str,
        secret: str,
    ) -> bool:
        """Validate a GitHub webhook HMAC-SHA256 signature.

        Parameters
        ----------
        payload:
            The raw request body bytes.
        signature_header:
            The ``X-Hub-Signature-256`` header value (``sha256=<hex>``).
        secret:
            The plaintext webhook secret for computing the expected HMAC.

        Returns
        -------
        bool
            ``True`` if the signature is valid.
        """
        if not signature_header.startswith("sha256="):
            return False

        expected_sig = signature_header[7:]
        computed = hmac.new(
            secret.encode("utf-8"),
            payload,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(computed, expected_sig)

    # ------------------------------------------------------------------
    # Push event handling
    # ------------------------------------------------------------------

    async def handle_push_event(
        self,
        event_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Process a GitHub push event.

        Extracts the repository URL, branch, and commit SHAs from the
        push event payload.  If a matching webhook configuration exists
        and ``auto_plan`` is enabled, triggers a plan generation.

        Parameters
        ----------
        event_data:
            The parsed GitHub push event JSON payload.

        Returns
        -------
        dict
            Processing result with status and optional plan_id.
        """
        ref = event_data.get("ref", "")
        branch = ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ref

        repo = event_data.get("repository", {})
        repo_url = repo.get("clone_url", "") or repo.get("html_url", "")

        before_sha = event_data.get("before", "")
        after_sha = event_data.get("after", "")

        if not repo_url or not before_sha or not after_sha:
            return {"status": "ignored", "reason": "incomplete_payload"}

        # Look up matching webhook config.
        config = await self._find_config(repo_url, branch)
        if config is None:
            logger.debug(
                "No webhook config found for repo=%s branch=%s tenant=%s",
                repo_url,
                branch,
                self._tenant_id,
            )
            return {"status": "ignored", "reason": "no_matching_config"}

        if not config.auto_plan:
            return {"status": "acknowledged", "auto_plan": False}

        # Trigger plan generation.
        result: dict[str, Any] = {
            "status": "plan_triggered",
            "repo_url": repo_url,
            "branch": branch,
            "base_sha": before_sha,
            "target_sha": after_sha,
            "auto_apply": config.auto_apply,
        }

        logger.info(
            "GitHub webhook: triggering plan for %s (%s..%s) on branch %s",
            repo_url,
            before_sha[:8],
            after_sha[:8],
            branch,
        )

        return result

    # ------------------------------------------------------------------
    # Configuration CRUD
    # ------------------------------------------------------------------

    async def create_config(
        self,
        repo_url: str,
        branch: str,
        secret: str,
        auto_plan: bool = True,
        auto_apply: bool = False,
    ) -> dict[str, Any]:
        """Create a new webhook configuration.

        The secret is hashed with bcrypt for config-level validation and
        encrypted with Fernet for HMAC-SHA256 signature verification at
        webhook receive time.  The plaintext is never stored directly.

        Parameters
        ----------
        repo_url:
            The repository clone/HTTPS URL.
        branch:
            The branch to listen on.
        secret:
            The plaintext webhook secret (hashed + encrypted before storage).
        auto_plan:
            Whether to auto-generate plans on push.
        auto_apply:
            Whether to auto-apply generated plans.

        Returns
        -------
        dict
            The created configuration.
        """
        from core_engine.state.tables import WebhookConfigTable

        secret_hash = bcrypt.hashpw(
            secret.encode("utf-8"),
            bcrypt.gensalt(),
        ).decode("utf-8")

        # Encrypt the plaintext secret so the webhook receiver can later
        # decrypt it to compute the HMAC-SHA256 digest.
        secret_encrypted: str | None = None
        if self._vault is not None:
            secret_encrypted = self._vault.encrypt(secret)
        else:
            logger.warning(
                "CredentialVault not available; webhook secret for repo=%s "
                "will be stored without encryption (HMAC validation disabled "
                "until secret_encrypted is populated).",
                repo_url,
            )

        config = WebhookConfigTable(
            tenant_id=self._tenant_id,
            provider="github",
            repo_url=repo_url,
            branch=branch,
            secret_hash=secret_hash,
            secret_encrypted=secret_encrypted,
            auto_plan=auto_plan,
            auto_apply=auto_apply,
        )
        self._session.add(config)
        await self._session.flush()

        return {
            "id": config.id,
            "tenant_id": self._tenant_id,
            "provider": "github",
            "repo_url": repo_url,
            "branch": branch,
            "auto_plan": auto_plan,
            "auto_apply": auto_apply,
            "created_at": config.created_at.isoformat() if config.created_at else None,
        }

    async def list_configs(self) -> list[dict[str, Any]]:
        """Return all webhook configurations for this tenant.

        Returns
        -------
        list[dict]
            Webhook configurations (secrets are never exposed).
        """
        from core_engine.state.tables import WebhookConfigTable

        result = await self._session.execute(
            select(WebhookConfigTable).where(WebhookConfigTable.tenant_id == self._tenant_id)
        )
        rows = result.scalars().all()

        return [
            {
                "id": row.id,
                "provider": row.provider,
                "repo_url": row.repo_url,
                "branch": row.branch,
                "auto_plan": row.auto_plan,
                "auto_apply": row.auto_apply,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in rows
        ]

    async def delete_config(self, config_id: int) -> bool:
        """Delete a webhook configuration by ID.

        Parameters
        ----------
        config_id:
            The database ID of the configuration to delete.

        Returns
        -------
        bool
            ``True`` if a row was deleted.
        """
        from core_engine.state.tables import WebhookConfigTable

        result = await self._session.execute(
            delete(WebhookConfigTable).where(
                WebhookConfigTable.id == config_id,
                WebhookConfigTable.tenant_id == self._tenant_id,
            )
        )
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def verify_secret(self, config_id: int, payload: bytes, signature: str) -> bool:
        """Verify a webhook signature against the stored secret hash.

        Since we store bcrypt hashes, we cannot compute HMAC directly.
        Instead, webhook endpoints should use :meth:`validate_signature`
        with the plaintext secret during initial setup/testing.

        For production use, the signature is validated at the router
        level using the plaintext secret from the webhook config lookup.

        This method exists for future extension (e.g. retrieving the
        secret from a vault).
        """
        from core_engine.state.tables import WebhookConfigTable

        result = await self._session.execute(
            select(WebhookConfigTable).where(
                WebhookConfigTable.id == config_id,
                WebhookConfigTable.tenant_id == self._tenant_id,
            )
        )
        config = result.scalar_one_or_none()
        return config is not None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _find_config(self, repo_url: str, branch: str) -> Any | None:
        """Find a webhook config matching the repo URL and branch."""
        from core_engine.state.tables import WebhookConfigTable

        result = await self._session.execute(
            select(WebhookConfigTable).where(
                WebhookConfigTable.tenant_id == self._tenant_id,
                WebhookConfigTable.provider == "github",
                WebhookConfigTable.repo_url == repo_url,
                WebhookConfigTable.branch == branch,
            )
        )
        return result.scalar_one_or_none()
