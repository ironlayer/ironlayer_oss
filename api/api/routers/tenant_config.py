"""Per-tenant configuration management, provisioning, and self-service settings.

Includes two routers:

- ``router`` (``/admin/tenants/...``) — platform admin endpoints for managing
  any tenant (requires MANAGE_SETTINGS).
- ``settings_router`` (``/settings/...``) — self-service endpoints for the
  caller's own tenant (LLM key management, config read).
"""

from __future__ import annotations

import logging
from typing import Any

from core_engine.state.repository import TenantConfigRepository
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, SecretStr

from api.dependencies import AdminSessionDep, SessionDep, SettingsDep, TenantDep, UserDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.audit_service import AuditService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/tenants", tags=["admin"])
settings_router = APIRouter(prefix="/settings", tags=["settings"])


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class TenantConfigResponse(BaseModel):
    """Tenant configuration state."""

    tenant_id: str
    llm_enabled: bool
    llm_monthly_budget_usd: float | None = None
    llm_daily_budget_usd: float | None = None
    created_at: str | None = None
    updated_at: str | None = None
    updated_by: str | None = None
    deactivated_at: str | None = None


class TenantConfigUpdateRequest(BaseModel):
    """Request body for updating tenant configuration."""

    llm_enabled: bool = Field(..., description="Whether LLM-powered advisory features are enabled for this tenant.")
    llm_monthly_budget_usd: float | None = Field(
        default=None,
        ge=0,
        description="Monthly LLM spending limit in USD. None = unlimited.",
    )
    llm_daily_budget_usd: float | None = Field(
        default=None,
        ge=0,
        description="Daily LLM spending limit in USD. None = unlimited.",
    )


class TenantCreateRequest(BaseModel):
    """Request body for provisioning a new tenant."""

    tenant_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Unique tenant identifier.",
    )
    llm_enabled: bool = Field(
        default=True,
        description="Whether LLM-powered advisory features are enabled.",
    )


class TenantListResponse(BaseModel):
    """Response body for listing tenants."""

    tenants: list[TenantConfigResponse]
    total: int


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _row_to_response(row: Any) -> TenantConfigResponse:
    """Map a :class:`TenantConfigTable` row to a response model."""
    return TenantConfigResponse(
        tenant_id=row.tenant_id,
        llm_enabled=row.llm_enabled,
        llm_monthly_budget_usd=getattr(row, "llm_monthly_budget_usd", None),
        llm_daily_budget_usd=getattr(row, "llm_daily_budget_usd", None),
        created_at=row.created_at.isoformat() if getattr(row, "created_at", None) else None,
        updated_at=row.updated_at.isoformat() if row.updated_at else None,
        updated_by=row.updated_by,
        deactivated_at=row.deactivated_at.isoformat() if getattr(row, "deactivated_at", None) else None,
    )


# ---------------------------------------------------------------------------
# Tenant provisioning endpoints
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=TenantConfigResponse,
    status_code=201,
    summary="Provision a new tenant",
)
async def create_tenant(
    body: TenantCreateRequest,
    session: AdminSessionDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> TenantConfigResponse:
    """Provision a new tenant with default configuration.

    Requires MANAGE_SETTINGS permission (ADMIN role).  Returns 409 if the
    tenant already exists.
    """
    repo = TenantConfigRepository(session, tenant_id=body.tenant_id)
    try:
        row = await repo.create(llm_enabled=body.llm_enabled, created_by=user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    # Audit log the provisioning event.
    audit = AuditService(session, tenant_id=body.tenant_id, actor=user)
    await audit.log(
        action="TENANT_PROVISIONED",
        entity_type="tenant",
        entity_id=body.tenant_id,
        llm_enabled=body.llm_enabled,
    )

    logger.info("Tenant provisioned: tenant=%s by=%s", body.tenant_id, user)
    return _row_to_response(row)


@router.get(
    "",
    response_model=TenantListResponse,
    summary="List all tenants",
)
async def list_tenants(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    include_deactivated: bool = False,
) -> TenantListResponse:
    """List all provisioned tenants.

    Requires MANAGE_SETTINGS permission (ADMIN role).  By default only
    active tenants are returned; pass ``include_deactivated=true`` to
    include soft-deleted tenants.
    """
    # AdminSessionDep provides a non-tenant-scoped session, matching the
    # cross-tenant nature of this query.
    repo = TenantConfigRepository(session, tenant_id="__admin__")
    tenants = await repo.list_all(include_deactivated=include_deactivated)
    return TenantListResponse(
        tenants=[_row_to_response(t) for t in tenants],
        total=len(tenants),
    )


@router.delete(
    "/{tenant_id}",
    response_model=TenantConfigResponse,
    summary="Deactivate a tenant",
)
async def deactivate_tenant(
    tenant_id: str,
    session: AdminSessionDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> TenantConfigResponse:
    """Soft-delete a tenant by setting ``deactivated_at``.

    Requires MANAGE_SETTINGS permission (ADMIN role).  The tenant's data
    is retained but the tenant is marked as inactive and will be excluded
    from default listing.  Returns 404 if the tenant does not exist.
    """
    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    row = await repo.deactivate(deactivated_by=user)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    # Audit log the deactivation event.
    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action="TENANT_DEACTIVATED",
        entity_type="tenant",
        entity_id=tenant_id,
    )

    logger.info("Tenant deactivated: tenant=%s by=%s", tenant_id, user)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# Tenant configuration endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/{tenant_id}/config",
    response_model=TenantConfigResponse,
    summary="Get tenant configuration",
)
async def get_tenant_config(
    tenant_id: str,
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> TenantConfigResponse:
    """Retrieve configuration for a tenant.

    Requires MANAGE_SETTINGS permission (ADMIN role).  Returns default
    values if no explicit configuration has been set.
    """
    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    row = await repo.get()
    if row is None:
        return TenantConfigResponse(
            tenant_id=tenant_id,
            llm_enabled=True,
        )
    return _row_to_response(row)


@router.put(
    "/{tenant_id}/config",
    response_model=TenantConfigResponse,
    summary="Update tenant configuration",
)
async def update_tenant_config(
    tenant_id: str,
    body: TenantConfigUpdateRequest,
    session: AdminSessionDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> TenantConfigResponse:
    """Create or update configuration for a tenant.

    Requires MANAGE_SETTINGS permission (ADMIN role).
    """
    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    row = await repo.upsert(
        llm_enabled=body.llm_enabled,
        updated_by=user,
        llm_monthly_budget_usd=body.llm_monthly_budget_usd,
        llm_daily_budget_usd=body.llm_daily_budget_usd,
    )

    # Audit log the configuration change.
    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action="TENANT_CONFIG_UPDATED",
        entity_type="tenant_config",
        entity_id=tenant_id,
        llm_enabled=body.llm_enabled,
        llm_monthly_budget_usd=body.llm_monthly_budget_usd,
        llm_daily_budget_usd=body.llm_daily_budget_usd,
    )

    logger.info(
        "Tenant config updated: tenant=%s llm_enabled=%s budget_monthly=%s budget_daily=%s by=%s",
        tenant_id,
        body.llm_enabled,
        body.llm_monthly_budget_usd,
        body.llm_daily_budget_usd,
        user,
    )

    return _row_to_response(row)


# ---------------------------------------------------------------------------
# Self-service settings endpoints (/settings/...)
# ---------------------------------------------------------------------------

LLM_CREDENTIAL_NAME = "llm_api_key"


def _redact_key(message: str, key: str) -> str:
    """Remove all occurrences of the API key from an error message.

    Prevents leaking secret key material in HTTP responses or log output.
    Redacts the full key and, for keys longer than 8 characters, also
    redacts the last-8-character suffix (some provider error messages
    include a truncated key).
    """
    if not key:
        return message
    # Redact the full key.
    redacted = message.replace(key, "[REDACTED]")
    # Also redact partial key (last 8 chars might appear in some error messages).
    if len(key) > 8:
        redacted = redacted.replace(key[-8:], "[REDACTED]")
    return redacted


class SetLLMKeyRequest(BaseModel):
    """Request body for storing a per-tenant LLM API key."""

    api_key: SecretStr = Field(
        ...,
        description="LLM provider API key (Anthropic, OpenAI, etc.).",
        min_length=10,
    )


class LLMKeyStatusResponse(BaseModel):
    """Response showing whether the tenant has an LLM key configured."""

    has_key: bool
    key_prefix: str | None = None
    llm_enabled: bool


class SettingsResponse(BaseModel):
    """Aggregated tenant settings response."""

    tenant_id: str
    llm_enabled: bool
    llm_has_key: bool
    llm_key_prefix: str | None = None
    llm_monthly_budget_usd: float | None = None
    llm_daily_budget_usd: float | None = None


@settings_router.get(
    "",
    response_model=SettingsResponse,
    summary="Get current tenant settings",
)
async def get_settings(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> SettingsResponse:
    """Return the caller's tenant settings including LLM key status."""
    from api.security import CredentialVault

    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    row = await repo.get()

    vault = CredentialVault(settings.credential_encryption_key.get_secret_value())
    plaintext = await vault.get_credential(session, tenant_id, LLM_CREDENTIAL_NAME)

    return SettingsResponse(
        tenant_id=tenant_id,
        llm_enabled=row.llm_enabled if row else True,
        llm_has_key=plaintext is not None,
        llm_key_prefix=plaintext[:8] + "..." if plaintext and len(plaintext) > 8 else None,
        llm_monthly_budget_usd=getattr(row, "llm_monthly_budget_usd", None) if row else None,
        llm_daily_budget_usd=getattr(row, "llm_daily_budget_usd", None) if row else None,
    )


@settings_router.put(
    "/llm-key",
    response_model=LLMKeyStatusResponse,
    summary="Store your LLM API key",
)
async def set_llm_key(
    body: SetLLMKeyRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> LLMKeyStatusResponse:
    """Encrypt and store an LLM API key for the caller's tenant.

    Overwrites any previously stored key.
    """
    from api.security import CredentialVault

    vault = CredentialVault(settings.credential_encryption_key.get_secret_value())
    plaintext = body.api_key.get_secret_value()

    await vault.store_credential(session, tenant_id, LLM_CREDENTIAL_NAME, plaintext)
    await session.commit()

    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action="LLM_KEY_STORED",
        entity_type="credential",
        entity_id=LLM_CREDENTIAL_NAME,
    )

    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    await repo.upsert(llm_enabled=True, updated_by=user)
    await session.commit()

    logger.info("LLM API key stored for tenant=%s by=%s", tenant_id, user)

    return LLMKeyStatusResponse(
        has_key=True,
        key_prefix=plaintext[:8] + "..." if len(plaintext) > 8 else None,
        llm_enabled=True,
    )


@settings_router.delete(
    "/llm-key",
    response_model=LLMKeyStatusResponse,
    summary="Remove your LLM API key",
)
async def delete_llm_key(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> LLMKeyStatusResponse:
    """Delete the stored LLM API key for the caller's tenant."""
    from api.security import CredentialVault

    vault = CredentialVault(settings.credential_encryption_key.get_secret_value())
    deleted = await vault.delete_credential(session, tenant_id, LLM_CREDENTIAL_NAME)
    await session.commit()

    if deleted:
        audit = AuditService(session, tenant_id=tenant_id, actor=user)
        await audit.log(
            action="LLM_KEY_DELETED",
            entity_type="credential",
            entity_id=LLM_CREDENTIAL_NAME,
        )
        logger.info("LLM API key deleted for tenant=%s by=%s", tenant_id, user)

    repo = TenantConfigRepository(session, tenant_id=tenant_id)
    row = await repo.get()

    return LLMKeyStatusResponse(
        has_key=False,
        key_prefix=None,
        llm_enabled=row.llm_enabled if row else True,
    )


@settings_router.post(
    "/llm-key/test",
    summary="Test your LLM API key",
)
async def test_llm_key(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, Any]:
    """Test the stored LLM API key by making a minimal API call."""
    from api.security import CredentialVault

    vault = CredentialVault(settings.credential_encryption_key.get_secret_value())
    plaintext = await vault.get_credential(session, tenant_id, LLM_CREDENTIAL_NAME)

    if not plaintext:
        raise HTTPException(
            status_code=404,
            detail="No LLM API key stored. Add one in Settings first.",
        )

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=plaintext, timeout=10.0)
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=10,
            messages=[{"role": "user", "content": "ping"}],
        )
        return {"status": "ok", "model": response.model}
    except Exception as exc:
        redacted_message = _redact_key(str(exc), plaintext)
        logger.warning("LLM key test failed for tenant=%s: %s", tenant_id, redacted_message)
        return {"status": "error", "detail": "LLM key validation failed", "error": redacted_message}
