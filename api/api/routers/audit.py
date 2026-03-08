"""Audit log query and chain-verification endpoints.

All endpoints require the ``READ_AUDIT`` permission, which is granted to
the OPERATOR role and above.  VIEWER-role users cannot access the audit log.
The GDPR right-to-erasure endpoint requires the ``ADMIN`` role.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from core_engine.license.feature_flags import Feature
from core_engine.state.repository import AuditRepository
from fastapi import APIRouter, Depends, Query

from api.dependencies import SessionDep, TenantDep, require_feature
from api.middleware.rbac import Permission, Role, require_permission, require_role
from api.services.audit_service import AuditService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/audit", tags=["audit"])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def query_audit_log(
    session: SessionDep,
    tenant_id: TenantDep,
    action: str | None = Query(default=None, description="Filter by action type."),
    entity_type: str | None = Query(default=None, description="Filter by entity type."),
    entity_id: str | None = Query(default=None, description="Filter by entity ID."),
    since: datetime | None = Query(default=None, description="Only entries after this timestamp."),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _role: Role = Depends(require_permission(Permission.READ_AUDIT)),
    _gate: None = Depends(require_feature(Feature.AUDIT_LOG)),
) -> list[dict[str, Any]]:
    """Query the append-only audit log with optional filters.

    Returns entries ordered by ``created_at`` descending (most recent first).
    Requires the ``READ_AUDIT`` permission (OPERATOR role or above).
    """
    repo = AuditRepository(session, tenant_id=tenant_id)
    entries = await repo.query(
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        since=since,
        limit=limit,
        offset=offset,
    )
    return [
        {
            "id": entry.id,
            "tenant_id": entry.tenant_id,
            "actor": entry.actor,
            "action": entry.action,
            "entity_type": entry.entity_type,
            "entity_id": entry.entity_id,
            "metadata": entry.metadata_json,
            "previous_hash": entry.previous_hash,
            "entry_hash": entry.entry_hash,
            "created_at": entry.created_at.isoformat() if entry.created_at else None,
        }
        for entry in entries
    ]


@router.get("/verify")
async def verify_audit_chain(
    session: SessionDep,
    tenant_id: TenantDep,
    limit: int = Query(default=1000, ge=1, le=10000),
    _role: Role = Depends(require_permission(Permission.READ_AUDIT)),
    _gate: None = Depends(require_feature(Feature.AUDIT_LOG)),
) -> dict[str, Any]:
    """Verify the integrity of the audit log hash chain.

    Returns the verification result and the number of entries checked.
    Requires the ``READ_AUDIT`` permission (OPERATOR role or above).
    """
    repo = AuditRepository(session, tenant_id=tenant_id)
    is_valid, entries_checked = await repo.verify_chain(limit=limit)
    return {
        "is_valid": is_valid,
        "entries_checked": entries_checked,
    }


@router.delete("/users/{user_id}")
async def anonymize_user_audit_data(
    user_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_role(Role.ADMIN)),
    _gate: None = Depends(require_feature(Feature.AUDIT_LOG)),
) -> dict[str, Any]:
    """GDPR right-to-erasure: anonymize all audit log entries for a user.

    Replaces the ``actor`` field with ``[REDACTED]`` and clears
    ``metadata_json`` for every entry belonging to ``user_id`` in this
    tenant's audit log.  The audit record structure and counts are
    preserved; only identifying information is removed.

    Requires the ``ADMIN`` role.
    """
    svc = AuditService(session, tenant_id=tenant_id)
    count = await svc.anonymize_user_data(user_id)
    logger.info(
        "GDPR anonymize: tenant=%s user=%s entries_anonymized=%d",
        tenant_id,
        user_id,
        count,
    )
    return {"user_id": user_id, "entries_anonymized": count}
