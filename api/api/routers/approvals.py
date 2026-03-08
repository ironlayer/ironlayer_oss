"""Approval and rejection endpoints for execution plans."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from core_engine.state.repository import PlanRepository
from core_engine.state.tables import PlanTable
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import update

from api.dependencies import SessionDep, SettingsDep, TenantDep, UserDep
from api.middleware.rbac import Permission, Role, require_permission
from api.schemas import PlanApprovalResponse
from api.services.audit_service import AuditAction, AuditService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plans", tags=["approvals"])


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class ApproveRequest(BaseModel):
    """Request body for ``POST /plans/{plan_id}/approve``."""

    # NOTE: ``display_name`` is an optional informational field only.  The
    # authoritative approver identity always comes from the JWT token
    # (``request.state.sub``).  This field is NOT used for audit or identity.
    display_name: str | None = Field(
        default=None, description="Optional display name (informational only, NOT used for audit)."
    )
    comment: str | None = Field(default=None, description="Optional comment for the approval.")


class RejectRequest(BaseModel):
    """Request body for ``POST /plans/{plan_id}/reject``."""

    # NOTE: ``display_name`` is an optional informational field only.  The
    # authoritative rejector identity always comes from the JWT token
    # (``request.state.sub``).  This field is NOT used for audit or identity.
    display_name: str | None = Field(
        default=None, description="Optional display name (informational only, NOT used for audit)."
    )
    reason: str = Field(..., min_length=1, description="Reason for rejection.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/{plan_id}/approve", response_model=PlanApprovalResponse)
async def approve_plan(
    plan_id: str,
    body: ApproveRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep = "",
    _role: Role = Depends(require_permission(Permission.APPROVE_PLANS)),
) -> dict[str, Any]:
    """Record an approval on a plan.

    The approver identity is always taken from the authenticated JWT token
    (``request.state.sub``), never from the request body.  The optional
    ``display_name`` in the body is informational only.

    In dev environments, adding an approval may auto-trigger execution
    if the plan was generated with ``auto_approve`` semantics.
    """
    if not user_identity:
        raise HTTPException(
            status_code=401,
            detail="Authenticated identity required for approval",
        )

    repo = PlanRepository(session, tenant_id=tenant_id)
    plan_row = await repo.get_plan(plan_id)
    if plan_row is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")

    # Check for duplicate approval by the same authenticated user.
    existing: list[dict[str, str]] = json.loads(plan_row.approvals_json) if plan_row.approvals_json else []  # type: ignore[arg-type]
    already_approved = any(a["user"] == user_identity for a in existing)
    if already_approved:
        raise HTTPException(
            status_code=409,
            detail=f"User {user_identity} has already approved plan {plan_id}",
        )

    # Add the approval using the authenticated identity.
    await repo.add_approval(
        plan_id=plan_id,
        user=user_identity,
        comment=body.comment or "",
    )

    # Audit trail — always uses the authenticated identity.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.PLAN_APPROVED,
        entity_type="plan",
        entity_id=plan_id,
        comment=body.comment or "",
    )

    logger.info("Plan %s approved by authenticated user %s", plan_id[:12], user_identity)

    # Reload the plan to return updated state.
    plan_row = await repo.get_plan(plan_id)
    assert plan_row is not None, f"Plan {plan_id} disappeared after approval"
    plan_data = json.loads(plan_row.plan_json)  # type: ignore[arg-type]
    plan_data["approvals"] = json.loads(plan_row.approvals_json) if plan_row.approvals_json else []  # type: ignore[arg-type]
    plan_data["auto_approved"] = plan_row.auto_approved
    plan_data["created_at"] = plan_row.created_at.isoformat() if plan_row.created_at else None

    return plan_data


@router.post("/{plan_id}/reject", response_model=PlanApprovalResponse)
async def reject_plan(
    plan_id: str,
    body: RejectRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    user_identity: UserDep = "",
    _role: Role = Depends(require_permission(Permission.APPROVE_PLANS)),
) -> dict[str, Any]:
    """Mark a plan as rejected.

    The rejector identity is always taken from the authenticated JWT token
    (``request.state.sub``), never from the request body.  The optional
    ``display_name`` in the body is informational only.

    Rejection is recorded as a special approval entry with a
    ``rejected`` flag so that the audit trail is preserved.
    """
    if not user_identity:
        raise HTTPException(
            status_code=401,
            detail="Authenticated identity required for rejection",
        )

    repo = PlanRepository(session, tenant_id=tenant_id)
    plan_row = await repo.get_plan(plan_id)
    if plan_row is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")

    # Build the rejection record using the authenticated identity.
    approvals: list[dict[str, Any]] = json.loads(plan_row.approvals_json) if plan_row.approvals_json else []  # type: ignore[arg-type]
    approvals.append(
        {
            "user": user_identity,
            "comment": body.reason,
            "at": datetime.now(UTC).isoformat(),
            "rejected": True,
        }
    )

    # Persist the updated approvals and mark auto_approved = False.
    stmt = (
        update(PlanTable)
        .where(PlanTable.tenant_id == tenant_id, PlanTable.plan_id == plan_id)
        .values(
            approvals_json=json.dumps(approvals),
            auto_approved=False,
        )
    )
    await session.execute(stmt)
    await session.flush()

    # Audit trail — always uses the authenticated identity.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.PLAN_REJECTED,
        entity_type="plan",
        entity_id=plan_id,
        reason=body.reason,
    )

    logger.info("Plan %s rejected by authenticated user %s: %s", plan_id[:12], user_identity, body.reason)

    # Return updated plan.
    plan_row = await repo.get_plan(plan_id)
    assert plan_row is not None, f"Plan {plan_id} disappeared after rejection"
    plan_data = json.loads(plan_row.plan_json)  # type: ignore[arg-type]
    plan_data["approvals"] = approvals
    plan_data["auto_approved"] = False
    plan_data["rejected"] = True
    plan_data["rejected_by"] = user_identity
    plan_data["rejection_reason"] = body.reason
    plan_data["created_at"] = plan_row.created_at.isoformat() if plan_row.created_at else None

    return plan_data
