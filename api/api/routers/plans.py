"""Plan management endpoints: generate, list, get, augment, and apply."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from core_engine.license.feature_flags import Feature
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from api.dependencies import AIClientDep, MeteringDep, SessionDep, SettingsDep, TenantDep, UserDep, require_feature
from api.middleware.rbac import Permission, Role, require_permission
from api.schemas import (
    PlanFeedbackResponse,
    PlanFeedbackStatsResponse,
    PlanListItemResponse,
    PlanResponse,
    RunRecordResponse,
)
from api.services.ai_feedback_service import AIFeedbackService
from api.services.audit_service import AuditAction, AuditService
from api.services.execution_service import ExecutionService
from api.services.plan_service import PlanService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plans", tags=["plans"])


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class GeneratePlanRequest(BaseModel):
    """Request body for ``POST /plans/generate``."""

    repo_path: str = Field(..., description="Absolute path to the git repository.")
    base_sha: str = Field(..., description="Git SHA of the base (current) commit.")
    target_sha: str = Field(..., description="Git SHA of the target (desired) commit.")

    @field_validator("repo_path")
    @classmethod
    def validate_repo_path(cls, v: str) -> str:
        """Reject path traversal attempts and ensure the path is absolute."""
        resolved = str(Path(v).resolve())
        if ".." in Path(v).parts:
            raise ValueError("repo_path must not contain '..' path segments")
        if not Path(resolved).is_absolute():
            raise ValueError("repo_path must be an absolute path")
        return resolved


class ApplyPlanRequest(BaseModel):
    """Request body for ``POST /plans/{plan_id}/apply``."""

    approved_by: str | None = Field(
        default=None,
        description="User who authorised execution.",
    )
    cluster_override: str | None = Field(
        default=None,
        description="Optional cluster size override (small / medium / large).",
    )
    auto_approve: bool = Field(
        default=False,
        description="When true, bypass the approval gate.",
    )


class SuggestionFeedbackItem(BaseModel):
    """Single feedback entry for an AI suggestion."""

    step_id: str
    model_name: str
    feedback_type: str = Field(..., pattern="^(cost|risk|classification)$")
    accepted: bool


class SuggestionFeedbackRequest(BaseModel):
    """Request body for ``POST /plans/{plan_id}/feedback``."""

    feedbacks: list[SuggestionFeedbackItem]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/generate", response_model=PlanResponse)
async def generate_plan(
    body: GeneratePlanRequest,
    session: SessionDep,
    ai_client: AIClientDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    metering: MeteringDep,
    _role: Role = Depends(require_permission(Permission.CREATE_PLANS)),
) -> dict[str, Any]:
    """Generate a deterministic execution plan from a git diff.

    The plan is persisted to the database and returned in full.
    """
    from core_engine.metering.events import UsageEventType

    # Enforce plan quota before generation.
    from api.services.quota_service import QuotaService

    quota = QuotaService(session, tenant_id)
    allowed, reason = await quota.check_plan_quota()
    if not allowed:
        raise HTTPException(status_code=429, detail=reason)

    service = PlanService(session, ai_client, settings, tenant_id=tenant_id, metering=metering)
    try:
        plan = await service.generate_plan(
            repo_path=body.repo_path,
            base_sha=body.base_sha,
            target_sha=body.target_sha,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Meter the plan run.
    metering.record_event(
        tenant_id=tenant_id,
        event_type=UsageEventType.PLAN_RUN,
        metadata={
            "plan_id": plan.get("plan_id"),
            "base_sha": body.base_sha,
            "target_sha": body.target_sha,
            "total_steps": plan.get("summary", {}).get("total_steps", 0),
        },
    )

    # Audit trail.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.PLAN_CREATED,
        entity_type="plan",
        entity_id=plan.get("plan_id"),
        base_sha=body.base_sha,
        target_sha=body.target_sha,
    )

    return plan


@router.get("", response_model=list[PlanListItemResponse])
async def list_plans(
    session: SessionDep,
    ai_client: AIClientDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    """Return a paginated list of plan summaries."""
    service = PlanService(session, ai_client, settings, tenant_id=tenant_id)
    return await service.list_plans(limit=limit, offset=offset)


@router.get("/{plan_id}", response_model=PlanResponse)
async def get_plan(
    plan_id: str,
    session: SessionDep,
    ai_client: AIClientDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, Any]:
    """Retrieve a single plan by its identifier."""
    service = PlanService(session, ai_client, settings, tenant_id=tenant_id)
    plan = await service.get_plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")
    return plan


@router.post("/{plan_id}/augment", response_model=PlanResponse)
async def augment_plan(
    plan_id: str,
    session: SessionDep,
    ai_client: AIClientDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    metering: MeteringDep,
    _role: Role = Depends(require_permission(Permission.CREATE_PLANS)),
    _gate: None = Depends(require_feature(Feature.AI_ADVISORY)),
) -> dict[str, Any]:
    """Attach AI advisory metadata (semantic classification, cost, risk) to a plan."""
    # Enforce AI quota and LLM budget before AI calls.
    from api.services.quota_service import QuotaService

    quota = QuotaService(session, tenant_id)
    ai_allowed, ai_reason = await quota.check_ai_quota()
    if not ai_allowed:
        raise HTTPException(status_code=429, detail=ai_reason)

    budget_allowed, budget_reason = await quota.check_llm_budget()
    if not budget_allowed:
        raise HTTPException(status_code=402, detail=budget_reason)

    service = PlanService(session, ai_client, settings, tenant_id=tenant_id, metering=metering)
    try:
        return await service.generate_augmented_plan(plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{plan_id}/apply", response_model=list[RunRecordResponse])
async def apply_plan(
    plan_id: str,
    body: ApplyPlanRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    metering: MeteringDep,
    caller_role: Role = Depends(require_permission(Permission.APPLY_PLANS)),
) -> list[dict[str, Any]]:
    """Execute every step in a plan in topological order.

    Requires prior approval in non-dev environments unless
    ``auto_approve`` is set.  Using ``auto_approve=True`` requires
    ADMIN role.
    """
    from core_engine.metering.events import UsageEventType

    exec_service = ExecutionService(session, settings, tenant_id=tenant_id)
    try:
        result = await exec_service.apply_plan(
            plan_id=plan_id,
            approved_by=body.approved_by,
            cluster_override=body.cluster_override,
            auto_approve=body.auto_approve,
            caller_role=caller_role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except RuntimeError as exc:
        logger.error("Plan apply failed for %s: %s", plan_id, exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred. Check server logs for details.",
        ) from exc

    # Meter the plan apply.
    metering.record_event(
        tenant_id=tenant_id,
        event_type=UsageEventType.PLAN_APPLY,
        metadata={
            "plan_id": plan_id,
            "steps_executed": len(result),
            "approved_by": body.approved_by,
        },
    )

    return result


# ---------------------------------------------------------------------------
# AI Feedback endpoints
# ---------------------------------------------------------------------------


@router.post("/{plan_id}/feedback", response_model=PlanFeedbackResponse)
async def submit_feedback(
    plan_id: str,
    body: SuggestionFeedbackRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    _role: Role = Depends(require_permission(Permission.CREATE_PLANS)),
) -> dict[str, Any]:
    """Record operator acceptance/rejection of AI suggestions for a plan."""
    service = AIFeedbackService(session, tenant_id=tenant_id)
    updated = await service.record_suggestion_feedback(
        plan_id=plan_id,
        feedbacks=[fb.model_dump() for fb in body.feedbacks],
    )

    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.AI_FEEDBACK_SUBMITTED,
        entity_type="plan",
        entity_id=plan_id,
        feedback_count=updated,
    )

    return {"updated": updated, "plan_id": plan_id}


@router.get("/{plan_id}/feedback/stats", response_model=PlanFeedbackStatsResponse)
async def get_feedback_stats(
    plan_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
    feedback_type: str | None = Query(default=None),
    model_name: str | None = Query(default=None),
) -> dict[str, Any]:
    """Return accuracy and acceptance statistics for AI predictions."""
    service = AIFeedbackService(session, tenant_id=tenant_id)
    stats = await service.get_accuracy_stats(
        feedback_type=feedback_type,
        model_name=model_name,
    )
    return {"plan_id": plan_id, **stats}
