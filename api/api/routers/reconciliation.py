"""API router for execution reconciliation operations.

Provides endpoints to trigger reconciliation checks, view discrepancies,
resolve them, retrieve statistics, manage schema drift detection, and
configure background reconciliation schedules.
"""

from __future__ import annotations

import logging
from datetime import UTC
from typing import Any

from core_engine.license.feature_flags import Feature
from core_engine.state.repository import ReconciliationScheduleRepository
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, TenantDep, require_feature
from api.middleware.rbac import Permission, Role, require_permission
from api.services.reconciliation_service import ReconciliationService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reconciliation", tags=["reconciliation"])


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class TriggerRequest(BaseModel):
    """Request body for triggering a reconciliation check."""

    plan_id: str | None = Field(None, description="Specific plan ID to reconcile, or None for recent runs.")
    hours_back: int = Field(24, ge=1, le=168, description="Hours of history to check (max 7 days).")


class ResolveRequest(BaseModel):
    """Request body for resolving a discrepancy."""

    resolved_by: str = Field(..., min_length=1, description="Identity of the person resolving.")
    resolution_note: str = Field(..., min_length=1, description="Explanation of the resolution.")


class SchemaDriftRequest(BaseModel):
    """Request body for triggering a schema drift check."""

    model_name: str | None = Field(None, description="Specific model to check, or None for all models.")


class ScheduleRequest(BaseModel):
    """Request body for creating/updating a reconciliation schedule."""

    schedule_type: str = Field(
        ...,
        min_length=1,
        description="Schedule type: 'run_reconciliation' or 'schema_drift'.",
    )
    cron_expression: str = Field(
        ...,
        min_length=1,
        description="Cron expression (e.g. '0 * * * *' for hourly).",
    )
    enabled: bool = Field(
        default=True,
        description="Whether the schedule is active.",
    )


# ---------------------------------------------------------------------------
# Endpoints -- Run reconciliation
# ---------------------------------------------------------------------------


@router.post("/trigger")
async def trigger_reconciliation(
    body: TriggerRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Trigger reconciliation checks against recent runs.

    Compares the control-plane run status against the execution backend
    (Databricks) for all runs with an external_run_id.
    """
    service = ReconciliationService(session, tenant_id=tenant_id)
    return await service.trigger_reconciliation(
        plan_id=body.plan_id,
        hours_back=body.hours_back,
    )


@router.get("/discrepancies")
async def get_discrepancies(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """List unresolved reconciliation discrepancies."""
    service = ReconciliationService(session, tenant_id=tenant_id)
    return await service.get_discrepancies(limit=limit)


@router.post("/resolve/{check_id}")
async def resolve_discrepancy(
    check_id: int,
    body: ResolveRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Mark a reconciliation discrepancy as resolved."""
    service = ReconciliationService(session, tenant_id=tenant_id)
    result = await service.resolve_discrepancy(
        check_id=check_id,
        resolved_by=body.resolved_by,
        resolution_note=body.resolution_note,
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"Reconciliation check {check_id} not found")
    return result


@router.get("/stats")
async def get_reconciliation_stats(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Return summary statistics for reconciliation checks."""
    service = ReconciliationService(session, tenant_id=tenant_id)
    return await service.get_stats()


# ---------------------------------------------------------------------------
# Endpoints -- Schema drift
# ---------------------------------------------------------------------------


@router.post("/schema-drift")
async def check_schema_drift(
    body: SchemaDriftRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Trigger a schema drift check for one or all models.

    When ``model_name`` is provided, checks that single model.  Otherwise,
    checks all registered models.  Returns a drift summary.
    """
    service = ReconciliationService(session, tenant_id=tenant_id)
    if body.model_name:
        return await service.check_schema_drift(body.model_name)
    return await service.check_all_schemas()


@router.get("/schema-drifts")
async def get_schema_drifts(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """List unresolved schema drift checks."""
    service = ReconciliationService(session, tenant_id=tenant_id)
    return await service.get_schema_drifts(limit=limit)


@router.put("/schema-drifts/{check_id}/resolve")
async def resolve_schema_drift(
    check_id: int,
    body: ResolveRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Resolve a schema drift check."""
    service = ReconciliationService(session, tenant_id=tenant_id)
    result = await service.resolve_schema_drift(
        check_id=check_id,
        resolved_by=body.resolved_by,
        resolution_note=body.resolution_note,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Schema drift check {check_id} not found",
        )
    return result


# ---------------------------------------------------------------------------
# Endpoints -- Reconciliation schedules
# ---------------------------------------------------------------------------


@router.get("/schedule")
async def get_reconciliation_schedules(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> list[dict[str, Any]]:
    """Return all reconciliation schedules for this tenant."""
    schedule_repo = ReconciliationScheduleRepository(session, tenant_id=tenant_id)
    schedules = await schedule_repo.get_all_enabled()
    return [
        {
            "id": s.id,
            "schedule_type": s.schedule_type,
            "cron_expression": s.cron_expression,
            "enabled": s.enabled,
            "last_run_at": s.last_run_at.isoformat() if s.last_run_at else None,
            "next_run_at": s.next_run_at.isoformat() if s.next_run_at else None,
        }
        for s in schedules
    ]


@router.put("/schedule")
async def upsert_reconciliation_schedule(
    body: ScheduleRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.RECONCILIATION)),
) -> dict[str, Any]:
    """Create or update a reconciliation schedule.

    The schedule is identified by ``(tenant_id, schedule_type)`` and is
    upserted: if a schedule already exists for that combination, it is
    updated; otherwise a new one is created.
    """
    from datetime import datetime

    from api.services.reconciliation_scheduler import compute_next_run

    # Validate the cron expression by computing a next-run time.
    now = datetime.now(UTC)
    try:
        next_run = compute_next_run(body.cron_expression, now)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cron expression: {exc}",
        )

    schedule_repo = ReconciliationScheduleRepository(session, tenant_id=tenant_id)
    schedule = await schedule_repo.upsert_schedule(
        schedule_type=body.schedule_type,
        cron_expression=body.cron_expression,
        enabled=body.enabled,
    )

    # Set the initial next_run_at if not already set.
    if schedule.next_run_at is None:
        await schedule_repo.update_last_run(
            schedule_type=body.schedule_type,
            last_run_at=schedule.last_run_at or now,
            next_run_at=next_run,
        )

    return {
        "id": schedule.id,
        "schedule_type": schedule.schedule_type,
        "cron_expression": schedule.cron_expression,
        "enabled": schedule.enabled,
        "last_run_at": schedule.last_run_at.isoformat() if schedule.last_run_at else None,
        "next_run_at": next_run.isoformat(),
    }
