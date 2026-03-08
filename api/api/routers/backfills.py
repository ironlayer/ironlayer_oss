"""Backfill endpoints: trigger, monitor, resume, and inspect backfills."""

from __future__ import annotations

import json
import logging
from typing import Any

from core_engine.state.repository import PlanRepository, RunRepository
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import MeteringDep, SessionDep, SettingsDep, TenantDep, UserDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.audit_service import AuditAction, AuditService
from api.services.execution_service import ExecutionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/backfills", tags=["backfills"])


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class BackfillRequest(BaseModel):
    """Request body for ``POST /backfills``."""

    model_name: str = Field(..., description="Canonical model name to backfill.")
    start_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date (inclusive) in YYYY-MM-DD format.",
    )
    end_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date (inclusive) in YYYY-MM-DD format.",
    )
    cluster_size: str | None = Field(
        default=None,
        description="Optional cluster size override (small / medium / large).",
    )


class ChunkedBackfillRequest(BaseModel):
    """Request body for ``POST /backfills/chunked``."""

    model_name: str = Field(..., description="Canonical model name to backfill.")
    start_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date (inclusive) in YYYY-MM-DD format.",
    )
    end_date: str = Field(
        ...,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date (inclusive) in YYYY-MM-DD format.",
    )
    cluster_size: str | None = Field(
        default=None,
        description="Optional cluster size override (small / medium / large).",
    )
    chunk_size_days: int = Field(
        default=7,
        ge=1,
        le=365,
        description="Number of days per chunk (default 7).",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("")
async def create_backfill(
    body: BackfillRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    metering: MeteringDep,
    _role: Role = Depends(require_permission(Permission.CREATE_BACKFILLS)),
) -> dict[str, Any]:
    """Trigger a single-model backfill over the specified date range.

    Generates a synthetic plan, checks locks, executes, and returns the
    plan together with its run records.
    """
    from core_engine.metering.events import UsageEventType

    service = ExecutionService(session, settings, tenant_id=tenant_id)
    try:
        result = await service.backfill(
            model_name=body.model_name,
            start_date=body.start_date,
            end_date=body.end_date,
            cluster_size=body.cluster_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # Meter the backfill run.
    metering.record_event(
        tenant_id=tenant_id,
        event_type=UsageEventType.BACKFILL_RUN,
        metadata={
            "model_name": body.model_name,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "cluster_size": body.cluster_size,
        },
    )

    # Audit trail.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.BACKFILL_REQUESTED,
        entity_type="model",
        entity_id=body.model_name,
        start_date=body.start_date,
        end_date=body.end_date,
        cluster_size=body.cluster_size,
    )

    return result


@router.post("/chunked")
async def create_chunked_backfill(
    body: ChunkedBackfillRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    metering: MeteringDep,
    _role: Role = Depends(require_permission(Permission.CREATE_BACKFILLS)),
) -> dict[str, Any]:
    """Trigger a chunked backfill with checkpoint-based resume.

    Splits the date range into chunks and executes them sequentially.
    If a chunk fails, the backfill can be resumed from the last
    successfully completed chunk via ``POST /backfills/{backfill_id}/resume``.
    """
    from core_engine.metering.events import UsageEventType

    service = ExecutionService(session, settings, tenant_id=tenant_id)
    try:
        result = await service.chunked_backfill(
            model_name=body.model_name,
            start_date=body.start_date,
            end_date=body.end_date,
            cluster_size=body.cluster_size,
            chunk_size_days=body.chunk_size_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # Meter the backfill run.
    metering.record_event(
        tenant_id=tenant_id,
        event_type=UsageEventType.BACKFILL_RUN,
        metadata={
            "model_name": body.model_name,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "chunk_size_days": body.chunk_size_days,
            "cluster_size": body.cluster_size,
            "backfill_type": "chunked",
        },
    )

    # Audit trail.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.BACKFILL_REQUESTED,
        entity_type="model",
        entity_id=body.model_name,
        start_date=body.start_date,
        end_date=body.end_date,
        cluster_size=body.cluster_size,
        chunk_size_days=body.chunk_size_days,
        backfill_type="chunked",
    )

    return result


@router.post("/{backfill_id}/resume")
async def resume_backfill(
    backfill_id: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    metering: MeteringDep,
    _role: Role = Depends(require_permission(Permission.CREATE_BACKFILLS)),
) -> dict[str, Any]:
    """Resume a failed or interrupted chunked backfill.

    Picks up execution from the last successfully completed chunk.
    """
    from core_engine.metering.events import UsageEventType

    service = ExecutionService(session, settings, tenant_id=tenant_id)
    try:
        result = await service.resume_backfill(backfill_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # Meter the resumed backfill.
    metering.record_event(
        tenant_id=tenant_id,
        event_type=UsageEventType.BACKFILL_RUN,
        metadata={
            "backfill_id": backfill_id,
            "backfill_type": "resume",
        },
    )

    # Audit trail.
    audit = AuditService(session, tenant_id=tenant_id, actor=user_identity)
    await audit.log(
        AuditAction.BACKFILL_REQUESTED,
        entity_type="backfill",
        entity_id=backfill_id,
        backfill_type="resume",
    )

    return result


@router.get("/status/{backfill_id}")
async def get_backfill_status(
    backfill_id: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
) -> dict[str, Any]:
    """Get the detailed status of a chunked backfill including per-chunk audit."""
    service = ExecutionService(session, settings, tenant_id=tenant_id)
    try:
        return await service.get_backfill_status(backfill_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/history/{model_name}")
async def get_backfill_history(
    model_name: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    limit: int = Query(default=20, ge=1, le=100),
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
) -> list[dict[str, Any]]:
    """Get the backfill history for a model, newest first."""
    service = ExecutionService(session, settings, tenant_id=tenant_id)
    return await service.get_backfill_history(model_name, limit=limit)


@router.get("/{plan_id}")
async def get_backfill(
    plan_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
) -> dict[str, Any]:
    """Retrieve a backfill plan with its current run status."""
    plan_repo = PlanRepository(session, tenant_id=tenant_id)
    plan_row = await plan_repo.get_plan(plan_id)
    if plan_row is None:
        raise HTTPException(status_code=404, detail=f"Backfill plan {plan_id} not found")

    plan_data = json.loads(plan_row.plan_json)  # type: ignore[arg-type]

    # Attach run records.
    run_repo = RunRepository(session, tenant_id=tenant_id)
    run_rows = await run_repo.get_by_plan(plan_id)
    runs = [
        {
            "run_id": r.run_id,
            "step_id": r.step_id,
            "model_name": r.model_name,
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "error_message": r.error_message,
        }
        for r in run_rows
    ]

    return {
        "plan": plan_data,
        "runs": runs,
        "created_at": plan_row.created_at.isoformat() if plan_row.created_at else None,
    }
