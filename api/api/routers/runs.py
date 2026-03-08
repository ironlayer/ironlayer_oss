"""Run record endpoints: list and detail."""

from __future__ import annotations

import logging
from typing import Any

from core_engine.state.repository import RunRepository, TelemetryRepository
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import SessionDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/runs", tags=["runs"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a ``RunTable`` ORM row into a JSON-serialisable dictionary."""
    return {
        "run_id": row.run_id,
        "plan_id": row.plan_id,
        "step_id": row.step_id,
        "model_name": row.model_name,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "input_range_start": (row.input_range_start.isoformat() if row.input_range_start else None),
        "input_range_end": (row.input_range_end.isoformat() if row.input_range_end else None),
        "error_message": row.error_message,
        "logs_uri": row.logs_uri,
        "cluster_used": row.cluster_used,
        "executor_version": row.executor_version,
        "retry_count": row.retry_count,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def list_runs(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
    plan_id: str | None = Query(default=None, description="Filter by plan ID."),
    model_name: str | None = Query(default=None, description="Filter by model name."),
    status: str | None = Query(default=None, description="Filter by run status."),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    """Return a paginated, optionally filtered list of run records."""
    repo = RunRepository(session, tenant_id=tenant_id)

    # The repository does not expose a generic filtered query, so we
    # apply filters in two paths to minimise data transfer.
    if plan_id:
        rows = await repo.get_by_plan(plan_id)
    else:
        # Fetch recent runs via a broad query.  The RunRepository
        # supports get_by_plan; for a cross-plan listing we query all
        # plans and aggregate.  This works for moderate volumes; a
        # dedicated repository method should be added for scale.
        from core_engine.state.tables import RunTable
        from sqlalchemy import select

        stmt = select(RunTable).where(RunTable.tenant_id == tenant_id).order_by(RunTable.started_at.desc().nulls_last())

        if model_name:
            stmt = stmt.where(RunTable.model_name == model_name)
        if status:
            stmt = stmt.where(RunTable.status == status)

        stmt = stmt.offset(offset).limit(limit)
        result = await session.execute(stmt)
        rows = list(result.scalars().all())
        return [_row_to_dict(r) for r in rows]

    # Apply in-memory filters for the plan_id path.
    if model_name:
        rows = [r for r in rows if r.model_name == model_name]
    if status:
        rows = [r for r in rows if r.status == status]

    # Paginate.
    rows = rows[offset : offset + limit]

    return [_row_to_dict(r) for r in rows]


@router.get("/{run_id}")
async def get_run(
    run_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_RUNS)),
) -> dict[str, Any]:
    """Retrieve a single run record with associated telemetry data."""
    repo = RunRepository(session, tenant_id=tenant_id)
    row = await repo.get_by_id(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    run_dict = _row_to_dict(row)

    # Attach telemetry.
    telemetry_repo = TelemetryRepository(session, tenant_id=tenant_id)
    telemetry_rows = await telemetry_repo.get_for_run(run_id)
    run_dict["telemetry"] = [
        {
            "runtime_seconds": t.runtime_seconds,
            "shuffle_bytes": t.shuffle_bytes,
            "input_rows": t.input_rows,
            "output_rows": t.output_rows,
            "partition_count": t.partition_count,
            "cluster_id": t.cluster_id,
            "captured_at": t.captured_at.isoformat() if t.captured_at else None,
        }
        for t in telemetry_rows
    ]

    return run_dict
