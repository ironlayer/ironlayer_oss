"""Reporting endpoints for per-tenant cost, usage, and LLM analytics.

All endpoints require ``VIEW_REPORTS`` permission (admin-only).
Supports date-range queries, multiple grouping modes, and CSV/JSON export.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from api.dependencies import SessionDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.reporting_service import ReportingService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/reports", tags=["reports"])


def _parse_date(value: str | None, default_days_ago: int = 30) -> datetime:
    """Parse an ISO date string or return a default."""
    if value:
        try:
            return datetime.fromisoformat(value).replace(tzinfo=UTC)
        except ValueError:
            logger.warning("Invalid date format in report query: %r", value)
            raise HTTPException(
                status_code=400,
                detail="Invalid request parameters. Expected ISO date format for since/until.",
            )
    return datetime.now(UTC) - timedelta(days=default_days_ago)


@router.get("/cost")
async def get_cost_report(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_REPORTS)),
    since: str | None = Query(None, description="ISO date start"),
    until: str | None = Query(None, description="ISO date end"),
    group_by: str = Query("model", description="Group by: model, day, week, month"),
) -> dict[str, Any]:
    """Return cost report for this tenant grouped by model or time bucket."""
    since_dt = _parse_date(since, 30)
    until_dt = _parse_date(until, 0) if until else datetime.now(UTC)
    service = ReportingService(session, tenant_id)
    return await service.cost_report(since_dt, until_dt, group_by)


@router.get("/usage")
async def get_usage_report(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_REPORTS)),
    since: str | None = Query(None),
    until: str | None = Query(None),
    group_by: str = Query("actor", description="Group by: actor, day, week, month"),
) -> dict[str, Any]:
    """Return usage report for this tenant grouped by actor or time bucket."""
    since_dt = _parse_date(since, 30)
    until_dt = _parse_date(until, 0) if until else datetime.now(UTC)
    service = ReportingService(session, tenant_id)
    return await service.usage_report(since_dt, until_dt, group_by)


@router.get("/llm")
async def get_llm_report(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_REPORTS)),
    since: str | None = Query(None),
    until: str | None = Query(None),
) -> dict[str, Any]:
    """Return LLM cost and token usage report."""
    since_dt = _parse_date(since, 30)
    until_dt = _parse_date(until, 0) if until else datetime.now(UTC)
    service = ReportingService(session, tenant_id)
    return await service.llm_report(since_dt, until_dt)


@router.get("/export")
async def export_report(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_REPORTS)),
    report_type: str = Query(..., description="Report type: cost, usage, llm"),
    since: str | None = Query(None),
    until: str | None = Query(None),
    fmt: str = Query("csv", alias="format", description="Export format: csv, json"),
) -> StreamingResponse:
    """Export report data as a downloadable CSV or JSON file."""
    since_dt = _parse_date(since, 30)
    until_dt = _parse_date(until, 0) if until else datetime.now(UTC)

    if report_type not in ("cost", "usage", "llm"):
        logger.warning("Invalid report_type in export: %r", report_type)
        raise HTTPException(
            status_code=400,
            detail="Invalid request parameters. report_type must be one of: cost, usage, llm.",
        )
    if fmt not in ("csv", "json"):
        logger.warning("Invalid format in export: %r", fmt)
        raise HTTPException(
            status_code=400,
            detail="Invalid request parameters. format must be csv or json.",
        )

    service = ReportingService(session, tenant_id)
    data_bytes, content_type, filename = await service.export_data(report_type, since_dt, until_dt, fmt)

    return StreamingResponse(
        iter([data_bytes]),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
