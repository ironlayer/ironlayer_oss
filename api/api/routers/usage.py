"""Usage metering API endpoints.

Provides usage summaries and event listings for billing and quota
enforcement.  Requires ADMIN role.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from api.dependencies import RoleDep, SessionDep, TenantDep
from api.middleware.rbac import Role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/usage", tags=["usage"])


@router.get("/summary")
async def get_usage_summary(
    session: SessionDep,
    tenant_id: TenantDep,
    role: RoleDep,
    days: int = Query(default=30, ge=1, le=365, description="Number of days to summarize"),
) -> dict[str, Any]:
    """Return aggregated usage summary for the current tenant.

    Requires ADMIN role.
    """
    if role not in (Role.ADMIN,):
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail="Admin role required")

    from datetime import timedelta

    from core_engine.state.tables import UsageEventTable

    cutoff = datetime.now(UTC) - timedelta(days=days)

    stmt = (
        select(
            UsageEventTable.event_type,
            func.count().label("count"),
            func.sum(UsageEventTable.quantity).label("total_quantity"),
        )
        .where(UsageEventTable.tenant_id == tenant_id)
        .where(UsageEventTable.created_at >= cutoff)
        .group_by(UsageEventTable.event_type)
    )

    result = await session.execute(stmt)
    rows = result.all()

    summary: dict[str, dict[str, int]] = {}
    total_events = 0

    for event_type, count, total_quantity in rows:
        summary[event_type] = {
            "count": count,
            "total_quantity": total_quantity or 0,
        }
        total_events += count

    return {
        "tenant_id": tenant_id,
        "period_days": days,
        "total_events": total_events,
        "by_type": summary,
    }


@router.get("/events")
async def list_usage_events(
    session: SessionDep,
    tenant_id: TenantDep,
    role: RoleDep,
    event_type: str | None = Query(default=None, description="Filter by event type"),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum events to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
) -> dict[str, Any]:
    """Return paginated usage events for the current tenant.

    Requires ADMIN role.
    """
    if role not in (Role.ADMIN,):
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail="Admin role required")

    from core_engine.state.tables import UsageEventTable

    stmt = (
        select(UsageEventTable)
        .where(UsageEventTable.tenant_id == tenant_id)
        .order_by(UsageEventTable.created_at.desc())
        .limit(limit)
        .offset(offset)
    )

    if event_type:
        stmt = stmt.where(UsageEventTable.event_type == event_type)

    result = await session.execute(stmt)
    rows = result.scalars().all()

    events = [
        {
            "event_id": row.event_id,
            "event_type": row.event_type,
            "quantity": row.quantity,
            "metadata": row.metadata_json,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]

    return {
        "tenant_id": tenant_id,
        "events": events,
        "limit": limit,
        "offset": offset,
        "count": len(events),
    }
