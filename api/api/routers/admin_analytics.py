"""Admin analytics endpoints for cross-tenant platform insights.

All endpoints require ``VIEW_ANALYTICS`` permission (admin-only).
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Query

from api.dependencies import AdminSessionDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.analytics_service import AnalyticsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/analytics", tags=["admin-analytics"])


@router.get("/overview")
async def get_overview(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.VIEW_ANALYTICS)),
    days: int = Query(30, ge=1, le=365, description="Lookback window in days"),
) -> dict[str, Any]:
    """Return platform-wide aggregate metrics.

    Includes total/active tenant counts, total usage events, total runs,
    and total cost for the specified lookback window.
    """
    service = AnalyticsService(session)
    return await service.get_overview(days)


@router.get("/tenants")
async def get_tenant_breakdown(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.VIEW_ANALYTICS)),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Return per-tenant usage, cost, and billing breakdown.

    Results are paginated and include plan tier, usage counts by type,
    run cost, and LLM cost per tenant.
    """
    service = AnalyticsService(session)
    return await service.get_tenant_breakdown(days, limit, offset)


@router.get("/revenue")
async def get_revenue(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.VIEW_ANALYTICS)),
) -> dict[str, Any]:
    """Return MRR and subscription counts grouped by plan tier.

    Tier pricing: community=$0, team=$49, enterprise=$199.
    """
    service = AnalyticsService(session)
    return await service.get_revenue()


@router.get("/cost-breakdown")
async def get_cost_breakdown(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.VIEW_ANALYTICS)),
    days: int = Query(30, ge=1, le=365),
    group_by: str = Query("model", description="Group by: model, day, week, month"),
) -> dict[str, Any]:
    """Return cost breakdown grouped by model or time bucket."""
    service = AnalyticsService(session)
    return await service.get_cost_breakdown(days, group_by)


@router.get("/health")
async def get_health(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.VIEW_ANALYTICS)),
    days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    """Return platform health metrics.

    Includes run error rate, P95 runtime, AI acceptance rate, and
    AI prediction accuracy.
    """
    service = AnalyticsService(session)
    return await service.get_health(days)
