"""Customer health endpoints for cross-tenant engagement monitoring.

All endpoints require ``MANAGE_HEALTH`` permission (admin-only).
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import AdminSessionDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.customer_health_service import CustomerHealthService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/health", tags=["customer-health"])


@router.get("/tenants")
async def list_tenants(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_HEALTH)),
    status: str | None = Query(None, description="Filter by health status: active, at_risk, churning"),
    sort_by: str = Query("health_score", description="Sort by: health_score, health_score_desc, updated_at, tenant_id"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """List all tenants with health scores, filterable by status.

    Returns paginated results with summary counts of active, at-risk, and
    churning tenants.
    """
    if status and status not in ("active", "at_risk", "churning"):
        raise HTTPException(status_code=400, detail=f"Invalid status filter: {status}")

    service = CustomerHealthService(session)
    return await service.list_tenants(
        status_filter=status,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
    )


@router.get("/tenants/{tenant_id}")
async def get_tenant_health(
    tenant_id: str,
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_HEALTH)),
) -> dict[str, Any]:
    """Return detailed health breakdown for a specific tenant.

    Includes health score, status, trend direction, engagement metrics
    breakdown, and activity timestamps.
    """
    service = CustomerHealthService(session)
    detail = await service.get_health_detail(tenant_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"No health data for tenant '{tenant_id}'")
    return detail


@router.post("/compute")
async def compute_health(
    session: AdminSessionDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_HEALTH)),
) -> dict[str, Any]:
    """Recompute health scores for all active tenants.

    Returns the count of tenants processed and execution duration.
    This is an expensive operation — use sparingly or schedule it.
    """
    service = CustomerHealthService(session)
    result = await service.compute_all()
    return result
