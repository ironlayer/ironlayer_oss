"""Model test endpoints: run tests, view results, and test history.

Provides a pre-apply quality gate by allowing test execution against
DuckDB (local) or the configured execution backend.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.test_service import TestService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tests", tags=["tests"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class RunTestsRequest(BaseModel):
    """Request body for running tests."""

    model_name: str | None = Field(
        default=None,
        description="Run tests for a specific model. Mutually exclusive with plan_id.",
    )
    plan_id: str | None = Field(
        default=None,
        description="Run tests for all models in a plan. Mutually exclusive with model_name.",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/run")
async def run_tests(
    body: RunTestsRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.RUN_TESTS)),
) -> dict[str, Any]:
    """Run tests for a model or all models in a plan.

    Exactly one of ``model_name`` or ``plan_id`` must be provided.
    Returns a summary of test results including pass/fail counts
    and whether any blocking failures were detected.
    """
    if body.model_name and body.plan_id:
        raise HTTPException(
            status_code=400,
            detail="Provide either model_name or plan_id, not both.",
        )
    if not body.model_name and not body.plan_id:
        raise HTTPException(
            status_code=400,
            detail="Provide either model_name or plan_id.",
        )

    service = TestService(session, tenant_id=tenant_id)

    if body.model_name:
        return await service.run_tests_for_model(body.model_name)

    assert body.plan_id is not None
    return await service.run_tests_for_plan(body.plan_id)


@router.get("/results/{plan_id}")
async def get_plan_test_results(
    plan_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_TEST_RESULTS)),
) -> dict[str, Any]:
    """Get test results for a plan.

    Returns the summary (total, passed, failed, blocked) and a list of
    individual test results.
    """
    service = TestService(session, tenant_id=tenant_id)
    return await service.get_plan_test_results(plan_id)


@router.get("/history/{model_name:path}")
async def get_test_history(
    model_name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_TEST_RESULTS)),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict[str, Any]]:
    """Get test execution history for a model.

    Returns a list of recent test results ordered by execution time
    (newest first), up to the specified limit.
    """
    service = TestService(session, tenant_id=tenant_id)
    return await service.get_test_history(model_name, limit=limit)
