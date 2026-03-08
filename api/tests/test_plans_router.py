"""Tests for api/api/routers/plans.py

Covers:
- POST /api/plans/generate: valid request, missing parameters (422)
- GET /api/plans/{plan_id}: existing plan, non-existent (404)
- GET /api/plans: list with filters / pagination
- POST /api/plans/{plan_id}/apply: successful apply, already running (409 via RuntimeError)
- POST /api/plans/{plan_id}/augment: AI augmentation, plan not found (404)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# POST /api/plans/generate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_plan_success(
    client: AsyncClient,
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
    test_settings,
) -> None:
    """A valid generate request returns a plan dictionary with 200."""
    expected_plan = {
        "plan_id": "generated-plan-id",
        "base": "sha-base",
        "target": "sha-target",
        "summary": {"total_steps": 1, "estimated_cost_usd": 1.0, "models_changed": ["m1"]},
        "steps": [],
    }

    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.generate_plan = AsyncMock(return_value=expected_plan)

        resp = await client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "sha-base",
                "target_sha": "sha-target",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["plan_id"] == "generated-plan-id"
    assert body["summary"]["total_steps"] == 1
    assert body["steps"] == []


@pytest.mark.asyncio
async def test_generate_plan_missing_repo_path(client: AsyncClient) -> None:
    """Missing required field repo_path returns 422."""
    resp = await client.post(
        "/api/v1/plans/generate",
        json={
            "base_sha": "sha-base",
            "target_sha": "sha-target",
        },
    )
    assert resp.status_code == 422
    body = resp.json()
    assert "detail" in body


@pytest.mark.asyncio
async def test_generate_plan_missing_all_fields(client: AsyncClient) -> None:
    """Empty request body returns 422."""
    resp = await client.post("/api/v1/plans/generate", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_generate_plan_missing_target_sha(client: AsyncClient) -> None:
    """Missing target_sha returns 422."""
    resp = await client.post(
        "/api/v1/plans/generate",
        json={
            "repo_path": "/tmp/repo",
            "base_sha": "sha-base",
        },
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_generate_plan_runtime_error_returns_422(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """RuntimeError from PlanService maps to 422."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.generate_plan = AsyncMock(side_effect=RuntimeError("git diff failed (exit 128): fatal: bad revision"))

        resp = await client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "bad-sha",
                "target_sha": "also-bad",
            },
        )

    assert resp.status_code == 422
    assert "git diff failed" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_generate_plan_value_error_returns_400(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """ValueError from PlanService maps to 400."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.generate_plan = AsyncMock(side_effect=ValueError("Not a valid git repository: /tmp/repo"))

        resp = await client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "aaa",
                "target_sha": "bbb",
            },
        )

    assert resp.status_code == 400
    assert "Not a valid git repository" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/plans/{plan_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_plan_existing(
    client: AsyncClient,
    sample_plan_json: dict[str, Any],
) -> None:
    """Fetching an existing plan returns its data with 200."""
    plan_data = dict(sample_plan_json)
    plan_data["approvals"] = []
    plan_data["auto_approved"] = False
    plan_data["created_at"] = "2024-06-15T12:00:00+00:00"

    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.get_plan = AsyncMock(return_value=plan_data)

        resp = await client.get("/api/v1/plans/abc123def456")

    assert resp.status_code == 200
    body = resp.json()
    assert body["plan_id"] == "abc123def456"
    assert body["approvals"] == []
    assert body["auto_approved"] is False
    assert len(body["steps"]) == 2


@pytest.mark.asyncio
async def test_get_plan_not_found(client: AsyncClient) -> None:
    """Fetching a non-existent plan returns 404."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.get_plan = AsyncMock(return_value=None)

        resp = await client.get("/api/v1/plans/nonexistent-id")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# GET /api/plans (list)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_plans_returns_summaries(client: AsyncClient) -> None:
    """List endpoint returns plan summaries with pagination."""
    summaries = [
        {
            "plan_id": "plan-1",
            "base_sha": "aaa",
            "target_sha": "bbb",
            "total_steps": 2,
            "estimated_cost_usd": 3.0,
            "models_changed": ["m1", "m2"],
            "created_at": "2024-06-15T12:00:00+00:00",
        },
        {
            "plan_id": "plan-2",
            "base_sha": "ccc",
            "target_sha": "ddd",
            "total_steps": 1,
            "estimated_cost_usd": 1.0,
            "models_changed": ["m3"],
            "created_at": "2024-06-14T10:00:00+00:00",
        },
    ]

    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.list_plans = AsyncMock(return_value=summaries)

        resp = await client.get("/api/v1/plans?limit=10&offset=0")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["plan_id"] == "plan-1"
    assert body[1]["total_steps"] == 1


@pytest.mark.asyncio
async def test_list_plans_empty(client: AsyncClient) -> None:
    """List endpoint returns an empty list when no plans exist."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.list_plans = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/plans")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_list_plans_pagination_params(client: AsyncClient) -> None:
    """Limit and offset query parameters are forwarded to the service."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.list_plans = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/plans?limit=5&offset=10")

    assert resp.status_code == 200
    instance.list_plans.assert_awaited_once_with(limit=5, offset=10)


@pytest.mark.asyncio
async def test_list_plans_invalid_limit(client: AsyncClient) -> None:
    """Limit out of range (>100 or <1) returns 422."""
    resp = await client.get("/api/v1/plans?limit=200")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/plans/{plan_id}/apply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_success(client: AsyncClient) -> None:
    """Applying a plan returns a list of run records."""
    run_records = [
        {
            "run_id": "run-001",
            "plan_id": "abc123",
            "step_id": "step-001",
            "model_name": "staging.orders",
            "status": "SUCCESS",
        }
    ]

    with patch("api.routers.plans.ExecutionService") as MockExecService:
        instance = MockExecService.return_value
        instance.apply_plan = AsyncMock(return_value=run_records)

        resp = await client.post(
            "/api/v1/plans/abc123/apply",
            json={"approved_by": "user@test.com", "auto_approve": True},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["status"] == "SUCCESS"
    assert body[0]["model_name"] == "staging.orders"


@pytest.mark.asyncio
async def test_apply_plan_not_found(client: AsyncClient) -> None:
    """Applying a non-existent plan returns 404."""
    with patch("api.routers.plans.ExecutionService") as MockExecService:
        instance = MockExecService.return_value
        instance.apply_plan = AsyncMock(side_effect=ValueError("Plan xyz not found"))

        resp = await client.post(
            "/api/v1/plans/xyz/apply",
            json={"auto_approve": True},
        )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_apply_plan_permission_denied(client: AsyncClient) -> None:
    """Applying a plan without approval in non-dev environment returns 403."""
    with patch("api.routers.plans.ExecutionService") as MockExecService:
        instance = MockExecService.return_value
        instance.apply_plan = AsyncMock(side_effect=PermissionError("Plan abc has no approvals."))

        resp = await client.post(
            "/api/v1/plans/abc/apply",
            json={"auto_approve": False},
        )

    assert resp.status_code == 403
    assert "no approvals" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_apply_plan_runtime_error_returns_500(client: AsyncClient) -> None:
    """RuntimeError during execution maps to 500."""
    with patch("api.routers.plans.ExecutionService") as MockExecService:
        instance = MockExecService.return_value
        instance.apply_plan = AsyncMock(side_effect=RuntimeError("Databricks cluster unreachable"))

        resp = await client.post(
            "/api/v1/plans/abc/apply",
            json={"auto_approve": True},
        )

    assert resp.status_code == 500
    # RuntimeError details are sanitised to prevent information leaks (fix 2.3).
    assert "internal error" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /api/plans/{plan_id}/augment
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_augment_plan_success(
    client: AsyncClient,
    sample_plan_json: dict[str, Any],
) -> None:
    """Augmenting a plan returns plan data with advisory metadata."""
    augmented = dict(sample_plan_json)
    augmented["advisory"] = {
        "staging.orders": {
            "semantic_classification": {"change_type": "non_breaking"},
            "cost_prediction": {"estimated_cost_usd": 1.5},
            "risk_score": {"risk_score": 1.0, "risk_level": "low"},
        }
    }

    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.generate_augmented_plan = AsyncMock(return_value=augmented)

        resp = await client.post("/api/v1/plans/abc123def456/augment")

    assert resp.status_code == 200
    body = resp.json()
    assert "advisory" in body
    assert "staging.orders" in body["advisory"]
    assert body["advisory"]["staging.orders"]["risk_score"]["risk_level"] == "low"


@pytest.mark.asyncio
async def test_augment_plan_not_found(client: AsyncClient) -> None:
    """Augmenting a non-existent plan returns 404."""
    with patch("api.routers.plans.PlanService") as MockPlanService:
        instance = MockPlanService.return_value
        instance.generate_augmented_plan = AsyncMock(side_effect=ValueError("Plan nonexistent not found"))

        resp = await client.post("/api/v1/plans/nonexistent/augment")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()
