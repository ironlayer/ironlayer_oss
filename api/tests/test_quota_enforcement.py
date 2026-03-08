"""Tests for quota enforcement integration in api/api/routers/plans.py

Covers:
- POST /plans/generate: QuotaService.check_plan_quota gate (429 when exceeded)
- POST /plans/{plan_id}/augment: QuotaService.check_ai_quota gate (429)
  and QuotaService.check_llm_budget gate (402 Payment Required)

The plans router imports QuotaService inline inside endpoint functions,
so we patch at the source module: api.services.quota_service.QuotaService.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")


# ---------------------------------------------------------------------------
# POST /plans/generate - plan quota enforcement
# ---------------------------------------------------------------------------


class TestPlanQuotaEnforcement:
    """Verify quota checks gate plan generation at POST /api/v1/plans/generate."""

    @pytest.mark.asyncio
    async def test_generate_plan_allowed(self, client: AsyncClient) -> None:
        """When check_plan_quota returns (True, None), plan generation proceeds."""
        expected_plan: dict[str, Any] = {
            "plan_id": "quota-ok-plan",
            "base": "sha-a",
            "target": "sha-b",
            "summary": {"total_steps": 1, "estimated_cost_usd": 1.0, "models_changed": ["m1"]},
            "steps": [],
        }

        with (
            patch("api.services.quota_service.QuotaService") as MockQuota,
            patch("api.routers.plans.PlanService") as MockPlanService,
            patch("api.routers.plans.AuditService") as MockAudit,
        ):
            quota_instance = MockQuota.return_value
            quota_instance.check_plan_quota = AsyncMock(return_value=(True, None))

            plan_instance = MockPlanService.return_value
            plan_instance.generate_plan = AsyncMock(return_value=expected_plan)

            audit_instance = MockAudit.return_value
            audit_instance.log = AsyncMock()

            resp = await client.post(
                "/api/v1/plans/generate",
                json={
                    "repo_path": "/tmp/repo",
                    "base_sha": "sha-a",
                    "target_sha": "sha-b",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["plan_id"] == "quota-ok-plan"
        assert body["summary"]["total_steps"] == 1

        quota_instance.check_plan_quota.assert_awaited_once()
        plan_instance.generate_plan.assert_awaited_once()
        call_kwargs = plan_instance.generate_plan.call_args.kwargs
        assert call_kwargs["base_sha"] == "sha-a"
        assert call_kwargs["target_sha"] == "sha-b"

    @pytest.mark.asyncio
    async def test_generate_plan_quota_exceeded(self, client: AsyncClient) -> None:
        """When check_plan_quota returns (False, reason), endpoint returns 429."""
        exceeded_msg = "Monthly plan run quota exceeded (100/100). Upgrade your plan for higher limits."

        with patch("api.services.quota_service.QuotaService") as MockQuota:
            quota_instance = MockQuota.return_value
            quota_instance.check_plan_quota = AsyncMock(return_value=(False, exceeded_msg))

            resp = await client.post(
                "/api/v1/plans/generate",
                json={
                    "repo_path": "/tmp/repo",
                    "base_sha": "sha-a",
                    "target_sha": "sha-b",
                },
            )

        assert resp.status_code == 429
        body = resp.json()
        assert body["detail"] == exceeded_msg

        quota_instance.check_plan_quota.assert_awaited_once()


# ---------------------------------------------------------------------------
# POST /plans/{plan_id}/augment - AI quota and LLM budget enforcement
# ---------------------------------------------------------------------------


class TestAugmentQuotaEnforcement:
    """Verify quota and budget checks gate AI augmentation at POST /api/v1/plans/{plan_id}/augment."""

    @pytest.mark.asyncio
    async def test_augment_plan_ai_quota_exceeded(self, client: AsyncClient) -> None:
        """When check_ai_quota returns (False, reason), endpoint returns 429."""
        ai_exceeded_msg = "Monthly AI call quota exceeded (500/500). Upgrade your plan for higher limits."

        with patch("api.services.quota_service.QuotaService") as MockQuota:
            quota_instance = MockQuota.return_value
            quota_instance.check_ai_quota = AsyncMock(return_value=(False, ai_exceeded_msg))

            resp = await client.post("/api/v1/plans/plan-abc/augment")

        assert resp.status_code == 429
        body = resp.json()
        assert body["detail"] == ai_exceeded_msg

        quota_instance.check_ai_quota.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_augment_plan_llm_budget_exceeded(self, client: AsyncClient) -> None:
        """When AI quota passes but LLM budget is exceeded, endpoint returns 402."""
        budget_exceeded_msg = "Daily LLM budget exceeded ($10.00/$10.00). Increase your budget in tenant settings."

        with patch("api.services.quota_service.QuotaService") as MockQuota:
            quota_instance = MockQuota.return_value
            quota_instance.check_ai_quota = AsyncMock(return_value=(True, None))
            quota_instance.check_llm_budget = AsyncMock(return_value=(False, budget_exceeded_msg))

            resp = await client.post("/api/v1/plans/plan-abc/augment")

        assert resp.status_code == 402
        body = resp.json()
        assert body["detail"] == budget_exceeded_msg

        quota_instance.check_ai_quota.assert_awaited_once()
        quota_instance.check_llm_budget.assert_awaited_once()
