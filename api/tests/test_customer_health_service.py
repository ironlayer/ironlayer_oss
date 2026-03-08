"""Tests for api/api/services/customer_health_service.py

Covers:
- _score_recency: None input, recent date, old date, mid-range date
- _classify_status: active, at_risk, churning, boundary values
- _compute_trend: no previous, improving, declining, stable
- CustomerHealthService.compute_health_score: 4-dimension scoring, status, trend, upsert
- CustomerHealthService.compute_all: iterates tenant configs
- CustomerHealthService.get_health_detail: None and found cases
- CustomerHealthService.list_tenants: paginated results with summary
- CustomerHealthService._fire_status_change_event: logs at WARNING level
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.customer_health_service import (
    CustomerHealthService,
    _classify_status,
    _compute_trend,
    _score_recency,
)

# ---------------------------------------------------------------------------
# _score_recency
# ---------------------------------------------------------------------------


class TestScoreRecency:
    """Verify _score_recency computes linear decay from 1 to 30 days."""

    def test_none_input_returns_zero(self) -> None:
        """A None timestamp always yields 0 points."""
        assert _score_recency(None) == 0.0

    def test_none_input_ignores_max_points(self) -> None:
        """A None timestamp yields 0 regardless of max_points."""
        assert _score_recency(None, max_points=100.0) == 0.0

    def test_recent_date_within_one_day_returns_max(self) -> None:
        """A timestamp less than 1 day old returns full points."""
        recent = datetime.now(UTC) - timedelta(hours=12)
        assert _score_recency(recent) == 25.0

    def test_very_recent_date_seconds_ago(self) -> None:
        """A timestamp just seconds ago returns full points."""
        recent = datetime.now(UTC) - timedelta(seconds=30)
        assert _score_recency(recent) == 25.0

    def test_old_date_beyond_30_days_returns_zero(self) -> None:
        """A timestamp 30+ days old returns 0."""
        old = datetime.now(UTC) - timedelta(days=31)
        assert _score_recency(old) == 0.0

    def test_exactly_30_days_returns_zero(self) -> None:
        """A timestamp exactly 30 days old returns 0."""
        old = datetime.now(UTC) - timedelta(days=30)
        assert _score_recency(old) == 0.0

    def test_mid_range_date_returns_partial_score(self) -> None:
        """A timestamp ~15 days old returns roughly half of max_points."""
        mid = datetime.now(UTC) - timedelta(days=15)
        score = _score_recency(mid)
        # At 15 days: max_points * (1 - (15 - 1) / 29) = 25 * (1 - 14/29) ~ 12.07
        assert 10.0 < score < 15.0

    def test_custom_max_points(self) -> None:
        """max_points parameter scales the returned score."""
        recent = datetime.now(UTC) - timedelta(hours=6)
        assert _score_recency(recent, max_points=50.0) == 50.0

    def test_two_days_old_near_max(self) -> None:
        """A timestamp 2 days old should return close to max but not full."""
        two_days = datetime.now(UTC) - timedelta(days=2)
        score = _score_recency(two_days)
        # 25 * (1 - (2-1)/29) = 25 * (28/29) ~ 24.14
        assert 23.0 < score < 25.0


# ---------------------------------------------------------------------------
# _classify_status
# ---------------------------------------------------------------------------


class TestClassifyStatus:
    """Verify _classify_status maps score to correct status label."""

    def test_high_score_is_active(self) -> None:
        """Score 80 should classify as 'active'."""
        assert _classify_status(80.0) == "active"

    def test_score_100_is_active(self) -> None:
        """Perfect score classifies as 'active'."""
        assert _classify_status(100.0) == "active"

    def test_score_exactly_60_is_active(self) -> None:
        """Boundary: score 60 is 'active' (>= 60)."""
        assert _classify_status(60.0) == "active"

    def test_score_59_is_at_risk(self) -> None:
        """Score just below 60 classifies as 'at_risk'."""
        assert _classify_status(59.9) == "at_risk"

    def test_mid_range_is_at_risk(self) -> None:
        """Score 45 classifies as 'at_risk'."""
        assert _classify_status(45.0) == "at_risk"

    def test_score_exactly_30_is_at_risk(self) -> None:
        """Boundary: score 30 is 'at_risk' (>= 30)."""
        assert _classify_status(30.0) == "at_risk"

    def test_score_29_is_churning(self) -> None:
        """Score just below 30 classifies as 'churning'."""
        assert _classify_status(29.9) == "churning"

    def test_zero_score_is_churning(self) -> None:
        """Score 0 classifies as 'churning'."""
        assert _classify_status(0.0) == "churning"

    def test_low_score_is_churning(self) -> None:
        """Score 10 classifies as 'churning'."""
        assert _classify_status(10.0) == "churning"


# ---------------------------------------------------------------------------
# _compute_trend
# ---------------------------------------------------------------------------


class TestComputeTrend:
    """Verify _compute_trend determines correct trend direction."""

    def test_no_previous_returns_stable(self) -> None:
        """When previous score is None, trend is 'stable'."""
        assert _compute_trend(50.0, None) == "stable"

    def test_large_positive_delta_is_improving(self) -> None:
        """A delta > 5 indicates 'improving'."""
        assert _compute_trend(70.0, 60.0) == "improving"

    def test_delta_exactly_6_is_improving(self) -> None:
        """A delta of exactly 6 triggers 'improving'."""
        assert _compute_trend(56.0, 50.0) == "improving"

    def test_large_negative_delta_is_declining(self) -> None:
        """A delta < -5 indicates 'declining'."""
        assert _compute_trend(40.0, 60.0) == "declining"

    def test_delta_exactly_minus_6_is_declining(self) -> None:
        """A delta of exactly -6 triggers 'declining'."""
        assert _compute_trend(44.0, 50.0) == "declining"

    def test_small_positive_delta_is_stable(self) -> None:
        """A delta of +3 (within +/-5) is 'stable'."""
        assert _compute_trend(53.0, 50.0) == "stable"

    def test_small_negative_delta_is_stable(self) -> None:
        """A delta of -3 (within +/-5) is 'stable'."""
        assert _compute_trend(47.0, 50.0) == "stable"

    def test_zero_delta_is_stable(self) -> None:
        """No change at all is 'stable'."""
        assert _compute_trend(50.0, 50.0) == "stable"

    def test_delta_exactly_5_is_stable(self) -> None:
        """A delta of exactly 5 is still 'stable' (must be >5 to improve)."""
        assert _compute_trend(55.0, 50.0) == "stable"

    def test_delta_exactly_minus_5_is_stable(self) -> None:
        """A delta of exactly -5 is still 'stable' (must be <-5 to decline)."""
        assert _compute_trend(45.0, 50.0) == "stable"


# ---------------------------------------------------------------------------
# Shared fixture for service tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Return a mock AsyncSession for CustomerHealthService tests."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = 0
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


# ---------------------------------------------------------------------------
# CustomerHealthService.compute_health_score
# ---------------------------------------------------------------------------


class TestComputeHealthScore:
    """Verify compute_health_score aggregates 4 dimensions correctly."""

    @pytest.mark.asyncio
    async def test_all_recent_activity_yields_high_score(self, mock_session: AsyncMock) -> None:
        """All four dimensions returning recent activity should produce a high score."""
        now = datetime.now(UTC)
        recent_dt = now - timedelta(hours=6)

        # Build 4 result mocks for the 4 session.execute calls:
        # 1. last_login (scalar_one_or_none -> recent datetime)
        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = recent_dt

        # 2. last_plan_run (scalar_one_or_none -> recent datetime)
        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = recent_dt

        # 3. last_ai_call (scalar_one_or_none -> recent datetime)
        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = recent_dt

        # 4. distinct_types (scalar_one -> 6 = max feature breadth)
        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 6

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_health_score("tenant-1")

        assert result["tenant_id"] == "tenant-1"
        # All 4 dimensions at max: 25 * 4 = 100
        assert result["health_score"] == 100.0
        assert result["health_status"] == "active"
        assert result["trend_direction"] == "stable"  # No previous score
        assert result["engagement_metrics"]["login_recency"] == 25.0
        assert result["engagement_metrics"]["plan_activity"] == 25.0
        assert result["engagement_metrics"]["ai_adoption"] == 25.0
        assert result["engagement_metrics"]["feature_breadth"] == 25.0
        repo_instance.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_activity_yields_churning(self, mock_session: AsyncMock) -> None:
        """No activity across all dimensions produces score 0 and 'churning' status."""
        # All queries return None / 0
        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = None

        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = None

        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = None

        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 0

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_health_score("tenant-2")

        assert result["health_score"] == 0.0
        assert result["health_status"] == "churning"

    @pytest.mark.asyncio
    async def test_partial_activity_yields_at_risk(self, mock_session: AsyncMock) -> None:
        """Mixed recent and absent activity lands in the at_risk range."""
        now = datetime.now(UTC)
        recent_dt = now - timedelta(hours=3)

        # Only login and plan are recent; AI and features are absent
        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = recent_dt

        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = recent_dt

        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = None

        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 0

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_health_score("tenant-3")

        # Login=25, Plan=25, AI=0, Feature=0 => 50
        assert result["health_score"] == 50.0
        assert result["health_status"] == "at_risk"

    @pytest.mark.asyncio
    async def test_trend_improving_with_previous_score(self, mock_session: AsyncMock) -> None:
        """When new score exceeds previous by >5, trend is 'improving'."""
        now = datetime.now(UTC)
        recent_dt = now - timedelta(hours=2)

        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = recent_dt

        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = recent_dt

        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = recent_dt

        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 6

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        # Previous record with low score
        existing_record = MagicMock()
        existing_record.health_score = 40.0
        existing_record.health_status = "at_risk"

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = existing_record
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_health_score("tenant-4")

        assert result["health_score"] == 100.0
        assert result["previous_score"] == 40.0
        assert result["trend_direction"] == "improving"

    @pytest.mark.asyncio
    async def test_status_change_fires_event(self, mock_session: AsyncMock) -> None:
        """When status changes from the existing record, _fire_status_change_event is called."""
        now = datetime.now(UTC)
        recent_dt = now - timedelta(hours=1)

        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = recent_dt

        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = recent_dt

        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = recent_dt

        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 6

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        # Previous record with churning status
        existing_record = MagicMock()
        existing_record.health_score = 20.0
        existing_record.health_status = "churning"

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = existing_record
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            with patch.object(service, "_fire_status_change_event") as mock_fire:
                await service.compute_health_score("tenant-5")
                mock_fire.assert_called_once_with("tenant-5", "churning", "active", 100.0)

    @pytest.mark.asyncio
    async def test_feature_breadth_capped_at_25(self, mock_session: AsyncMock) -> None:
        """Even with more than 6 distinct event types, feature breadth caps at 25."""
        now = datetime.now(UTC)
        recent_dt = now - timedelta(hours=1)

        login_result = MagicMock()
        login_result.scalar_one_or_none.return_value = None

        plan_result = MagicMock()
        plan_result.scalar_one_or_none.return_value = None

        ai_result = MagicMock()
        ai_result.scalar_one_or_none.return_value = None

        feature_result = MagicMock()
        feature_result.scalar_one.return_value = 10  # More than 6

        mock_session.execute = AsyncMock(side_effect=[login_result, plan_result, ai_result, feature_result])

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            repo_instance.upsert.return_value = MagicMock()
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_health_score("tenant-cap")

        assert result["engagement_metrics"]["feature_breadth"] == 25.0


# ---------------------------------------------------------------------------
# CustomerHealthService.compute_all
# ---------------------------------------------------------------------------


class TestComputeAll:
    """Verify compute_all iterates all tenant configs."""

    @pytest.mark.asyncio
    async def test_computes_for_all_tenants(self, mock_session: AsyncMock) -> None:
        """compute_all should call compute_health_score for each active tenant config."""
        config_a = MagicMock()
        config_a.tenant_id = "tenant-a"
        config_b = MagicMock()
        config_b.tenant_id = "tenant-b"
        config_c = MagicMock()
        config_c.tenant_id = "tenant-c"

        with (
            patch("api.services.customer_health_service.TenantConfigRepository") as MockConfigRepo,
            patch("api.services.customer_health_service.CustomerHealthRepository"),
        ):
            config_repo_instance = AsyncMock()
            config_repo_instance.list_all.return_value = [config_a, config_b, config_c]
            MockConfigRepo.return_value = config_repo_instance

            service = CustomerHealthService(mock_session)

            with patch.object(service, "compute_health_score", new_callable=AsyncMock) as mock_compute:
                mock_compute.return_value = {"health_score": 50.0}
                result = await service.compute_all()

        assert result["computed_count"] == 3
        assert "duration_ms" in result
        assert result["duration_ms"] >= 0
        assert mock_compute.call_count == 3
        mock_compute.assert_any_call("tenant-a")
        mock_compute.assert_any_call("tenant-b")
        mock_compute.assert_any_call("tenant-c")

    @pytest.mark.asyncio
    async def test_compute_all_handles_individual_failure(self, mock_session: AsyncMock) -> None:
        """If one tenant fails, others still get computed and count reflects successes."""
        config_a = MagicMock()
        config_a.tenant_id = "tenant-ok"
        config_b = MagicMock()
        config_b.tenant_id = "tenant-fail"
        config_c = MagicMock()
        config_c.tenant_id = "tenant-ok-2"

        with (
            patch("api.services.customer_health_service.TenantConfigRepository") as MockConfigRepo,
            patch("api.services.customer_health_service.CustomerHealthRepository"),
        ):
            config_repo_instance = AsyncMock()
            config_repo_instance.list_all.return_value = [config_a, config_b, config_c]
            MockConfigRepo.return_value = config_repo_instance

            service = CustomerHealthService(mock_session)

            async def side_effect(tenant_id: str) -> dict:
                if tenant_id == "tenant-fail":
                    raise RuntimeError("DB error")
                return {"health_score": 50.0}

            with patch.object(service, "compute_health_score", side_effect=side_effect):
                result = await service.compute_all()

        assert result["computed_count"] == 2

    @pytest.mark.asyncio
    async def test_compute_all_empty_configs(self, mock_session: AsyncMock) -> None:
        """No tenant configs means zero computed."""
        with (
            patch("api.services.customer_health_service.TenantConfigRepository") as MockConfigRepo,
            patch("api.services.customer_health_service.CustomerHealthRepository"),
        ):
            config_repo_instance = AsyncMock()
            config_repo_instance.list_all.return_value = []
            MockConfigRepo.return_value = config_repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.compute_all()

        assert result["computed_count"] == 0


# ---------------------------------------------------------------------------
# CustomerHealthService.get_health_detail
# ---------------------------------------------------------------------------


class TestGetHealthDetail:
    """Verify get_health_detail returns correct data or None."""

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, mock_session: AsyncMock) -> None:
        """When the repository returns None, the method returns None."""
        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = None
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.get_health_detail("nonexistent-tenant")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_formatted_dict_when_found(self, mock_session: AsyncMock) -> None:
        """When a record exists, returns a properly formatted dict with all fields."""
        now = datetime.now(UTC)
        record = MagicMock()
        record.tenant_id = "tenant-found"
        record.health_score = 72.5
        record.health_status = "active"
        record.trend_direction = "improving"
        record.previous_score = 60.0
        record.engagement_metrics_json = {
            "login_recency": 25.0,
            "plan_activity": 22.5,
            "ai_adoption": 15.0,
            "feature_breadth": 10.0,
        }
        record.last_login_at = now
        record.last_plan_run_at = now - timedelta(days=2)
        record.last_ai_call_at = now - timedelta(days=5)
        record.computed_at = now
        record.updated_at = now

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = record
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.get_health_detail("tenant-found")

        assert result is not None
        assert result["tenant_id"] == "tenant-found"
        assert result["health_score"] == 72.5
        assert result["health_status"] == "active"
        assert result["trend_direction"] == "improving"
        assert result["previous_score"] == 60.0
        assert result["engagement_metrics"]["login_recency"] == 25.0
        assert result["last_login_at"] == now.isoformat()
        assert result["computed_at"] == now.isoformat()
        assert result["updated_at"] == now.isoformat()

    @pytest.mark.asyncio
    async def test_handles_none_datetime_fields(self, mock_session: AsyncMock) -> None:
        """When datetime fields are None, the output dict shows None instead of crashing."""
        record = MagicMock()
        record.tenant_id = "tenant-sparse"
        record.health_score = 10.0
        record.health_status = "churning"
        record.trend_direction = "stable"
        record.previous_score = None
        record.engagement_metrics_json = {}
        record.last_login_at = None
        record.last_plan_run_at = None
        record.last_ai_call_at = None
        record.computed_at = None
        record.updated_at = None

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get.return_value = record
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.get_health_detail("tenant-sparse")

        assert result["last_login_at"] is None
        assert result["last_plan_run_at"] is None
        assert result["last_ai_call_at"] is None
        assert result["computed_at"] is None
        assert result["updated_at"] is None


# ---------------------------------------------------------------------------
# CustomerHealthService.list_tenants
# ---------------------------------------------------------------------------


class TestListTenants:
    """Verify list_tenants returns paginated results with summary counts."""

    @pytest.mark.asyncio
    async def test_returns_paginated_results_with_summary(self, mock_session: AsyncMock) -> None:
        """list_tenants returns tenants list, total count, and status summary."""
        now = datetime.now(UTC)

        row_active = MagicMock()
        row_active.tenant_id = "t-active"
        row_active.health_score = 80.0
        row_active.health_status = "active"
        row_active.trend_direction = "stable"
        row_active.last_login_at = now
        row_active.last_plan_run_at = now
        row_active.computed_at = now

        row_risk = MagicMock()
        row_risk.tenant_id = "t-risk"
        row_risk.health_score = 45.0
        row_risk.health_status = "at_risk"
        row_risk.trend_direction = "declining"
        row_risk.last_login_at = now - timedelta(days=10)
        row_risk.last_plan_run_at = None
        row_risk.computed_at = now

        row_churning = MagicMock()
        row_churning.tenant_id = "t-churning"
        row_churning.health_score = 10.0
        row_churning.health_status = "churning"
        row_churning.trend_direction = "declining"
        row_churning.last_login_at = None
        row_churning.last_plan_run_at = None
        row_churning.computed_at = now

        # Build a mock result for the GROUP BY summary query executed by
        # list_tenants via self._session.execute().  Each row returned by
        # .all() must expose .health_status and .cnt attributes.
        summary_row_active = MagicMock()
        summary_row_active.health_status = "active"
        summary_row_active.cnt = 1
        summary_row_at_risk = MagicMock()
        summary_row_at_risk.health_status = "at_risk"
        summary_row_at_risk.cnt = 1
        summary_row_churning = MagicMock()
        summary_row_churning.health_status = "churning"
        summary_row_churning.cnt = 1

        summary_result = MagicMock()
        summary_result.all.return_value = [
            summary_row_active,
            summary_row_at_risk,
            summary_row_churning,
        ]
        mock_session.execute = AsyncMock(return_value=summary_result)

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_all.return_value = ([row_active, row_risk], 3)
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.list_tenants(status_filter=None, sort_by="health_score", limit=2, offset=0)

        assert result["total"] == 3
        assert len(result["tenants"]) == 2
        assert result["tenants"][0]["tenant_id"] == "t-active"
        assert result["tenants"][1]["tenant_id"] == "t-risk"
        assert result["summary"]["active"] == 1
        assert result["summary"]["at_risk"] == 1
        assert result["summary"]["churning"] == 1

    @pytest.mark.asyncio
    async def test_empty_results(self, mock_session: AsyncMock) -> None:
        """list_tenants with no data returns empty tenants and zero summary."""
        # The summary GROUP BY query returns no rows when the table is empty.
        summary_result = MagicMock()
        summary_result.all.return_value = []
        mock_session.execute = AsyncMock(return_value=summary_result)

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_all.return_value = ([], 0)
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.list_tenants()

        assert result["total"] == 0
        assert result["tenants"] == []
        assert result["summary"] == {"active": 0, "at_risk": 0, "churning": 0}

    @pytest.mark.asyncio
    async def test_none_datetime_fields_in_list(self, mock_session: AsyncMock) -> None:
        """Tenants with None datetime fields are serialized as None strings."""
        row = MagicMock()
        row.tenant_id = "t-null-dates"
        row.health_score = 15.0
        row.health_status = "churning"
        row.trend_direction = "stable"
        row.last_login_at = None
        row.last_plan_run_at = None
        row.computed_at = None

        # Summary GROUP BY returns one churning row.
        summary_row = MagicMock()
        summary_row.health_status = "churning"
        summary_row.cnt = 1
        summary_result = MagicMock()
        summary_result.all.return_value = [summary_row]
        mock_session.execute = AsyncMock(return_value=summary_result)

        with patch("api.services.customer_health_service.CustomerHealthRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_all.return_value = ([row], 1)
            MockRepo.return_value = repo_instance

            service = CustomerHealthService(mock_session)
            result = await service.list_tenants()

        tenant = result["tenants"][0]
        assert tenant["last_login_at"] is None
        assert tenant["last_plan_run_at"] is None
        assert tenant["computed_at"] is None


# ---------------------------------------------------------------------------
# CustomerHealthService._fire_status_change_event
# ---------------------------------------------------------------------------


class TestFireStatusChange:
    """Verify _fire_status_change_event logs at WARNING level."""

    def test_logs_warning_on_status_change(self, mock_session: AsyncMock, caplog: pytest.LogCaptureFixture) -> None:
        """_fire_status_change_event should emit a WARNING log message."""
        with patch("api.services.customer_health_service.CustomerHealthRepository"):
            service = CustomerHealthService(mock_session)

        with caplog.at_level(logging.WARNING, logger="api.services.customer_health_service"):
            service._fire_status_change_event("tenant-log", "at_risk", "active", 65.0)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.WARNING
        assert "tenant-log" in record.message
        assert "at_risk" in record.message
        assert "active" in record.message
        assert "65.0" in record.message

    def test_logs_churning_to_at_risk(self, mock_session: AsyncMock, caplog: pytest.LogCaptureFixture) -> None:
        """Verify log message for churning -> at_risk transition."""
        with patch("api.services.customer_health_service.CustomerHealthRepository"):
            service = CustomerHealthService(mock_session)

        with caplog.at_level(logging.WARNING, logger="api.services.customer_health_service"):
            service._fire_status_change_event("tenant-x", "churning", "at_risk", 35.0)

        assert len(caplog.records) == 1
        assert "churning" in caplog.records[0].message
        assert "at_risk" in caplog.records[0].message
