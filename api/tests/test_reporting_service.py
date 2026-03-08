"""Tests for api/api/services/reporting_service.py

Covers:
- ReportingService: cost_report grouped by model and by time bucket
- ReportingService: usage_report grouped by actor and by time bucket
- ReportingService: llm_report aggregation by call type and time, total computation
- ReportingService: export_data as CSV and JSON, empty data handling, unknown report type
- ReportingService: comparison_report with absolute/percentage delta, zero-previous handling
"""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.reporting_service import ReportingService

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Mock async database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


@pytest.fixture()
def since() -> datetime:
    """Standard 'since' datetime for test periods."""
    return datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture()
def until() -> datetime:
    """Standard 'until' datetime for test periods."""
    return datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Cost Report
# ---------------------------------------------------------------------------


class TestCostReport:
    """Verify cost_report delegates to the correct repo method based on group_by."""

    @pytest.mark.asyncio
    async def test_group_by_model_calls_get_cost_by_model(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """group_by='model' delegates to get_cost_by_model."""
        items = [
            {"model": "staging.orders", "cost_usd": 10.5},
            {"model": "marts.revenue", "cost_usd": 5.25},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.cost_report(since, until, group_by="model")

        mock_repo_instance.get_cost_by_model.assert_called_once_with(since, until)
        assert result["items"] == items
        assert result["total_cost_usd"] == 15.75
        assert result["group_by"] == "model"
        assert result["period"]["since"] == since.isoformat()
        assert result["period"]["until"] == until.isoformat()

    @pytest.mark.asyncio
    async def test_group_by_day_calls_get_cost_by_time(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """group_by='day' delegates to get_cost_by_time."""
        items = [
            {"bucket": "2026-01-01", "cost_usd": 3.0},
            {"bucket": "2026-01-02", "cost_usd": 4.0},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_time = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.cost_report(since, until, group_by="day")

        mock_repo_instance.get_cost_by_time.assert_called_once_with(since, until, "day")
        assert result["items"] == items
        assert result["total_cost_usd"] == 7.0
        assert result["group_by"] == "day"

    @pytest.mark.asyncio
    async def test_empty_items_returns_zero_total(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """An empty items list produces a total_cost_usd of 0."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.cost_report(since, until, group_by="model")

        assert result["items"] == []
        assert result["total_cost_usd"] == 0

    @pytest.mark.asyncio
    async def test_total_cost_rounded_to_four_decimals(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """Total cost is rounded to 4 decimal places."""
        items = [
            {"model": "a", "cost_usd": 1.11111},
            {"model": "b", "cost_usd": 2.22222},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.cost_report(since, until, group_by="model")

        assert result["total_cost_usd"] == round(1.11111 + 2.22222, 4)


# ---------------------------------------------------------------------------
# Usage Report
# ---------------------------------------------------------------------------


class TestUsageReport:
    """Verify usage_report delegates to the correct repo method based on group_by."""

    @pytest.mark.asyncio
    async def test_group_by_actor_calls_get_usage_by_actor(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """group_by='actor' delegates to get_usage_by_actor."""
        items = [
            {"actor": "user-a", "plan_count": 10, "ai_count": 50},
            {"actor": "user-b", "plan_count": 5, "ai_count": 20},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_usage_by_actor = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.usage_report(since, until, group_by="actor")

        mock_repo_instance.get_usage_by_actor.assert_called_once_with(since, until)
        assert result["items"] == items
        assert result["group_by"] == "actor"
        assert result["period"]["since"] == since.isoformat()
        assert result["period"]["until"] == until.isoformat()

    @pytest.mark.asyncio
    async def test_group_by_day_calls_get_usage_by_type_over_time(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """group_by='day' delegates to get_usage_by_type_over_time."""
        items = [
            {"bucket": "2026-01-01", "plan_run": 3, "ai_call": 10},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_usage_by_type_over_time = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.usage_report(since, until, group_by="day")

        mock_repo_instance.get_usage_by_type_over_time.assert_called_once_with(since, until, "day")
        assert result["items"] == items
        assert result["group_by"] == "day"

    @pytest.mark.asyncio
    async def test_default_group_by_is_actor(self, mock_session: AsyncMock, since: datetime, until: datetime) -> None:
        """Default group_by is 'actor'."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_usage_by_actor = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.usage_report(since, until)

        mock_repo_instance.get_usage_by_actor.assert_called_once()
        assert result["group_by"] == "actor"


# ---------------------------------------------------------------------------
# LLM Report
# ---------------------------------------------------------------------------


class TestLlmReport:
    """Verify llm_report aggregates by_call_type and by_time, and computes total."""

    @pytest.mark.asyncio
    async def test_aggregates_by_call_type_and_time(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """llm_report returns both by_call_type and by_time from the repo."""
        by_type = [
            {"call_type": "classify", "cost_usd": 1.5, "token_count": 5000},
            {"call_type": "optimize", "cost_usd": 3.0, "token_count": 12000},
        ]
        by_time = [
            {"bucket": "2026-01-01", "cost_usd": 2.0},
            {"bucket": "2026-01-02", "cost_usd": 2.5},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(return_value=by_type)
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=by_time)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.llm_report(since, until)

        assert result["by_call_type"] == by_type
        assert result["by_time"] == by_time
        mock_repo_instance.get_llm_cost_by_call_type.assert_called_once_with(since, until)
        mock_repo_instance.get_llm_cost_by_time.assert_called_once_with(since, until, "day")

    @pytest.mark.asyncio
    async def test_total_cost_computed_from_by_call_type(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """total_cost_usd is the sum of cost_usd from by_call_type items."""
        by_type = [
            {"call_type": "classify", "cost_usd": 1.123456},
            {"call_type": "optimize", "cost_usd": 2.654321},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(return_value=by_type)
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.llm_report(since, until)

        expected_total = round(1.123456 + 2.654321, 6)
        assert result["total_cost_usd"] == expected_total

    @pytest.mark.asyncio
    async def test_empty_by_call_type_returns_zero_total(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """Empty by_call_type produces total_cost_usd of 0."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(return_value=[])
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.llm_report(since, until)

        assert result["total_cost_usd"] == 0
        assert result["by_call_type"] == []
        assert result["by_time"] == []

    @pytest.mark.asyncio
    async def test_period_included_in_result(self, mock_session: AsyncMock, since: datetime, until: datetime) -> None:
        """Result includes period with since and until as ISO strings."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(return_value=[])
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.llm_report(since, until)

        assert result["period"]["since"] == since.isoformat()
        assert result["period"]["until"] == until.isoformat()


# ---------------------------------------------------------------------------
# Export Data
# ---------------------------------------------------------------------------


class TestExportData:
    """Verify export_data produces correct bytes, content types, and filenames."""

    @pytest.mark.asyncio
    async def test_csv_format_returns_text_csv(self, mock_session: AsyncMock, since: datetime, until: datetime) -> None:
        """CSV export returns text/csv content type and .csv filename."""
        items = [
            {"model": "staging.orders", "cost_usd": 10.5},
            {"model": "marts.revenue", "cost_usd": 5.25},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            data_bytes, content_type, filename = await service.export_data("cost", since, until, fmt="csv")

        assert content_type == "text/csv"
        assert filename.endswith(".csv")
        assert filename.startswith("ironlayer_cost_report_")

        # Verify CSV content is parseable and has correct rows.
        reader = csv.DictReader(io.StringIO(data_bytes.decode("utf-8")))
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["model"] == "staging.orders"
        assert rows[1]["model"] == "marts.revenue"

    @pytest.mark.asyncio
    async def test_json_format_returns_application_json(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """JSON export returns application/json content type and .json filename."""
        items = [
            {"model": "staging.orders", "cost_usd": 10.5},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            data_bytes, content_type, filename = await service.export_data("cost", since, until, fmt="json")

        assert content_type == "application/json"
        assert filename.endswith(".json")
        assert filename.startswith("ironlayer_cost_report_")

        # Verify JSON content is parseable.
        parsed = json.loads(data_bytes.decode("utf-8"))
        assert "items" in parsed
        assert "exported_at" in parsed
        assert len(parsed["items"]) == 1
        assert parsed["items"][0]["model"] == "staging.orders"

    @pytest.mark.asyncio
    async def test_empty_data_csv_returns_empty_bytes(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """Empty items list in CSV format returns empty bytes."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            data_bytes, content_type, filename = await service.export_data("cost", since, until, fmt="csv")

        assert data_bytes == b""
        assert content_type == "text/csv"
        assert filename.endswith(".csv")

    @pytest.mark.asyncio
    async def test_unknown_report_type_raises_value_error(
        self, mock_session: AsyncMock, since: datetime, until: datetime
    ) -> None:
        """An unknown report_type raises ValueError."""
        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")

            with pytest.raises(ValueError, match="Unknown report type"):
                await service.export_data("invalid_type", since, until)

    @pytest.mark.asyncio
    async def test_usage_report_type_export(self, mock_session: AsyncMock, since: datetime, until: datetime) -> None:
        """Export with report_type='usage' calls usage_report and exports items."""
        items = [
            {"actor": "user-a", "plan_count": 10},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_usage_by_actor = AsyncMock(return_value=items)
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            data_bytes, content_type, filename = await service.export_data("usage", since, until, fmt="csv")

        assert content_type == "text/csv"
        assert filename.startswith("ironlayer_usage_report_")
        reader = csv.DictReader(io.StringIO(data_bytes.decode("utf-8")))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["actor"] == "user-a"

    @pytest.mark.asyncio
    async def test_llm_report_type_export(self, mock_session: AsyncMock, since: datetime, until: datetime) -> None:
        """Export with report_type='llm' calls llm_report and exports by_call_type items."""
        by_type = [
            {"call_type": "classify", "cost_usd": 1.5},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(return_value=by_type)
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            data_bytes, content_type, filename = await service.export_data("llm", since, until, fmt="json")

        assert content_type == "application/json"
        assert filename.startswith("ironlayer_llm_report_")
        parsed = json.loads(data_bytes.decode("utf-8"))
        assert len(parsed["items"]) == 1
        assert parsed["items"][0]["call_type"] == "classify"


# ---------------------------------------------------------------------------
# Comparison Report
# ---------------------------------------------------------------------------


class TestComparisonReport:
    """Verify comparison_report computes deltas correctly across periods."""

    @pytest.fixture()
    def current_period(self) -> tuple[datetime, datetime]:
        """Current comparison period: Feb 2026."""
        return (
            datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2026, 2, 28, 23, 59, 59, tzinfo=UTC),
        )

    @pytest.fixture()
    def previous_period(self) -> tuple[datetime, datetime]:
        """Previous comparison period: Jan 2026."""
        return (
            datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC),
        )

    @pytest.mark.asyncio
    async def test_cost_comparison_computes_absolute_delta(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """Absolute delta is current_total - previous_total for cost reports."""
        current_items = [{"model": "a", "cost_usd": 30.0}]
        previous_items = [{"model": "a", "cost_usd": 20.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        assert result["delta"]["absolute"] == 10.0
        assert result["delta"]["direction"] == "up"

    @pytest.mark.asyncio
    async def test_cost_comparison_computes_percentage_delta(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """Percentage delta is (absolute / previous_total) * 100."""
        current_items = [{"model": "a", "cost_usd": 30.0}]
        previous_items = [{"model": "a", "cost_usd": 20.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        # (10 / 20) * 100 = 50.0%
        assert result["delta"]["percentage"] == 50.0

    @pytest.mark.asyncio
    async def test_zero_previous_total_percentage_is_none(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """When previous total is zero, percentage delta is None (avoid division by zero)."""
        current_items = [{"model": "a", "cost_usd": 10.0}]
        previous_items: list[dict] = []

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        assert result["delta"]["absolute"] == 10.0
        assert result["delta"]["percentage"] is None

    @pytest.mark.asyncio
    async def test_decrease_shows_down_direction(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """When current < previous, delta direction is 'down'."""
        current_items = [{"model": "a", "cost_usd": 5.0}]
        previous_items = [{"model": "a", "cost_usd": 20.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        assert result["delta"]["absolute"] == -15.0
        assert result["delta"]["direction"] == "down"
        assert result["delta"]["percentage"] == -75.0

    @pytest.mark.asyncio
    async def test_flat_when_both_equal(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """When current == previous, delta direction is 'flat'."""
        current_items = [{"model": "a", "cost_usd": 20.0}]
        previous_items = [{"model": "a", "cost_usd": 20.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        assert result["delta"]["absolute"] == 0
        assert result["delta"]["direction"] == "flat"
        assert result["delta"]["percentage"] == 0.0

    @pytest.mark.asyncio
    async def test_llm_comparison_report(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """LLM comparison uses total_cost_usd from llm_report."""
        current_by_type = [{"call_type": "classify", "cost_usd": 5.0}]
        previous_by_type = [{"call_type": "classify", "cost_usd": 2.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_llm_cost_by_call_type = AsyncMock(side_effect=[current_by_type, previous_by_type])
            mock_repo_instance.get_llm_cost_by_time = AsyncMock(return_value=[])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="llm",
            )

        assert result["current"]["total_cost_usd"] == 5.0
        assert result["previous"]["total_cost_usd"] == 2.0
        assert result["delta"]["absolute"] == 3.0
        assert result["delta"]["direction"] == "up"

    @pytest.mark.asyncio
    async def test_usage_comparison_uses_item_count(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """Usage comparison uses len(items) as the total, not cost."""
        current_items = [
            {"actor": "a", "plan_count": 10},
            {"actor": "b", "plan_count": 5},
            {"actor": "c", "plan_count": 3},
        ]
        previous_items = [
            {"actor": "a", "plan_count": 10},
        ]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_usage_by_actor = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="usage",
            )

        # 3 actors current - 1 actor previous = delta of 2.
        assert result["delta"]["absolute"] == 2
        assert result["delta"]["direction"] == "up"
        assert result["delta"]["percentage"] == 200.0

    @pytest.mark.asyncio
    async def test_result_includes_both_period_reports(
        self,
        mock_session: AsyncMock,
        current_period: tuple[datetime, datetime],
        previous_period: tuple[datetime, datetime],
    ) -> None:
        """Result includes full current and previous report objects."""
        current_items = [{"model": "a", "cost_usd": 10.0}]
        previous_items = [{"model": "a", "cost_usd": 8.0}]

        with patch("api.services.reporting_service.ReportingRepository") as MockRepo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_cost_by_model = AsyncMock(side_effect=[current_items, previous_items])
            MockRepo.return_value = mock_repo_instance

            service = ReportingService(mock_session, "test-tenant")
            result = await service.comparison_report(
                current_period[0],
                current_period[1],
                previous_period[0],
                previous_period[1],
                report_type="cost",
            )

        assert "current" in result
        assert "previous" in result
        assert "delta" in result
        assert result["current"]["items"] == current_items
        assert result["previous"]["items"] == previous_items
        assert result["current"]["period"]["since"] == current_period[0].isoformat()
        assert result["previous"]["period"]["since"] == previous_period[0].isoformat()
