"""Tests for export_data and comparison_report methods of ReportingService.

Covers:
- CSV export: headers, rows, empty items, special characters
- JSON export: valid JSON, required keys, items match
- Unknown report type raises ValueError
- Comparison report: cost delta, direction up/down/flat, zero previous, LLM, usage
"""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.reporting_service import ReportingService

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Return a mock AsyncSession for ReportingService tests."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


def _make_service(session: AsyncMock, tenant_id: str = "test-tenant") -> ReportingService:
    """Create a ReportingService with a mocked ReportingRepository."""
    with patch("api.services.reporting_service.ReportingRepository"):
        return ReportingService(session, tenant_id)


SINCE = datetime(2024, 6, 1, tzinfo=UTC)
UNTIL = datetime(2024, 7, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# TestExportCSV
# ---------------------------------------------------------------------------


class TestExportCSV:
    """Verify CSV export format, headers, rows, and edge cases."""

    @pytest.mark.asyncio
    async def test_csv_headers_match_item_keys(self, mock_session: AsyncMock) -> None:
        """CSV header row contains all keys from the first item."""
        items = [
            {"model": "gpt-4", "cost_usd": 1.50, "tokens": 1000},
            {"model": "gpt-3.5", "cost_usd": 0.50, "tokens": 500},
        ]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 2.0}
            data_bytes, content_type, filename = await service.export_data("cost", SINCE, UNTIL, fmt="csv")

        assert content_type == "text/csv"
        assert filename.endswith(".csv")

        text = data_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        assert set(reader.fieldnames) == {"model", "cost_usd", "tokens"}

    @pytest.mark.asyncio
    async def test_csv_rows_match_item_values(self, mock_session: AsyncMock) -> None:
        """Each CSV data row corresponds to an item in the report."""
        items = [
            {"model": "gpt-4", "cost_usd": 1.50, "tokens": 1000},
            {"model": "gpt-3.5", "cost_usd": 0.50, "tokens": 500},
        ]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 2.0}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="csv")

        text = data_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["model"] == "gpt-4"
        assert rows[0]["cost_usd"] == "1.5"
        assert rows[1]["model"] == "gpt-3.5"
        assert rows[1]["cost_usd"] == "0.5"

    @pytest.mark.asyncio
    async def test_csv_empty_items_returns_empty_bytes(self, mock_session: AsyncMock) -> None:
        """When there are no items, CSV export returns empty bytes."""
        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": [], "total_cost_usd": 0}
            data_bytes, content_type, filename = await service.export_data("cost", SINCE, UNTIL, fmt="csv")

        assert data_bytes == b""
        assert content_type == "text/csv"

    @pytest.mark.asyncio
    async def test_csv_special_characters_escaped(self, mock_session: AsyncMock) -> None:
        """Values containing commas, quotes, and newlines are properly CSV-escaped."""
        items = [
            {"model": 'model "with quotes"', "description": "has, comma", "notes": "line\nbreak"},
        ]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 0}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="csv")

        text = data_bytes.decode("utf-8")
        # Re-parse the CSV to verify data integrity survives round-trip
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["model"] == 'model "with quotes"'
        assert rows[0]["description"] == "has, comma"
        assert rows[0]["notes"] == "line\nbreak"

    @pytest.mark.asyncio
    async def test_csv_usage_report_type(self, mock_session: AsyncMock) -> None:
        """CSV export with report_type='usage' calls usage_report."""
        items = [{"actor": "user-1", "event_count": 42}]

        service = _make_service(mock_session)

        with patch.object(service, "usage_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items}
            data_bytes, _, _ = await service.export_data("usage", SINCE, UNTIL, fmt="csv")

        text = data_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["actor"] == "user-1"

    @pytest.mark.asyncio
    async def test_csv_llm_report_type(self, mock_session: AsyncMock) -> None:
        """CSV export with report_type='llm' uses by_call_type items."""
        items = [{"call_type": "classify", "cost_usd": 0.05}]

        service = _make_service(mock_session)

        with patch.object(service, "llm_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"by_call_type": items, "total_cost_usd": 0.05}
            data_bytes, _, _ = await service.export_data("llm", SINCE, UNTIL, fmt="csv")

        text = data_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["call_type"] == "classify"


# ---------------------------------------------------------------------------
# TestExportJSON
# ---------------------------------------------------------------------------


class TestExportJSON:
    """Verify JSON export format and content."""

    @pytest.mark.asyncio
    async def test_valid_json_output(self, mock_session: AsyncMock) -> None:
        """JSON export produces valid JSON bytes."""
        items = [{"model": "gpt-4", "cost_usd": 1.50}]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 1.5}
            data_bytes, content_type, filename = await service.export_data("cost", SINCE, UNTIL, fmt="json")

        assert content_type == "application/json"
        assert filename.endswith(".json")

        parsed = json.loads(data_bytes)
        assert isinstance(parsed, dict)

    @pytest.mark.asyncio
    async def test_contains_required_keys(self, mock_session: AsyncMock) -> None:
        """JSON output contains 'items' and 'exported_at' keys."""
        items = [{"model": "gpt-4", "cost_usd": 1.50}]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 1.5}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="json")

        parsed = json.loads(data_bytes)
        assert "items" in parsed
        assert "exported_at" in parsed

    @pytest.mark.asyncio
    async def test_items_match_input_data(self, mock_session: AsyncMock) -> None:
        """JSON items match the items from the report."""
        items = [
            {"model": "gpt-4", "cost_usd": 1.50},
            {"model": "claude-3", "cost_usd": 2.00},
        ]

        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": items, "total_cost_usd": 3.5}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="json")

        parsed = json.loads(data_bytes)
        assert len(parsed["items"]) == 2
        assert parsed["items"][0]["model"] == "gpt-4"
        assert parsed["items"][0]["cost_usd"] == 1.50
        assert parsed["items"][1]["model"] == "claude-3"
        assert parsed["items"][1]["cost_usd"] == 2.00

    @pytest.mark.asyncio
    async def test_exported_at_is_valid_iso_timestamp(self, mock_session: AsyncMock) -> None:
        """The exported_at field is a valid ISO 8601 timestamp."""
        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": [], "total_cost_usd": 0}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="json")

        parsed = json.loads(data_bytes)
        # Should not raise
        dt = datetime.fromisoformat(parsed["exported_at"])
        assert dt.year >= 2024

    @pytest.mark.asyncio
    async def test_empty_items_json(self, mock_session: AsyncMock) -> None:
        """JSON export with empty items still produces valid output."""
        service = _make_service(mock_session)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_report:
            mock_report.return_value = {"items": [], "total_cost_usd": 0}
            data_bytes, _, _ = await service.export_data("cost", SINCE, UNTIL, fmt="json")

        parsed = json.loads(data_bytes)
        assert parsed["items"] == []


# ---------------------------------------------------------------------------
# TestExportUnknownType
# ---------------------------------------------------------------------------


class TestExportUnknownType:
    """Verify unknown report types raise ValueError."""

    @pytest.mark.asyncio
    async def test_raises_value_error_for_unknown_type(self, mock_session: AsyncMock) -> None:
        """An unrecognized report_type raises ValueError."""
        service = _make_service(mock_session)

        with pytest.raises(ValueError, match="Unknown report type: invalid_type"):
            await service.export_data("invalid_type", SINCE, UNTIL, fmt="csv")

    @pytest.mark.asyncio
    async def test_raises_for_empty_string_type(self, mock_session: AsyncMock) -> None:
        """An empty string report_type raises ValueError."""
        service = _make_service(mock_session)

        with pytest.raises(ValueError, match="Unknown report type"):
            await service.export_data("", SINCE, UNTIL, fmt="csv")

    @pytest.mark.asyncio
    async def test_raises_for_json_format_too(self, mock_session: AsyncMock) -> None:
        """Unknown report type raises ValueError even with JSON format."""
        service = _make_service(mock_session)

        with pytest.raises(ValueError, match="Unknown report type: bogus"):
            await service.export_data("bogus", SINCE, UNTIL, fmt="json")


# ---------------------------------------------------------------------------
# TestComparisonReport
# ---------------------------------------------------------------------------


class TestComparisonReport:
    """Verify comparison_report computes deltas correctly across report types."""

    @pytest.mark.asyncio
    async def test_cost_comparison_positive_delta(self, mock_session: AsyncMock) -> None:
        """Cost comparison with higher current total produces positive delta and 'up' direction."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_cost:
            mock_cost.side_effect = [
                {"items": [], "total_cost_usd": 150.0, "period": {}, "group_by": "model"},
                {"items": [], "total_cost_usd": 100.0, "period": {}, "group_by": "model"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="cost"
            )

        assert result["delta"]["absolute"] == 50.0
        assert result["delta"]["percentage"] == 50.0
        assert result["delta"]["direction"] == "up"

    @pytest.mark.asyncio
    async def test_cost_comparison_negative_delta(self, mock_session: AsyncMock) -> None:
        """Cost comparison with lower current total produces negative delta and 'down' direction."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_cost:
            mock_cost.side_effect = [
                {"items": [], "total_cost_usd": 80.0, "period": {}, "group_by": "model"},
                {"items": [], "total_cost_usd": 100.0, "period": {}, "group_by": "model"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="cost"
            )

        assert result["delta"]["absolute"] == -20.0
        assert result["delta"]["percentage"] == -20.0
        assert result["delta"]["direction"] == "down"

    @pytest.mark.asyncio
    async def test_cost_comparison_zero_delta(self, mock_session: AsyncMock) -> None:
        """Identical current and previous totals produce zero delta and 'flat' direction."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_cost:
            mock_cost.side_effect = [
                {"items": [], "total_cost_usd": 100.0, "period": {}, "group_by": "model"},
                {"items": [], "total_cost_usd": 100.0, "period": {}, "group_by": "model"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="cost"
            )

        assert result["delta"]["absolute"] == 0.0
        assert result["delta"]["direction"] == "flat"
        assert result["delta"]["percentage"] == 0.0

    @pytest.mark.asyncio
    async def test_cost_comparison_zero_previous_gives_none_percentage(self, mock_session: AsyncMock) -> None:
        """When previous total is 0, percentage is None (avoid division by zero)."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_cost:
            mock_cost.side_effect = [
                {"items": [], "total_cost_usd": 50.0, "period": {}, "group_by": "model"},
                {"items": [], "total_cost_usd": 0.0, "period": {}, "group_by": "model"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="cost"
            )

        assert result["delta"]["absolute"] == 50.0
        assert result["delta"]["percentage"] is None
        assert result["delta"]["direction"] == "up"

    @pytest.mark.asyncio
    async def test_llm_comparison(self, mock_session: AsyncMock) -> None:
        """LLM comparison uses total_cost_usd from llm_report."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "llm_report", new_callable=AsyncMock) as mock_llm:
            mock_llm.side_effect = [
                {"by_call_type": [], "by_time": [], "total_cost_usd": 0.05, "period": {}},
                {"by_call_type": [], "by_time": [], "total_cost_usd": 0.02, "period": {}},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="llm"
            )

        assert result["delta"]["absolute"] == 0.03
        assert result["delta"]["direction"] == "up"
        assert result["delta"]["percentage"] == 150.0  # 0.03 / 0.02 * 100

    @pytest.mark.asyncio
    async def test_usage_comparison_uses_item_count(self, mock_session: AsyncMock) -> None:
        """Usage comparison uses len(items) for total, not a cost field."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        current_items = [{"actor": "a"}, {"actor": "b"}, {"actor": "c"}]
        previous_items = [{"actor": "a"}, {"actor": "b"}]

        with patch.object(service, "usage_report", new_callable=AsyncMock) as mock_usage:
            mock_usage.side_effect = [
                {"items": current_items, "period": {}, "group_by": "actor"},
                {"items": previous_items, "period": {}, "group_by": "actor"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="usage"
            )

        # Current has 3 items, previous has 2 => delta = 1
        assert result["delta"]["absolute"] == 1.0
        assert result["delta"]["direction"] == "up"
        assert result["delta"]["percentage"] == 50.0  # 1/2 * 100

    @pytest.mark.asyncio
    async def test_usage_comparison_zero_previous(self, mock_session: AsyncMock) -> None:
        """Usage comparison with zero previous items gives None percentage."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "usage_report", new_callable=AsyncMock) as mock_usage:
            mock_usage.side_effect = [
                {"items": [{"actor": "a"}], "period": {}, "group_by": "actor"},
                {"items": [], "period": {}, "group_by": "actor"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="usage"
            )

        assert result["delta"]["absolute"] == 1.0
        assert result["delta"]["percentage"] is None

    @pytest.mark.asyncio
    async def test_comparison_includes_current_and_previous(self, mock_session: AsyncMock) -> None:
        """Comparison result contains both 'current' and 'previous' report data."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        current_report = {"items": [{"model": "gpt-4"}], "total_cost_usd": 10.0, "period": {}, "group_by": "model"}
        previous_report = {"items": [{"model": "gpt-4"}], "total_cost_usd": 8.0, "period": {}, "group_by": "model"}

        with patch.object(service, "cost_report", new_callable=AsyncMock) as mock_cost:
            mock_cost.side_effect = [current_report, previous_report]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="cost"
            )

        assert "current" in result
        assert "previous" in result
        assert "delta" in result
        assert result["current"]["total_cost_usd"] == 10.0
        assert result["previous"]["total_cost_usd"] == 8.0

    @pytest.mark.asyncio
    async def test_usage_comparison_declining(self, mock_session: AsyncMock) -> None:
        """Usage comparison with fewer current items gives 'down' direction."""
        service = _make_service(mock_session)

        current_start = datetime(2024, 7, 1, tzinfo=UTC)
        current_end = datetime(2024, 8, 1, tzinfo=UTC)
        previous_start = datetime(2024, 6, 1, tzinfo=UTC)
        previous_end = datetime(2024, 7, 1, tzinfo=UTC)

        with patch.object(service, "usage_report", new_callable=AsyncMock) as mock_usage:
            mock_usage.side_effect = [
                {"items": [{"actor": "a"}], "period": {}, "group_by": "actor"},
                {"items": [{"actor": "a"}, {"actor": "b"}, {"actor": "c"}], "period": {}, "group_by": "actor"},
            ]
            result = await service.comparison_report(
                current_start, current_end, previous_start, previous_end, report_type="usage"
            )

        assert result["delta"]["absolute"] == -2.0
        assert result["delta"]["direction"] == "down"
