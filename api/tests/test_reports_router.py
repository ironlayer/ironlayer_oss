"""Tests for api/api/routers/reports.py

Covers:
- GET /admin/reports/cost: cost report with date range and grouping
- GET /admin/reports/usage: usage report with date range and grouping
- GET /admin/reports/llm: LLM cost and token usage report
- GET /admin/reports/export: CSV and JSON export with streaming response
- RBAC: all endpoints require VIEW_REPORTS (admin-only)
- Input validation: invalid report_type, invalid format, date parsing
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_dev_token(
    tenant_id: str = "default",
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
) -> str:
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": scopes or ["read", "write"],
        "jti": "test-jti-conftest",
        "identity_kind": "user",
        "role": role,
    }
    payload_json = json.dumps(payload)
    signature = hmac.new(
        _DEV_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token_bytes = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{token_bytes}.{signature}"


_VIEWER_TOKEN = _make_dev_token(role="viewer")
_VIEWER_HEADERS = {"Authorization": f"Bearer {_VIEWER_TOKEN}"}

_BASE = "/api/v1/admin/reports"


# ---------------------------------------------------------------------------
# Sample response data
# ---------------------------------------------------------------------------

_COST_REPORT: dict[str, Any] = {
    "items": [
        {"group": "staging.orders", "total_cost_usd": 340.0, "run_count": 28},
        {"group": "marts.revenue", "total_cost_usd": 510.0, "run_count": 42},
    ],
    "total_cost_usd": 850.0,
    "group_by": "model",
    "since": "2024-06-01T00:00:00+00:00",
    "until": "2024-07-01T00:00:00+00:00",
}

_USAGE_REPORT: dict[str, Any] = {
    "items": [
        {"group": "deploy-bot", "event_count": 250, "run_count": 90},
        {"group": "alice@acme.com", "event_count": 180, "run_count": 55},
    ],
    "total_events": 430,
    "group_by": "actor",
    "since": "2024-06-01T00:00:00+00:00",
    "until": "2024-07-01T00:00:00+00:00",
}

_LLM_REPORT: dict[str, Any] = {
    "total_tokens": 1_250_000,
    "total_cost_usd": 62.50,
    "by_model": {
        "gpt-4o": {"tokens": 800_000, "cost_usd": 48.0},
        "claude-3-haiku": {"tokens": 450_000, "cost_usd": 14.50},
    },
    "since": "2024-06-01T00:00:00+00:00",
    "until": "2024-07-01T00:00:00+00:00",
}

_CSV_BYTES = b"model,cost_usd,run_count\nstaging.orders,340.00,28\nmarts.revenue,510.00,42\n"
_JSON_BYTES = b'[{"model":"staging.orders","cost_usd":340.00},{"model":"marts.revenue","cost_usd":510.00}]'


# ---------------------------------------------------------------------------
# GET /admin/reports/cost
# ---------------------------------------------------------------------------


class TestCostReportEndpoint:
    """Verify GET /api/v1/admin/reports/cost responses."""

    @pytest.mark.asyncio
    async def test_returns_cost_report(self, client: AsyncClient) -> None:
        """Admin token returns 200 with cost report items."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.cost_report = AsyncMock(return_value=_COST_REPORT)

            resp = await client.get(f"{_BASE}/cost")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_cost_usd"] == 850.0
        assert len(body["items"]) == 2
        assert body["items"][0]["group"] == "staging.orders"
        assert body["group_by"] == "model"

    @pytest.mark.asyncio
    async def test_with_date_params(self, client: AsyncClient) -> None:
        """Explicit since/until dates are forwarded to the service."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.cost_report = AsyncMock(return_value=_COST_REPORT)

            resp = await client.get(
                f"{_BASE}/cost",
                params={
                    "since": "2024-06-01T00:00:00",
                    "until": "2024-07-01T00:00:00",
                },
            )

        assert resp.status_code == 200
        call_args = instance.cost_report.call_args
        since_arg = call_args[0][0]
        until_arg = call_args[0][1]
        assert since_arg.year == 2024
        assert since_arg.month == 6
        assert until_arg.month == 7

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role cannot access cost report endpoint."""
        resp = await client.get(f"{_BASE}/cost", headers=_VIEWER_HEADERS)

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /admin/reports/usage
# ---------------------------------------------------------------------------


class TestUsageReportEndpoint:
    """Verify GET /api/v1/admin/reports/usage responses."""

    @pytest.mark.asyncio
    async def test_returns_usage_report(self, client: AsyncClient) -> None:
        """Admin token returns 200 with usage report items."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.usage_report = AsyncMock(return_value=_USAGE_REPORT)

            resp = await client.get(f"{_BASE}/usage")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_events"] == 430
        assert len(body["items"]) == 2
        assert body["items"][0]["group"] == "deploy-bot"
        assert body["group_by"] == "actor"

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role cannot access usage report endpoint."""
        resp = await client.get(f"{_BASE}/usage", headers=_VIEWER_HEADERS)

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /admin/reports/llm
# ---------------------------------------------------------------------------


class TestLlmReportEndpoint:
    """Verify GET /api/v1/admin/reports/llm responses."""

    @pytest.mark.asyncio
    async def test_returns_llm_report(self, client: AsyncClient) -> None:
        """Admin token returns 200 with LLM token and cost report."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.llm_report = AsyncMock(return_value=_LLM_REPORT)

            resp = await client.get(f"{_BASE}/llm")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_tokens"] == 1_250_000
        assert body["total_cost_usd"] == 62.50
        assert "gpt-4o" in body["by_model"]
        assert body["by_model"]["claude-3-haiku"]["cost_usd"] == 14.50

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role cannot access LLM report endpoint."""
        resp = await client.get(f"{_BASE}/llm", headers=_VIEWER_HEADERS)

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /admin/reports/export
# ---------------------------------------------------------------------------


class TestExportEndpoint:
    """Verify GET /api/v1/admin/reports/export responses."""

    @pytest.mark.asyncio
    async def test_csv_export(self, client: AsyncClient) -> None:
        """CSV export returns text/csv content type with Content-Disposition."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.export_data = AsyncMock(return_value=(_CSV_BYTES, "text/csv", "cost_report.csv"))

            resp = await client.get(
                f"{_BASE}/export",
                params={
                    "report_type": "cost",
                    "since": "2024-06-01T00:00:00",
                    "until": "2024-07-01T00:00:00",
                    "format": "csv",
                },
            )

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "cost_report.csv" in resp.headers["content-disposition"]
        assert b"staging.orders" in resp.content

    @pytest.mark.asyncio
    async def test_json_export(self, client: AsyncClient) -> None:
        """JSON export returns application/json content type."""
        with patch("api.routers.reports.ReportingService") as MockService:
            instance = MockService.return_value
            instance.export_data = AsyncMock(return_value=(_JSON_BYTES, "application/json", "cost_report.json"))

            resp = await client.get(
                f"{_BASE}/export",
                params={
                    "report_type": "cost",
                    "since": "2024-06-01T00:00:00",
                    "until": "2024-07-01T00:00:00",
                    "format": "json",
                },
            )

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")
        assert "cost_report.json" in resp.headers["content-disposition"]

    @pytest.mark.asyncio
    async def test_invalid_report_type(self, client: AsyncClient) -> None:
        """Invalid report_type returns 400."""
        resp = await client.get(
            f"{_BASE}/export",
            params={
                "report_type": "nonexistent",
                "format": "csv",
            },
        )

        assert resp.status_code == 400
        assert "Invalid report_type" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_invalid_format(self, client: AsyncClient) -> None:
        """Invalid export format returns 400."""
        resp = await client.get(
            f"{_BASE}/export",
            params={
                "report_type": "cost",
                "format": "xml",
            },
        )

        assert resp.status_code == 400
        assert "Invalid format" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role cannot access export endpoint."""
        resp = await client.get(
            f"{_BASE}/export",
            params={"report_type": "cost", "format": "csv"},
            headers=_VIEWER_HEADERS,
        )

        assert resp.status_code == 403
