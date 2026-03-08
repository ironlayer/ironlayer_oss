"""Tests for execution reconciliation endpoints.

Covers:
- POST /api/v1/reconciliation/trigger returns expected summary
- GET /api/v1/reconciliation/discrepancies returns list
- POST /api/v1/reconciliation/resolve/{id} resolves a discrepancy
- GET /api/v1/reconciliation/stats returns statistics
- 404 for non-existent check_id on resolve
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# POST /api/v1/reconciliation/trigger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_reconciliation_success(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Triggering reconciliation returns a summary of results."""
    expected_result = {
        "total_runs": 5,
        "checked": 3,
        "matched": 2,
        "discrepancies": 1,
        "skipped": 2,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.trigger_reconciliation = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/trigger",
            json={"plan_id": None, "hours_back": 24},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_runs"] == 5
    assert body["checked"] == 3
    assert body["matched"] == 2
    assert body["discrepancies"] == 1
    assert body["skipped"] == 2


@pytest.mark.asyncio
async def test_trigger_reconciliation_with_plan_id(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Triggering reconciliation for a specific plan forwards the plan_id."""
    expected_result = {
        "total_runs": 2,
        "checked": 2,
        "matched": 2,
        "discrepancies": 0,
        "skipped": 0,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.trigger_reconciliation = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/trigger",
            json={"plan_id": "abc123def456", "hours_back": 48},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_runs"] == 2
    assert body["discrepancies"] == 0
    instance.trigger_reconciliation.assert_awaited_once_with(
        plan_id="abc123def456",
        hours_back=48,
    )


@pytest.mark.asyncio
async def test_trigger_reconciliation_invalid_hours_back(
    client: AsyncClient,
) -> None:
    """hours_back outside allowed range (1-168) returns 422."""
    resp = await client.post(
        "/api/v1/reconciliation/trigger",
        json={"hours_back": 0},
    )
    assert resp.status_code == 422

    resp = await client.post(
        "/api/v1/reconciliation/trigger",
        json={"hours_back": 200},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_trigger_reconciliation_default_body(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Trigger with default body values uses None plan_id and 24 hours."""
    expected_result = {
        "total_runs": 0,
        "checked": 0,
        "matched": 0,
        "discrepancies": 0,
        "skipped": 0,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.trigger_reconciliation = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/trigger",
            json={},
        )

    assert resp.status_code == 200
    instance.trigger_reconciliation.assert_awaited_once_with(
        plan_id=None,
        hours_back=24,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/reconciliation/discrepancies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_discrepancies_returns_list(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Discrepancies endpoint returns a list of unresolved discrepancies."""
    expected_discrepancies = [
        {
            "id": 1,
            "run_id": "run-001",
            "model_name": "staging.orders",
            "expected_status": "SUCCESS",
            "warehouse_status": "FAIL",
            "discrepancy_type": "phantom_success",
            "checked_at": "2026-02-21T12:00:00+00:00",
        },
        {
            "id": 2,
            "run_id": "run-002",
            "model_name": "marts.revenue",
            "expected_status": "RUNNING",
            "warehouse_status": "SUCCESS",
            "discrepancy_type": "stale_running",
            "checked_at": "2026-02-21T11:30:00+00:00",
        },
    ]

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_discrepancies = AsyncMock(return_value=expected_discrepancies)

        resp = await client.get("/api/v1/reconciliation/discrepancies")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["discrepancy_type"] == "phantom_success"
    assert body[1]["model_name"] == "marts.revenue"


@pytest.mark.asyncio
async def test_get_discrepancies_empty(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Discrepancies endpoint returns an empty list when no discrepancies exist."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_discrepancies = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/reconciliation/discrepancies")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_get_discrepancies_with_limit(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Discrepancies endpoint forwards the limit parameter."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_discrepancies = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/reconciliation/discrepancies?limit=10")

    assert resp.status_code == 200
    instance.get_discrepancies.assert_awaited_once_with(limit=10)


@pytest.mark.asyncio
async def test_get_discrepancies_invalid_limit(
    client: AsyncClient,
) -> None:
    """Limit out of range returns 422."""
    resp = await client.get("/api/v1/reconciliation/discrepancies?limit=0")
    assert resp.status_code == 422

    resp = await client.get("/api/v1/reconciliation/discrepancies?limit=600")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/v1/reconciliation/resolve/{check_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_discrepancy_success(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Resolving a discrepancy returns the updated record."""
    expected_result = {
        "id": 1,
        "run_id": "run-001",
        "model_name": "staging.orders",
        "resolved": True,
        "resolved_by": "admin@company.com",
        "resolved_at": "2026-02-21T14:00:00+00:00",
        "resolution_note": "Confirmed warehouse state is correct; updated control plane.",
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.resolve_discrepancy = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/resolve/1",
            json={
                "resolved_by": "admin@company.com",
                "resolution_note": "Confirmed warehouse state is correct; updated control plane.",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["resolved"] is True
    assert body["resolved_by"] == "admin@company.com"
    assert body["resolution_note"] == "Confirmed warehouse state is correct; updated control plane."


@pytest.mark.asyncio
async def test_resolve_discrepancy_not_found(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Resolving a non-existent check_id returns 404."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.resolve_discrepancy = AsyncMock(return_value=None)

        resp = await client.post(
            "/api/v1/reconciliation/resolve/99999",
            json={
                "resolved_by": "admin@company.com",
                "resolution_note": "Attempting to resolve non-existent check.",
            },
        )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_resolve_discrepancy_missing_fields(
    client: AsyncClient,
) -> None:
    """Missing required fields in resolve body returns 422."""
    resp = await client.post(
        "/api/v1/reconciliation/resolve/1",
        json={},
    )
    assert resp.status_code == 422

    resp = await client.post(
        "/api/v1/reconciliation/resolve/1",
        json={"resolved_by": "admin@company.com"},
    )
    assert resp.status_code == 422

    resp = await client.post(
        "/api/v1/reconciliation/resolve/1",
        json={"resolution_note": "some note"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_resolve_discrepancy_empty_strings(
    client: AsyncClient,
) -> None:
    """Empty strings for required fields returns 422 (min_length=1)."""
    resp = await client.post(
        "/api/v1/reconciliation/resolve/1",
        json={
            "resolved_by": "",
            "resolution_note": "",
        },
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/reconciliation/stats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_stats_returns_statistics(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Stats endpoint returns reconciliation summary statistics."""
    expected_stats = {
        "total_checks": 50,
        "total_discrepancies": 5,
        "unresolved_discrepancies": 2,
        "resolved_discrepancies": 3,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_stats = AsyncMock(return_value=expected_stats)

        resp = await client.get("/api/v1/reconciliation/stats")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_checks"] == 50
    assert body["total_discrepancies"] == 5
    assert body["unresolved_discrepancies"] == 2
    assert body["resolved_discrepancies"] == 3


@pytest.mark.asyncio
async def test_get_stats_empty(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Stats endpoint returns zeroes when no checks have been performed."""
    expected_stats = {
        "total_checks": 0,
        "total_discrepancies": 0,
        "unresolved_discrepancies": 0,
        "resolved_discrepancies": 0,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_stats = AsyncMock(return_value=expected_stats)

        resp = await client.get("/api/v1/reconciliation/stats")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_checks"] == 0
    assert body["unresolved_discrepancies"] == 0


# ---------------------------------------------------------------------------
# ReconciliationService unit tests
# ---------------------------------------------------------------------------


class TestClassifyDiscrepancy:
    """Verify the static discrepancy classification logic."""

    def test_phantom_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("SUCCESS", "FAIL")
        assert result == "phantom_success"

    def test_missed_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("FAIL", "SUCCESS")
        assert result == "missed_success"

    def test_stale_running(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("RUNNING", "SUCCESS")
        assert result == "stale_running"

    def test_stale_running_failed(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("RUNNING", "FAIL")
        assert result == "stale_running_failed"

    def test_stale_pending_success(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("PENDING", "SUCCESS")
        assert result == "stale_pending"

    def test_stale_pending_fail(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("PENDING", "FAIL")
        assert result == "stale_pending"

    def test_generic_mismatch(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("SUCCESS", "CANCELLED")
        assert result == "status_mismatch"

    def test_cancelled_to_running(self) -> None:
        from api.services.reconciliation_service import ReconciliationService

        result = ReconciliationService._classify_discrepancy("CANCELLED", "RUNNING")
        assert result == "status_mismatch"


# ---------------------------------------------------------------------------
# ReconciliationCheckTable ORM tests
# ---------------------------------------------------------------------------


class TestReconciliationCheckTable:
    """Verify ReconciliationCheckTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import ReconciliationCheckTable

        assert ReconciliationCheckTable.__tablename__ == "reconciliation_checks"

    def test_primary_key(self) -> None:
        from core_engine.state.tables import ReconciliationCheckTable

        pk_cols = [c.name for c in ReconciliationCheckTable.__table__.columns if c.primary_key]
        assert pk_cols == ["id"]

    def test_required_columns_exist(self) -> None:
        from core_engine.state.tables import ReconciliationCheckTable

        col_names = [c.name for c in ReconciliationCheckTable.__table__.columns]
        assert "tenant_id" in col_names
        assert "run_id" in col_names
        assert "model_name" in col_names
        assert "expected_status" in col_names
        assert "warehouse_status" in col_names
        assert "discrepancy_type" in col_names
        assert "resolved" in col_names
        assert "resolved_by" in col_names
        assert "resolved_at" in col_names
        assert "resolution_note" in col_names
        assert "checked_at" in col_names

    def test_indexes_exist(self) -> None:
        from core_engine.state.tables import ReconciliationCheckTable

        index_names = [idx.name for idx in ReconciliationCheckTable.__table__.indexes]
        assert "ix_reconciliation_tenant_run" in index_names
        assert "ix_reconciliation_tenant_unresolved" in index_names
        assert "ix_reconciliation_checked_at" in index_names
