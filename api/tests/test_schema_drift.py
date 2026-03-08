"""Tests for schema drift detection endpoints and service layer.

Covers:
- ReconciliationService.check_schema_drift with mocked model repo
- ReconciliationService.get_schema_drifts returns list
- ReconciliationService.resolve_schema_drift success and not_found
- Router endpoint tests for all new Phase 3 endpoints

All API test URLs use the /api/v1 prefix since routes are mounted there.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# POST /api/v1/reconciliation/schema-drift
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_schema_drift_single_model(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schema drift check for a single model returns its result."""
    expected_result = {
        "model_name": "staging.orders",
        "status": "checked",
        "drift_type": "NONE",
        "drifts": [],
        "check_id": 1,
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.check_schema_drift = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/schema-drift",
            json={"model_name": "staging.orders"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["model_name"] == "staging.orders"
    assert body["status"] == "checked"
    instance.check_schema_drift.assert_awaited_once_with("staging.orders")


@pytest.mark.asyncio
async def test_check_schema_drift_all_models(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schema drift check without model_name checks all models."""
    expected_result = {
        "models_requested": 3,
        "models_checked": 3,
        "drifts_found": 1,
        "results": [],
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.check_all_schemas = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/schema-drift",
            json={},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["models_requested"] == 3
    assert body["drifts_found"] == 1


@pytest.mark.asyncio
async def test_check_schema_drift_null_model_name(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Explicit null model_name triggers check_all_schemas."""
    expected_result = {
        "models_requested": 0,
        "models_checked": 0,
        "drifts_found": 0,
        "results": [],
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.check_all_schemas = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/reconciliation/schema-drift",
            json={"model_name": None},
        )

    assert resp.status_code == 200
    instance.check_all_schemas.assert_awaited_once()


# ---------------------------------------------------------------------------
# GET /api/v1/reconciliation/schema-drifts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_schema_drifts_returns_list(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schema drifts endpoint returns unresolved drift records."""
    expected_drifts = [
        {
            "id": 1,
            "model_name": "staging.orders",
            "drift_type": "COLUMN_REMOVED",
            "drift_details": {
                "drifts": [
                    {
                        "column_name": "old_col",
                        "expected": "old_col: STRING",
                        "actual": "(missing)",
                        "message": "removed",
                    }
                ]
            },
            "expected_columns": None,
            "actual_columns": None,
            "resolved": False,
            "checked_at": "2026-02-21T12:00:00+00:00",
        },
        {
            "id": 2,
            "model_name": "marts.revenue",
            "drift_type": "TYPE_CHANGED",
            "drift_details": {
                "drifts": [
                    {"column_name": "amount", "expected": "FLOAT", "actual": "DOUBLE", "message": "type changed"}
                ]
            },
            "expected_columns": None,
            "actual_columns": None,
            "resolved": False,
            "checked_at": "2026-02-21T11:00:00+00:00",
        },
    ]

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_schema_drifts = AsyncMock(return_value=expected_drifts)

        resp = await client.get("/api/v1/reconciliation/schema-drifts")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["drift_type"] == "COLUMN_REMOVED"
    assert body[1]["model_name"] == "marts.revenue"


@pytest.mark.asyncio
async def test_get_schema_drifts_empty(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schema drifts endpoint returns empty list when no drifts exist."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_schema_drifts = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/reconciliation/schema-drifts")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_get_schema_drifts_with_limit(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schema drifts endpoint forwards the limit parameter."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.get_schema_drifts = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/reconciliation/schema-drifts?limit=5")

    assert resp.status_code == 200
    instance.get_schema_drifts.assert_awaited_once_with(limit=5)


# ---------------------------------------------------------------------------
# PUT /api/v1/reconciliation/schema-drifts/{check_id}/resolve
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_schema_drift_success(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Resolving a schema drift returns the updated record."""
    expected_result = {
        "id": 1,
        "model_name": "staging.orders",
        "drift_type": "COLUMN_REMOVED",
        "resolved": True,
        "resolved_by": "engineer@company.com",
        "resolved_at": "2026-02-21T15:00:00+00:00",
        "resolution_note": "Column intentionally removed in migration.",
    }

    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.resolve_schema_drift = AsyncMock(return_value=expected_result)

        resp = await client.put(
            "/api/v1/reconciliation/schema-drifts/1/resolve",
            json={
                "resolved_by": "engineer@company.com",
                "resolution_note": "Column intentionally removed in migration.",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["resolved"] is True
    assert body["resolved_by"] == "engineer@company.com"


@pytest.mark.asyncio
async def test_resolve_schema_drift_not_found(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Resolving a non-existent drift check returns 404."""
    with patch("api.routers.reconciliation.ReconciliationService") as MockService:
        instance = MockService.return_value
        instance.resolve_schema_drift = AsyncMock(return_value=None)

        resp = await client.put(
            "/api/v1/reconciliation/schema-drifts/99999/resolve",
            json={
                "resolved_by": "admin@company.com",
                "resolution_note": "Attempting to resolve non-existent drift.",
            },
        )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_resolve_schema_drift_missing_fields(
    client: AsyncClient,
) -> None:
    """Missing required fields in resolve body returns 422."""
    resp = await client.put(
        "/api/v1/reconciliation/schema-drifts/1/resolve",
        json={},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_resolve_schema_drift_empty_strings(
    client: AsyncClient,
) -> None:
    """Empty strings for required fields returns 422."""
    resp = await client.put(
        "/api/v1/reconciliation/schema-drifts/1/resolve",
        json={"resolved_by": "", "resolution_note": ""},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/reconciliation/schedule
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_schedules(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schedule endpoint returns enabled reconciliation schedules."""
    with patch("api.routers.reconciliation.ReconciliationScheduleRepository") as MockRepo:
        mock_schedule = MagicMock()
        mock_schedule.id = 1
        mock_schedule.schedule_type = "run_reconciliation"
        mock_schedule.cron_expression = "0 * * * *"
        mock_schedule.enabled = True
        mock_schedule.last_run_at = datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)
        mock_schedule.next_run_at = datetime(2026, 2, 21, 13, 0, 0, tzinfo=timezone.utc)

        instance = MockRepo.return_value
        instance.get_all_enabled = AsyncMock(return_value=[mock_schedule])

        resp = await client.get("/api/v1/reconciliation/schedule")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["schedule_type"] == "run_reconciliation"
    assert body[0]["cron_expression"] == "0 * * * *"
    assert body[0]["enabled"] is True


@pytest.mark.asyncio
async def test_get_schedules_empty(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Schedule endpoint returns empty list when no schedules exist."""
    with patch("api.routers.reconciliation.ReconciliationScheduleRepository") as MockRepo:
        instance = MockRepo.return_value
        instance.get_all_enabled = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/reconciliation/schedule")

    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# PUT /api/v1/reconciliation/schedule
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_schedule_success(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Creating/updating a schedule returns the schedule details."""
    with patch("api.routers.reconciliation.ReconciliationScheduleRepository") as MockRepo:
        mock_schedule = MagicMock()
        mock_schedule.id = 1
        mock_schedule.schedule_type = "schema_drift"
        mock_schedule.cron_expression = "0 0 * * *"
        mock_schedule.enabled = True
        mock_schedule.last_run_at = None
        mock_schedule.next_run_at = None

        instance = MockRepo.return_value
        instance.upsert_schedule = AsyncMock(return_value=mock_schedule)
        instance.update_last_run = AsyncMock(return_value=True)

        resp = await client.put(
            "/api/v1/reconciliation/schedule",
            json={
                "schedule_type": "schema_drift",
                "cron_expression": "0 0 * * *",
                "enabled": True,
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["schedule_type"] == "schema_drift"
    assert body["cron_expression"] == "0 0 * * *"
    assert body["enabled"] is True
    assert body["next_run_at"] is not None  # Computed from cron expression


@pytest.mark.asyncio
async def test_upsert_schedule_invalid_cron(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Invalid cron expression returns 400."""
    resp = await client.put(
        "/api/v1/reconciliation/schedule",
        json={
            "schedule_type": "schema_drift",
            "cron_expression": "invalid cron",
            "enabled": True,
        },
    )

    assert resp.status_code == 400
    assert "cron" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_upsert_schedule_missing_fields(
    client: AsyncClient,
) -> None:
    """Missing required fields returns 422."""
    resp = await client.put(
        "/api/v1/reconciliation/schedule",
        json={},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# ReconciliationService schema drift unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_check_schema_drift_model_not_found(
    mock_session: AsyncMock,
) -> None:
    """check_schema_drift returns model_not_found for unknown model."""
    from api.services.reconciliation_service import ReconciliationService

    with patch.object(ReconciliationService, "__init__", lambda self, *a, **k: None):
        service = ReconciliationService.__new__(ReconciliationService)
        service._session = mock_session
        service._tenant_id = "default"
        service._drift_repo = AsyncMock()
        service._model_repo = AsyncMock()
        service._model_repo.get = AsyncMock(return_value=None)

        result = await service.check_schema_drift("nonexistent.model")

    assert result["status"] == "model_not_found"
    assert result["drifts"] == []


@pytest.mark.asyncio
async def test_service_check_schema_drift_no_actual_schema(
    mock_session: AsyncMock,
) -> None:
    """check_schema_drift without actual_schema returns no_actual_schema status."""
    from api.services.reconciliation_service import ReconciliationService

    mock_model = MagicMock()
    mock_model.model_name = "staging.orders"

    with patch.object(ReconciliationService, "__init__", lambda self, *a, **k: None):
        service = ReconciliationService.__new__(ReconciliationService)
        service._session = mock_session
        service._tenant_id = "default"
        service._drift_repo = AsyncMock()
        service._model_repo = AsyncMock()
        service._model_repo.get = AsyncMock(return_value=mock_model)

        result = await service.check_schema_drift("staging.orders")

    assert result["status"] == "no_actual_schema"


@pytest.mark.asyncio
async def test_service_get_schema_drifts(
    mock_session: AsyncMock,
) -> None:
    """get_schema_drifts returns serialised drift records."""
    from api.services.reconciliation_service import ReconciliationService

    mock_drift = MagicMock()
    mock_drift.id = 1
    mock_drift.model_name = "staging.orders"
    mock_drift.drift_type = "COLUMN_REMOVED"
    mock_drift.drift_details_json = {"drifts": []}
    mock_drift.expected_columns_json = None
    mock_drift.actual_columns_json = None
    mock_drift.resolved = False
    mock_drift.checked_at = datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)

    with patch.object(ReconciliationService, "__init__", lambda self, *a, **k: None):
        service = ReconciliationService.__new__(ReconciliationService)
        service._session = mock_session
        service._tenant_id = "default"
        service._drift_repo = AsyncMock()
        service._drift_repo.get_unresolved = AsyncMock(return_value=[mock_drift])

        results = await service.get_schema_drifts(limit=50)

    assert len(results) == 1
    assert results[0]["model_name"] == "staging.orders"
    assert results[0]["drift_type"] == "COLUMN_REMOVED"
    assert results[0]["resolved"] is False


@pytest.mark.asyncio
async def test_service_resolve_schema_drift_success(
    mock_session: AsyncMock,
) -> None:
    """resolve_schema_drift returns the updated record."""
    from api.services.reconciliation_service import ReconciliationService

    mock_resolved = MagicMock()
    mock_resolved.id = 1
    mock_resolved.model_name = "staging.orders"
    mock_resolved.drift_type = "COLUMN_REMOVED"
    mock_resolved.resolved = True
    mock_resolved.resolved_by = "admin@co.com"
    mock_resolved.resolved_at = datetime(2026, 2, 21, 15, 0, 0, tzinfo=timezone.utc)
    mock_resolved.resolution_note = "Fixed."

    with patch.object(ReconciliationService, "__init__", lambda self, *a, **k: None):
        service = ReconciliationService.__new__(ReconciliationService)
        service._session = mock_session
        service._tenant_id = "default"
        service._drift_repo = AsyncMock()
        service._drift_repo.resolve = AsyncMock(return_value=mock_resolved)

        result = await service.resolve_schema_drift(1, "admin@co.com", "Fixed.")

    assert result is not None
    assert result["resolved"] is True
    assert result["resolved_by"] == "admin@co.com"


@pytest.mark.asyncio
async def test_service_resolve_schema_drift_not_found(
    mock_session: AsyncMock,
) -> None:
    """resolve_schema_drift returns None when check not found."""
    from api.services.reconciliation_service import ReconciliationService

    with patch.object(ReconciliationService, "__init__", lambda self, *a, **k: None):
        service = ReconciliationService.__new__(ReconciliationService)
        service._session = mock_session
        service._tenant_id = "default"
        service._drift_repo = AsyncMock()
        service._drift_repo.resolve = AsyncMock(return_value=None)

        result = await service.resolve_schema_drift(99999, "admin@co.com", "note")

    assert result is None


# ---------------------------------------------------------------------------
# ORM table tests
# ---------------------------------------------------------------------------


class TestSchemaDriftCheckTable:
    """Verify SchemaDriftCheckTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import SchemaDriftCheckTable

        assert SchemaDriftCheckTable.__tablename__ == "schema_drift_checks"

    def test_primary_key(self) -> None:
        from core_engine.state.tables import SchemaDriftCheckTable

        pk_cols = [c.name for c in SchemaDriftCheckTable.__table__.columns if c.primary_key]
        assert pk_cols == ["id"]

    def test_required_columns_exist(self) -> None:
        from core_engine.state.tables import SchemaDriftCheckTable

        col_names = [c.name for c in SchemaDriftCheckTable.__table__.columns]
        for expected_col in [
            "tenant_id",
            "model_name",
            "expected_columns_json",
            "actual_columns_json",
            "drift_type",
            "drift_details_json",
            "resolved",
            "resolved_by",
            "resolved_at",
            "resolution_note",
            "checked_at",
        ]:
            assert expected_col in col_names, f"Missing column: {expected_col}"

    def test_indexes_exist(self) -> None:
        from core_engine.state.tables import SchemaDriftCheckTable

        index_names = [idx.name for idx in SchemaDriftCheckTable.__table__.indexes]
        assert "ix_schema_drift_tenant_model" in index_names
        assert "ix_schema_drift_tenant_unresolved" in index_names
        assert "ix_schema_drift_checked_at" in index_names


class TestReconciliationScheduleTable:
    """Verify ReconciliationScheduleTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import ReconciliationScheduleTable

        assert ReconciliationScheduleTable.__tablename__ == "reconciliation_schedules"

    def test_primary_key(self) -> None:
        from core_engine.state.tables import ReconciliationScheduleTable

        pk_cols = [c.name for c in ReconciliationScheduleTable.__table__.columns if c.primary_key]
        assert pk_cols == ["id"]

    def test_required_columns_exist(self) -> None:
        from core_engine.state.tables import ReconciliationScheduleTable

        col_names = [c.name for c in ReconciliationScheduleTable.__table__.columns]
        for expected_col in [
            "tenant_id",
            "schedule_type",
            "cron_expression",
            "enabled",
            "last_run_at",
            "next_run_at",
            "created_at",
            "updated_at",
        ]:
            assert expected_col in col_names, f"Missing column: {expected_col}"

    def test_unique_index_exists(self) -> None:
        from core_engine.state.tables import ReconciliationScheduleTable

        index_names = [idx.name for idx in ReconciliationScheduleTable.__table__.indexes]
        assert "ix_recon_schedule_tenant_type" in index_names
