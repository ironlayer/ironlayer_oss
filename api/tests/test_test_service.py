"""Tests for the model testing API endpoints and service.

Covers:
- POST /api/v1/tests/run — run tests for a model or plan
- GET /api/v1/tests/results/{plan_id} — get test results for a plan
- GET /api/v1/tests/history/{model_name} — get test history for a model
- TestService unit tests: run_tests_for_model, run_tests_for_plan,
  get_test_history, get_plan_test_results, sync_tests_from_definitions
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

# ---------------------------------------------------------------------------
# POST /api/v1/tests/run — model
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tests_for_model(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Running tests for a model returns a summary."""
    expected_result = {
        "model_name": "staging.orders",
        "total": 3,
        "passed": 2,
        "failed": 1,
        "blocked": 1,
        "results": [
            {
                "test_id": "abc123",
                "model_name": "staging.orders",
                "test_type": "NOT_NULL",
                "passed": True,
                "failure_message": None,
                "duration_ms": 5,
            },
            {
                "test_id": "def456",
                "model_name": "staging.orders",
                "test_type": "UNIQUE",
                "passed": True,
                "failure_message": None,
                "duration_ms": 3,
            },
            {
                "test_id": "ghi789",
                "model_name": "staging.orders",
                "test_type": "ROW_COUNT_MIN",
                "passed": False,
                "failure_message": "Test ROW_COUNT_MIN failed: 1 row(s) violating assertion",
                "duration_ms": 2,
            },
        ],
    }

    with patch("api.routers.tests.TestService") as MockService:
        instance = MockService.return_value
        instance.run_tests_for_model = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/tests/run",
            json={"model_name": "staging.orders"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["model_name"] == "staging.orders"
    assert body["total"] == 3
    assert body["passed"] == 2
    assert body["failed"] == 1
    assert body["blocked"] == 1
    assert len(body["results"]) == 3


# ---------------------------------------------------------------------------
# POST /api/v1/tests/run — plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tests_for_plan(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Running tests for a plan returns aggregate results."""
    expected_result = {
        "plan_id": "plan-001",
        "total": 5,
        "passed": 4,
        "failed": 1,
        "blocked": 0,
        "models": [],
    }

    with patch("api.routers.tests.TestService") as MockService:
        instance = MockService.return_value
        instance.run_tests_for_plan = AsyncMock(return_value=expected_result)

        resp = await client.post(
            "/api/v1/tests/run",
            json={"plan_id": "plan-001"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["plan_id"] == "plan-001"
    assert body["total"] == 5


# ---------------------------------------------------------------------------
# POST /api/v1/tests/run — validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tests_both_params_returns_400(
    client: AsyncClient,
) -> None:
    """Providing both model_name and plan_id returns 400."""
    resp = await client.post(
        "/api/v1/tests/run",
        json={"model_name": "staging.orders", "plan_id": "plan-001"},
    )
    assert resp.status_code == 400
    assert "not both" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_run_tests_no_params_returns_400(
    client: AsyncClient,
) -> None:
    """Providing neither model_name nor plan_id returns 400."""
    resp = await client.post(
        "/api/v1/tests/run",
        json={},
    )
    assert resp.status_code == 400
    assert "model_name" in resp.json()["detail"].lower() or "plan_id" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# GET /api/v1/tests/results/{plan_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_plan_test_results(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Plan test results endpoint returns summary and results."""
    expected = {
        "total": 2,
        "passed": 1,
        "failed": 1,
        "blocked": 1,
        "results": [
            {
                "test_id": "abc123",
                "plan_id": "plan-001",
                "model_name": "staging.orders",
                "test_type": "NOT_NULL",
                "passed": True,
                "failure_message": None,
                "execution_mode": "local_duckdb",
                "duration_ms": 5,
                "executed_at": "2026-02-21T12:00:00+00:00",
            },
            {
                "test_id": "def456",
                "plan_id": "plan-001",
                "model_name": "staging.orders",
                "test_type": "UNIQUE",
                "passed": False,
                "failure_message": "Test UNIQUE failed: 1 row(s) violating assertion",
                "execution_mode": "local_duckdb",
                "duration_ms": 3,
                "executed_at": "2026-02-21T12:00:01+00:00",
            },
        ],
    }

    with patch("api.routers.tests.TestService") as MockService:
        instance = MockService.return_value
        instance.get_plan_test_results = AsyncMock(return_value=expected)

        resp = await client.get("/api/v1/tests/results/plan-001")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert body["passed"] == 1
    assert body["failed"] == 1
    assert body["blocked"] == 1
    assert len(body["results"]) == 2


# ---------------------------------------------------------------------------
# GET /api/v1/tests/history/{model_name}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_test_history(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Test history endpoint returns a list of results."""
    expected = [
        {
            "test_id": "abc123",
            "plan_id": "plan-001",
            "model_name": "staging.orders",
            "test_type": "NOT_NULL",
            "passed": True,
            "failure_message": None,
            "execution_mode": "local_duckdb",
            "duration_ms": 5,
            "executed_at": "2026-02-21T12:00:00+00:00",
        },
    ]

    with patch("api.routers.tests.TestService") as MockService:
        instance = MockService.return_value
        instance.get_test_history = AsyncMock(return_value=expected)

        resp = await client.get("/api/v1/tests/history/staging.orders")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["model_name"] == "staging.orders"


@pytest.mark.asyncio
async def test_get_test_history_with_limit(
    client: AsyncClient,
    mock_session: AsyncMock,
) -> None:
    """Test history respects the limit parameter."""
    with patch("api.routers.tests.TestService") as MockService:
        instance = MockService.return_value
        instance.get_test_history = AsyncMock(return_value=[])

        resp = await client.get("/api/v1/tests/history/staging.orders?limit=10")

    assert resp.status_code == 200
    instance.get_test_history.assert_awaited_once_with("staging.orders", limit=10)


@pytest.mark.asyncio
async def test_get_test_history_invalid_limit(
    client: AsyncClient,
) -> None:
    """Invalid limit returns 422."""
    resp = await client.get("/api/v1/tests/history/staging.orders?limit=0")
    assert resp.status_code == 422

    resp = await client.get("/api/v1/tests/history/staging.orders?limit=600")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestService.sync_tests_from_definitions
# ---------------------------------------------------------------------------


class TestSyncFromDefinitions:
    """Verify sync_tests_from_definitions persists test definitions."""

    @pytest.mark.asyncio
    async def test_sync_creates_tests(self, mock_session: AsyncMock) -> None:
        """Syncing models with tests creates DB entries."""
        from core_engine.models.model_definition import (
            ModelDefinition,
            ModelKind,
            ModelTestDefinition,
            ModelTestType,
            TestSeverity,
        )

        model = ModelDefinition(
            name="staging.orders",
            kind=ModelKind.FULL_REFRESH,
            file_path="models/staging/orders.sql",
            raw_sql="-- stub",
            tests=[
                ModelTestDefinition(
                    test_type=ModelTestType.NOT_NULL,
                    column="id",
                    severity=TestSeverity.BLOCK,
                ),
                ModelTestDefinition(
                    test_type=ModelTestType.UNIQUE,
                    column="id",
                    severity=TestSeverity.BLOCK,
                ),
            ],
        )

        with patch("api.services.test_service.ModelTestRepository") as MockTestRepo:
            mock_repo = MockTestRepo.return_value
            mock_repo.delete_for_model = AsyncMock(return_value=0)
            mock_repo.save_test = AsyncMock()

            with patch("api.services.test_service.TestResultRepository"):
                with patch("api.services.test_service.ModelRepository"):
                    from api.services.test_service import TestService

                    service = TestService(mock_session, tenant_id="default")
                    service._test_repo = mock_repo

                    result = await service.sync_tests_from_definitions([model])

        assert result["models_synced"] == 1
        assert result["tests_created"] == 2
        assert result["tests_deleted"] == 0

    @pytest.mark.asyncio
    async def test_sync_no_tests_skips_model(self, mock_session: AsyncMock) -> None:
        """Models without tests are skipped during sync."""
        from core_engine.models.model_definition import (
            ModelDefinition,
            ModelKind,
        )

        model = ModelDefinition(
            name="staging.orders",
            kind=ModelKind.FULL_REFRESH,
            file_path="models/staging/orders.sql",
            raw_sql="-- stub",
            tests=[],
        )

        with patch("api.services.test_service.ModelTestRepository") as MockTestRepo:
            mock_repo = MockTestRepo.return_value

            with patch("api.services.test_service.TestResultRepository"):
                with patch("api.services.test_service.ModelRepository"):
                    from api.services.test_service import TestService

                    service = TestService(mock_session, tenant_id="default")
                    service._test_repo = mock_repo

                    result = await service.sync_tests_from_definitions([model])

        assert result["models_synced"] == 0
        assert result["tests_created"] == 0


# ---------------------------------------------------------------------------
# ORM table verification
# ---------------------------------------------------------------------------


class TestModelTestTable:
    """Verify ModelTestTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import ModelTestTable

        assert ModelTestTable.__tablename__ == "model_tests"

    def test_primary_key(self) -> None:
        from core_engine.state.tables import ModelTestTable

        pk_cols = [c.name for c in ModelTestTable.__table__.columns if c.primary_key]
        assert pk_cols == ["id"]

    def test_required_columns(self) -> None:
        from core_engine.state.tables import ModelTestTable

        col_names = [c.name for c in ModelTestTable.__table__.columns]
        for expected in ["tenant_id", "test_id", "model_name", "test_type", "severity"]:
            assert expected in col_names

    def test_indexes(self) -> None:
        from core_engine.state.tables import ModelTestTable

        index_names = [idx.name for idx in ModelTestTable.__table__.indexes]
        assert "ix_model_test_tenant_model" in index_names
        assert "ix_model_test_tenant_id" in index_names


class TestTestResultTable:
    """Verify TestResultTable ORM definition."""

    def test_table_name(self) -> None:
        from core_engine.state.tables import TestResultTable

        assert TestResultTable.__tablename__ == "test_results"

    def test_required_columns(self) -> None:
        from core_engine.state.tables import TestResultTable

        col_names = [c.name for c in TestResultTable.__table__.columns]
        for expected in [
            "tenant_id",
            "test_id",
            "plan_id",
            "model_name",
            "test_type",
            "passed",
            "failure_message",
            "execution_mode",
            "duration_ms",
            "executed_at",
        ]:
            assert expected in col_names

    def test_indexes(self) -> None:
        from core_engine.state.tables import TestResultTable

        index_names = [idx.name for idx in TestResultTable.__table__.indexes]
        assert "ix_test_result_tenant_plan" in index_names
        assert "ix_test_result_tenant_model" in index_names
        assert "ix_test_result_executed_at" in index_names
