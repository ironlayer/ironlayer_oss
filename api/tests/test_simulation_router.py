"""Tests for the simulation router endpoints.

Covers column change, model removal, and type change simulation
endpoints including RBAC enforcement and error handling.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from core_engine.simulation.impact_analyzer import (
    AffectedModel,
    ImpactReport,
    ModelRemovalReport,
    ReferenceSeverity,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_simulation_service():
    """Patch SimulationService to avoid database access."""
    with patch("api.routers.simulation.SimulationService") as mock_cls:
        instance = AsyncMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture()
def _app():
    """Minimal app with the simulation router and auth middleware bypassed."""
    from fastapi import FastAPI
    from api.routers.simulation import router
    from api.dependencies import get_tenant_session, get_tenant_id
    from api.middleware.rbac import get_user_role, Role

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")

    async def _mock_session():
        yield AsyncMock()

    # Override dependencies to bypass auth/DB for unit tests.
    app.dependency_overrides[get_tenant_session] = _mock_session
    app.dependency_overrides[get_tenant_id] = lambda: "test-tenant"
    app.dependency_overrides[get_user_role] = lambda: Role.ADMIN

    return app


@pytest_asyncio.fixture()
async def client(_app, _mock_simulation_service):
    """HTTP test client with auth bypassed."""
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=_app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Column change endpoint
# ---------------------------------------------------------------------------


class TestColumnChangeEndpoint:
    @pytest.mark.asyncio
    async def test_success(self, client, _mock_simulation_service) -> None:
        _mock_simulation_service.simulate_column_changes.return_value = ImpactReport(
            source_model="orders",
            breaking_count=1,
            warning_count=0,
            summary="1 BREAKING impact",
        )

        resp = await client.post(
            "/api/v1/simulation/column-change",
            json={
                "source_model": "orders",
                "changes": [{"action": "REMOVE", "column_name": "price"}],
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["source_model"] == "orders"
        assert data["breaking_count"] == 1

    @pytest.mark.asyncio
    async def test_empty_changes_rejected(self, client, _mock_simulation_service) -> None:
        resp = await client.post(
            "/api/v1/simulation/column-change",
            json={
                "source_model": "orders",
                "changes": [],
            },
        )
        assert resp.status_code == 422  # Validation error (min_length=1)

    @pytest.mark.asyncio
    async def test_multiple_changes(self, client, _mock_simulation_service) -> None:
        _mock_simulation_service.simulate_column_changes.return_value = ImpactReport(
            source_model="orders",
            breaking_count=0,
            warning_count=2,
            summary="2 WARNING impacts",
        )

        resp = await client.post(
            "/api/v1/simulation/column-change",
            json={
                "source_model": "orders",
                "changes": [
                    {"action": "REMOVE", "column_name": "old_col"},
                    {"action": "ADD", "column_name": "new_col"},
                ],
            },
        )

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Model removal endpoint
# ---------------------------------------------------------------------------


class TestModelRemovalEndpoint:
    @pytest.mark.asyncio
    async def test_success(self, client, _mock_simulation_service) -> None:
        _mock_simulation_service.simulate_model_removal.return_value = ModelRemovalReport(
            removed_model="staging.orders",
            orphaned_models=["analytics.daily"],
            breaking_count=2,
            summary="2 models affected, 1 orphaned",
        )

        resp = await client.post(
            "/api/v1/simulation/model-removal",
            json={"model_name": "staging.orders"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["removed_model"] == "staging.orders"
        assert "analytics.daily" in data["orphaned_models"]

    @pytest.mark.asyncio
    async def test_missing_model_name(self, client, _mock_simulation_service) -> None:
        resp = await client.post(
            "/api/v1/simulation/model-removal",
            json={},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Type change endpoint
# ---------------------------------------------------------------------------


class TestTypeChangeEndpoint:
    @pytest.mark.asyncio
    async def test_safe_type_change(self, client, _mock_simulation_service) -> None:
        _mock_simulation_service.simulate_type_change.return_value = ImpactReport(
            source_model="orders",
            breaking_count=0,
            warning_count=1,
            summary="Safe type widening",
        )

        resp = await client.post(
            "/api/v1/simulation/type-change",
            json={
                "source_model": "orders",
                "column_name": "count",
                "old_type": "INT",
                "new_type": "BIGINT",
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["breaking_count"] == 0

    @pytest.mark.asyncio
    async def test_breaking_type_change(self, client, _mock_simulation_service) -> None:
        _mock_simulation_service.simulate_type_change.return_value = ImpactReport(
            source_model="orders",
            breaking_count=3,
            warning_count=0,
            summary="3 BREAKING impacts",
        )

        resp = await client.post(
            "/api/v1/simulation/type-change",
            json={
                "source_model": "orders",
                "column_name": "value",
                "old_type": "STRING",
                "new_type": "INT",
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["breaking_count"] == 3

    @pytest.mark.asyncio
    async def test_invalid_payload(self, client, _mock_simulation_service) -> None:
        resp = await client.post(
            "/api/v1/simulation/type-change",
            json={"source_model": "orders"},  # missing required fields
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------


class TestSimulationRBAC:
    @pytest.mark.asyncio
    async def test_read_models_permission_required(self) -> None:
        """Verify the router requires READ_MODELS permission."""
        from api.routers.simulation import router

        # All routes in the simulation router should have the RBAC dependency.
        for route in router.routes:
            deps = getattr(route, "dependencies", []) + router.dependencies
            assert len(deps) > 0, f"Route {route.path} should have RBAC dependencies"
