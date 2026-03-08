"""Tests for api/api/routers/environments.py

Covers:
- POST /api/v1/environments -- create standard environment
- POST /api/v1/environments/ephemeral -- create ephemeral PR environment
- GET /api/v1/environments -- list all environments
- GET /api/v1/environments/{name} -- get environment details
- DELETE /api/v1/environments/{name} -- soft-delete environment
- POST /api/v1/environments/{name}/promote -- promote snapshot
- GET /api/v1/environments/promotions -- promotion history
- POST /api/v1/environments/cleanup -- cleanup expired
- RBAC permission enforcement
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_db_session, get_metering_collector, get_settings, get_tenant_session
from api.config import APISettings
from api.main import create_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_auth_headers(role: str = "admin") -> dict[str, str]:
    """Generate valid dev-mode auth headers with a specific role."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": "test-user",
        "tenant_id": "default",
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": "test-jti-env-router",
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
    token = f"bmdev.{token_bytes}.{signature}"
    return {"Authorization": f"Bearer {token}"}


def _create_test_app() -> tuple[Any, AsyncMock]:
    """Create a FastAPI app with mock deps for testing."""
    app = create_app()

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)

    async def _override_session():
        yield mock_session

    settings = APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
    )

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    app.dependency_overrides[get_db_session] = _override_session
    app.dependency_overrides[get_tenant_session] = _override_session
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return app, mock_session


# ---------------------------------------------------------------------------
# POST /api/v1/environments
# ---------------------------------------------------------------------------


class TestCreateEnvironment:
    """Verify POST /api/v1/environments responses."""

    @pytest.mark.asyncio
    async def test_create_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.create_environment = AsyncMock(
                return_value={
                    "id": 1,
                    "name": "staging",
                    "catalog": "dev",
                    "schema_prefix": "stg",
                    "is_default": False,
                    "is_production": False,
                    "is_ephemeral": False,
                    "pr_number": None,
                    "branch_name": None,
                    "expires_at": None,
                    "created_by": "admin",
                    "deleted_at": None,
                    "created_at": "2024-06-01T00:00:00+00:00",
                    "updated_at": "2024-06-01T00:00:00+00:00",
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/environments",
                    json={
                        "name": "staging",
                        "catalog": "dev",
                        "schema_prefix": "stg",
                        "created_by": "admin",
                    },
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "staging"

    @pytest.mark.asyncio
    async def test_create_conflict_returns_409(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.create_environment = AsyncMock(side_effect=ValueError("Environment 'staging' already exists"))

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/environments",
                    json={
                        "name": "staging",
                        "catalog": "dev",
                        "schema_prefix": "stg",
                        "created_by": "admin",
                    },
                )

        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_create_requires_admin(self) -> None:
        """ENGINEER role should not have MANAGE_ENVIRONMENTS permission."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/environments",
                json={
                    "name": "staging",
                    "catalog": "dev",
                    "schema_prefix": "stg",
                    "created_by": "viewer",
                },
            )

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /api/v1/environments/ephemeral
# ---------------------------------------------------------------------------


class TestCreateEphemeral:
    """Verify POST /api/v1/environments/ephemeral responses."""

    @pytest.mark.asyncio
    async def test_create_ephemeral_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("engineer")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.create_ephemeral_environment = AsyncMock(
                return_value={
                    "id": 10,
                    "name": "pr-42",
                    "catalog": "dev",
                    "schema_prefix": "pr_42",
                    "is_default": False,
                    "is_production": False,
                    "is_ephemeral": True,
                    "pr_number": 42,
                    "branch_name": "feature/new",
                    "expires_at": "2024-06-04T00:00:00+00:00",
                    "created_by": "ci-bot",
                    "deleted_at": None,
                    "created_at": "2024-06-01T00:00:00+00:00",
                    "updated_at": "2024-06-01T00:00:00+00:00",
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/environments/ephemeral",
                    json={
                        "pr_number": 42,
                        "branch_name": "feature/new",
                        "catalog": "dev",
                        "schema_prefix": "pr_42",
                        "created_by": "ci-bot",
                        "ttl_hours": 72,
                    },
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["is_ephemeral"] is True
        assert body["pr_number"] == 42

    @pytest.mark.asyncio
    async def test_create_ephemeral_viewer_forbidden(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/environments/ephemeral",
                json={
                    "pr_number": 42,
                    "branch_name": "feature/new",
                    "catalog": "dev",
                    "schema_prefix": "pr_42",
                    "created_by": "ci-bot",
                },
            )

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/v1/environments
# ---------------------------------------------------------------------------


class TestListEnvironments:
    """Verify GET /api/v1/environments responses."""

    @pytest.mark.asyncio
    async def test_list_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.list_environments = AsyncMock(
                return_value=[
                    {"id": 1, "name": "production"},
                    {"id": 2, "name": "staging"},
                ]
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/environments")

        assert resp.status_code == 200
        assert len(resp.json()) == 2

    @pytest.mark.asyncio
    async def test_list_requires_auth(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/environments")

        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/v1/environments/{name}
# ---------------------------------------------------------------------------


class TestGetEnvironment:
    """Verify GET /api/v1/environments/{name} responses."""

    @pytest.mark.asyncio
    async def test_get_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.get_environment = AsyncMock(
                return_value={
                    "id": 1,
                    "name": "staging",
                    "catalog": "dev",
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/environments/staging")

        assert resp.status_code == 200
        assert resp.json()["name"] == "staging"

    @pytest.mark.asyncio
    async def test_get_not_found(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.get_environment = AsyncMock(return_value=None)

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/environments/nonexistent")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/v1/environments/{name}
# ---------------------------------------------------------------------------


class TestDeleteEnvironment:
    """Verify DELETE /api/v1/environments/{name} responses."""

    @pytest.mark.asyncio
    async def test_delete_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.delete_environment = AsyncMock(return_value=True)

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.delete("/api/v1/environments/staging")

        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    @pytest.mark.asyncio
    async def test_delete_not_found(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.delete_environment = AsyncMock(return_value=False)

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.delete("/api/v1/environments/nonexistent")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_requires_manage_permission(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.delete("/api/v1/environments/staging")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /api/v1/environments/{name}/promote
# ---------------------------------------------------------------------------


class TestPromote:
    """Verify POST /api/v1/environments/{name}/promote responses."""

    @pytest.mark.asyncio
    async def test_promote_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.promote = AsyncMock(
                return_value={
                    "id": 1,
                    "source_environment": "staging",
                    "target_environment": "production",
                    "source_snapshot_id": "snap-abc",
                    "target_snapshot_id": "snap-abc",
                    "promoted_by": "deploy-bot",
                    "promoted_at": "2024-06-15T00:00:00+00:00",
                    "metadata": None,
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/environments/staging/promote",
                    json={
                        "target_environment": "production",
                        "snapshot_id": "snap-abc",
                        "promoted_by": "deploy-bot",
                    },
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["source_environment"] == "staging"
        assert body["target_environment"] == "production"

    @pytest.mark.asyncio
    async def test_promote_missing_env_returns_404(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.promote = AsyncMock(side_effect=ValueError("Source environment 'staging' not found"))

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/environments/staging/promote",
                    json={
                        "target_environment": "production",
                        "snapshot_id": "snap-abc",
                        "promoted_by": "deploy-bot",
                    },
                )

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_promote_requires_permission(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/environments/staging/promote",
                json={
                    "target_environment": "production",
                    "snapshot_id": "snap-abc",
                    "promoted_by": "deploy-bot",
                },
            )

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/v1/environments/promotions
# ---------------------------------------------------------------------------


class TestPromotionHistory:
    """Verify GET /api/v1/environments/promotions responses."""

    @pytest.mark.asyncio
    async def test_promotions_list(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.get_promotion_history = AsyncMock(
                return_value=[
                    {
                        "id": 1,
                        "source_environment": "staging",
                        "target_environment": "production",
                        "promoted_by": "admin",
                        "promoted_at": "2024-06-15T00:00:00+00:00",
                    }
                ]
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/environments/promotions")

        assert resp.status_code == 200
        assert len(resp.json()) == 1


# ---------------------------------------------------------------------------
# POST /api/v1/environments/cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    """Verify POST /api/v1/environments/cleanup responses."""

    @pytest.mark.asyncio
    async def test_cleanup_success(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        with patch("api.routers.environments.EnvironmentService") as MockService:
            instance = MockService.return_value
            instance.cleanup_expired = AsyncMock(return_value={"deleted_count": 2})

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post("/api/v1/environments/cleanup")

        assert resp.status_code == 200
        assert resp.json()["deleted_count"] == 2

    @pytest.mark.asyncio
    async def test_cleanup_requires_admin(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/environments/cleanup")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestRequestValidation:
    """Verify request body validation."""

    @pytest.mark.asyncio
    async def test_create_missing_fields_returns_422(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/environments", json={})

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_ephemeral_missing_pr_returns_422(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/environments/ephemeral",
                json={
                    "branch_name": "feature/x",
                },
            )

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_promote_missing_fields_returns_422(self) -> None:
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)
        headers = _make_auth_headers("admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/environments/staging/promote", json={})

        assert resp.status_code == 422
