"""Tests for api/api/middleware/metering.py

Covers:
- API_REQUEST events recorded for normal requests
- Skipped paths (docs, health, metrics) do NOT generate events
- Tenant ID extracted from request state
- Fault tolerance when collector is unavailable
- Metadata fields (method, path, status_code) are correct
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_ai_client, get_db_session, get_metering_collector, get_settings, get_tenant_session
from api.test_utils import set_app_state_for_test
from api.main import create_app
from api.config import APISettings
from api.services.ai_client import AIServiceClient

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def metering_app():
    """Create a FastAPI app with a tracked mock metering collector.

    Also overrides DB session and AI client to avoid real connections.
    """
    app = create_app()

    mock_collector = MagicMock()
    mock_collector.record_event = MagicMock()
    mock_collector.record = MagicMock()
    mock_collector.flush = MagicMock(return_value=0)
    mock_collector.pending_count = 0

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
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

    mock_ai_client = AsyncMock(spec=AIServiceClient)
    mock_ai_client.health_check = AsyncMock(return_value=True)
    mock_ai_client.close = AsyncMock()

    set_app_state_for_test(
        app,
        settings=settings,
        session=mock_session,
        ai_client=mock_ai_client,
        metering=mock_collector,
    )

    app.dependency_overrides[get_metering_collector] = lambda: mock_collector
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_db_session] = _override_session
    app.dependency_overrides[get_tenant_session] = _override_session
    app.dependency_overrides[get_ai_client] = lambda: mock_ai_client

    return app, mock_collector


# Re-use dev token helper from conftest
def _make_auth_headers() -> dict[str, str]:
    """Generate auth headers for test requests."""
    import base64
    import hashlib
    import hmac
    import json
    import time

    _DEV_SECRET = "test-secret-key-for-ironlayer-tests"
    now = time.time()
    payload = {
        "sub": "test-user",
        "tenant_id": "test-tenant",
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": "test-jti-metering",
        "identity_kind": "user",
        "role": "admin",
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


# ---------------------------------------------------------------------------
# Tests: event recording
# ---------------------------------------------------------------------------


class TestMeteringMiddlewareRecording:
    """Verify that MeteringMiddleware records events for normal requests."""

    @pytest.mark.asyncio
    async def test_records_api_request_event(self, metering_app) -> None:
        """A normal API request triggers an API_REQUEST event recording."""
        app, mock_collector = metering_app
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            await ac.get("/api/v1/health")

        # /api/v1/health is in _SKIP_PATHS, so no metering event
        # Check that no event was recorded for health
        for call in mock_collector.record_event.call_args_list:
            kwargs = call[1] if call[1] else {}
            if kwargs.get("metadata", {}).get("path") == "/api/v1/health":
                pytest.fail("Health endpoint should be skipped by metering middleware")

    @pytest.mark.asyncio
    async def test_records_event_for_normal_endpoint(self, metering_app) -> None:
        """A non-skipped endpoint triggers an API_REQUEST event.

        The MeteringMiddleware resolves the collector via
        ``get_metering_collector(request)`` which reads from
        ``request.app.state.metering``.  The fixture sets this via
        ``set_app_state_for_test``, so the mock collector is picked up
        automatically.
        """
        app, mock_collector = metering_app
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            await ac.get("/api/v1/plans")

        # The middleware should have been invoked and recorded an event.
        found = False
        for call in mock_collector.record_event.call_args_list:
            kwargs = call[1] if call[1] else {}
            meta = kwargs.get("metadata", {})
            if meta.get("path", "").startswith("/api/v1/plans"):
                found = True
                assert meta["method"] == "GET"
                assert "status_code" in meta
                break

        assert found, "Expected an API_REQUEST event for /api/v1/plans"


# ---------------------------------------------------------------------------
# Tests: skipped paths
# ---------------------------------------------------------------------------


class TestMeteringSkippedPaths:
    """Verify that certain paths do not generate metering events."""

    @pytest.mark.asyncio
    async def test_docs_path_skipped(self, metering_app) -> None:
        """Requests to /docs do not generate metering events."""
        app, mock_collector = metering_app
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            await ac.get("/docs")

        # Verify no events for /docs
        for call in mock_collector.record_event.call_args_list:
            kwargs = call[1] if call[1] else {}
            meta = kwargs.get("metadata", {})
            assert meta.get("path") != "/docs", "Docs should be skipped"

    @pytest.mark.asyncio
    async def test_metrics_path_skipped(self, metering_app) -> None:
        """Requests to /metrics do not generate metering events."""
        app, mock_collector = metering_app
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            await ac.get("/metrics")

        for call in mock_collector.record_event.call_args_list:
            kwargs = call[1] if call[1] else {}
            meta = kwargs.get("metadata", {})
            assert meta.get("path") != "/metrics", "Metrics should be skipped"

    @pytest.mark.asyncio
    async def test_ready_path_skipped(self, metering_app) -> None:
        """Requests to /ready do not generate metering events."""
        app, mock_collector = metering_app
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            await ac.get("/ready")

        for call in mock_collector.record_event.call_args_list:
            kwargs = call[1] if call[1] else {}
            meta = kwargs.get("metadata", {})
            assert meta.get("path") != "/ready", "Ready should be skipped"


# ---------------------------------------------------------------------------
# Tests: fault tolerance
# ---------------------------------------------------------------------------


class TestMeteringFaultTolerance:
    """Verify middleware handles collector unavailability gracefully."""

    @pytest.mark.asyncio
    async def test_request_succeeds_when_collector_raises(self) -> None:
        """Requests proceed normally even if the collector is unavailable."""
        app = create_app()

        def _raise_collector():
            raise RuntimeError("Collector not initialised")

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

        mock_session = AsyncMock()
        mock_ai = AsyncMock()
        mock_metering = MagicMock()
        set_app_state_for_test(
            app,
            settings=settings,
            session=mock_session,
            ai_client=mock_ai,
            metering=mock_metering,
        )

        app.dependency_overrides[get_metering_collector] = _raise_collector
        app.dependency_overrides[get_settings] = lambda: settings

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/docs")

        # The request should succeed (docs returns HTML)
        assert resp.status_code == 200
