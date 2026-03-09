"""App startup, middleware, and health endpoint tests (BL-015).

Covers:
- /health returns {"status": "ok"} with 200 (no auth needed)
- SharedSecretMiddleware: valid Bearer passes; missing/wrong → 401
- RequestSizeLimitMiddleware: Content-Length > 1 MiB + 1 byte → 413
- CORS: OPTIONS preflight includes Access-Control response headers
- App creation works without LLM credentials (LLM disabled by default)
- create_app() returns a FastAPI instance

The app lifespan is driven manually via the ``_lifespan_client`` helper
so that module-level engine globals are initialised before requests are
made.  This mirrors what Starlette's TestClient does internally.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI

SECRET = "lifecycle-test-secret"
AUTH_HEADER = {"Authorization": f"Bearer {SECRET}"}

# 1 MiB + 1 byte — just over the default RequestSizeLimitMiddleware limit.
_OVER_LIMIT_SIZE = 1_048_576 + 1


# ---------------------------------------------------------------------------
# Lifespan helper (duplicated from test_routers so this module is standalone)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan_client(app, base_url: str = "http://testserver"):
    """Async context manager: drives app lifespan, then yields an AsyncClient."""
    receive_queue: asyncio.Queue = asyncio.Queue()
    send_queue: asyncio.Queue = asyncio.Queue()

    async def _receive():
        return await receive_queue.get()

    async def _send(message):
        await send_queue.put(message)

    lifespan_task = asyncio.create_task(
        app({"type": "lifespan", "asgi": {"version": "3.0"}}, _receive, _send)
    )

    await receive_queue.put({"type": "lifespan.startup"})
    startup_msg = await send_queue.get()
    assert startup_msg["type"] == "lifespan.startup.complete", f"Unexpected: {startup_msg}"

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=base_url,
        ) as http_client:
            yield http_client
    finally:
        await receive_queue.put({"type": "lifespan.shutdown"})
        await send_queue.get()
        lifespan_task.cancel()
        try:
            await lifespan_task
        except (asyncio.CancelledError, Exception):
            pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def app():
    """Create the FastAPI app once for the entire test module."""
    os.environ["AI_ENGINE_SHARED_SECRET"] = SECRET
    from ai_engine.main import create_app  # noqa: PLC0415

    return create_app()


@pytest.fixture(scope="module")
async def client(app):
    """Module-scoped async HTTP client with lifespan driven."""
    async with _lifespan_client(app) as c:
        yield c


# ===========================================================================
# create_app() smoke test
# ===========================================================================


class TestCreateApp:
    def test_create_app_returns_fastapi_instance(self):
        os.environ["AI_ENGINE_SHARED_SECRET"] = SECRET
        from ai_engine.main import create_app  # noqa: PLC0415

        instance = create_app()
        assert isinstance(instance, FastAPI)

    def test_create_app_without_llm_credentials(self):
        """App factory must succeed when no LLM env vars are set.

        The LLMClient is disabled by default; the service runs in
        deterministic-only mode.
        """
        env_keys = [
            "ANTHROPIC_API_KEY",
            "LLM_PROVIDER",
            "LLM_DAILY_BUDGET_USD",
            "LLM_MONTHLY_BUDGET_USD",
        ]
        saved = {k: os.environ.pop(k, None) for k in env_keys}
        os.environ["AI_ENGINE_SHARED_SECRET"] = SECRET
        try:
            from ai_engine.main import create_app  # noqa: PLC0415

            instance = create_app()
            assert isinstance(instance, FastAPI)
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v


# ===========================================================================
# /health endpoint
# ===========================================================================


class TestHealthEndpoint:
    async def test_health_returns_200(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_returns_status_ok(self, client):
        resp = await client.get("/health")
        assert resp.json()["status"] == "ok"

    async def test_health_no_auth_required(self, client):
        """Health check must bypass SharedSecretMiddleware."""
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_returns_version(self, client):
        resp = await client.get("/health")
        assert "version" in resp.json()


# ===========================================================================
# SharedSecretMiddleware
# ===========================================================================


class TestSharedSecretMiddleware:
    """Verify auth enforcement on a non-public endpoint."""

    async def test_valid_bearer_passes(self, client):
        resp = await client.get("/cache/stats", headers=AUTH_HEADER)
        assert resp.status_code == 200

    async def test_missing_auth_header_returns_401(self, client):
        resp = await client.get("/cache/stats")
        assert resp.status_code == 401

    async def test_malformed_auth_header_returns_401(self, client):
        # Missing "Bearer " prefix.
        resp = await client.get("/cache/stats", headers={"Authorization": SECRET})
        assert resp.status_code == 401

    async def test_wrong_secret_returns_401(self, client):
        resp = await client.get(
            "/cache/stats",
            headers={"Authorization": "Bearer completely-wrong-secret"},
        )
        assert resp.status_code == 401

    async def test_401_response_has_detail_key(self, client):
        resp = await client.get("/cache/stats")
        assert "detail" in resp.json()

    async def test_health_bypasses_auth_middleware(self, client):
        """Public paths are exempt from the shared-secret check."""
        resp = await client.get("/health")
        assert resp.status_code == 200


# ===========================================================================
# RequestSizeLimitMiddleware
# ===========================================================================


class TestRequestSizeLimitMiddleware:
    async def test_body_over_limit_returns_413(self, client):
        """A Content-Length header announcing > 1 MiB must be rejected."""
        resp = await client.post(
            "/semantic_classify",
            content=b"x" * _OVER_LIMIT_SIZE,
            headers={
                **AUTH_HEADER,
                "Content-Type": "application/json",
                "Content-Length": str(_OVER_LIMIT_SIZE),
            },
        )
        assert resp.status_code == 413

    async def test_413_response_has_detail_key(self, client):
        resp = await client.post(
            "/semantic_classify",
            content=b"x" * _OVER_LIMIT_SIZE,
            headers={
                **AUTH_HEADER,
                "Content-Type": "application/json",
                "Content-Length": str(_OVER_LIMIT_SIZE),
            },
        )
        assert "detail" in resp.json()

    async def test_body_at_limit_is_allowed_through_size_middleware(self, client):
        """A request whose Content-Length == limit exactly must not get a 413.

        The body itself is not valid JSON so we expect 422, not 413.
        """
        at_limit = 1_048_576
        resp = await client.post(
            "/semantic_classify",
            content=b"x" * at_limit,
            headers={
                **AUTH_HEADER,
                "Content-Type": "application/json",
                "Content-Length": str(at_limit),
            },
        )
        # 413 would mean the size guard fired; any other status code is fine.
        assert resp.status_code != 413


# ===========================================================================
# CORS middleware
# ===========================================================================


class TestCORSMiddleware:
    async def test_options_preflight_includes_access_control_header(self, client):
        """OPTIONS preflight must return Access-Control-Allow-Origin."""
        resp = await client.options(
            "/health",
            headers={
                "Origin": "http://localhost:8000",
                "Access-Control-Request-Method": "GET",
            },
        )
        # CORS middleware should handle the preflight.
        assert resp.status_code in (200, 204)
        # At least one Access-Control-* header must be present.
        ac_headers = [h for h in resp.headers if h.lower().startswith("access-control")]
        assert ac_headers, "No Access-Control-* headers found in OPTIONS response"

    async def test_cors_header_present_on_normal_request(self, client):
        """Cross-origin GET to /health must include the CORS allow-origin header."""
        resp = await client.get(
            "/health",
            headers={"Origin": "http://localhost:8000"},
        )
        assert resp.status_code == 200
        assert "access-control-allow-origin" in resp.headers


# ===========================================================================
# AIRateLimitMiddleware — basic pass-through
# ===========================================================================


class TestRateLimitMiddleware:
    async def test_requests_under_limit_are_allowed(self, client):
        """A small burst of requests must all succeed (well under rate limit)."""
        for _ in range(3):
            resp = await client.get("/health")
            assert resp.status_code == 200
