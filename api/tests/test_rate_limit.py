"""Tests for api/api/middleware/rate_limit.py

Covers:
- Requests within limit pass through with 200.
- Requests exceeding the burst limit receive 429.
- Rate-limit headers (X-RateLimit-Limit, X-RateLimit-Remaining,
  X-RateLimit-Reset) are present on every response.
- Exempt paths bypass rate limiting entirely.
- Expensive endpoint patterns use lower per-minute caps.
- The sliding window resets after sufficient time passes.
- Tenant-based keying isolates quotas between tenants.
- Disabled middleware is a transparent pass-through.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from api.middleware.rate_limit import (
    RateLimitConfig,
    RateLimitMiddleware,
    SlidingWindowCounter,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(
    config: RateLimitConfig | None = None,
    set_tenant: str | None = None,
) -> Starlette:
    """Build a minimal Starlette app with the rate-limit middleware.

    If *set_tenant* is provided, a pre-middleware hook sets
    ``request.state.tenant_id`` to simulate the auth middleware.
    """

    async def _ok(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    async def _health(request: Request) -> JSONResponse:
        return JSONResponse({"status": "healthy"})

    app = Starlette(
        routes=[
            Route("/api/v1/test", _ok),
            Route("/api/v1/health", _health),
            Route("/api/v1/plans/generate", _ok, methods=["POST"]),
            Route("/api/v1/auth/login", _ok, methods=["POST"]),
            Route("/api/v1/backfills", _ok, methods=["POST"]),
        ],
    )

    if set_tenant:
        from starlette.middleware.base import BaseHTTPMiddleware

        class _TenantSetter(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                request.state.tenant_id = set_tenant
                return await call_next(request)

        # Add tenant setter as innermost so it runs before rate limit.
        app.add_middleware(_TenantSetter)

    cfg = config or RateLimitConfig(
        default_requests_per_minute=5,
        burst_multiplier=1.0,
        exempt_paths={"/api/v1/health"},
        expensive_endpoints={"/api/v1/plans/generate": 2, "/api/v1/backfills": 2},
        auth_endpoints_per_minute=3,
    )
    app.add_middleware(RateLimitMiddleware, config=cfg)

    return app


@pytest_asyncio.fixture()
async def client() -> AsyncClient:
    """Yield an async client bound to a test app with a 5-rpm limit."""
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# SlidingWindowCounter unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_counter_hit_and_count() -> None:
    """hit() increments the count; count() reads without incrementing."""
    counter = SlidingWindowCounter(window_seconds=60.0)
    assert await counter.count("k") == 0
    assert await counter.hit("k") == 1
    assert await counter.hit("k") == 2
    assert await counter.count("k") == 2
    await counter.stop()


@pytest.mark.asyncio
async def test_counter_expiry() -> None:
    """Entries older than the window are pruned on the next access."""
    counter = SlidingWindowCounter(window_seconds=0.1)
    await counter.hit("k")
    assert await counter.count("k") == 1

    await asyncio.sleep(0.15)
    assert await counter.count("k") == 0
    await counter.stop()


# ---------------------------------------------------------------------------
# Middleware integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_requests_within_limit(client: AsyncClient) -> None:
    """Requests below the burst cap receive 200 and rate-limit headers."""
    for _ in range(5):
        resp = await client.get("/api/v1/test")
        assert resp.status_code == 200
        assert "x-ratelimit-limit" in resp.headers
        assert "x-ratelimit-remaining" in resp.headers
        assert "x-ratelimit-reset" in resp.headers


@pytest.mark.asyncio
async def test_requests_exceeding_limit(client: AsyncClient) -> None:
    """The sixth request exceeds the 5-rpm cap and returns 429."""
    for _ in range(5):
        resp = await client.get("/api/v1/test")
        assert resp.status_code == 200

    resp = await client.get("/api/v1/test")
    assert resp.status_code == 429
    body = resp.json()
    assert "retry_after" in body
    assert "Retry-After" in resp.headers


@pytest.mark.asyncio
async def test_rate_limit_headers_values(client: AsyncClient) -> None:
    """X-RateLimit-Remaining decreases with each request."""
    resp = await client.get("/api/v1/test")
    assert resp.status_code == 200
    assert resp.headers["x-ratelimit-limit"] == "5"
    assert resp.headers["x-ratelimit-remaining"] == "4"

    resp = await client.get("/api/v1/test")
    assert resp.status_code == 200
    assert resp.headers["x-ratelimit-remaining"] == "3"


@pytest.mark.asyncio
async def test_exempt_paths_bypass(client: AsyncClient) -> None:
    """Exempt paths (/api/health) are never rate-limited."""
    for _ in range(20):
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
    assert "x-ratelimit-limit" not in resp.headers


@pytest.mark.asyncio
async def test_expensive_endpoint_lower_limit() -> None:
    """Expensive endpoints use their own lower per-minute limit."""
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # /api/plans/generate has a 2-rpm limit.
        for _ in range(2):
            resp = await ac.post("/api/v1/plans/generate")
            assert resp.status_code == 200

        resp = await ac.post("/api/v1/plans/generate")
        assert resp.status_code == 429


@pytest.mark.asyncio
async def test_auth_endpoint_limit() -> None:
    """Auth endpoints use the auth-specific per-minute limit."""
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for _ in range(3):
            resp = await ac.post("/api/v1/auth/login")
            assert resp.status_code == 200

        resp = await ac.post("/api/v1/auth/login")
        assert resp.status_code == 429


@pytest.mark.asyncio
async def test_sliding_window_resets() -> None:
    """After the window elapses, the counter resets and requests pass."""
    config = RateLimitConfig(
        default_requests_per_minute=2,
        burst_multiplier=1.0,
        exempt_paths=set(),
        expensive_endpoints={},
        auth_endpoints_per_minute=20,
    )
    app = _make_app(config=config)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Use a patched window to avoid waiting 60 seconds.
        # We manipulate time.monotonic to simulate window expiry.
        base_time = time.monotonic()
        call_count = 0

        original_monotonic = time.monotonic

        def _fake_monotonic() -> float:
            nonlocal call_count
            call_count += 1
            # Immediately advance time by 61 seconds to simulate window expiry.
            return original_monotonic() + 61.0

        # Exhaust the limit.
        for _ in range(2):
            resp = await ac.get("/api/v1/test")
            assert resp.status_code == 200

        resp = await ac.get("/api/v1/test")
        assert resp.status_code == 429

        # Patch monotonic so the window expires.
        with patch("api.middleware.rate_limit.time.monotonic", side_effect=_fake_monotonic):
            resp = await ac.get("/api/v1/test")
            assert resp.status_code == 200


@pytest.mark.asyncio
async def test_tenant_based_keying() -> None:
    """Different tenants have isolated rate-limit quotas."""
    config = RateLimitConfig(
        default_requests_per_minute=3,
        burst_multiplier=1.0,
        exempt_paths=set(),
        expensive_endpoints={},
        auth_endpoints_per_minute=20,
    )

    app_a = _make_app(config=config, set_tenant="tenant-alpha")
    app_b = _make_app(config=config, set_tenant="tenant-beta")

    transport_a = ASGITransport(app=app_a)
    transport_b = ASGITransport(app=app_b)

    async with (
        AsyncClient(transport=transport_a, base_url="http://test") as client_a,
        AsyncClient(transport=transport_b, base_url="http://test") as client_b,
    ):
        # Exhaust tenant-alpha's quota.
        for _ in range(3):
            resp = await client_a.get("/api/v1/test")
            assert resp.status_code == 200

        resp = await client_a.get("/api/v1/test")
        assert resp.status_code == 429

        # Tenant-beta still has its full quota.
        for _ in range(3):
            resp = await client_b.get("/api/v1/test")
            assert resp.status_code == 200


@pytest.mark.asyncio
async def test_disabled_middleware_passthrough() -> None:
    """When rate limiting is disabled, all requests pass through."""
    config = RateLimitConfig(
        enabled=False,
        default_requests_per_minute=1,
        burst_multiplier=1.0,
    )
    app = _make_app(config=config)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for _ in range(10):
            resp = await ac.get("/api/v1/test")
            assert resp.status_code == 200
        # No rate-limit headers when disabled.
        assert "x-ratelimit-limit" not in resp.headers


@pytest.mark.asyncio
async def test_burst_multiplier() -> None:
    """Burst multiplier raises the effective limit above the base rate."""
    config = RateLimitConfig(
        default_requests_per_minute=4,
        burst_multiplier=1.5,  # effective limit = 6
        exempt_paths=set(),
        expensive_endpoints={},
        auth_endpoints_per_minute=20,
    )
    app = _make_app(config=config)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # 6 requests should succeed (4 * 1.5 = 6).
        for i in range(6):
            resp = await ac.get("/api/v1/test")
            assert resp.status_code == 200, f"Request {i + 1} unexpectedly failed"

        # 7th should be rejected.
        resp = await ac.get("/api/v1/test")
        assert resp.status_code == 429


@pytest.mark.asyncio
async def test_429_response_body() -> None:
    """429 response includes structured error detail and retry_after."""
    config = RateLimitConfig(
        default_requests_per_minute=1,
        burst_multiplier=1.0,
        exempt_paths=set(),
        expensive_endpoints={},
        auth_endpoints_per_minute=20,
    )
    app = _make_app(config=config)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.get("/api/v1/test")
        resp = await ac.get("/api/v1/test")
        assert resp.status_code == 429
        body = resp.json()
        assert body["detail"] == "Rate limit exceeded. Try again later."
        assert isinstance(body["retry_after"], int)
        assert body["retry_after"] >= 1
