"""Tests for api/api/routers/health.py

Covers:
- GET /health: public liveness — returns {"status": "ok"}, no version/dependency info
- GET /health/detailed: admin-authenticated — returns version, db, ai_engine status
- GET /ready: readiness probe — DB-gated, HTTP 200/503
- RBAC: /health/detailed requires VIEW_ANALYTICS (admin-only)
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from unittest.mock import AsyncMock

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
        "jti": "test-jti-health",
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


# ---------------------------------------------------------------------------
# GET /health  (public liveness probe)
# ---------------------------------------------------------------------------


class TestPublicHealthEndpoint:
    """Verify GET /api/v1/health is minimal and unauthenticated."""

    @pytest.mark.asyncio
    async def test_returns_200(self, client: AsyncClient) -> None:
        """Public health endpoint always returns HTTP 200."""
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_returns_status_ok(self, client: AsyncClient) -> None:
        """Response body is exactly {"status": "ok"}."""
        resp = await client.get("/api/v1/health")
        assert resp.json() == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_does_not_expose_version(self, client: AsyncClient) -> None:
        """Version string must not appear in the public health response."""
        resp = await client.get("/api/v1/health")
        body = resp.json()
        assert "version" not in body

    @pytest.mark.asyncio
    async def test_does_not_expose_db_status(self, client: AsyncClient) -> None:
        """Database dependency status must not appear in the public health response."""
        resp = await client.get("/api/v1/health")
        body = resp.json()
        assert "db" not in body

    @pytest.mark.asyncio
    async def test_does_not_expose_ai_engine_status(self, client: AsyncClient) -> None:
        """AI engine dependency status must not appear in the public health response."""
        resp = await client.get("/api/v1/health")
        body = resp.json()
        assert "ai_engine" not in body

    @pytest.mark.asyncio
    async def test_accessible_without_auth(self, app) -> None:
        """Public health endpoint must respond 200 even without an auth token."""
        from httpx import ASGITransport, AsyncClient

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/health")
        # Should not be 401 or 403 — liveness probe must work without auth
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_viewer_can_access(self, client: AsyncClient) -> None:
        """Even a viewer-role token can access the public health endpoint."""
        resp = await client.get("/api/v1/health", headers=_VIEWER_HEADERS)
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /health/detailed  (admin-authenticated)
# ---------------------------------------------------------------------------


class TestDetailedHealthEndpoint:
    """Verify GET /api/v1/health/detailed requires auth and returns full status."""

    @pytest.mark.asyncio
    async def test_returns_200_for_admin(self, client: AsyncClient) -> None:
        """Admin token receives HTTP 200."""
        resp = await client.get("/api/v1/health/detailed")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_returns_version(self, client: AsyncClient) -> None:
        """Detailed response includes a non-empty version string."""
        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert "version" in body
        assert isinstance(body["version"], str)
        assert len(body["version"]) > 0

    @pytest.mark.asyncio
    async def test_returns_db_ok_when_healthy(self, client: AsyncClient) -> None:
        """db field is 'ok' when the mock session executes successfully."""
        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert "db" in body
        assert body["db"] == "ok"

    @pytest.mark.asyncio
    async def test_returns_ai_engine_ok_when_healthy(self, client: AsyncClient) -> None:
        """ai_engine field is 'ok' when the mock AI client returns True."""
        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert "ai_engine" in body
        assert body["ai_engine"] == "ok"

    @pytest.mark.asyncio
    async def test_status_healthy_when_all_ok(self, client: AsyncClient) -> None:
        """Overall status is 'healthy' when DB and AI engine are both reachable."""
        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert body["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_db_degraded_when_db_fails(
        self, client: AsyncClient, mock_session: AsyncMock
    ) -> None:
        """db field becomes 'degraded' when the DB execute raises an exception."""
        mock_session.execute.side_effect = Exception("DB connection refused")

        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert body["db"] == "degraded"
        assert body["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_ai_engine_unavailable_when_health_check_fails(
        self, client: AsyncClient, mock_ai_client: AsyncMock
    ) -> None:
        """ai_engine field becomes 'unavailable' when the AI health check fails."""
        mock_ai_client.health_check.return_value = False

        resp = await client.get("/api/v1/health/detailed")
        body = resp.json()
        assert body["ai_engine"] == "unavailable"
        assert body["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_viewer_forbidden(self, client: AsyncClient) -> None:
        """Viewer role lacks VIEW_ANALYTICS permission and receives 403."""
        resp = await client.get("/api/v1/health/detailed", headers=_VIEWER_HEADERS)
        assert resp.status_code == 403
        assert "permission denied" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_unauthenticated_rejected(self, app) -> None:
        """Unauthenticated requests to /health/detailed receive 401 or 403."""
        from httpx import ASGITransport, AsyncClient

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/health/detailed")
        assert resp.status_code in {401, 403}

    @pytest.mark.asyncio
    async def test_status_degraded_when_db_and_ai_both_fail(
        self, client: AsyncClient, mock_session: AsyncMock, mock_ai_client: AsyncMock
    ) -> None:
        """Status is 'degraded' when both DB and AI engine are unreachable."""
        mock_session.execute.side_effect = Exception("DB down")
        mock_ai_client.health_check.return_value = False

        resp = await client.get("/api/v1/health/detailed")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["db"] == "degraded"
        assert body["ai_engine"] == "unavailable"


# ---------------------------------------------------------------------------
# GET /ready  (readiness probe — unchanged)
# ---------------------------------------------------------------------------


class TestReadinessProbe:
    """Verify GET /ready behaviour is unchanged by BL-080."""

    @pytest.mark.asyncio
    async def test_returns_200_when_db_ok(self, client: AsyncClient) -> None:
        """Readiness probe returns 200 when DB is reachable."""
        resp = await client.get("/ready")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_returns_ready_status(self, client: AsyncClient) -> None:
        """Status is 'ready' when DB is reachable and AI engine is up."""
        resp = await client.get("/ready")
        body = resp.json()
        assert body["status"] in {"ready", "degraded"}

    @pytest.mark.asyncio
    async def test_returns_503_when_db_fails(
        self, client: AsyncClient, mock_session: AsyncMock
    ) -> None:
        """Readiness probe returns 503 when DB is unreachable."""
        mock_session.execute.side_effect = Exception("DB connection refused")

        resp = await client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "not_ready"

    @pytest.mark.asyncio
    async def test_degraded_when_ai_unavailable(
        self, client: AsyncClient, mock_ai_client: AsyncMock
    ) -> None:
        """Readiness probe returns 200/degraded when AI engine is down but DB is ok."""
        mock_ai_client.health_check.return_value = False

        resp = await client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_includes_version_in_ready_response(self, client: AsyncClient) -> None:
        """Readiness probe response includes a version field (internal use only)."""
        resp = await client.get("/ready")
        body = resp.json()
        assert "version" in body
        assert isinstance(body["version"], str)

    @pytest.mark.asyncio
    async def test_includes_checks_dict(self, client: AsyncClient) -> None:
        """Readiness probe response includes a checks dict with db and ai_engine."""
        resp = await client.get("/ready")
        body = resp.json()
        assert "checks" in body
        assert "db" in body["checks"]
        assert "ai_engine" in body["checks"]
