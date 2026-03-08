"""Tests for api/api/routers/customer_health.py

Covers:
- GET /admin/health/tenants: list with summary, status filtering, invalid status
- GET /admin/health/tenants/{tenant_id}: health detail, 404 for missing tenants
- POST /admin/health/compute: compute result with count and duration
- RBAC: all endpoints require MANAGE_HEALTH (admin-only)
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
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


# Viewer token for RBAC tests (lacks MANAGE_HEALTH permission).
_VIEWER_TOKEN = _make_dev_token(role="viewer")
_VIEWER_HEADERS = {"Authorization": f"Bearer {_VIEWER_TOKEN}"}


# ---------------------------------------------------------------------------
# GET /admin/health/tenants
# ---------------------------------------------------------------------------


class TestListTenantsEndpoint:
    """Verify GET /api/v1/admin/health/tenants responses."""

    @pytest.mark.asyncio
    async def test_returns_tenant_list(self, client: AsyncClient) -> None:
        """Admin receives 200 with tenant list, total count, and summary."""
        mock_result: dict[str, Any] = {
            "tenants": [
                {
                    "tenant_id": "acme-corp",
                    "health_score": 85.0,
                    "status": "active",
                    "trend": "improving",
                    "last_activity": "2026-02-20T14:30:00+00:00",
                },
                {
                    "tenant_id": "widgets-inc",
                    "health_score": 42.0,
                    "status": "at_risk",
                    "trend": "declining",
                    "last_activity": "2026-02-18T09:00:00+00:00",
                },
            ],
            "total": 2,
            "summary": {
                "active": 1,
                "at_risk": 1,
                "churning": 0,
            },
        }

        with patch("api.routers.customer_health.CustomerHealthService") as MockService:
            instance = MockService.return_value
            instance.list_tenants = AsyncMock(return_value=mock_result)

            resp = await client.get("/api/v1/admin/health/tenants")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert len(body["tenants"]) == 2
        assert body["tenants"][0]["tenant_id"] == "acme-corp"
        assert body["tenants"][1]["status"] == "at_risk"
        assert body["summary"]["active"] == 1
        assert body["summary"]["churning"] == 0

        instance.list_tenants.assert_awaited_once_with(
            status_filter=None,
            sort_by="health_score",
            limit=50,
            offset=0,
        )

    @pytest.mark.asyncio
    async def test_filter_by_status(self, client: AsyncClient) -> None:
        """Passing status=at_risk filters results and forwards to service."""
        mock_result: dict[str, Any] = {
            "tenants": [
                {
                    "tenant_id": "widgets-inc",
                    "health_score": 42.0,
                    "status": "at_risk",
                    "trend": "declining",
                    "last_activity": "2026-02-18T09:00:00+00:00",
                },
            ],
            "total": 1,
            "summary": {"active": 0, "at_risk": 1, "churning": 0},
        }

        with patch("api.routers.customer_health.CustomerHealthService") as MockService:
            instance = MockService.return_value
            instance.list_tenants = AsyncMock(return_value=mock_result)

            resp = await client.get("/api/v1/admin/health/tenants?status=at_risk")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["tenants"][0]["status"] == "at_risk"

        instance.list_tenants.assert_awaited_once_with(
            status_filter="at_risk",
            sort_by="health_score",
            limit=50,
            offset=0,
        )

    @pytest.mark.asyncio
    async def test_invalid_status_filter(self, client: AsyncClient) -> None:
        """An unrecognised status value returns 400."""
        resp = await client.get("/api/v1/admin/health/tenants?status=invalid")

        assert resp.status_code == 400
        assert "Invalid status filter" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role lacks MANAGE_HEALTH permission and gets 403."""
        resp = await client.get(
            "/api/v1/admin/health/tenants",
            headers=_VIEWER_HEADERS,
        )

        assert resp.status_code == 403
        assert "permission denied" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# GET /admin/health/tenants/{tenant_id}
# ---------------------------------------------------------------------------


class TestGetTenantHealthEndpoint:
    """Verify GET /api/v1/admin/health/tenants/{tenant_id} responses."""

    @pytest.mark.asyncio
    async def test_returns_health_detail(self, client: AsyncClient) -> None:
        """Admin receives 200 with full health breakdown for an existing tenant."""
        mock_detail: dict[str, Any] = {
            "tenant_id": "acme-corp",
            "health_score": 85.0,
            "status": "active",
            "trend": "improving",
            "engagement_metrics": {
                "plan_runs_30d": 47,
                "ai_calls_30d": 120,
                "active_users_7d": 5,
                "last_plan_run": "2026-02-20T14:30:00+00:00",
            },
            "created_at": "2025-06-01T00:00:00+00:00",
            "last_activity": "2026-02-20T14:30:00+00:00",
        }

        with patch("api.routers.customer_health.CustomerHealthService") as MockService:
            instance = MockService.return_value
            instance.get_health_detail = AsyncMock(return_value=mock_detail)

            resp = await client.get("/api/v1/admin/health/tenants/acme-corp")

        assert resp.status_code == 200
        body = resp.json()
        assert body["tenant_id"] == "acme-corp"
        assert body["health_score"] == 85.0
        assert body["status"] == "active"
        assert body["engagement_metrics"]["plan_runs_30d"] == 47
        assert body["engagement_metrics"]["active_users_7d"] == 5

        instance.get_health_detail.assert_awaited_once_with("acme-corp")

    @pytest.mark.asyncio
    async def test_not_found(self, client: AsyncClient) -> None:
        """Service returning None maps to 404."""
        with patch("api.routers.customer_health.CustomerHealthService") as MockService:
            instance = MockService.return_value
            instance.get_health_detail = AsyncMock(return_value=None)

            resp = await client.get("/api/v1/admin/health/tenants/nonexistent")

        assert resp.status_code == 404
        assert "No health data" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role lacks MANAGE_HEALTH permission and gets 403."""
        resp = await client.get(
            "/api/v1/admin/health/tenants/acme-corp",
            headers=_VIEWER_HEADERS,
        )

        assert resp.status_code == 403
        assert "permission denied" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /admin/health/compute
# ---------------------------------------------------------------------------


class TestComputeHealthEndpoint:
    """Verify POST /api/v1/admin/health/compute responses."""

    @pytest.mark.asyncio
    async def test_returns_compute_result(self, client: AsyncClient) -> None:
        """Admin receives 200 with computed_count and duration_ms."""
        mock_result: dict[str, Any] = {
            "computed_count": 15,
            "duration_ms": 2340,
        }

        with patch("api.routers.customer_health.CustomerHealthService") as MockService:
            instance = MockService.return_value
            instance.compute_all = AsyncMock(return_value=mock_result)

            resp = await client.post("/api/v1/admin/health/compute")

        assert resp.status_code == 200
        body = resp.json()
        assert body["computed_count"] == 15
        assert body["duration_ms"] == 2340

        instance.compute_all.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_admin_forbidden(self, client: AsyncClient) -> None:
        """Viewer role lacks MANAGE_HEALTH permission and gets 403."""
        resp = await client.post(
            "/api/v1/admin/health/compute",
            headers=_VIEWER_HEADERS,
        )

        assert resp.status_code == 403
        assert "permission denied" in resp.json()["detail"].lower()
