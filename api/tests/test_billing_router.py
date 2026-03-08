"""Tests for api/api/routers/billing.py

Covers:
- GET /billing/subscription: response shapes, billing disabled/enabled
- POST /billing/portal: portal session creation, billing disabled
- POST /billing/webhooks: Stripe signature validation, event dispatch
- Authentication and RBAC requirements
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.config import APISettings
from api.dependencies import get_db_session, get_metering_collector, get_settings, get_tenant_session
from api.main import create_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _billing_settings(billing_enabled: bool = False) -> APISettings:
    """Return settings with configurable billing flag."""
    return APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
        billing_enabled=billing_enabled,
        stripe_secret_key="sk_test_xxx",
        stripe_webhook_secret="whsec_test_xxx",
        stripe_price_id_community="price_community",
        stripe_price_id_team="price_team",
        stripe_price_id_enterprise="price_enterprise",
        stripe_metered_price_id="price_metered",
    )


def _make_auth_headers(role: str = "admin") -> dict[str, str]:
    """Generate valid dev-mode auth headers."""
    import base64
    import hashlib
    import hmac
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
        "jti": "test-jti-billing",
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


def _create_test_app(billing_enabled: bool = False) -> Any:
    """Create a FastAPI app with billing settings and mock deps."""
    app = create_app()

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

    settings = _billing_settings(billing_enabled)

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
# GET /billing/subscription
# ---------------------------------------------------------------------------


class TestGetSubscription:
    """Verify GET /api/v1/billing/subscription responses."""

    @pytest.mark.asyncio
    async def test_billing_disabled_returns_community(self) -> None:
        """When billing is disabled, return community tier."""
        app, _ = _create_test_app(billing_enabled=False)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/billing/subscription")

        assert resp.status_code == 200
        body = resp.json()
        assert body["plan_tier"] == "community"
        assert body["billing_enabled"] is False

    @pytest.mark.asyncio
    async def test_billing_enabled_calls_service(self) -> None:
        """When billing is enabled, delegate to BillingService."""
        app, mock_session = _create_test_app(billing_enabled=True)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        with patch("api.routers.billing.BillingService") as MockBillingService:
            instance = MockBillingService.return_value
            instance.get_subscription_info = AsyncMock(
                return_value={
                    "plan_tier": "team",
                    "status": "active",
                    "subscription_id": "sub_abc",
                    "period_start": "2024-06-01T00:00:00+00:00",
                    "period_end": "2024-07-01T00:00:00+00:00",
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/billing/subscription")

        assert resp.status_code == 200
        body = resp.json()
        assert body["plan_tier"] == "team"
        assert body["billing_enabled"] is True

    @pytest.mark.asyncio
    async def test_subscription_requires_auth(self) -> None:
        """Unauthenticated requests get 401."""
        app, _ = _create_test_app(billing_enabled=False)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/billing/subscription")

        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /billing/portal
# ---------------------------------------------------------------------------


class TestCreatePortalSession:
    """Verify POST /api/v1/billing/portal responses."""

    @pytest.mark.asyncio
    async def test_portal_billing_disabled_returns_404(self) -> None:
        """When billing is disabled, portal endpoint returns 404."""
        app, _ = _create_test_app(billing_enabled=False)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/billing/portal",
                json={"return_url": "https://app.ironlayer.app/billing"},
            )

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_portal_billing_enabled(self) -> None:
        """When billing is enabled, portal returns a Stripe URL."""
        app, _ = _create_test_app(billing_enabled=True)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        with patch("api.routers.billing.BillingService") as MockBillingService:
            instance = MockBillingService.return_value
            instance.create_portal_session = AsyncMock(
                return_value={"url": "https://billing.stripe.com/session/ses_xxx"}
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/billing/portal",
                    json={"return_url": "https://app.ironlayer.app/billing"},
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["url"] == "https://billing.stripe.com/session/ses_xxx"

    @pytest.mark.asyncio
    async def test_portal_missing_return_url_returns_422(self) -> None:
        """Missing return_url field returns 422 validation error."""
        app, _ = _create_test_app(billing_enabled=True)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers()

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/billing/portal", json={})

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /billing/webhooks
# ---------------------------------------------------------------------------


class TestStripeWebhook:
    """Verify POST /api/v1/billing/webhooks responses."""

    @pytest.mark.asyncio
    async def test_webhook_billing_disabled(self) -> None:
        """When billing is disabled, webhook returns billing_disabled."""
        app, _ = _create_test_app(billing_enabled=False)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/billing/webhooks",
                content=b'{"type": "invoice.paid"}',
                headers={"stripe-signature": "t=123,v1=abc"},
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "billing_disabled"

    @pytest.mark.asyncio
    async def test_webhook_missing_signature(self) -> None:
        """Missing Stripe signature header returns 400."""
        app, _ = _create_test_app(billing_enabled=True)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/billing/webhooks",
                content=b'{"type": "invoice.paid"}',
            )

        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_webhook_invalid_signature(self) -> None:
        """Invalid Stripe signature returns 400."""
        app, _ = _create_test_app(billing_enabled=True)
        transport = ASGITransport(app=app)

        with patch("stripe.Webhook.construct_event", side_effect=Exception("bad sig")):
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.post(
                    "/api/v1/billing/webhooks",
                    content=b'{"type": "invoice.paid"}',
                    headers={"stripe-signature": "t=123,v1=invalid"},
                )

        assert resp.status_code == 400
