"""Tests for tier-based feature gating via require_feature() dependency.

Covers:
- Community tier gets 403 on AI advisory endpoints (Team+ only)
- Community tier gets 403 on audit endpoints (Enterprise only)
- Community tier gets 403 on reconciliation endpoints (Enterprise only)
- Team tier passes AI gate, gets 403 on enterprise gates
- Enterprise tier passes all gates
- Error messages include tier name and upgrade guidance
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from api.config import APISettings
from api.dependencies import (
    get_admin_session,
    get_ai_client,
    get_db_session,
    get_metering_collector,
    get_settings,
    get_tenant_session,
)
from api.main import create_app
from api.services.ai_client import AIServiceClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_auth_headers(
    role: str = "admin",
    tenant_id: str = "test-tenant",
) -> dict[str, str]:
    """Generate valid dev-mode auth headers."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": "test-user",
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": "test-jti-feature-gates",
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


def _make_settings() -> APISettings:
    return APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
    )


def _create_app_with_tier(tier: str) -> Any:
    """Create a test app where BillingCustomerTable.plan_tier returns *tier*.

    The mock session is configured so that any SELECT on
    BillingCustomerTable.plan_tier returns the given tier value.
    """
    app = create_app()

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()

    # Build result mock that returns the tier for billing customer lookups.
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = tier
    result_mock.scalars.return_value.all.return_value = []
    result_mock.scalars.return_value.first.return_value = None

    mock_session.execute = AsyncMock(return_value=result_mock)

    async def _override_session():
        yield mock_session

    settings = _make_settings()

    mock_ai_client = AsyncMock(spec=AIServiceClient)
    mock_ai_client.semantic_classify = AsyncMock(return_value={"change_type": "non_breaking"})
    mock_ai_client.predict_cost = AsyncMock(return_value={"estimated_cost_usd": 1.0})
    mock_ai_client.score_risk = AsyncMock(return_value={"risk_score": 1.0})
    mock_ai_client.optimize_sql = AsyncMock(return_value={"suggestions": []})
    mock_ai_client.health_check = AsyncMock(return_value=True)
    mock_ai_client.close = AsyncMock()

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    app.dependency_overrides[get_db_session] = _override_session
    app.dependency_overrides[get_tenant_session] = _override_session
    app.dependency_overrides[get_admin_session] = _override_session
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_ai_client] = lambda: mock_ai_client
    app.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return app


# ---------------------------------------------------------------------------
# AI Advisory feature gate (Team+ required)
# ---------------------------------------------------------------------------


class TestAIAdvisoryFeatureGate:
    """POST /plans/{plan_id}/augment requires Feature.AI_ADVISORY (Team+)."""

    @pytest.mark.asyncio
    async def test_community_gets_403(self) -> None:
        """Community tier is blocked from AI augment endpoint."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="engineer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/plans/test-plan/augment")

        assert resp.status_code == 403
        body = resp.json()
        assert "ai_advisory" in body["detail"].lower() or "Team" in body["detail"]
        assert "upgrade" in body["detail"].lower()

    @pytest.mark.asyncio
    async def test_team_passes_gate(self) -> None:
        """Team tier passes the AI advisory feature gate.

        The endpoint may return 404/500/429 for other reasons, but NOT 403.
        We use raise_app_exceptions=False so that downstream errors (e.g. mock
        session returning wrong types for quota checks) result in a 500 HTTP
        response rather than crashing the test runner.
        """
        app = _create_app_with_tier("team")
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        headers = _make_auth_headers(role="engineer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/plans/test-plan/augment")

        # Should NOT be 403 (gate passed). Could be 404/429/500 for other reasons.
        assert resp.status_code != 403

    @pytest.mark.asyncio
    async def test_enterprise_passes_gate(self) -> None:
        """Enterprise tier passes the AI advisory feature gate."""
        app = _create_app_with_tier("enterprise")
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        headers = _make_auth_headers(role="engineer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post("/api/v1/plans/test-plan/augment")

        assert resp.status_code != 403


# ---------------------------------------------------------------------------
# Audit Log feature gate (Enterprise only)
# ---------------------------------------------------------------------------


class TestAuditLogFeatureGate:
    """GET /audit requires Feature.AUDIT_LOG (Enterprise only)."""

    @pytest.mark.asyncio
    async def test_community_gets_403(self) -> None:
        """Community tier is blocked from audit endpoints."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        assert resp.status_code == 403
        body = resp.json()
        assert "audit_log" in body["detail"].lower() or "Enterprise" in body["detail"]

    @pytest.mark.asyncio
    async def test_team_gets_403(self) -> None:
        """Team tier is blocked from audit endpoints (Enterprise only)."""
        app = _create_app_with_tier("team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_enterprise_passes_gate(self) -> None:
        """Enterprise tier passes the audit log feature gate."""
        app = _create_app_with_tier("enterprise")
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        assert resp.status_code != 403

    @pytest.mark.asyncio
    async def test_verify_community_gets_403(self) -> None:
        """Community tier is blocked from audit verify endpoint."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit/verify")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Reconciliation feature gate (Enterprise only)
# ---------------------------------------------------------------------------


class TestReconciliationFeatureGate:
    """Reconciliation endpoints require Feature.RECONCILIATION (Enterprise only)."""

    @pytest.mark.asyncio
    async def test_trigger_community_gets_403(self) -> None:
        """Community tier is blocked from reconciliation trigger."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/reconciliation/trigger",
                json={"hours_back": 24},
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_trigger_team_gets_403(self) -> None:
        """Team tier is blocked from reconciliation trigger (Enterprise only)."""
        app = _create_app_with_tier("team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/reconciliation/trigger",
                json={"hours_back": 24},
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_trigger_enterprise_passes(self) -> None:
        """Enterprise tier passes the reconciliation trigger gate."""
        app = _create_app_with_tier("enterprise")
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/reconciliation/trigger",
                json={"hours_back": 24},
            )

        assert resp.status_code != 403

    @pytest.mark.asyncio
    async def test_discrepancies_community_gets_403(self) -> None:
        """Community tier is blocked from viewing discrepancies."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/reconciliation/discrepancies")

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_stats_community_gets_403(self) -> None:
        """Community tier is blocked from viewing reconciliation stats."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/reconciliation/stats")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Error message content
# ---------------------------------------------------------------------------


class TestFeatureGateErrorMessages:
    """Verify error messages include helpful upgrade guidance."""

    @pytest.mark.asyncio
    async def test_error_includes_feature_name(self) -> None:
        """Error message names the blocked feature."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        body = resp.json()
        assert "audit_log" in body["detail"]

    @pytest.mark.asyncio
    async def test_error_includes_required_tier(self) -> None:
        """Error message names the required tier."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        body = resp.json()
        assert "Enterprise" in body["detail"]

    @pytest.mark.asyncio
    async def test_error_includes_current_tier(self) -> None:
        """Error message names the user's current tier."""
        app = _create_app_with_tier("community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        body = resp.json()
        assert "community" in body["detail"]

    @pytest.mark.asyncio
    async def test_error_includes_upgrade_guidance(self) -> None:
        """Error message tells the user to upgrade."""
        app = _create_app_with_tier("team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.get("/api/v1/audit")

        body = resp.json()
        assert "upgrade" in body["detail"].lower()
