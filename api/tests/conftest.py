"""Shared fixtures for IronLayer API tests.

Provides mock database sessions, mock AI clients, a FastAPI TestClient,
and sample data factories used across all test modules.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Set JWT_SECRET env var BEFORE importing application modules so the
# AuthenticationMiddleware picks up a deterministic secret in dev mode
# instead of generating a random one.
_TEST_JWT_SECRET = "test-secret-key-for-ironlayer-tests"
os.environ.setdefault("JWT_SECRET", _TEST_JWT_SECRET)

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
# Dev auth token (shared across all tests)
# ---------------------------------------------------------------------------

_DEV_SECRET = _TEST_JWT_SECRET


def _make_dev_token(
    tenant_id: str = "default",
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
) -> str:
    """Generate a valid development-mode HMAC token with a role claim.

    Mirrors the signing logic in :class:`api.security.TokenManager`.
    The ``role`` field is included directly in the payload so the
    AuthenticationMiddleware can extract it for RBAC enforcement.
    """
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


_DEV_TOKEN = _make_dev_token()
_AUTH_HEADERS: dict[str, str] = {"Authorization": f"Bearer {_DEV_TOKEN}"}


# ---------------------------------------------------------------------------
# Settings fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def test_settings() -> APISettings:
    """Return a settings object suitable for testing."""
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


# ---------------------------------------------------------------------------
# Mock database session
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Return a mock AsyncSession that behaves like a real SQLAlchemy session.

    The mock supports add, flush, commit, rollback, close, and execute.
    The execute mock returns a result whose ``scalar_one_or_none()``
    returns ``None`` and ``scalars().all()`` returns ``[]`` by default,
    which satisfies the audit-log hash-chain lookup and list queries.
    """
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    # Build a result mock that handles common ORM result patterns.
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    result_mock.scalars.return_value.first.return_value = None

    session.execute = AsyncMock(return_value=result_mock)
    return session


# ---------------------------------------------------------------------------
# Mock AI client
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_ai_client() -> AsyncMock:
    """Return a mock AIServiceClient with all methods as AsyncMock.

    By default, every advisory method returns a plausible non-None result.
    Tests can override individual return_values as needed.
    """
    client = AsyncMock(spec=AIServiceClient)
    client.semantic_classify = AsyncMock(
        return_value={
            "change_type": "non_breaking",
            "description": "Minor filter adjustment",
            "confidence": 0.92,
        }
    )
    client.predict_cost = AsyncMock(
        return_value={
            "estimated_cost_usd": 1.50,
            "estimated_runtime_seconds": 120.0,
            "cluster_recommendation": "small",
        }
    )
    client.score_risk = AsyncMock(
        return_value={
            "risk_score": 1.5,
            "risk_level": "low",
            "factors": ["no_sla_dependency", "low_downstream_impact"],
        }
    )
    client.optimize_sql = AsyncMock(
        return_value={
            "suggestions": [],
            "optimized_sql": None,
        }
    )
    client.health_check = AsyncMock(return_value=True)
    client.close = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# FastAPI TestClient (async httpx)
# ---------------------------------------------------------------------------


@pytest.fixture()
def app(test_settings: APISettings, mock_session: AsyncMock, mock_ai_client: AsyncMock):
    """Create a FastAPI app with dependency overrides for testing.

    Injects the mock database session and mock AI client so that no real
    database or external service is required.
    """
    application = create_app()

    async def _override_session():
        yield mock_session

    def _override_settings():
        return test_settings

    def _override_ai_client():
        return mock_ai_client

    # Mock metering collector — no-op for tests.
    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    def _override_metering():
        return mock_metering

    application.dependency_overrides[get_db_session] = _override_session
    application.dependency_overrides[get_tenant_session] = _override_session
    application.dependency_overrides[get_admin_session] = _override_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = _override_ai_client
    application.dependency_overrides[get_metering_collector] = _override_metering

    return application


@pytest_asyncio.fixture()
async def client(app) -> AsyncClient:
    """Yield an async httpx client bound to the test app.

    Uses ASGITransport so requests go directly to the ASGI app without
    opening a real TCP socket.  All requests include a valid dev-mode
    Bearer token so the AuthenticationMiddleware passes them through.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_AUTH_HEADERS,
    ) as ac:
        yield ac


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_plan_json() -> dict[str, Any]:
    """Return a minimal but complete plan dictionary."""
    return {
        "plan_id": "abc123def456",
        "base": "aaa111",
        "target": "bbb222",
        "summary": {
            "total_steps": 2,
            "estimated_cost_usd": 3.0,
            "models_changed": ["staging.orders", "marts.revenue"],
        },
        "steps": [
            {
                "step_id": "step-001",
                "model": "staging.orders",
                "run_type": "FULL_REFRESH",
                "input_range": None,
                "depends_on": [],
                "parallel_group": 0,
                "reason": "SQL body changed",
                "estimated_compute_seconds": 60.0,
                "estimated_cost_usd": 1.5,
            },
            {
                "step_id": "step-002",
                "model": "marts.revenue",
                "run_type": "INCREMENTAL",
                "input_range": {"start": "2024-01-01", "end": "2024-01-31"},
                "depends_on": ["step-001"],
                "parallel_group": 1,
                "reason": "Upstream changed",
                "estimated_compute_seconds": 120.0,
                "estimated_cost_usd": 1.5,
            },
        ],
    }


@pytest.fixture()
def sample_plan_row(sample_plan_json: dict[str, Any]) -> MagicMock:
    """Return a mock PlanTable row matching sample_plan_json."""
    row = MagicMock()
    row.plan_id = "abc123def456"
    row.tenant_id = "default"
    row.base_sha = "aaa111"
    row.target_sha = "bbb222"
    row.plan_json = json.dumps(sample_plan_json)
    row.approvals_json = None
    row.advisory_json = None
    row.auto_approved = False
    row.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    return row


@pytest.fixture()
def sample_model_row() -> MagicMock:
    """Return a mock ModelTable row for a staging model."""
    row = MagicMock()
    row.model_name = "staging.orders"
    row.tenant_id = "default"
    row.repo_path = "models/staging/orders.sql"
    row.current_version = "v1-abc"
    row.kind = "sql"
    row.time_column = "order_date"
    row.unique_key = "order_id"
    row.materialization = "incremental"
    row.owner = "data-team"
    row.tags = json.dumps(["staging", "core"])
    row.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    row.last_modified_at = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    return row


@pytest.fixture()
def sample_run_dict() -> dict[str, Any]:
    """Return a sample run record dictionary."""
    return {
        "run_id": "run-001",
        "plan_id": "abc123def456",
        "step_id": "step-001",
        "model_name": "staging.orders",
        "status": "SUCCESS",
        "started_at": datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2024, 6, 15, 12, 1, 30, tzinfo=timezone.utc),
        "input_range_start": None,
        "input_range_end": None,
        "error_message": None,
        "logs_uri": None,
        "cluster_used": None,
        "executor_version": "api-control-plane-0.1.0",
        "retry_count": 0,
    }


@pytest.fixture()
def sample_run_row(sample_run_dict: dict[str, Any]) -> MagicMock:
    """Return a mock RunTable row from sample_run_dict."""
    row = MagicMock()
    for key, value in sample_run_dict.items():
        setattr(row, key, value)
    row.tenant_id = "default"
    return row


# ---------------------------------------------------------------------------
# Feature-gate bypass
# ---------------------------------------------------------------------------


@pytest.fixture()
def bypass_feature_gate():
    """Bypass billing-tier feature gates for tests that hit gated endpoints.

    The shared ``mock_session`` returns ``None`` for
    ``scalar_one_or_none()`` which maps to ``community`` tier, causing
    403 responses on gated endpoints (AI advisory, audit, reconciliation).
    Patching ``is_feature_enabled`` to always return ``True`` isolates
    endpoint tests from tier-gating logic.

    Usage: add ``pytestmark = pytest.mark.usefixtures("bypass_feature_gate")``
    at module level in test files that exercise gated endpoints but are NOT
    specifically testing the feature gates themselves.
    """
    with patch("core_engine.license.feature_flags.is_feature_enabled", return_value=True):
        yield
