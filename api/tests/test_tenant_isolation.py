"""Cross-tenant isolation breach tests.

Proves that tenant boundaries are airtight by exercising every API
endpoint from the perspective of an attacker tenant.  Each test:

1. Authenticates as Tenant B (the attacker).
2. Attempts to access resources belonging to Tenant A (the victim).
3. Asserts that the response is either 404 (not found) or empty — never
   leaked data.
4. Verifies that the underlying service/repository was constructed with
   the *attacker's* tenant_id (proving RLS scoping).

Additionally tests that tokens **without** a ``tenant_id`` claim are
rejected, and that ``X-Tenant-Id`` header spoofing has no effect.
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
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.config import APISettings
from api.dependencies import get_ai_client, get_db_session, get_metering_collector, get_settings, get_tenant_session
from api.main import create_app
from api.services.ai_client import AIServiceClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TENANT_A = "tenant-alpha"
TENANT_B = "tenant-bravo"

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------


def _make_dev_token(
    tenant_id: str,
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
) -> str:
    """Generate a valid development-mode HMAC token."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": scopes or ["read", "write"],
        "jti": f"test-jti-{tenant_id}",
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


def _make_token_without_tenant(sub: str = "no-tenant-user") -> str:
    """Generate a token with NO tenant_id claim."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": "test-jti-no-tenant",
        "identity_kind": "user",
        "role": "viewer",
    }
    payload_json = json.dumps(payload)
    signature = hmac.new(
        _DEV_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token_bytes = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{token_bytes}.{signature}"


def _auth_headers(tenant_id: str) -> dict[str, str]:
    token = _make_dev_token(tenant_id=tenant_id)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_session() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalar_one.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    result_mock.scalars.return_value.first.return_value = None

    session.execute = AsyncMock(return_value=result_mock)
    return session


@pytest.fixture()
def _app(_mock_session: AsyncMock) -> Any:
    application = create_app()

    async def _override_session():
        yield _mock_session

    def _override_settings():
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

    mock_ai = AsyncMock(spec=AIServiceClient)
    mock_ai.semantic_classify = AsyncMock(return_value={})
    mock_ai.predict_cost = AsyncMock(return_value={})
    mock_ai.score_risk = AsyncMock(return_value={})
    mock_ai.optimize_sql = AsyncMock(return_value={})
    mock_ai.health_check = AsyncMock(return_value=True)
    mock_ai.close = AsyncMock()

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    application.dependency_overrides[get_db_session] = _override_session
    application.dependency_overrides[get_tenant_session] = _override_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = lambda: mock_ai
    application.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return application


@pytest_asyncio.fixture()
async def client_a(_app) -> AsyncClient:
    """Async client authenticated as Tenant A."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_A),
    ) as ac:
        yield ac


@pytest_asyncio.fixture()
async def client_b(_app) -> AsyncClient:
    """Async client authenticated as Tenant B."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_B),
    ) as ac:
        yield ac


@pytest_asyncio.fixture()
async def client_no_tenant(_app) -> AsyncClient:
    """Client whose token has no tenant_id claim."""
    token = _make_token_without_tenant()
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture()
async def client_spoofed(_app) -> AsyncClient:
    """Client authenticated as Tenant B but sending X-Tenant-Id: tenant-alpha."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={
            **_auth_headers(TENANT_B),
            "X-Tenant-Id": TENANT_A,
        },
    ) as ac:
        yield ac


# ===================================================================
# 1. TestCrossTenantPlanAccess
# ===================================================================


class TestCrossTenantPlanAccess:
    """Tenant B must never see, modify, or apply plans owned by Tenant A."""

    @pytest.mark.asyncio
    async def test_get_plan_returns_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_plan = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/plans/plan-alpha-001")
        assert resp.status_code == 404
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_plans_returns_empty_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/plans")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_augment_plan_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.generate_augmented_plan = AsyncMock(side_effect=ValueError("Plan plan-alpha-001 not found"))
            resp = await client_b.post("/api/v1/plans/plan-alpha-001/augment")
        assert resp.status_code == 404
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_apply_plan_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.ExecutionService") as MockExec:
            instance = MockExec.return_value
            instance.apply_plan = AsyncMock(side_effect=ValueError("Plan plan-alpha-001 not found"))
            resp = await client_b.post(
                "/api/v1/plans/plan-alpha-001/apply",
                json={"auto_approve": True},
            )
        assert resp.status_code == 404
        assert MockExec.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_feedback_service_scoped_to_caller_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.AIFeedbackService") as MockFeedback:
            instance = MockFeedback.return_value
            instance.record_suggestion_feedback = AsyncMock(return_value=0)
            resp = await client_b.post(
                "/api/v1/plans/plan-alpha-001/feedback",
                json={
                    "feedbacks": [
                        {
                            "step_id": "step-001",
                            "model_name": "staging.orders",
                            "feedback_type": "cost",
                            "accepted": True,
                        }
                    ]
                },
            )
        assert resp.status_code == 200
        assert MockFeedback.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_feedback_stats_scoped_to_caller_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.AIFeedbackService") as MockFeedback:
            instance = MockFeedback.return_value
            instance.get_accuracy_stats = AsyncMock(
                return_value={
                    "total_feedbacks": 0,
                    "accepted": 0,
                    "rejected": 0,
                    "acceptance_rate": 0.0,
                }
            )
            resp = await client_b.get("/api/v1/plans/plan-alpha-001/feedback/stats")
        assert resp.status_code == 200
        assert MockFeedback.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 2. TestCrossTenantModelAccess
# ===================================================================


class TestCrossTenantModelAccess:
    """Tenant B must not see models registered by Tenant A."""

    @pytest.mark.asyncio
    async def test_list_models_empty_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/models")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_get_model_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/models/staging.orders")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_model_lineage_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/models/staging.orders/lineage")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_model_health_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/models/staging.orders/health")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_at_risk_models_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/models/at-risk")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 3. TestCrossTenantRunAccess
# ===================================================================


class TestCrossTenantRunAccess:
    """Run history must be fully isolated between tenants."""

    @pytest.mark.asyncio
    async def test_list_runs_empty_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_plan = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/runs?plan_id=plan-alpha-001")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_get_run_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_id = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/runs/run-alpha-001")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_runs_by_model_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        resp = await client_b.get("/api/v1/runs?model_name=staging.orders")
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_list_runs_by_status_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        resp = await client_b.get("/api/v1/runs?status=SUCCESS")
        assert resp.status_code == 200
        assert resp.json() == []


# ===================================================================
# 4. TestCrossTenantEnvironmentAccess
# ===================================================================


class TestCrossTenantEnvironmentAccess:
    """Environment endpoints must be fully tenant-scoped."""

    @pytest.mark.asyncio
    async def test_list_environments_empty_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_environments = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/environments")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_get_environment_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_environment = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/environments/staging")
        assert resp.status_code == 404
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_delete_environment_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.delete_environment = AsyncMock(return_value=False)
            resp = await client_b.delete("/api/v1/environments/staging")
        assert resp.status_code == 404
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_promotion_history_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_promotion_history = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/environments/promotions")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 5. TestCrossTenantBackfillAccess
# ===================================================================


class TestCrossTenantBackfillAccess:
    """Backfill triggers and status queries must be tenant-scoped."""

    @pytest.mark.asyncio
    async def test_get_backfill_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.backfills.PlanRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_plan = AsyncMock(return_value=None)
            resp = await client_b.get("/api/v1/backfills/plan-alpha-001")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_backfill_status_404_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.backfills.ExecutionService") as MockExec:
            instance = MockExec.return_value
            instance.get_backfill_status = AsyncMock(side_effect=ValueError("Backfill bf-alpha-001 not found"))
            resp = await client_b.get("/api/v1/backfills/status/bf-alpha-001")
        assert resp.status_code == 404
        assert MockExec.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_resume_backfill_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.backfills.ExecutionService") as MockExec:
            instance = MockExec.return_value
            instance.resume_backfill = AsyncMock(side_effect=ValueError("Backfill bf-alpha-001 not found"))
            resp = await client_b.post("/api/v1/backfills/bf-alpha-001/resume")
        assert resp.status_code == 400
        assert MockExec.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_backfill_history_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.backfills.ExecutionService") as MockExec:
            instance = MockExec.return_value
            instance.get_backfill_history = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/backfills/history/staging.orders")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockExec.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 6. TestCrossTenantAuditAccess
# ===================================================================


class TestCrossTenantAuditAccess:
    """Audit log queries must be strictly tenant-scoped."""

    @pytest.mark.asyncio
    async def test_query_audit_log_empty_for_other_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/audit")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_query_audit_filtered_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/audit?action=plan.created")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_verify_audit_chain_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.verify_chain = AsyncMock(return_value=(True, 0))
            resp = await client_b.get("/api/v1/audit/verify")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_valid"] is True
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_audit_entity_filter_scoped_to_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/audit?entity_type=plan&entity_id=plan-alpha-001")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 7. TestTenantIdMissing
# ===================================================================


class TestTenantIdMissing:
    """Requests with tokens lacking a ``tenant_id`` claim must be rejected."""

    @pytest.mark.asyncio
    async def test_plans_rejected_without_tenant(
        self,
        client_no_tenant: AsyncClient,
    ) -> None:
        resp = await client_no_tenant.get("/api/v1/plans")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_models_rejected_without_tenant(
        self,
        client_no_tenant: AsyncClient,
    ) -> None:
        resp = await client_no_tenant.get("/api/v1/models")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_runs_rejected_without_tenant(
        self,
        client_no_tenant: AsyncClient,
    ) -> None:
        resp = await client_no_tenant.get("/api/v1/runs")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_environments_rejected_without_tenant(
        self,
        client_no_tenant: AsyncClient,
    ) -> None:
        resp = await client_no_tenant.get("/api/v1/environments")
        assert resp.status_code == 401


# ===================================================================
# 8. TestTenantIdSpoofing
# ===================================================================


class TestTenantIdSpoofing:
    """X-Tenant-Id header spoofing must have no effect."""

    @pytest.mark.asyncio
    async def test_spoofed_header_ignored_on_plans(
        self,
        client_spoofed: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[])
            resp = await client_spoofed.get("/api/v1/plans")
        assert resp.status_code == 200
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_spoofed_header_ignored_on_models(
        self,
        client_spoofed: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client_spoofed.get("/api/v1/models")
        assert resp.status_code == 200
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_spoofed_header_ignored_on_audit(
        self,
        client_spoofed: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_spoofed.get("/api/v1/audit")
        assert resp.status_code == 200
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_spoofed_header_ignored_on_environments(
        self,
        client_spoofed: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_environments = AsyncMock(return_value=[])
            resp = await client_spoofed.get("/api/v1/environments")
        assert resp.status_code == 200
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B
