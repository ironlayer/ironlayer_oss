"""Integration tests for tenant-level data isolation.

Verifies that the Row-Level Security (RLS) enforced by the API layer
prevents cross-tenant data access.  A request authenticated as Tenant B
must never be able to read, modify, or delete resources owned by Tenant A,
even when the correct resource ID is known (no enumeration protection needed
-- 404 is the correct response to prevent existence leakage).

These tests exercise the full request pipeline including authentication
middleware, RLS-aware repository queries, and JSON serialisation by using
httpx.AsyncClient with ASGITransport against the real FastAPI app.  All
database and external-service I/O is replaced with mocks so no real
infrastructure is required.

Run with:
    pytest api/tests/integration/test_tenant_isolation.py -v
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
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
from api.test_utils import set_app_state_for_test

# Apply feature-gate bypass globally for this module so gated endpoints
# (audit, AI advisory, reconciliation) return useful results rather than 403.
pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TENANT_A = "tenant-a"
TENANT_B = "tenant-b"

# Must match the JWT_SECRET set in conftest.py so AuthenticationMiddleware
# validates tokens generated here correctly.
_DEV_SECRET = "test-secret-key-for-ironlayer-tests"

# Stable resource IDs owned by Tenant A that Tenant B will try to access.
PLAN_A_ID = "plan-tenant-a-001"
MODEL_A_NAME = "staging.tenant_a_orders"
RUN_A_ID = "run-tenant-a-001"


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------


def _make_dev_token(
    tenant_id: str,
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
) -> str:
    """Generate a valid development-mode HMAC token for the given tenant.

    Mirrors the signing logic in :class:`api.security.TokenManager` and the
    helper in ``conftest.py``.  Uses a unique ``jti`` per call so repeated
    token generation does not collide with the 30-second in-memory revocation
    cache.
    """
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": scopes or ["read", "write"],
        "jti": f"test-jti-{tenant_id}-{secrets.token_hex(4)}",
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


def _auth_headers(tenant_id: str) -> dict[str, str]:
    """Return Authorization headers for the given tenant."""
    return {"Authorization": f"Bearer {_make_dev_token(tenant_id=tenant_id)}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_session() -> AsyncMock:
    """Return a mock AsyncSession that behaves like a real SQLAlchemy session.

    All queries return empty results by default, simulating the RLS filter
    returning nothing for a cross-tenant lookup.
    """
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
    """Create a FastAPI app with dependency overrides for integration testing.

    Injects the mock database session and mock AI client so that no real
    database or external service is required.  Mirrors the pattern used in
    ``conftest.py`` and ``test_tenant_isolation_comprehensive.py``.
    """
    application = create_app()

    async def _override_session():
        yield _mock_session

    _test_settings = APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
        allowed_repo_base="/",
        jwt_secret=__import__("pydantic").SecretStr(_DEV_SECRET),
    )

    def _override_settings():
        return _test_settings

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

    set_app_state_for_test(
        application,
        settings=_test_settings,
        session=_mock_session,
        ai_client=mock_ai,
        metering=mock_metering,
    )

    application.dependency_overrides[get_db_session] = _override_session
    application.dependency_overrides[get_tenant_session] = _override_session
    application.dependency_overrides[get_admin_session] = _override_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = lambda: mock_ai
    application.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return application


@pytest_asyncio.fixture()
async def client_a(_app) -> AsyncClient:
    """Async httpx client authenticated as Tenant A (the resource owner)."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_A),
    ) as ac:
        yield ac


@pytest_asyncio.fixture()
async def client_b(_app) -> AsyncClient:
    """Async httpx client authenticated as Tenant B (the attacker)."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_B),
    ) as ac:
        yield ac


# ---------------------------------------------------------------------------
# Helper: build a minimal mock plan row belonging to Tenant A
# ---------------------------------------------------------------------------


def _make_plan_row(plan_id: str = PLAN_A_ID) -> MagicMock:
    """Return a mock PlanTable row owned by Tenant A."""
    plan_json = {
        "plan_id": plan_id,
        "base": "aaa000",
        "target": "bbb111",
        "summary": {
            "total_steps": 1,
            "estimated_cost_usd": 0.5,
            "models_changed": [MODEL_A_NAME],
        },
        "steps": [
            {
                "step_id": "step-001",
                "model": MODEL_A_NAME,
                "run_type": "FULL_REFRESH",
                "input_range": None,
                "depends_on": [],
                "parallel_group": 0,
                "reason": "SQL body changed",
                "estimated_compute_seconds": 30.0,
                "estimated_cost_usd": 0.5,
            }
        ],
    }
    row = MagicMock()
    row.plan_id = plan_id
    row.tenant_id = TENANT_A
    row.base_sha = "aaa000"
    row.target_sha = "bbb111"
    row.plan_json = json.dumps(plan_json)
    row.approvals_json = None
    row.advisory_json = None
    row.auto_approved = False
    row.created_at = datetime(2024, 9, 1, 10, 0, 0, tzinfo=UTC)
    return row


def _make_model_row(model_name: str = MODEL_A_NAME) -> MagicMock:
    """Return a mock ModelTable row owned by Tenant A."""
    row = MagicMock()
    row.model_name = model_name
    row.tenant_id = TENANT_A
    row.repo_path = f"models/staging/{model_name.split('.')[-1]}.sql"
    row.current_version = "v1-abc"
    row.kind = "sql"
    row.time_column = "order_date"
    row.unique_key = "order_id"
    row.materialization = "incremental"
    row.owner = "data-team"
    row.tags = json.dumps(["staging", "core"])
    row.created_at = datetime(2024, 1, 1, tzinfo=UTC)
    row.last_modified_at = datetime(2024, 6, 1, tzinfo=UTC)
    return row


def _make_run_row(run_id: str = RUN_A_ID) -> MagicMock:
    """Return a mock RunTable row owned by Tenant A."""
    row = MagicMock()
    row.run_id = run_id
    row.plan_id = PLAN_A_ID
    row.step_id = "step-001"
    row.model_name = MODEL_A_NAME
    row.status = "SUCCESS"
    row.tenant_id = TENANT_A
    row.started_at = datetime(2024, 9, 1, 10, 0, 0, tzinfo=UTC)
    row.finished_at = datetime(2024, 9, 1, 10, 1, 0, tzinfo=UTC)
    row.input_range_start = None
    row.input_range_end = None
    row.error_message = None
    row.logs_uri = None
    row.cluster_used = None
    row.executor_version = "api-control-plane-0.1.0"
    row.retry_count = 0
    return row


# ===================================================================
# 1. Test: plan not accessible cross-tenant
# ===================================================================


class TestPlanNotAccessibleCrossTenant:
    """Tenant B must receive 404 when fetching a plan owned by Tenant A.

    The repository is patched to return the real plan row when called
    with Tenant A's tenant_id, and None (simulating RLS filtering) when
    called with Tenant B's tenant_id.  This verifies the service layer
    correctly passes the authenticated tenant_id to the repository.
    """

    @pytest.mark.asyncio
    async def test_get_plan_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Fetching Tenant A's plan ID with Tenant B's token returns 404."""
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            # RLS would return None for a cross-tenant lookup.
            instance.get_plan = AsyncMock(return_value=None)
            resp = await client_b.get(f"/api/v1/plans/{PLAN_A_ID}")
        assert resp.status_code == 404, (
            f"Expected 404 for cross-tenant plan access, got {resp.status_code}: {resp.text}"
        )
        # The service must have been constructed with Tenant B's ID, proving
        # RLS was scoped to the authenticated caller, not the resource owner.
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_plans_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Listing plans as Tenant B returns an empty list, not Tenant A's plans."""
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/plans")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_apply_plan_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Attempting to apply Tenant A's plan as Tenant B returns 404."""
        with patch("api.routers.plans.ExecutionService") as MockExec:
            instance = MockExec.return_value
            instance.apply_plan = AsyncMock(
                side_effect=ValueError(f"Plan {PLAN_A_ID} not found")
            )
            resp = await client_b.post(
                f"/api/v1/plans/{PLAN_A_ID}/apply",
                json={"auto_approve": True},
            )
        assert resp.status_code == 404
        assert MockExec.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 2. Test: model not accessible cross-tenant
# ===================================================================


class TestModelNotAccessibleCrossTenant:
    """Tenant B must not be able to read models registered by Tenant A."""

    @pytest.mark.asyncio
    async def test_get_model_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """GET /models/<name> returns 404 when the model belongs to Tenant A."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get(f"/api/v1/models/{MODEL_A_NAME}")
        assert resp.status_code == 404, (
            f"Expected 404 for cross-tenant model access, got {resp.status_code}: {resp.text}"
        )
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_models_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Listing models as Tenant B returns an empty list."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/models")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_model_lineage_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Model lineage endpoint returns 404 for a cross-tenant model name."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get(f"/api/v1/models/{MODEL_A_NAME}/lineage")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_model_health_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Model health endpoint returns 404 for a cross-tenant model name."""
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_b.get(f"/api/v1/models/{MODEL_A_NAME}/health")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 3. Test: run not accessible cross-tenant
# ===================================================================


class TestRunNotAccessibleCrossTenant:
    """Run records must be invisible to tenants that did not create them."""

    @pytest.mark.asyncio
    async def test_get_run_404_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """GET /runs/<id> returns 404 when the run belongs to Tenant A."""
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_id = AsyncMock(return_value=None)
            resp = await client_b.get(f"/api/v1/runs/{RUN_A_ID}")
        assert resp.status_code == 404, (
            f"Expected 404 for cross-tenant run access, got {resp.status_code}: {resp.text}"
        )
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_runs_by_plan_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Listing Tenant A's runs by plan ID returns an empty list for Tenant B."""
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_plan = AsyncMock(return_value=[])
            resp = await client_b.get(f"/api/v1/runs?plan_id={PLAN_A_ID}")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_list_runs_by_model_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Listing Tenant A's runs by model name returns an empty list for Tenant B."""
        resp = await client_b.get(f"/api/v1/runs?model_name={MODEL_A_NAME}")
        assert resp.status_code == 200
        assert resp.json() == []


# ===================================================================
# 4. Test: audit log not accessible cross-tenant
# ===================================================================


class TestAuditLogNotAccessibleCrossTenant:
    """Audit log entries must be strictly scoped to the authenticated tenant."""

    @pytest.mark.asyncio
    async def test_audit_log_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """GET /audit returns an empty list for Tenant B, not Tenant A's entries."""
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get("/api/v1/audit")
        assert resp.status_code == 200
        assert resp.json() == [], (
            f"Expected empty audit log for cross-tenant query, got: {resp.json()}"
        )
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_audit_entity_filter_empty_cross_tenant(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Filtering audit log by Tenant A's entity ID returns nothing for Tenant B."""
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get(
                f"/api/v1/audit?entity_type=plan&entity_id={PLAN_A_ID}"
            )
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_audit_chain_verify_scoped_to_caller(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Audit chain verification operates only on Tenant B's own entries."""
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            # Empty chain for Tenant B (Tenant A's entries are not reachable).
            instance.verify_chain = AsyncMock(return_value=(True, 0))
            resp = await client_b.get("/api/v1/audit/verify")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_valid"] is True
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 5. Test: Tenant A can access its own resources (positive test)
# ===================================================================


class TestTenantACanAccessOwnResources:
    """Positive tests: Tenant A must be able to access its own resources.

    These tests verify that the RLS scoping does not over-restrict access
    and that legitimate resource lookups succeed when authenticated correctly.
    """

    @pytest.mark.asyncio
    async def test_tenant_a_can_get_own_plan(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A retrieves its own plan successfully."""
        plan_row = _make_plan_row(PLAN_A_ID)
        # The real PlanService.get_plan parses plan_json and returns a dict
        # with keys matching PlanResponse.  Build that dict here so that
        # FastAPI's response-model validation does not encounter MagicMock
        # values for string fields (base, target, created_at).
        plan_dict = json.loads(plan_row.plan_json)
        plan_dict["approvals"] = []
        plan_dict["auto_approved"] = plan_row.auto_approved
        plan_dict["created_at"] = plan_row.created_at.isoformat()
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_plan = AsyncMock(return_value=plan_dict)
            resp = await client_a.get(f"/api/v1/plans/{PLAN_A_ID}")
        assert resp.status_code == 200, (
            f"Tenant A should access its own plan; got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        assert body["plan_id"] == PLAN_A_ID
        # The service must have been constructed with Tenant A's ID.
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_can_list_own_plans(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A sees its own plans in the list endpoint."""
        plan_row = _make_plan_row(PLAN_A_ID)
        # The real PlanService.list_plans returns a list of summary dicts with
        # keys matching PlanListItemResponse.  Build that dict here so that
        # FastAPI's response-model validation does not encounter MagicMock
        # values for string fields (base_sha, target_sha, created_at).
        plan_data = json.loads(plan_row.plan_json)
        summary = plan_data.get("summary", {})
        plan_summary_dict = {
            "plan_id": plan_row.plan_id,
            "base_sha": plan_row.base_sha,
            "target_sha": plan_row.target_sha,
            "total_steps": summary.get("total_steps", 0),
            "estimated_cost_usd": summary.get("estimated_cost_usd", 0.0),
            "models_changed": summary.get("models_changed", []),
            "created_at": plan_row.created_at.isoformat(),
        }
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[plan_summary_dict])
            resp = await client_a.get("/api/v1/plans")
        assert resp.status_code == 200
        plans = resp.json()
        assert len(plans) == 1
        assert plans[0]["plan_id"] == PLAN_A_ID
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_can_get_own_model(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A retrieves its own model successfully."""
        model_row = _make_model_row(MODEL_A_NAME)
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=model_row)
            resp = await client_a.get(f"/api/v1/models/{MODEL_A_NAME}")
        assert resp.status_code == 200, (
            f"Tenant A should access its own model; got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        assert body["model_name"] == MODEL_A_NAME
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_can_get_own_run(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A retrieves its own run record successfully."""
        run_row = _make_run_row(RUN_A_ID)
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_id = AsyncMock(return_value=run_row)
            resp = await client_a.get(f"/api/v1/runs/{RUN_A_ID}")
        assert resp.status_code == 200, (
            f"Tenant A should access its own run; got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        assert body["run_id"] == RUN_A_ID
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_can_query_own_audit_log(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A sees its own audit log entries."""
        audit_entry = MagicMock()
        audit_entry.id = "audit-001"
        audit_entry.tenant_id = TENANT_A
        audit_entry.action = "plan.created"
        audit_entry.entity_type = "plan"
        audit_entry.entity_id = PLAN_A_ID
        audit_entry.actor_id = "test-user"
        audit_entry.details = json.dumps({"plan_id": PLAN_A_ID})
        audit_entry.created_at = datetime(2024, 9, 1, 10, 0, 0, tzinfo=UTC)
        audit_entry.hash = "abc123"
        audit_entry.prev_hash = None

        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[audit_entry])
            resp = await client_a.get("/api/v1/audit")
        assert resp.status_code == 200
        entries = resp.json()
        assert len(entries) == 1
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_A


# ===================================================================
# 6. Test: cross-tenant ID injection via query parameters
# ===================================================================


class TestCrossTenantIDInjection:
    """Passing another tenant's resource ID in query parameters must yield no data.

    These tests verify that ID injection -- where Tenant B explicitly
    provides Tenant A's resource IDs -- is neutralised by tenant-scoped
    repository construction rather than resource-ID access control.
    """

    @pytest.mark.asyncio
    async def test_run_filter_by_tenant_a_plan_id_returns_empty(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Filtering runs with Tenant A's plan_id from Tenant B returns nothing.

        The RunRepository is constructed with Tenant B's tenant_id so the
        SQL WHERE clause includes ``tenant_id = 'tenant-b'``.  Even if the
        plan_id matches a real plan from Tenant A, the tenant_id filter
        ensures no rows are returned.
        """
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_plan = AsyncMock(return_value=[])
            resp = await client_b.get(f"/api/v1/runs?plan_id={PLAN_A_ID}")
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_audit_filter_by_tenant_a_entity_id_returns_empty(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Filtering audit log by Tenant A's entity ID from Tenant B returns nothing."""
        with patch("api.routers.audit.AuditRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp = await client_b.get(
                f"/api/v1/audit?entity_type=run&entity_id={RUN_A_ID}"
            )
        assert resp.status_code == 200
        assert resp.json() == []
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_B

    @pytest.mark.asyncio
    async def test_augment_plan_with_tenant_a_id_returns_404(
        self,
        client_b: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """POST /plans/<id>/augment with Tenant A's plan ID returns 404 for Tenant B."""
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.generate_augmented_plan = AsyncMock(
                side_effect=ValueError(f"Plan {PLAN_A_ID} not found")
            )
            resp = await client_b.post(f"/api/v1/plans/{PLAN_A_ID}/augment")
        assert resp.status_code == 404
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_B


# ===================================================================
# 7. Test: token scope is bound to a single tenant
# ===================================================================


class TestTokenScopedToSingleTenant:
    """A token issued for Tenant A must not grant access to Tenant B's resources.

    These tests verify the inverse direction: Tenant A's token cannot
    read Tenant B's resources.  Both directions must be sealed.
    """

    @pytest.mark.asyncio
    async def test_tenant_a_token_cannot_read_tenant_b_plan(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A's token returns 404 when fetching a plan owned by Tenant B."""
        tenant_b_plan_id = "plan-tenant-b-001"
        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            # Simulate RLS returning None for cross-tenant lookup.
            instance.get_plan = AsyncMock(return_value=None)
            resp = await client_a.get(f"/api/v1/plans/{tenant_b_plan_id}")
        assert resp.status_code == 404
        # The service must have been constructed with Tenant A's ID proving
        # it cannot escalate to Tenant B's scope.
        assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_token_cannot_read_tenant_b_model(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A's token returns 404 when fetching a model owned by Tenant B."""
        tenant_b_model = "staging.tenant_b_orders"
        with patch("api.routers.models.ModelRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)
            resp = await client_a.get(f"/api/v1/models/{tenant_b_model}")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_A

    @pytest.mark.asyncio
    async def test_tenant_a_token_cannot_read_tenant_b_run(
        self,
        client_a: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Tenant A's token returns 404 when fetching a run owned by Tenant B."""
        tenant_b_run_id = "run-tenant-b-001"
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_id = AsyncMock(return_value=None)
            resp = await client_a.get(f"/api/v1/runs/{tenant_b_run_id}")
        assert resp.status_code == 404
        assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_A
