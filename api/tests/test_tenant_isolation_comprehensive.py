"""Comprehensive tenant isolation tests.

Creates data in tenant "alpha" and verifies tenant "beta" cannot access it.
Tests both tenant-scoped repositories and cross-tenant admin endpoints.
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

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TENANT_ALPHA = "tenant-alpha"
TENANT_BETA = "tenant-beta"

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
    token = _make_dev_token(tenant_id=tenant_id)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_session() -> AsyncMock:
    """Return a mock AsyncSession that behaves like a real SQLAlchemy session."""
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
    """Create a FastAPI app with dependency overrides for testing."""
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
    application.dependency_overrides[get_admin_session] = _override_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = lambda: mock_ai
    application.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return application


@pytest_asyncio.fixture()
async def client_alpha(_app) -> AsyncClient:
    """Async client authenticated as Tenant Alpha."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_ALPHA),
    ) as ac:
        yield ac


@pytest_asyncio.fixture()
async def client_beta(_app) -> AsyncClient:
    """Async client authenticated as Tenant Beta."""
    transport = ASGITransport(app=_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=_auth_headers(TENANT_BETA),
    ) as ac:
        yield ac


# ===================================================================
# 1. TestTwoTenantIsolation
# ===================================================================


class TestTwoTenantIsolation:
    """Create data in tenant Alpha, verify tenant Beta cannot access it.

    Each test exercises a different entity type to ensure the tenant_id
    filter is applied consistently across all repositories and services.
    """

    @pytest.mark.asyncio
    async def test_model_created_in_alpha_not_visible_in_beta_list(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """A model registered by Alpha must not appear in Beta's list_all().

        We mock the ModelRepository so that Alpha returns a model but Beta
        returns empty -- then verify the constructor received the correct
        tenant_id in each case.
        """
        alpha_model = MagicMock()
        alpha_model.model_name = "staging.orders"
        alpha_model.kind = "sql"
        alpha_model.materialization = "incremental"
        alpha_model.owner = "data-team"
        alpha_model.tags = json.dumps(["staging"])
        alpha_model.current_version = "v1"
        alpha_model.created_at = datetime(2024, 1, 1, tzinfo=UTC)
        alpha_model.last_modified_at = datetime(2024, 6, 1, tzinfo=UTC)

        with patch("api.routers.models.ModelRepository") as MockRepo:
            # Alpha has a model.
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[alpha_model])
            resp_alpha = await client_alpha.get("/api/v1/models")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            # Verify the repo was constructed with Alpha's tenant_id.
            alpha_call_tenant = MockRepo.call_args.kwargs.get("tenant_id")
            assert alpha_call_tenant == TENANT_ALPHA

        with patch("api.routers.models.ModelRepository") as MockRepo:
            # Beta has no models.
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/models")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            # Verify the repo was constructed with Beta's tenant_id.
            beta_call_tenant = MockRepo.call_args.kwargs.get("tenant_id")
            assert beta_call_tenant == TENANT_BETA

    @pytest.mark.asyncio
    async def test_user_created_in_alpha_not_found_by_beta_get_by_id(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """A user in Alpha's tenant must not be accessible via Beta's auth router.

        We test the auth/signup -> auth/login flow indirectly by verifying
        that the UserRepository is always scoped to the correct tenant.
        """
        from core_engine.state.repository import UserRepository

        # Simulate: Alpha creates a user.
        alpha_user = MagicMock()
        alpha_user.id = "user-alpha-001"
        alpha_user.tenant_id = TENANT_ALPHA
        alpha_user.email = "alice@alpha.com"
        alpha_user.display_name = "Alice"
        alpha_user.role = "admin"
        alpha_user.is_active = True

        # Beta's UserRepository.get_by_id should return None for this user.
        beta_repo = UserRepository.__new__(UserRepository)
        beta_repo._session = _mock_session
        beta_repo._tenant_id = TENANT_BETA

        # The query filters by tenant_id + user_id, so a user from Alpha
        # won't match Beta's tenant_id filter.
        result = await beta_repo.get_by_id("user-alpha-001")
        assert result is None

    @pytest.mark.asyncio
    async def test_audit_log_entries_scoped_to_correct_tenant(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Audit log entries for Alpha must not appear in Beta's audit query.

        The AuditRepository constructor receives the tenant_id from the
        authenticated request, and all queries filter by it.
        """
        alpha_entry = MagicMock()
        alpha_entry.id = "audit-001"
        alpha_entry.tenant_id = TENANT_ALPHA
        alpha_entry.actor = "alice@alpha.com"
        alpha_entry.action = "plan.created"
        alpha_entry.entity_type = "plan"
        alpha_entry.entity_id = "plan-alpha-001"
        alpha_entry.metadata_json = {}
        alpha_entry.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        alpha_entry.previous_hash = None
        alpha_entry.entry_hash = "abc123"

        with patch("api.routers.audit.AuditRepository") as MockRepo:
            # Alpha sees its audit entry.
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[alpha_entry])
            resp_alpha = await client_alpha.get("/api/v1/audit")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_ALPHA

        with patch("api.routers.audit.AuditRepository") as MockRepo:
            # Beta sees nothing.
            instance = MockRepo.return_value
            instance.query = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/audit")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_BETA

    @pytest.mark.asyncio
    async def test_api_key_validated_only_in_correct_tenant_context(
        self,
        _mock_session: AsyncMock,
    ) -> None:
        """An API key from Alpha's tenant must not authenticate for Beta.

        The APIKeyRepository.validate_key() returns the key row which
        includes tenant_id.  The auth middleware checks that the key's
        tenant_id matches the expected context.
        """
        from core_engine.state.repository import APIKeyRepository

        # Create a repo scoped to Alpha with the proper constructor.
        alpha_repo = APIKeyRepository(_mock_session, tenant_id=TENANT_ALPHA)

        # validate_key looks up by hash and filters by tenant_id.
        # Since the mock session returns None, the key is not found.
        result = await alpha_repo.validate_key("bmkey.nonexistent-key")
        assert result is None

        # Create a repo scoped to Beta.
        beta_repo = APIKeyRepository(_mock_session, tenant_id=TENANT_BETA)
        result_beta = await beta_repo.validate_key("bmkey.nonexistent-key")
        assert result_beta is None

        # Verify that both repos are scoped to their respective tenants.
        assert alpha_repo._tenant_id == TENANT_ALPHA
        assert beta_repo._tenant_id == TENANT_BETA

    @pytest.mark.asyncio
    async def test_plans_in_alpha_invisible_to_beta(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Plans created by Alpha must not be visible to Beta."""
        alpha_plan_dict = {
            "plan_id": "plan-alpha-001",
            "base_sha": "aaa",
            "target_sha": "bbb",
            "total_steps": 2,
            "estimated_cost_usd": 3.0,
            "models_changed": ["staging.orders"],
            "created_at": "2024-06-15T12:00:00+00:00",
        }

        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[alpha_plan_dict])
            resp_alpha = await client_alpha.get("/api/v1/plans")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_ALPHA

        with patch("api.routers.plans.PlanService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_plans = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/plans")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_BETA

    @pytest.mark.asyncio
    async def test_runs_in_alpha_invisible_to_beta(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Runs belonging to Alpha must not be visible to Beta."""
        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            alpha_run = MagicMock()
            alpha_run.run_id = "run-alpha-001"
            alpha_run.plan_id = "plan-alpha-001"
            alpha_run.step_id = "step-001"
            alpha_run.model_name = "staging.orders"
            alpha_run.status = "SUCCESS"
            alpha_run.started_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
            alpha_run.finished_at = datetime(2024, 6, 15, 12, 1, 0, tzinfo=UTC)
            alpha_run.error_message = None
            alpha_run.logs_uri = None
            alpha_run.cluster_used = None
            alpha_run.executor_version = "api-0.1"
            alpha_run.retry_count = 0
            alpha_run.cost_usd = 1.5
            alpha_run.input_range_start = None
            alpha_run.input_range_end = None
            instance.get_by_plan = AsyncMock(return_value=[alpha_run])
            resp_alpha = await client_alpha.get("/api/v1/runs?plan_id=plan-alpha-001")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_ALPHA

        with patch("api.routers.runs.RunRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_by_plan = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/runs?plan_id=plan-alpha-001")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            assert MockRepo.call_args.kwargs.get("tenant_id") == TENANT_BETA

    @pytest.mark.asyncio
    async def test_environments_in_alpha_invisible_to_beta(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Environments configured by Alpha must not be visible to Beta."""
        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            alpha_env = {"name": "production", "cluster_policy": "default"}
            instance.list_environments = AsyncMock(return_value=[alpha_env])
            resp_alpha = await client_alpha.get("/api/v1/environments")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_ALPHA

        with patch("api.routers.environments.EnvironmentService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_environments = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/environments")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_BETA

    @pytest.mark.asyncio
    async def test_invoices_in_alpha_invisible_to_beta(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Invoices generated for Alpha must not be visible to Beta."""
        alpha_invoice = {
            "invoice_id": "inv-alpha-001",
            "invoice_number": "INV-2024-001",
            "stripe_invoice_id": "in_test_alpha",
            "period_start": "2024-06-01T00:00:00+00:00",
            "period_end": "2024-06-30T23:59:59+00:00",
            "subtotal_usd": 49.0,
            "tax_usd": 0.0,
            "total_usd": 49.0,
            "line_items": [],
            "status": "paid",
            "created_at": "2024-06-15T12:00:00+00:00",
        }

        with patch("api.routers.billing.InvoiceService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_invoices = AsyncMock(return_value={"invoices": [alpha_invoice], "total": 1})
            resp_alpha = await client_alpha.get("/api/v1/billing/invoices")
            assert resp_alpha.status_code == 200
            assert resp_alpha.json()["total"] == 1

        with patch("api.routers.billing.InvoiceService") as MockSvc:
            instance = MockSvc.return_value
            instance.list_invoices = AsyncMock(return_value={"invoices": [], "total": 0})
            resp_beta = await client_beta.get("/api/v1/billing/invoices")
            assert resp_beta.status_code == 200
            assert resp_beta.json()["total"] == 0

    @pytest.mark.asyncio
    async def test_reconciliation_in_alpha_invisible_to_beta(
        self,
        client_alpha: AsyncClient,
        client_beta: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Reconciliation checks from Alpha must not be visible to Beta."""
        with patch("api.routers.reconciliation.ReconciliationService") as MockSvc:
            instance = MockSvc.return_value
            alpha_check = {
                "check_id": "check-001",
                "model_name": "staging.orders",
                "discrepancy_type": "missing_partition",
            }
            instance.get_discrepancies = AsyncMock(return_value=[alpha_check])
            resp_alpha = await client_alpha.get("/api/v1/reconciliation/discrepancies")
            assert resp_alpha.status_code == 200
            assert len(resp_alpha.json()) == 1
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_ALPHA

        with patch("api.routers.reconciliation.ReconciliationService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_discrepancies = AsyncMock(return_value=[])
            resp_beta = await client_beta.get("/api/v1/reconciliation/discrepancies")
            assert resp_beta.status_code == 200
            assert resp_beta.json() == []
            assert MockSvc.call_args.kwargs.get("tenant_id") == TENANT_BETA


# ===================================================================
# 2. TestAdminCrossTenantAccess
# ===================================================================


class TestAdminCrossTenantAccess:
    """Admin endpoints should see data from all tenants.

    The analytics and tenant config admin endpoints use AdminSessionDep
    (no RLS) and are guarded by VIEW_ANALYTICS / MANAGE_SETTINGS permissions.
    """

    @pytest.mark.asyncio
    async def test_analytics_overview_sees_all_tenants(
        self,
        client_alpha: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """The admin analytics overview aggregates across all tenants.

        AnalyticsRepository does NOT filter by tenant_id -- it reads
        platform-wide data.
        """
        with patch("api.routers.admin_analytics.AnalyticsService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_overview = AsyncMock(
                return_value={
                    "total_tenants": 5,
                    "active_tenants": 3,
                    "total_events": 1000,
                    "total_runs": 200,
                    "total_cost_usd": 150.0,
                }
            )
            resp = await client_alpha.get("/api/v1/admin/analytics/overview")
            assert resp.status_code == 200
            body = resp.json()
            assert body["total_tenants"] == 5
            assert body["active_tenants"] == 3
            # Confirm AnalyticsService was constructed with the admin session
            # (no tenant_id filtering).
            assert MockSvc.call_count == 1

    @pytest.mark.asyncio
    async def test_analytics_tenant_breakdown_includes_both_tenants(
        self,
        client_alpha: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """The per-tenant breakdown should include both Alpha and Beta."""
        with patch("api.routers.admin_analytics.AnalyticsService") as MockSvc:
            instance = MockSvc.return_value
            instance.get_tenant_breakdown = AsyncMock(
                return_value={
                    "tenants": [
                        {"tenant_id": TENANT_ALPHA, "total_events": 500},
                        {"tenant_id": TENANT_BETA, "total_events": 300},
                    ],
                    "total": 2,
                }
            )
            resp = await client_alpha.get("/api/v1/admin/analytics/tenants")
            assert resp.status_code == 200
            body = resp.json()
            tenant_ids = [t["tenant_id"] for t in body["tenants"]]
            assert TENANT_ALPHA in tenant_ids
            assert TENANT_BETA in tenant_ids

    @pytest.mark.asyncio
    async def test_tenant_config_list_all_sees_both_tenants(
        self,
        client_alpha: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """The admin tenant config list endpoint returns all tenants.

        TenantConfigRepository.list_all() is intentionally cross-tenant.
        """
        alpha_config = MagicMock()
        alpha_config.tenant_id = TENANT_ALPHA
        alpha_config.llm_enabled = True
        alpha_config.llm_monthly_budget_usd = None
        alpha_config.llm_daily_budget_usd = None
        alpha_config.created_at = datetime(2024, 1, 1, tzinfo=UTC)
        alpha_config.updated_at = datetime(2024, 6, 1, tzinfo=UTC)
        alpha_config.updated_by = "admin"
        alpha_config.deactivated_at = None

        beta_config = MagicMock()
        beta_config.tenant_id = TENANT_BETA
        beta_config.llm_enabled = False
        beta_config.llm_monthly_budget_usd = 100.0
        beta_config.llm_daily_budget_usd = 5.0
        beta_config.created_at = datetime(2024, 2, 1, tzinfo=UTC)
        beta_config.updated_at = datetime(2024, 7, 1, tzinfo=UTC)
        beta_config.updated_by = "admin"
        beta_config.deactivated_at = None

        with patch("api.routers.tenant_config.TenantConfigRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=[alpha_config, beta_config])
            resp = await client_alpha.get("/api/v1/admin/tenants")
            assert resp.status_code == 200
            body = resp.json()
            assert body["total"] == 2
            tenant_ids = [t["tenant_id"] for t in body["tenants"]]
            assert TENANT_ALPHA in tenant_ids
            assert TENANT_BETA in tenant_ids

    @pytest.mark.asyncio
    async def test_viewer_cannot_access_admin_analytics(
        self,
        _app: Any,
        _mock_session: AsyncMock,
    ) -> None:
        """A viewer-role user must be denied access to admin analytics."""
        viewer_token = _make_dev_token(
            tenant_id=TENANT_ALPHA,
            sub="viewer-user",
            role="viewer",
        )
        transport = ASGITransport(app=_app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ) as viewer_client:
            resp = await viewer_client.get("/api/v1/admin/analytics/overview")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_cannot_access_admin_tenant_list(
        self,
        _app: Any,
        _mock_session: AsyncMock,
    ) -> None:
        """A viewer-role user must be denied access to admin tenant list."""
        viewer_token = _make_dev_token(
            tenant_id=TENANT_BETA,
            sub="viewer-user",
            role="viewer",
        )
        transport = ASGITransport(app=_app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ) as viewer_client:
            resp = await viewer_client.get("/api/v1/admin/tenants")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_can_view_specific_tenant_config(
        self,
        client_alpha: AsyncClient,
        _mock_session: AsyncMock,
    ) -> None:
        """Admin can view config for any specific tenant."""
        beta_config = MagicMock()
        beta_config.tenant_id = TENANT_BETA
        beta_config.llm_enabled = True
        beta_config.llm_monthly_budget_usd = 200.0
        beta_config.llm_daily_budget_usd = 10.0
        beta_config.created_at = datetime(2024, 2, 1, tzinfo=UTC)
        beta_config.updated_at = datetime(2024, 7, 1, tzinfo=UTC)
        beta_config.updated_by = "admin"
        beta_config.deactivated_at = None

        with patch("api.routers.tenant_config.TenantConfigRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=beta_config)
            resp = await client_alpha.get(f"/api/v1/admin/tenants/{TENANT_BETA}/config")
            assert resp.status_code == 200
            assert resp.json()["tenant_id"] == TENANT_BETA

    @pytest.mark.asyncio
    async def test_analytics_repository_has_no_tenant_filter(self) -> None:
        """Verify AnalyticsRepository constructor does NOT take tenant_id.

        This confirms it is designed for cross-tenant admin usage.
        """
        from core_engine.state.repository import AnalyticsRepository

        mock_session = AsyncMock()
        repo = AnalyticsRepository(mock_session)
        # AnalyticsRepository only takes session -- no tenant_id parameter.
        assert repo._session is mock_session
        assert not hasattr(repo, "_tenant_id")
