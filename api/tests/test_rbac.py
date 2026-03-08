"""Tests for the Role-Based Access Control system.

Covers:
- Role enum ordering and hierarchy
- Permission mapping per role
- require_permission dependency enforcement
- require_role dependency enforcement
- Integration with auth middleware (role extraction from JWT)
- Default role assignment when token omits role claim
- Invalid role rejection
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
from api.middleware.rbac import (
    ROLE_PERMISSIONS,
    Permission,
    Role,
    get_user_role,
    parse_role,
    require_permission,
    require_role,
    role_has_permission,
)

# ---------------------------------------------------------------------------
# Helpers: generate development-mode tokens with role claims
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_dev_token(
    tenant_id: str = "test-tenant",
    sub: str = "testuser@example.com",
    role: str | None = None,
    scopes: list[str] | None = None,
    expired: bool = False,
) -> str:
    """Generate a valid development-mode HMAC token with an optional role claim.

    Mirrors the signing logic in :class:`api.security.TokenManager`.
    """
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": (now - 3600) if expired else (now + 3600),
        "scopes": scopes or ["read", "write"],
    }
    if role is not None:
        payload["role"] = role

    payload_json = json.dumps(payload)
    signature = hmac.new(
        _DEV_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token_bytes = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{token_bytes}.{signature}"


def _auth_header(role: str | None = None, **kwargs: Any) -> dict[str, str]:
    """Return an ``Authorization: Bearer ...`` header dict."""
    token = _make_dev_token(role=role, **kwargs)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Unit tests: Role enum
# ---------------------------------------------------------------------------


class TestRoleEnum:
    """Verify Role ordering and hierarchy."""

    def test_hierarchy_order(self) -> None:
        assert Role.VIEWER < Role.OPERATOR < Role.ENGINEER < Role.ADMIN

    def test_viewer_is_lowest(self) -> None:
        assert Role.VIEWER == 0

    def test_admin_is_highest(self) -> None:
        assert Role.ADMIN == 3


# ---------------------------------------------------------------------------
# Unit tests: parse_role
# ---------------------------------------------------------------------------


class TestParseRole:
    """Verify string-to-Role mapping."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("viewer", Role.VIEWER),
            ("VIEWER", Role.VIEWER),
            ("Viewer", Role.VIEWER),
            ("operator", Role.OPERATOR),
            ("engineer", Role.ENGINEER),
            ("admin", Role.ADMIN),
            ("  admin  ", Role.ADMIN),
        ],
    )
    def test_valid_roles(self, raw: str, expected: Role) -> None:
        assert parse_role(raw) == expected

    def test_invalid_role_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown role"):
            parse_role("superadmin")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown role"):
            parse_role("")


# ---------------------------------------------------------------------------
# Unit tests: Permission mapping
# ---------------------------------------------------------------------------


class TestPermissionMapping:
    """Verify ROLE_PERMISSIONS is correctly configured."""

    def test_viewer_read_only(self) -> None:
        perms = ROLE_PERMISSIONS[Role.VIEWER]
        assert Permission.READ_PLANS in perms
        assert Permission.READ_MODELS in perms
        assert Permission.READ_RUNS in perms
        # Must not have write/mutate permissions.
        assert Permission.CREATE_PLANS not in perms
        assert Permission.APPROVE_PLANS not in perms
        assert Permission.APPLY_PLANS not in perms
        assert Permission.WRITE_MODELS not in perms
        assert Permission.CREATE_BACKFILLS not in perms
        assert Permission.MANAGE_CREDENTIALS not in perms

    def test_operator_extends_viewer(self) -> None:
        viewer_perms = ROLE_PERMISSIONS[Role.VIEWER]
        operator_perms = ROLE_PERMISSIONS[Role.OPERATOR]
        assert viewer_perms.issubset(operator_perms)
        assert Permission.APPROVE_PLANS in operator_perms
        assert Permission.CREATE_BACKFILLS in operator_perms
        assert Permission.READ_AUDIT in operator_perms
        # Must not have engineer permissions.
        assert Permission.CREATE_PLANS not in operator_perms

    def test_engineer_extends_operator(self) -> None:
        operator_perms = ROLE_PERMISSIONS[Role.OPERATOR]
        engineer_perms = ROLE_PERMISSIONS[Role.ENGINEER]
        assert operator_perms.issubset(engineer_perms)
        assert Permission.CREATE_PLANS in engineer_perms
        assert Permission.APPLY_PLANS in engineer_perms
        assert Permission.WRITE_MODELS in engineer_perms
        # Must not have admin-only permissions.
        assert Permission.MANAGE_CREDENTIALS not in engineer_perms

    def test_admin_has_all(self) -> None:
        admin_perms = ROLE_PERMISSIONS[Role.ADMIN]
        all_perms = set(Permission)
        assert all_perms == admin_perms

    def test_role_has_permission_helper(self) -> None:
        assert role_has_permission(Role.VIEWER, Permission.READ_PLANS) is True
        assert role_has_permission(Role.VIEWER, Permission.CREATE_PLANS) is False
        assert role_has_permission(Role.ADMIN, Permission.MANAGE_CREDENTIALS) is True


# ---------------------------------------------------------------------------
# Integration tests: full HTTP stack (auth middleware + RBAC)
# ---------------------------------------------------------------------------


@pytest.fixture()
def _test_settings() -> APISettings:
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


@pytest.fixture()
def _mock_session() -> AsyncMock:
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


@pytest.fixture()
def _mock_ai_client() -> AsyncMock:
    from api.services.ai_client import AIServiceClient

    client = AsyncMock(spec=AIServiceClient)
    client.health_check = AsyncMock(return_value=True)
    client.close = AsyncMock()
    return client


@pytest.fixture()
def rbac_app(
    _test_settings: APISettings,
    _mock_session: AsyncMock,
    _mock_ai_client: AsyncMock,
):
    """Create a FastAPI app configured for RBAC integration testing."""
    application = create_app()

    async def _override_session():
        yield _mock_session

    async def _override_tenant_session():
        yield _mock_session

    def _override_settings():
        return _test_settings

    def _override_ai_client():
        return _mock_ai_client

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    def _override_metering():
        return mock_metering

    application.dependency_overrides[get_db_session] = _override_session
    application.dependency_overrides[get_tenant_session] = _override_tenant_session
    application.dependency_overrides[get_settings] = _override_settings
    application.dependency_overrides[get_ai_client] = _override_ai_client
    application.dependency_overrides[get_metering_collector] = _override_metering

    return application


@pytest_asyncio.fixture()
async def rbac_client(rbac_app) -> AsyncClient:
    transport = ASGITransport(app=rbac_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# VIEWER role tests
# ---------------------------------------------------------------------------


class TestViewerRole:
    """VIEWER can read plans but cannot create or apply them."""

    @pytest.mark.asyncio
    async def test_viewer_can_read_plans(self, rbac_client: AsyncClient) -> None:
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.list_plans = AsyncMock(return_value=[])
            resp = await rbac_client.get(
                "/api/v1/plans",
                headers=_auth_header(role="viewer"),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_viewer_cannot_generate_plans(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "aaa",
                "target_sha": "bbb",
            },
            headers=_auth_header(role="viewer"),
        )
        assert resp.status_code == 403
        assert "permission denied" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_viewer_cannot_apply_plans(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/plans/some-plan/apply",
            json={"auto_approve": True},
            headers=_auth_header(role="viewer"),
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_cannot_approve_plans(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/plans/some-plan/approve",
            json={"user": "tester", "comment": "lgtm"},
            headers=_auth_header(role="viewer"),
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_cannot_create_backfills(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/backfills",
            json={
                "model_name": "staging.orders",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
            headers=_auth_header(role="viewer"),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# OPERATOR role tests
# ---------------------------------------------------------------------------


class TestOperatorRole:
    """OPERATOR inherits VIEWER + can approve plans and create backfills."""

    @pytest.mark.asyncio
    async def test_operator_can_read_plans(self, rbac_client: AsyncClient) -> None:
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.list_plans = AsyncMock(return_value=[])
            resp = await rbac_client.get(
                "/api/v1/plans",
                headers=_auth_header(role="operator"),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_operator_cannot_generate_plans(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "aaa",
                "target_sha": "bbb",
            },
            headers=_auth_header(role="operator"),
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_operator_cannot_apply_plans(self, rbac_client: AsyncClient) -> None:
        resp = await rbac_client.post(
            "/api/v1/plans/plan-id/apply",
            json={"auto_approve": True},
            headers=_auth_header(role="operator"),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# ENGINEER role tests
# ---------------------------------------------------------------------------


class TestEngineerRole:
    """ENGINEER can generate and apply plans."""

    @pytest.mark.asyncio
    async def test_engineer_can_generate_plans(self, rbac_client: AsyncClient) -> None:
        expected = {"plan_id": "new-plan", "steps": []}
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.generate_plan = AsyncMock(return_value=expected)
            with patch("api.routers.plans.AuditService") as MockAudit:
                MockAudit.return_value.log = AsyncMock()
                resp = await rbac_client.post(
                    "/api/v1/plans/generate",
                    json={
                        "repo_path": "/tmp/repo",
                        "base_sha": "aaa",
                        "target_sha": "bbb",
                    },
                    headers=_auth_header(role="engineer"),
                )
        assert resp.status_code == 200
        assert resp.json()["plan_id"] == "new-plan"

    @pytest.mark.asyncio
    async def test_engineer_can_apply_plans(self, rbac_client: AsyncClient) -> None:
        run_records = [
            {
                "run_id": "r1",
                "plan_id": "some-plan",
                "step_id": "step-1",
                "model_name": "staging.orders",
                "status": "SUCCESS",
            },
        ]
        with patch("api.routers.plans.ExecutionService") as MockExec:
            MockExec.return_value.apply_plan = AsyncMock(return_value=run_records)
            resp = await rbac_client.post(
                "/api/v1/plans/some-plan/apply",
                json={"auto_approve": True},
                headers=_auth_header(role="engineer"),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_engineer_can_read_plans(self, rbac_client: AsyncClient) -> None:
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.list_plans = AsyncMock(return_value=[])
            resp = await rbac_client.get(
                "/api/v1/plans",
                headers=_auth_header(role="engineer"),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_engineer_cannot_manage_credentials(self) -> None:
        """ENGINEER does not have MANAGE_CREDENTIALS permission."""
        assert not role_has_permission(Role.ENGINEER, Permission.MANAGE_CREDENTIALS)


# ---------------------------------------------------------------------------
# ADMIN role tests
# ---------------------------------------------------------------------------


class TestAdminRole:
    """ADMIN has full access including credential management."""

    @pytest.mark.asyncio
    async def test_admin_can_generate_plans(self, rbac_client: AsyncClient) -> None:
        expected = {"plan_id": "admin-plan", "steps": []}
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.generate_plan = AsyncMock(return_value=expected)
            with patch("api.routers.plans.AuditService") as MockAudit:
                MockAudit.return_value.log = AsyncMock()
                resp = await rbac_client.post(
                    "/api/v1/plans/generate",
                    json={
                        "repo_path": "/tmp/repo",
                        "base_sha": "aaa",
                        "target_sha": "bbb",
                    },
                    headers=_auth_header(role="admin"),
                )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_admin_can_apply_plans(self, rbac_client: AsyncClient) -> None:
        with patch("api.routers.plans.ExecutionService") as MockExec:
            MockExec.return_value.apply_plan = AsyncMock(return_value=[])
            resp = await rbac_client.post(
                "/api/v1/plans/some-plan/apply",
                json={"auto_approve": True},
                headers=_auth_header(role="admin"),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_admin_can_read_runs(self, rbac_client: AsyncClient) -> None:
        with patch("api.routers.runs.RunRepository") as MockRepo:
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = []
            rbac_client._transport.app  # noqa: ensure app is accessible
            # Use a mock session execute that returns empty results.
            resp = await rbac_client.get(
                "/api/v1/runs",
                headers=_auth_header(role="admin"),
            )
        # Even if the DB query fails due to mocking, RBAC should not block.
        assert resp.status_code != 403

    def test_admin_has_manage_credentials(self) -> None:
        assert role_has_permission(Role.ADMIN, Permission.MANAGE_CREDENTIALS)

    def test_admin_has_manage_settings(self) -> None:
        assert role_has_permission(Role.ADMIN, Permission.MANAGE_SETTINGS)

    def test_admin_inherits_all_permissions(self) -> None:
        admin_perms = ROLE_PERMISSIONS[Role.ADMIN]
        for perm in Permission:
            assert perm in admin_perms, f"ADMIN missing permission {perm.value}"


# ---------------------------------------------------------------------------
# Default role and invalid role tests
# ---------------------------------------------------------------------------


class TestDefaultAndInvalidRoles:
    """Verify behavior when role claim is missing or invalid."""

    @pytest.mark.asyncio
    async def test_missing_role_defaults_to_viewer(self, rbac_client: AsyncClient) -> None:
        """Token without a role claim should be treated as VIEWER."""
        with patch("api.routers.plans.PlanService") as MockService:
            MockService.return_value.list_plans = AsyncMock(return_value=[])
            # No role in token -- should default to viewer and allow reads.
            resp = await rbac_client.get(
                "/api/v1/plans",
                headers=_auth_header(role=None),
            )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_missing_role_cannot_create(self, rbac_client: AsyncClient) -> None:
        """Token without a role claim (defaults to VIEWER) cannot create plans."""
        resp = await rbac_client.post(
            "/api/v1/plans/generate",
            json={
                "repo_path": "/tmp/repo",
                "base_sha": "aaa",
                "target_sha": "bbb",
            },
            headers=_auth_header(role=None),
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_invalid_role_returns_403(self, rbac_client: AsyncClient) -> None:
        """An unrecognised role claim should result in 403."""
        resp = await rbac_client.get(
            "/api/v1/plans",
            headers=_auth_header(role="superadmin"),
        )
        assert resp.status_code == 403
        assert "unrecognised role" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_no_auth_header_returns_401(self, rbac_client: AsyncClient) -> None:
        """Requests without an Authorization header get 401 from auth middleware."""
        resp = await rbac_client.get("/api/v1/plans")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Permission hierarchy tests
# ---------------------------------------------------------------------------


class TestPermissionHierarchy:
    """Verify that higher roles inherit all permissions from lower roles."""

    def test_operator_inherits_viewer(self) -> None:
        for perm in ROLE_PERMISSIONS[Role.VIEWER]:
            assert role_has_permission(Role.OPERATOR, perm), f"OPERATOR should inherit VIEWER permission {perm.value}"

    def test_engineer_inherits_operator(self) -> None:
        for perm in ROLE_PERMISSIONS[Role.OPERATOR]:
            assert role_has_permission(Role.ENGINEER, perm), f"ENGINEER should inherit OPERATOR permission {perm.value}"

    def test_admin_inherits_engineer(self) -> None:
        for perm in ROLE_PERMISSIONS[Role.ENGINEER]:
            assert role_has_permission(Role.ADMIN, perm), f"ADMIN should inherit ENGINEER permission {perm.value}"

    def test_strict_hierarchy_no_gaps(self) -> None:
        """Every tier is a strict superset of the one below."""
        ordered = [Role.VIEWER, Role.OPERATOR, Role.ENGINEER, Role.ADMIN]
        for i in range(1, len(ordered)):
            lower = ROLE_PERMISSIONS[ordered[i - 1]]
            higher = ROLE_PERMISSIONS[ordered[i]]
            assert lower < higher, f"{ordered[i].name} should be a strict superset of {ordered[i - 1].name}"
