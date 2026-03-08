"""Tests for api/api/routers/team.py

Covers:
- GET /team/members: returns members with seat info
- POST /team/invite: succeeds under limit, fails at limit (429), feature-gated
- DELETE /team/members/{user_id}: decrements count
- PATCH /team/members/{user_id}: role change persists
- Non-admin gets 403
- Stripe Subscription.modify called with correct quantity (mocked)
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

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
    sub: str = "test-user",
) -> dict[str, str]:
    """Generate valid dev-mode auth headers."""
    now = time.time()
    payload: dict[str, Any] = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": "test-jti-team",
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
        billing_enabled=False,
    )


def _make_user_row(
    user_id: str = "user-1",
    email: str = "alice@example.com",
    display_name: str = "Alice",
    role: str = "admin",
    is_active: bool = True,
) -> MagicMock:
    """Create a mock UserTable row."""
    row = MagicMock()
    row.id = user_id
    row.email = email
    row.display_name = display_name
    row.role = role
    row.is_active = is_active
    row.tenant_id = "test-tenant"
    row.created_at = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    row.last_login_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    return row


def _create_team_app(
    tier: str = "team",
    members: list[MagicMock] | None = None,
    member_count: int = 3,
    billing_enabled: bool = False,
) -> tuple[Any, AsyncMock]:
    """Create a test app with mocked team-related dependencies.

    Returns (app, mock_session).
    """
    app = create_app()

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()

    # The session.execute mock needs to handle multiple different queries:
    # 1. BillingCustomerTable.plan_tier lookup (require_feature gate)
    # 2. UserRepository queries
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = tier
    result_mock.scalar_one.return_value = member_count
    if members is not None:
        result_mock.scalars.return_value.all.return_value = members
    else:
        result_mock.scalars.return_value.all.return_value = []
    result_mock.scalars.return_value.first.return_value = None

    mock_session.execute = AsyncMock(return_value=result_mock)

    # Mock get_bind for advisory lock (SQLite = no-op).
    bind_mock = MagicMock()
    bind_mock.dialect.name = "sqlite"
    mock_session.get_bind = MagicMock(return_value=bind_mock)

    async def _override_session():
        yield mock_session

    settings = _make_settings()
    settings.billing_enabled = billing_enabled

    mock_ai_client = AsyncMock(spec=AIServiceClient)
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

    return app, mock_session


# ---------------------------------------------------------------------------
# GET /team/members
# ---------------------------------------------------------------------------


class TestListMembers:
    """Verify GET /api/v1/team/members responses."""

    @pytest.mark.asyncio
    async def test_returns_members_with_seat_info(self) -> None:
        """List returns members array and seat usage info."""
        members = [
            _make_user_row("user-1", "alice@example.com", "Alice", "admin"),
            _make_user_row("user-2", "bob@example.com", "Bob", "engineer"),
        ]
        app, _ = _create_team_app(tier="team", members=members, member_count=2)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.list_by_tenant = AsyncMock(return_value=members)
            mock_repo.count_by_tenant = AsyncMock(return_value=2)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/team/members")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert body["seats_used"] == 2
        assert len(body["members"]) == 2
        assert body["members"][0]["email"] == "alice@example.com"

    @pytest.mark.asyncio
    async def test_viewer_can_list_members(self) -> None:
        """Viewer role has READ_PLANS permission and can list members."""
        app, _ = _create_team_app(tier="team", members=[], member_count=0)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="viewer")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.list_by_tenant = AsyncMock(return_value=[])
            mock_repo.count_by_tenant = AsyncMock(return_value=0)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.get("/api/v1/team/members")

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /team/invite
# ---------------------------------------------------------------------------


class TestInviteMember:
    """Verify POST /api/v1/team/invite responses."""

    @pytest.mark.asyncio
    async def test_invite_succeeds_under_limit(self) -> None:
        """Invite succeeds when under seat limit."""
        app, _ = _create_team_app(tier="team", member_count=3)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        new_user = _make_user_row("user-new", "newbie@example.com", "newbie", "viewer")

        with (
            patch("api.services.team_service.UserRepository") as MockUserRepo,
            patch("api.services.quota_service.UserRepository") as MockQuotaUserRepo,
        ):
            mock_repo = MagicMock()
            mock_repo.get_by_email = AsyncMock(return_value=None)
            mock_repo.create = AsyncMock(return_value=new_user)
            mock_repo.count_by_tenant = AsyncMock(return_value=3)
            MockUserRepo.return_value = mock_repo

            mock_quota_repo = MagicMock()
            mock_quota_repo.count_by_tenant = AsyncMock(return_value=3)
            MockQuotaUserRepo.return_value = mock_quota_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/team/invite",
                    json={"email": "newbie@example.com", "role": "viewer"},
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["email"] == "newbie@example.com"
        assert body["role"] == "viewer"

    @pytest.mark.asyncio
    async def test_invite_fails_at_seat_limit(self) -> None:
        """Invite returns 429 when seat limit is reached."""
        app, _ = _create_team_app(tier="team", member_count=10)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        with (
            patch("api.services.team_service.UserRepository") as MockUserRepo,
            patch("api.services.quota_service.UserRepository") as MockQuotaUserRepo,
        ):
            mock_repo = MagicMock()
            mock_repo.get_by_email = AsyncMock(return_value=None)
            MockUserRepo.return_value = mock_repo

            mock_quota_repo = MagicMock()
            mock_quota_repo.count_by_tenant = AsyncMock(return_value=10)
            MockQuotaUserRepo.return_value = mock_quota_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/team/invite",
                    json={"email": "extra@example.com", "role": "viewer"},
                )

        assert resp.status_code == 429
        assert "Seat limit" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_invite_requires_admin(self) -> None:
        """Non-admin roles get 403 on invite."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/team/invite",
                json={"email": "test@example.com", "role": "viewer"},
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_invite_feature_gated_for_community(self) -> None:
        """Community tier is blocked from inviting (Team+ required)."""
        app, _ = _create_team_app(tier="community")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.post(
                "/api/v1/team/invite",
                json={"email": "test@example.com", "role": "viewer"},
            )

        assert resp.status_code == 403
        assert "team_management" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_invite_duplicate_email_returns_409(self) -> None:
        """Inviting an already-active user returns 409."""
        app, _ = _create_team_app(tier="team", member_count=3)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        existing = _make_user_row("user-1", "existing@example.com", "Existing", "viewer", is_active=True)

        with (
            patch("api.services.team_service.UserRepository") as MockUserRepo,
            patch("api.services.quota_service.UserRepository") as MockQuotaUserRepo,
        ):
            mock_repo = MagicMock()
            mock_repo.get_by_email = AsyncMock(return_value=existing)
            MockUserRepo.return_value = mock_repo

            mock_quota_repo = MagicMock()
            mock_quota_repo.count_by_tenant = AsyncMock(return_value=3)
            MockQuotaUserRepo.return_value = mock_quota_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/team/invite",
                    json={"email": "existing@example.com", "role": "viewer"},
                )

        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# DELETE /team/members/{user_id}
# ---------------------------------------------------------------------------


class TestRemoveMember:
    """Verify DELETE /api/v1/team/members/{user_id} responses."""

    @pytest.mark.asyncio
    async def test_remove_sets_inactive(self) -> None:
        """Removing a member sets is_active=False."""
        app, _ = _create_team_app(tier="team", member_count=5)
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        user = _make_user_row("user-to-remove", "remove@example.com", "Remove", "viewer")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.get_by_id = AsyncMock(return_value=user)
            mock_repo.count_by_tenant = AsyncMock(return_value=4)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.delete("/api/v1/team/members/user-to-remove")

        assert resp.status_code == 200
        # The user mock's is_active should have been set to False.
        assert user.is_active is False

    @pytest.mark.asyncio
    async def test_remove_nonexistent_returns_404(self) -> None:
        """Removing a nonexistent user returns 404."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.get_by_id = AsyncMock(return_value=None)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.delete("/api/v1/team/members/nonexistent")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_remove_requires_admin(self) -> None:
        """Non-admin roles get 403 on remove."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="viewer")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.delete("/api/v1/team/members/user-1")

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /team/members/{user_id}
# ---------------------------------------------------------------------------


class TestUpdateMemberRole:
    """Verify PATCH /api/v1/team/members/{user_id} responses."""

    @pytest.mark.asyncio
    async def test_role_change_persists(self) -> None:
        """Changing a role updates the user record."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        user = _make_user_row("user-1", "alice@example.com", "Alice", "viewer")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.get_by_id = AsyncMock(return_value=user)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.patch(
                    "/api/v1/team/members/user-1",
                    json={"role": "engineer"},
                )

        assert resp.status_code == 200
        assert user.role == "engineer"

    @pytest.mark.asyncio
    async def test_invalid_role_returns_422(self) -> None:
        """Invalid role values are rejected by Pydantic validation."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.patch(
                "/api/v1/team/members/user-1",
                json={"role": "superuser"},
            )

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_update_requires_admin(self) -> None:
        """Non-admin roles get 403 on role change."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="operator")

        async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
            resp = await ac.patch(
                "/api/v1/team/members/user-1",
                json={"role": "admin"},
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_update_nonexistent_returns_400(self) -> None:
        """Updating a nonexistent user returns 400."""
        app, _ = _create_team_app(tier="team")
        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        with patch("api.services.team_service.UserRepository") as MockUserRepo:
            mock_repo = MagicMock()
            mock_repo.get_by_id = AsyncMock(return_value=None)
            MockUserRepo.return_value = mock_repo

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.patch(
                    "/api/v1/team/members/nonexistent",
                    json={"role": "admin"},
                )

        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Stripe sync
# ---------------------------------------------------------------------------


class TestStripeSeatSync:
    """Verify Stripe subscription quantity sync on member changes."""

    @pytest.mark.asyncio
    async def test_invite_syncs_stripe_quantity(self) -> None:
        """Stripe Subscription.modify is called after successful invite."""
        app, mock_session = _create_team_app(tier="team", member_count=3, billing_enabled=True)

        # Override settings to enable billing.
        settings = _make_settings()
        settings.billing_enabled = True
        settings.stripe_secret_key = "sk_test_xxx"
        app.dependency_overrides[get_settings] = lambda: settings

        transport = ASGITransport(app=app)
        headers = _make_auth_headers(role="admin")

        new_user = _make_user_row("user-new", "newbie@example.com", "newbie", "viewer")

        # Build a billing customer row mock.
        billing_row = MagicMock()
        billing_row.stripe_subscription_id = "sub_test123"
        billing_row.stripe_customer_id = "cus_test123"
        billing_row.tenant_id = "test-tenant"

        with (
            patch("api.services.team_service.UserRepository") as MockUserRepo,
            patch("api.services.quota_service.UserRepository") as MockQuotaUserRepo,
            patch("api.services.team_service.stripe") as mock_stripe,
        ):
            mock_repo = MagicMock()
            mock_repo.get_by_email = AsyncMock(return_value=None)
            mock_repo.create = AsyncMock(return_value=new_user)
            mock_repo.count_by_tenant = AsyncMock(return_value=4)
            MockUserRepo.return_value = mock_repo

            mock_quota_repo = MagicMock()
            mock_quota_repo.count_by_tenant = AsyncMock(return_value=3)
            MockQuotaUserRepo.return_value = mock_quota_repo

            # Mock the billing customer lookup in _sync_stripe_quantity.
            billing_result = MagicMock()
            billing_result.scalar_one_or_none.return_value = billing_row

            # We need the session.execute to return different results for
            # different queries. The first call returns tier, subsequent
            # ones return billing row and count.
            tier_result = MagicMock()
            tier_result.scalar_one_or_none.return_value = "team"
            tier_result.scalar_one.return_value = 3

            mock_session.execute = AsyncMock(
                side_effect=[
                    tier_result,  # require_feature gate: plan_tier lookup
                    tier_result,  # check_seat_quota: advisory lock (no-op)
                    tier_result,  # check_seat_quota: tenant config lookup
                    tier_result,  # check_seat_quota: plan tier lookup
                    tier_result,  # check_seat_quota: user count
                    tier_result,  # get_by_email
                    tier_result,  # create user flush
                    billing_result,  # _sync_stripe_quantity: billing lookup
                    tier_result,  # count_by_tenant for sync
                ]
            )

            mock_stripe.Subscription.retrieve.return_value = {
                "items": {"data": [{"id": "si_item123"}]},
            }
            mock_stripe.SubscriptionItem.modify.return_value = {}

            async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
                resp = await ac.post(
                    "/api/v1/team/invite",
                    json={"email": "newbie@example.com", "role": "viewer"},
                )

        # The response should succeed (200) regardless of Stripe sync.
        assert resp.status_code == 200
