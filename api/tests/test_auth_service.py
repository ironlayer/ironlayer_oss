"""Tests for api/api/services/auth_service.py

Covers the AuthService at the service layer with mocked repositories
and token manager:
- Signup: user creation, tenant provisioning, duplicate detection, validation
- Login: credential verification, deactivated accounts
- Refresh: token validation, scope checks, user status
- GetCurrentUser: profile retrieval, not-found
- API Keys: creation, listing, revocation, validation
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.auth_service import AuthError, AuthService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_SENTINEL = object()


def _make_user_mock(
    *,
    user_id: str = "u_abc123",
    email: str = "test@example.com",
    display_name: str = "Test User",
    role: str = "admin",
    tenant_id: str = "t_xyz789",
    is_active: bool = True,
    email_verified: bool = True,
    created_at: datetime | None | object = _SENTINEL,
    last_login_at: datetime | None | object = _SENTINEL,
) -> MagicMock:
    """Build a mock user object matching the ORM model shape."""
    user = MagicMock()
    user.id = user_id
    user.email = email
    user.display_name = display_name
    user.role = role
    user.tenant_id = tenant_id
    user.is_active = is_active
    user.email_verified = email_verified
    user.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc) if created_at is _SENTINEL else created_at
    user.last_login_at = (
        datetime(2024, 6, 20, 9, 30, 0, tzinfo=timezone.utc) if last_login_at is _SENTINEL else last_login_at
    )
    return user


def _make_api_key_row_mock(
    *,
    key_id: str = "key_001",
    name: str = "CI/CD Key",
    key_prefix: str = "bmkey.abc12",
    scopes: list[str] | None = None,
    created_at: datetime | None = None,
    last_used_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> MagicMock:
    """Build a mock API key row matching the ORM model shape."""
    row = MagicMock()
    row.id = key_id
    row.name = name
    row.key_prefix = key_prefix
    row.scopes = scopes or ["read", "write"]
    row.created_at = created_at or datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    row.last_used_at = last_used_at
    row.expires_at = expires_at
    return row


def _make_token_manager_mock() -> MagicMock:
    """Build a mock TokenManager with generate and validate methods."""
    tm = MagicMock()
    tm.generate_token = MagicMock(return_value="access_token_xxx")
    tm.generate_refresh_token = MagicMock(return_value="refresh_token_xxx")
    return tm


def _make_claims_mock(
    *,
    sub: str = "u_abc123",
    tenant_id: str = "t_xyz789",
    scopes: list[str] | None = None,
) -> MagicMock:
    """Build a mock TokenClaims object."""
    claims = MagicMock()
    claims.sub = sub
    claims.tenant_id = tenant_id
    claims.scopes = scopes if scopes is not None else ["refresh"]
    return claims


# ---------------------------------------------------------------------------
# TestSignup
# ---------------------------------------------------------------------------


class TestSignup:
    """Verify AuthService.signup logic."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.TenantConfigRepository")
    @patch("api.services.auth_service.UserRepository")
    async def test_creates_user_and_tenant(self, MockUserRepo, MockTenantRepo):
        """Successful signup creates a user, provisions a tenant, and returns tokens."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        # First call: signup lookup (tenant_id="__signup__") - no existing user
        # Second call: creation (tenant_id=auto-generated)
        mock_user_repo_signup = AsyncMock()
        mock_user_repo_signup.get_by_email_any_tenant = AsyncMock(return_value=None)

        new_user = _make_user_mock()
        mock_user_repo_create = AsyncMock()
        mock_user_repo_create.create = AsyncMock(return_value=new_user)

        # UserRepository is instantiated twice: once for lookup, once for creation.
        MockUserRepo.side_effect = [mock_user_repo_signup, mock_user_repo_create]

        mock_tenant_repo = AsyncMock()
        mock_tenant_repo.upsert = AsyncMock()
        MockTenantRepo.return_value = mock_tenant_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.signup(
            email="new@example.com",
            password="strongpassword",
            display_name="New User",
        )

        assert result["access_token"] == "access_token_xxx"
        assert result["refresh_token"] == "refresh_token_xxx"
        assert result["user"]["email"] == "test@example.com"
        assert result["user"]["role"] == "admin"
        assert "tenant_id" in result

        mock_user_repo_signup.get_by_email_any_tenant.assert_awaited_once_with("new@example.com")
        mock_user_repo_create.create.assert_awaited_once()
        mock_tenant_repo.upsert.assert_awaited_once()
        mock_tm.generate_token.assert_called_once()
        mock_tm.generate_refresh_token.assert_called_once()

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_duplicate_email_raises_409(self, MockUserRepo):
        """Signup with an existing email raises AuthError with status 409."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_user_repo = AsyncMock()
        existing_user = _make_user_mock(email="exists@example.com")
        mock_user_repo.get_by_email_any_tenant = AsyncMock(return_value=existing_user)
        MockUserRepo.return_value = mock_user_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.signup(
                email="exists@example.com",
                password="strongpassword",
                display_name="Dupe",
            )

        assert exc_info.value.status_code == 409
        assert "already exists" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_invalid_email_raises(self):
        """Signup with empty email raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.signup(email="", password="strongpassword", display_name="Test")

        assert "email" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_short_password_raises(self):
        """Signup with password shorter than 8 chars raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.signup(
                email="test@example.com",
                password="short",
                display_name="Test",
            )

        assert "8 characters" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_empty_display_name_raises(self):
        """Signup with blank display_name raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.signup(
                email="test@example.com",
                password="strongpassword",
                display_name="",
            )

        assert "display name" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_whitespace_only_display_name_raises(self):
        """Signup with whitespace-only display_name raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.signup(
                email="test@example.com",
                password="strongpassword",
                display_name="   ",
            )

        assert "display name" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# TestLogin
# ---------------------------------------------------------------------------


class TestLogin:
    """Verify AuthService.login logic."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_successful_login(self, MockUserRepo):
        """Valid credentials return tokens and user info."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        user = _make_user_mock(is_active=True)
        mock_repo = AsyncMock()
        mock_repo.verify_password = AsyncMock(return_value=user)
        mock_repo.update_last_login = AsyncMock()
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.login(email="test@example.com", password="correctpass")

        assert result["access_token"] == "access_token_xxx"
        assert result["refresh_token"] == "refresh_token_xxx"
        assert result["user"]["email"] == "test@example.com"
        assert result["tenant_id"] == user.tenant_id
        mock_repo.verify_password.assert_awaited_once_with("test@example.com", "correctpass")
        mock_repo.update_last_login.assert_awaited_once_with(user.id)
        mock_tm.generate_token.assert_called_once()
        mock_tm.generate_refresh_token.assert_called_once()

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_invalid_credentials(self, MockUserRepo):
        """Invalid credentials raise AuthError with status 401."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_repo = AsyncMock()
        mock_repo.verify_password = AsyncMock(return_value=None)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.login(email="test@example.com", password="wrongpass")

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_deactivated_user(self, MockUserRepo):
        """Deactivated user raises AuthError with status 403."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        user = _make_user_mock(is_active=False)
        mock_repo = AsyncMock()
        mock_repo.verify_password = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.login(email="test@example.com", password="correctpass")

        assert exc_info.value.status_code == 403
        assert "deactivated" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# TestRefresh
# ---------------------------------------------------------------------------


class TestRefresh:
    """Verify AuthService.refresh logic."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_successful_refresh(self, MockUserRepo):
        """Valid refresh token generates a new token pair."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        claims = _make_claims_mock(scopes=["refresh"])
        mock_tm.validate_token = MagicMock(return_value=claims)

        user = _make_user_mock(is_active=True)
        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.refresh("rt_valid")

        assert result["access_token"] == "access_token_xxx"
        assert result["refresh_token"] == "refresh_token_xxx"
        mock_tm.validate_token.assert_called_once_with("rt_valid")
        mock_repo.get_by_id.assert_awaited_once_with(claims.sub)

    @pytest.mark.asyncio
    async def test_invalid_token(self):
        """Invalid refresh token raises AuthError with status 401."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        mock_tm.validate_token = MagicMock(side_effect=PermissionError("Token signature verification failed"))

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.refresh("rt_invalid")

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_not_refresh_token(self):
        """Token without 'refresh' scope raises AuthError with status 401."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        claims = _make_claims_mock(scopes=["read", "write"])
        mock_tm.validate_token = MagicMock(return_value=claims)

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.refresh("rt_not_refresh")

        assert exc_info.value.status_code == 401
        assert "refresh" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_user_deactivated(self, MockUserRepo):
        """Refresh for a deactivated user raises AuthError with status 401."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        claims = _make_claims_mock(scopes=["refresh"])
        mock_tm.validate_token = MagicMock(return_value=claims)

        user = _make_user_mock(is_active=False)
        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.refresh("rt_deactivated_user")

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_user_not_found_during_refresh(self, MockUserRepo):
        """Refresh for a deleted user raises AuthError with status 401."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        claims = _make_claims_mock(scopes=["refresh"])
        mock_tm.validate_token = MagicMock(return_value=claims)

        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=None)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.refresh("rt_deleted_user")

        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# TestGetCurrentUser
# ---------------------------------------------------------------------------


class TestGetCurrentUser:
    """Verify AuthService.get_current_user logic."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_returns_profile(self, MockUserRepo):
        """Returns formatted user profile dict when user exists."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        user = _make_user_mock(
            user_id="u_abc123",
            email="admin@example.com",
            display_name="Admin",
            role="admin",
            tenant_id="t_xyz789",
            is_active=True,
            email_verified=True,
        )
        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        profile = await svc.get_current_user(user_id="u_abc123", tenant_id="t_xyz789")

        assert profile["id"] == "u_abc123"
        assert profile["email"] == "admin@example.com"
        assert profile["display_name"] == "Admin"
        assert profile["role"] == "admin"
        assert profile["tenant_id"] == "t_xyz789"
        assert profile["is_active"] is True
        assert profile["email_verified"] is True
        assert profile["created_at"] is not None
        assert profile["last_login_at"] is not None
        mock_repo.get_by_id.assert_awaited_once_with("u_abc123")

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_user_not_found(self, MockUserRepo):
        """Raises AuthError with status 404 when user does not exist."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=None)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.get_current_user(user_id="nonexistent", tenant_id="t_xyz789")

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_profile_datetime_formatting(self, MockUserRepo):
        """Verifies that datetime fields are serialized as ISO-8601 strings."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        created = datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
        last_login = datetime(2024, 6, 20, 14, 30, 0, tzinfo=timezone.utc)
        user = _make_user_mock(created_at=created, last_login_at=last_login)
        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        profile = await svc.get_current_user(user_id="u_abc123", tenant_id="t_xyz789")

        assert profile["created_at"] == "2024-01-15T08:00:00+00:00"
        assert profile["last_login_at"] == "2024-06-20T14:30:00+00:00"

    @pytest.mark.asyncio
    @patch("api.services.auth_service.UserRepository")
    async def test_profile_null_datetimes(self, MockUserRepo):
        """Null datetimes are returned as None in the profile dict."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        user = _make_user_mock(created_at=None, last_login_at=None)
        mock_repo = AsyncMock()
        mock_repo.get_by_id = AsyncMock(return_value=user)
        MockUserRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        profile = await svc.get_current_user(user_id="u_abc123", tenant_id="t_xyz789")

        assert profile["created_at"] is None
        assert profile["last_login_at"] is None


# ---------------------------------------------------------------------------
# TestAPIKeys
# ---------------------------------------------------------------------------


class TestAPIKeys:
    """Verify AuthService API key management methods."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_create_key(self, MockAPIKeyRepo):
        """create_api_key returns key data including plaintext_key."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        row = _make_api_key_row_mock()
        plaintext = "bmkey.full_secret_key_value_here"

        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=(row, plaintext))
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.create_api_key(
            user_id="u_abc123",
            tenant_id="t_xyz789",
            name="CI/CD Key",
            scopes=["read", "write"],
        )

        assert result["id"] == "key_001"
        assert result["name"] == "CI/CD Key"
        assert result["key_prefix"] == "bmkey.abc12"
        assert result["plaintext_key"] == plaintext
        assert result["scopes"] == ["read", "write"]
        assert result["created_at"] is not None
        mock_repo.create.assert_awaited_once_with(
            user_id="u_abc123",
            name="CI/CD Key",
            scopes=["read", "write"],
        )

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_create_key_default_scopes(self, MockAPIKeyRepo):
        """create_api_key with scopes=None passes None to the repository."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        row = _make_api_key_row_mock()
        plaintext = "bmkey.default_scope_key"

        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=(row, plaintext))
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.create_api_key(
            user_id="u_abc123",
            tenant_id="t_xyz789",
            name="Default Key",
        )

        assert result["plaintext_key"] == plaintext
        mock_repo.create.assert_awaited_once_with(
            user_id="u_abc123",
            name="Default Key",
            scopes=None,
        )

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_list_keys(self, MockAPIKeyRepo):
        """list_api_keys returns formatted list of key dicts."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        key1 = _make_api_key_row_mock(key_id="key_001", name="Key A")
        key2 = _make_api_key_row_mock(
            key_id="key_002",
            name="Key B",
            key_prefix="bmkey.def34",
            last_used_at=datetime(2024, 6, 20, 9, 0, 0, tzinfo=timezone.utc),
        )

        mock_repo = AsyncMock()
        mock_repo.list_by_user = AsyncMock(return_value=[key1, key2])
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        keys = await svc.list_api_keys(user_id="u_abc123", tenant_id="t_xyz789")

        assert len(keys) == 2
        assert keys[0]["id"] == "key_001"
        assert keys[0]["name"] == "Key A"
        assert keys[0].get("plaintext_key") is None  # Never in list response
        assert keys[1]["id"] == "key_002"
        assert keys[1]["last_used_at"] == "2024-06-20T09:00:00+00:00"
        mock_repo.list_by_user.assert_awaited_once_with("u_abc123")

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_list_keys_empty(self, MockAPIKeyRepo):
        """list_api_keys returns empty list when user has no keys."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_repo = AsyncMock()
        mock_repo.list_by_user = AsyncMock(return_value=[])
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        keys = await svc.list_api_keys(user_id="u_abc123", tenant_id="t_xyz789")

        assert keys == []

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_revoke_key(self, MockAPIKeyRepo):
        """revoke_api_key returns True when key is successfully revoked."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_repo = AsyncMock()
        mock_repo.revoke = AsyncMock(return_value=True)
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.revoke_api_key(key_id="key_001", tenant_id="t_xyz789")

        assert result is True
        mock_repo.revoke.assert_awaited_once_with("key_001")

    @pytest.mark.asyncio
    @patch("api.services.auth_service.APIKeyRepository")
    async def test_revoke_key_not_found(self, MockAPIKeyRepo):
        """revoke_api_key returns False when key does not exist."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()

        mock_repo = AsyncMock()
        mock_repo.revoke = AsyncMock(return_value=False)
        MockAPIKeyRepo.return_value = mock_repo

        svc = AuthService(mock_session, token_manager=mock_tm)
        result = await svc.revoke_api_key(key_id="nonexistent", tenant_id="t_xyz789")

        assert result is False

    @pytest.mark.asyncio
    async def test_create_key_empty_name(self):
        """create_api_key with empty name raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.create_api_key(
                user_id="u_abc123",
                tenant_id="t_xyz789",
                name="",
            )

        assert "name" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_create_key_whitespace_name(self):
        """create_api_key with whitespace-only name raises AuthError."""
        mock_session = AsyncMock()
        mock_tm = _make_token_manager_mock()
        svc = AuthService(mock_session, token_manager=mock_tm)

        with pytest.raises(AuthError) as exc_info:
            await svc.create_api_key(
                user_id="u_abc123",
                tenant_id="t_xyz789",
                name="   ",
            )

        assert "name" in str(exc_info.value).lower()
