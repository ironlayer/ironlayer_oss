"""Tests for api/api/routers/auth.py

Covers all auth router endpoints:
- POST /auth/signup (public): user registration
- POST /auth/login (public): email/password login
- POST /auth/refresh (public): token refresh
- GET /auth/me (authenticated): current user profile
- POST /auth/api-keys (authenticated, ADMIN): create API key
- GET /auth/api-keys (authenticated, ADMIN): list API keys
- DELETE /auth/api-keys/{key_id} (authenticated, ADMIN): revoke API key
- POST /auth/revoke (authenticated, ADMIN): revoke token by jti
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.auth_service import AuthError

# ---------------------------------------------------------------------------
# TestSignup
# ---------------------------------------------------------------------------


class TestSignup:
    """Verify POST /api/v1/auth/signup responses."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_successful_signup(self, MockAuthService, client):
        """Successful registration returns 201 with tokens and user."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.signup = AsyncMock(
            return_value={
                "user": {
                    "id": "u1",
                    "email": "new@example.com",
                    "display_name": "New User",
                    "role": "admin",
                    "tenant_id": "t_abc123",
                },
                "access_token": "at_xxx",
                "refresh_token": "rt_xxx",
                "tenant_id": "t_abc123",
            }
        )

        resp = await client.post(
            "/api/v1/auth/signup",
            json={
                "email": "new@example.com",
                "password": "strongpassword1!",
                "display_name": "New User",
            },
        )

        assert resp.status_code == 201
        body = resp.json()
        assert body["access_token"] == "at_xxx"
        assert body["tenant_id"] == "t_abc123"
        assert body["user"]["email"] == "new@example.com"
        assert body["user"]["display_name"] == "New User"
        assert body["token_type"] == "bearer"
        # Refresh token is now set as an HttpOnly cookie, not in the JSON body.
        assert "refresh_token" not in body
        assert resp.cookies.get("refresh_token") == "rt_xxx"
        mock_svc.signup.assert_awaited_once_with(
            email="new@example.com",
            password="strongpassword1!",
            display_name="New User",
        )

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_duplicate_email_409(self, MockAuthService, client):
        """Duplicate email address returns 409 Conflict."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.signup = AsyncMock(
            side_effect=AuthError(
                "An account with this email already exists. Please log in instead.",
                status_code=409,
            )
        )

        resp = await client.post(
            "/api/v1/auth/signup",
            json={
                "email": "exists@example.com",
                "password": "strongpassword1!",
                "display_name": "Dupe User",
            },
        )

        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_weak_password_422(self, client):
        """Password shorter than 8 characters triggers Pydantic 422."""
        resp = await client.post(
            "/api/v1/auth/signup",
            json={
                "email": "test@example.com",
                "password": "short",
                "display_name": "Test",
            },
        )

        assert resp.status_code == 422
        body = resp.json()
        errors = body["detail"]
        assert any("password" in str(e).lower() for e in errors)

    @pytest.mark.asyncio
    async def test_missing_fields_422(self, client):
        """Empty request body returns 422 validation error."""
        resp = await client.post("/api/v1/auth/signup", json={})

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_email_format_422(self, client):
        """Malformed email address triggers Pydantic validation."""
        resp = await client.post(
            "/api/v1/auth/signup",
            json={
                "email": "not-an-email",
                "password": "strongpassword1!",
                "display_name": "Test",
            },
        )

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_display_name_422(self, client):
        """Empty display_name triggers Pydantic min_length validation."""
        resp = await client.post(
            "/api/v1/auth/signup",
            json={
                "email": "test@example.com",
                "password": "strongpassword1!",
                "display_name": "",
            },
        )

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestLogin
# ---------------------------------------------------------------------------


class TestLogin:
    """Verify POST /api/v1/auth/login responses."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_successful_login(self, MockAuthService, client):
        """Valid credentials return 200 with tokens and user info."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.login = AsyncMock(
            return_value={
                "user": {
                    "id": "u1",
                    "email": "test@example.com",
                    "display_name": "Test User",
                    "role": "admin",
                    "tenant_id": "t_abc",
                },
                "access_token": "at_login",
                "refresh_token": "rt_login",
                "tenant_id": "t_abc",
            }
        )

        resp = await client.post(
            "/api/v1/auth/login",
            json={"email": "test@example.com", "password": "correctpassword"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["access_token"] == "at_login"
        assert body["user"]["id"] == "u1"
        assert body["tenant_id"] == "t_abc"
        assert body["token_type"] == "bearer"
        # Refresh token is now set as an HttpOnly cookie, not in the JSON body.
        assert "refresh_token" not in body
        assert resp.cookies.get("refresh_token") == "rt_login"
        mock_svc.login.assert_awaited_once_with(
            email="test@example.com",
            password="correctpassword",
        )

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_wrong_password_401(self, MockAuthService, client):
        """Invalid password returns 401 Unauthorized."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.login = AsyncMock(side_effect=AuthError("Invalid email or password.", status_code=401))

        resp = await client.post(
            "/api/v1/auth/login",
            json={"email": "test@example.com", "password": "wrongpassword"},
        )

        assert resp.status_code == 401
        assert "Invalid" in resp.json()["detail"]

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_nonexistent_user_401(self, MockAuthService, client):
        """Login for non-existent user returns 401."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.login = AsyncMock(side_effect=AuthError("Invalid email or password.", status_code=401))

        resp = await client.post(
            "/api/v1/auth/login",
            json={"email": "nobody@example.com", "password": "anypassword123"},
        )

        assert resp.status_code == 401

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_deactivated_account_403(self, MockAuthService, client):
        """Deactivated account returns 403 Forbidden."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.login = AsyncMock(
            side_effect=AuthError(
                "This account has been deactivated. Contact your administrator.",
                status_code=403,
            )
        )

        resp = await client.post(
            "/api/v1/auth/login",
            json={"email": "deactivated@example.com", "password": "password123"},
        )

        assert resp.status_code == 403
        assert "deactivated" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_missing_email_422(self, client):
        """Missing email field returns 422."""
        resp = await client.post(
            "/api/v1/auth/login",
            json={"password": "somepassword"},
        )

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestRefresh
# ---------------------------------------------------------------------------


class TestRefresh:
    """Verify POST /api/v1/auth/refresh responses."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_successful_refresh(self, MockAuthService, client):
        """Valid refresh token cookie returns 200 with new access token."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.refresh = AsyncMock(
            return_value={
                "access_token": "at_refreshed",
                "refresh_token": "rt_refreshed",
            }
        )

        # The refresh token is now read from an HttpOnly cookie.
        # Include CSRF token (double-submit cookie pattern).
        csrf_token = "test-csrf-token-for-refresh"
        resp = await client.post(
            "/api/v1/auth/refresh",
            cookies={"refresh_token": "rt_original", "csrf_token": csrf_token},
            headers={"X-CSRF-Token": csrf_token},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["access_token"] == "at_refreshed"
        assert body["token_type"] == "bearer"
        # Refresh token is rotated via the cookie, not in the JSON body.
        assert "refresh_token" not in body
        assert resp.cookies.get("refresh_token") == "rt_refreshed"
        mock_svc.refresh.assert_awaited_once_with("rt_original")

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_expired_token_401(self, MockAuthService, client):
        """Expired refresh token returns 401 and clears the cookie."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.refresh = AsyncMock(side_effect=AuthError("Token has expired", status_code=401))

        csrf_token = "test-csrf-token-for-refresh"
        resp = await client.post(
            "/api/v1/auth/refresh",
            cookies={"refresh_token": "rt_expired", "csrf_token": csrf_token},
            headers={"X-CSRF-Token": csrf_token},
        )

        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_missing_refresh_cookie_401(self, client):
        """Missing refresh_token cookie returns 401."""
        resp = await client.post("/api/v1/auth/refresh")

        assert resp.status_code == 401
        assert "refresh token" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# TestGetProfile
# ---------------------------------------------------------------------------


class TestGetProfile:
    """Verify GET /api/v1/auth/me responses."""

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_returns_current_user(self, MockAuthService, client):
        """Authenticated request returns the current user profile."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.get_current_user = AsyncMock(
            return_value={
                "id": "test-user",
                "email": "admin@example.com",
                "display_name": "Admin User",
                "role": "admin",
                "tenant_id": "default",
                "is_active": True,
                "email_verified": True,
                "created_at": "2024-06-15T12:00:00",
                "last_login_at": "2024-06-20T09:30:00",
            }
        )

        resp = await client.get("/api/v1/auth/me")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "test-user"
        assert body["email"] == "admin@example.com"
        assert body["display_name"] == "Admin User"
        assert body["role"] == "admin"
        assert body["is_active"] is True
        assert body["email_verified"] is True
        mock_svc.get_current_user.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_user_not_found_404(self, MockAuthService, client):
        """When the authenticated user no longer exists, returns 404."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.get_current_user = AsyncMock(side_effect=AuthError("User not found.", status_code=404))

        resp = await client.get("/api/v1/auth/me")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_unauthenticated_401(self, app):
        """Request without auth token returns 401."""
        from httpx import ASGITransport, AsyncClient

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/auth/me")

        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# TestAPIKeyCRUD
# ---------------------------------------------------------------------------


class TestAPIKeyCRUD:
    """Verify API key management endpoints."""

    @pytest.mark.asyncio
    @patch("api.services.audit_service.AuditService")
    @patch("api.services.auth_service.AuthService")
    async def test_create_api_key(self, MockAuthService, MockAuditService, client):
        """POST /auth/api-keys creates a key and returns 201 with plaintext."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.create_api_key = AsyncMock(
            return_value={
                "id": "key_001",
                "name": "CI/CD Key",
                "key_prefix": "bmkey.abc12",
                "scopes": ["read", "write"],
                "created_at": "2024-06-15T12:00:00",
                "plaintext_key": "bmkey.full_secret_key_value",
            }
        )

        mock_audit = AsyncMock()
        MockAuditService.return_value = mock_audit
        mock_audit.log = AsyncMock()

        resp = await client.post(
            "/api/v1/auth/api-keys",
            json={"name": "CI/CD Key"},
        )

        assert resp.status_code == 201
        body = resp.json()
        assert body["id"] == "key_001"
        assert body["name"] == "CI/CD Key"
        assert body["key_prefix"] == "bmkey.abc12"
        assert body["plaintext_key"] == "bmkey.full_secret_key_value"
        assert body["scopes"] == ["read", "write"]
        mock_svc.create_api_key.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_create_api_key_with_scopes(self, MockAuthService, client):
        """POST /auth/api-keys with explicit scopes passes them through."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.create_api_key = AsyncMock(
            return_value={
                "id": "key_002",
                "name": "Read-Only Key",
                "key_prefix": "bmkey.def34",
                "scopes": ["read"],
                "created_at": "2024-06-15T12:00:00",
                "plaintext_key": "bmkey.readonly_key_value",
            }
        )

        resp = await client.post(
            "/api/v1/auth/api-keys",
            json={"name": "Read-Only Key", "scopes": ["read"]},
        )

        assert resp.status_code == 201
        body = resp.json()
        assert body["scopes"] == ["read"]

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_list_api_keys(self, MockAuthService, client):
        """GET /auth/api-keys returns a list of keys without plaintext."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.list_api_keys = AsyncMock(
            return_value=[
                {
                    "id": "key_001",
                    "name": "CI/CD Key",
                    "key_prefix": "bmkey.abc12",
                    "scopes": ["read", "write"],
                    "created_at": "2024-06-15T12:00:00",
                    "last_used_at": "2024-06-20T09:00:00",
                    "expires_at": None,
                },
                {
                    "id": "key_002",
                    "name": "Read-Only Key",
                    "key_prefix": "bmkey.def34",
                    "scopes": ["read"],
                    "created_at": "2024-06-16T08:00:00",
                    "last_used_at": None,
                    "expires_at": None,
                },
            ]
        )

        resp = await client.get("/api/v1/auth/api-keys")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        assert body[0]["id"] == "key_001"
        assert body[1]["id"] == "key_002"
        # List endpoint should not include plaintext_key
        assert body[0].get("plaintext_key") is None
        assert body[1].get("plaintext_key") is None

    @pytest.mark.asyncio
    @patch("api.services.audit_service.AuditService")
    @patch("api.services.auth_service.AuthService")
    async def test_revoke_api_key(self, MockAuthService, MockAuditService, client):
        """DELETE /auth/api-keys/{key_id} returns revoked=True on success."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.revoke_api_key = AsyncMock(return_value=True)

        mock_audit = AsyncMock()
        MockAuditService.return_value = mock_audit
        mock_audit.log = AsyncMock()

        resp = await client.delete("/api/v1/auth/api-keys/key_001")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "key_001"
        assert body["revoked"] is True

    @pytest.mark.asyncio
    @patch("api.services.auth_service.AuthService")
    async def test_revoke_nonexistent_key_404(self, MockAuthService, client):
        """DELETE /auth/api-keys/{key_id} returns 404 when key not found."""
        mock_svc = AsyncMock()
        MockAuthService.return_value = mock_svc
        mock_svc.revoke_api_key = AsyncMock(return_value=False)

        resp = await client.delete("/api/v1/auth/api-keys/nonexistent_key")

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_create_api_key_missing_name_422(self, client):
        """POST /auth/api-keys with empty body returns 422."""
        resp = await client.post("/api/v1/auth/api-keys", json={})

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestTokenRevocation
# ---------------------------------------------------------------------------


class TestTokenRevocation:
    """Verify POST /api/v1/auth/revoke responses."""

    @pytest.mark.asyncio
    @patch("api.services.audit_service.AuditService")
    @patch("core_engine.state.repository.TokenRevocationRepository")
    async def test_revoke_token(self, MockTokenRevRepo, MockAuditService, client):
        """POST /auth/revoke marks a token as revoked and returns 200."""
        mock_repo = AsyncMock()
        MockTokenRevRepo.return_value = mock_repo
        mock_repo.revoke = AsyncMock()

        mock_audit = AsyncMock()
        MockAuditService.return_value = mock_audit
        mock_audit.log = AsyncMock()

        resp = await client.post(
            "/api/v1/auth/revoke",
            json={"jti": "token-jti-to-revoke", "reason": "Compromised"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["jti"] == "token-jti-to-revoke"
        assert body["revoked"] is True
        assert "revoked successfully" in body["message"].lower()

    @pytest.mark.asyncio
    @patch("api.services.audit_service.AuditService")
    @patch("core_engine.state.repository.TokenRevocationRepository")
    async def test_revoke_token_with_expiry(self, MockTokenRevRepo, MockAuditService, client):
        """POST /auth/revoke with expires_at parses the ISO-8601 date."""
        mock_repo = AsyncMock()
        MockTokenRevRepo.return_value = mock_repo
        mock_repo.revoke = AsyncMock()

        mock_audit = AsyncMock()
        MockAuditService.return_value = mock_audit
        mock_audit.log = AsyncMock()

        resp = await client.post(
            "/api/v1/auth/revoke",
            json={
                "jti": "jti-with-expiry",
                "expires_at": "2024-12-31T23:59:59+00:00",
            },
        )

        assert resp.status_code == 200
        assert resp.json()["jti"] == "jti-with-expiry"

    @pytest.mark.asyncio
    @patch("api.services.audit_service.AuditService")
    @patch("core_engine.state.repository.TokenRevocationRepository")
    async def test_revoke_token_invalid_expiry_400(self, MockTokenRevRepo, MockAuditService, client):
        """POST /auth/revoke with malformed expires_at returns 400."""
        mock_repo = AsyncMock()
        MockTokenRevRepo.return_value = mock_repo
        mock_repo.revoke = AsyncMock()

        resp = await client.post(
            "/api/v1/auth/revoke",
            json={
                "jti": "jti-bad-expiry",
                "expires_at": "not-a-date",
            },
        )

        assert resp.status_code == 400
        assert "expires_at" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_revoke_missing_jti_422(self, client):
        """POST /auth/revoke without jti returns 422."""
        resp = await client.post("/api/v1/auth/revoke", json={})

        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_revoke_unauthenticated_401(self, app):
        """POST /auth/revoke without auth token returns 401."""
        from httpx import ASGITransport, AsyncClient

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/auth/revoke",
                json={"jti": "some-jti"},
            )

        assert resp.status_code == 401
