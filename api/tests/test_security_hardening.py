"""Security hardening tests -- validates Phase 0-1 fixes.

Tests cover:
- Tenant context SQL injection prevention (set_tenant_context)
- TokenConfig requires jwt_secret (no insecure defaults)
- OIDC algorithm hardcoded to RS256 (algorithm confusion prevention)
- SERVICE role cannot bypass role-based guards via numeric comparison
- CSRF double-submit cookie middleware enforcement
- LLM API key redaction from error messages
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr, ValidationError
from starlette.requests import Request
from starlette.testclient import TestClient

from api.middleware.rbac import (
    Permission,
    Role,
    get_user_role,
    require_permission,
    require_role,
    role_has_permission,
)
from api.routers.tenant_config import _redact_key
from api.security import OIDCProvider, TokenClaims, TokenConfig, TokenManager
from core_engine.state.database import _TENANT_ID_RE, set_tenant_context

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"


def _make_dev_token(
    tenant_id: str = "default",
    sub: str = "test-user",
    role: str = "admin",
    scopes: list[str] | None = None,
    identity_kind: str = "user",
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
        "jti": f"test-jti-{secrets.token_hex(4)}",
        "identity_kind": identity_kind,
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


# ===================================================================
# 1. TestTenantContextInjection
# ===================================================================


class TestTenantContextInjection:
    """Verify set_tenant_context rejects injection payloads.

    The ``_TENANT_ID_RE`` regex only allows ``[a-zA-Z0-9_-]{1,128}``.
    Anything outside this character set must be rejected with ValueError
    before any SQL is executed, preventing SQL injection via the
    ``SET LOCAL app.tenant_id`` path.
    """

    def test_valid_alphanumeric_id_matches_regex(self) -> None:
        """A standard alphanumeric tenant ID passes the regex check."""
        assert _TENANT_ID_RE.match("tenant-alpha-01") is not None

    def test_valid_underscore_id_matches_regex(self) -> None:
        """Underscores are allowed in tenant IDs."""
        assert _TENANT_ID_RE.match("my_tenant_123") is not None

    def test_sql_injection_payload_rejected(self) -> None:
        """A classic SQL injection attempt must not match the regex."""
        assert _TENANT_ID_RE.match("'; DROP TABLE--") is None

    def test_null_byte_rejected(self) -> None:
        """Null byte injection must not match the regex."""
        assert _TENANT_ID_RE.match("tenant%00id") is None
        assert _TENANT_ID_RE.match("%00") is None
        assert _TENANT_ID_RE.match("ten\x00ant") is None

    def test_empty_string_rejected(self) -> None:
        """Empty string must not match the tenant ID regex (min length 1)."""
        assert _TENANT_ID_RE.match("") is None

    def test_very_long_id_rejected(self) -> None:
        """IDs longer than 128 characters must not match the regex."""
        long_id = "a" * 129
        assert _TENANT_ID_RE.match(long_id) is None

    def test_exactly_128_chars_accepted(self) -> None:
        """An ID of exactly 128 chars should be accepted."""
        exact_id = "a" * 128
        assert _TENANT_ID_RE.match(exact_id) is not None

    def test_space_rejected(self) -> None:
        """Spaces are not in the allowed charset."""
        assert _TENANT_ID_RE.match("tenant alpha") is None

    def test_semicolon_rejected(self) -> None:
        """Semicolons (SQL statement terminators) must be rejected."""
        assert _TENANT_ID_RE.match("tenant;id") is None

    def test_single_quote_rejected(self) -> None:
        """Single quotes (SQL string delimiters) must be rejected."""
        assert _TENANT_ID_RE.match("tenant'id") is None

    @pytest.mark.asyncio
    async def test_set_tenant_context_rejects_injection_payload(self) -> None:
        """Verify the actual set_tenant_context function raises ValueError
        for a SQL injection attempt on a non-SQLite session.
        """
        mock_session = AsyncMock()
        # Simulate a PostgreSQL dialect so the validation path is exercised.
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind

        with pytest.raises(ValueError, match="Invalid tenant_id"):
            await set_tenant_context(mock_session, "'; DROP TABLE users--")

    @pytest.mark.asyncio
    async def test_set_tenant_context_rejects_empty_string(self) -> None:
        """Empty string must raise ValueError on PostgreSQL dialect."""
        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind

        with pytest.raises(ValueError, match="Invalid tenant_id"):
            await set_tenant_context(mock_session, "")

    @pytest.mark.asyncio
    async def test_set_tenant_context_rejects_long_id(self) -> None:
        """IDs exceeding 128 chars must raise ValueError."""
        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind

        with pytest.raises(ValueError, match="Invalid tenant_id"):
            await set_tenant_context(mock_session, "x" * 129)

    @pytest.mark.asyncio
    async def test_set_tenant_context_accepts_valid_id(self) -> None:
        """A valid tenant ID should execute the SET config call."""
        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind

        await set_tenant_context(mock_session, "valid-tenant-123")
        mock_session.execute.assert_called_once()


# ===================================================================
# 2. TestJWTSecretRequired
# ===================================================================


class TestJWTSecretRequired:
    """Verify TokenConfig requires jwt_secret -- no insecure defaults."""

    def test_token_config_without_jwt_secret_raises(self) -> None:
        """Creating TokenConfig without jwt_secret must raise ValidationError.

        The ``jwt_secret`` field is typed as ``SecretStr`` with no default,
        so Pydantic will refuse to construct the model without it.
        """
        with pytest.raises(ValidationError):
            TokenConfig()  # type: ignore[call-arg]

    def test_token_config_with_jwt_secret_succeeds(self) -> None:
        """TokenConfig with an explicit jwt_secret must construct successfully."""
        config = TokenConfig(jwt_secret=SecretStr("my-secure-secret"))
        assert config.jwt_secret.get_secret_value() == "my-secure-secret"

    def test_token_config_jwt_secret_not_exposed_in_repr(self) -> None:
        """SecretStr should mask the value in string representations."""
        config = TokenConfig(jwt_secret=SecretStr("super-secret-key"))
        config_str = str(config)
        assert "super-secret-key" not in config_str

    def test_token_config_defaults(self) -> None:
        """Verify sensible defaults for other fields when jwt_secret is provided."""
        config = TokenConfig(jwt_secret=SecretStr("test"))
        assert config.jwt_algorithm == "HS256"
        assert config.token_ttl_seconds == 3600
        assert config.max_token_ttl_seconds == 86400


# ===================================================================
# 3. TestOIDCAlgorithmHardcoded
# ===================================================================


class TestOIDCAlgorithmHardcoded:
    """Verify OIDC does not accept alg from the unverified token header.

    The OIDCProvider.validate_token() method hardcodes ``algorithms=["RS256"]``
    in the ``jwt.decode()`` call.  An attacker setting ``alg: HS256`` in the
    token header to exploit algorithm confusion must be rejected.
    """

    def test_validate_token_uses_rs256_only(self) -> None:
        """The jwt.decode call must be hardcoded to RS256 only.

        We mock the jwt module to intercept the decode() call and verify
        the ``algorithms`` argument.
        """
        provider = OIDCProvider(
            issuer_url="https://auth.example.com",
            audience="my-app",
        )

        # Create a fake token header claiming HS256.
        fake_header = {"alg": "HS256", "kid": "test-key-id"}
        fake_key_data = {"kid": "test-key-id", "kty": "RSA", "n": "fake", "e": "AQAB"}

        with (
            patch("jwt.get_unverified_header", return_value=fake_header),
            patch.object(provider, "_get_signing_key", return_value=fake_key_data),
            patch("jwt.algorithms.RSAAlgorithm.from_jwk", return_value=MagicMock()),
            patch("jwt.decode") as mock_decode,
        ):
            mock_decode.return_value = {
                "sub": "user1",
                "tenant_id": "t1",
                "iss": "https://auth.example.com",
                "iat": time.time(),
                "exp": time.time() + 3600,
            }

            provider.validate_token("fake.jwt.token")

            # Verify that algorithms is hardcoded to RS256.
            call_kwargs = mock_decode.call_args
            assert call_kwargs[1]["algorithms"] == ["RS256"]

    def test_alg_from_header_is_ignored(self) -> None:
        """Even if the token header says HS256, the provider must use RS256.

        This test verifies the security comment in validate_token():
        'The alg header from the unverified token is intentionally ignored.'
        """
        provider = OIDCProvider(
            issuer_url="https://auth.example.com",
            audience="my-app",
        )

        # Token with alg: HS256 in header (algorithm confusion attack).
        fake_header = {"alg": "HS256", "kid": "key-1"}
        fake_key_data = {"kid": "key-1", "kty": "RSA", "n": "fake", "e": "AQAB"}

        with (
            patch("jwt.get_unverified_header", return_value=fake_header),
            patch.object(provider, "_get_signing_key", return_value=fake_key_data),
            patch("jwt.algorithms.RSAAlgorithm.from_jwk", return_value=MagicMock()),
            patch("jwt.decode") as mock_decode,
        ):
            mock_decode.return_value = {
                "sub": "user1",
                "tenant_id": "t1",
                "iss": "https://auth.example.com",
            }

            provider.validate_token("fake.jwt.token")

            # The algorithms kwarg must NOT contain HS256.
            algorithms_used = mock_decode.call_args[1]["algorithms"]
            assert "HS256" not in algorithms_used
            assert algorithms_used == ["RS256"]


# ===================================================================
# 4. TestServiceRoleBypass
# ===================================================================


class TestServiceRoleBypass:
    """Verify SERVICE role cannot pass require_role checks.

    SERVICE has numeric value 10 (> ADMIN=3), so without an explicit
    guard, ``role >= min_role`` would incorrectly grant SERVICE the
    same access as ADMIN.  The require_role() guard must reject SERVICE
    with a 403 explaining that service accounts should use permission-based
    auth instead.
    """

    def test_require_role_admin_rejects_service(self) -> None:
        """require_role(Role.ADMIN) must reject Role.SERVICE with 403."""
        from fastapi import HTTPException

        guard = require_role(Role.ADMIN)
        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)
        assert exc_info.value.status_code == 403
        assert "Service accounts" in exc_info.value.detail

    def test_require_role_engineer_rejects_service(self) -> None:
        """require_role(Role.ENGINEER) must reject Role.SERVICE."""
        from fastapi import HTTPException

        guard = require_role(Role.ENGINEER)
        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)
        assert exc_info.value.status_code == 403
        assert "permission-based" in exc_info.value.detail

    def test_require_role_operator_rejects_service(self) -> None:
        """require_role(Role.OPERATOR) must reject Role.SERVICE."""
        from fastapi import HTTPException

        guard = require_role(Role.OPERATOR)
        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)
        assert exc_info.value.status_code == 403

    def test_require_role_viewer_rejects_service(self) -> None:
        """Even require_role(Role.VIEWER) must reject SERVICE."""
        from fastapi import HTTPException

        guard = require_role(Role.VIEWER)
        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)
        assert exc_info.value.status_code == 403

    def test_require_role_admin_accepts_admin(self) -> None:
        """Sanity check: require_role(Role.ADMIN) accepts ADMIN."""
        guard = require_role(Role.ADMIN)
        result = guard(role=Role.ADMIN)
        assert result == Role.ADMIN

    def test_require_role_engineer_accepts_admin(self) -> None:
        """Higher roles can pass lower role checks."""
        guard = require_role(Role.ENGINEER)
        result = guard(role=Role.ADMIN)
        assert result == Role.ADMIN

    def test_require_permission_works_for_service(self) -> None:
        """SERVICE accounts should use require_permission instead.

        SERVICE has READ_PLANS permission, so require_permission(READ_PLANS)
        should accept it.
        """
        guard = require_permission(Permission.READ_PLANS)
        result = guard(role=Role.SERVICE)
        assert result == Role.SERVICE

    def test_require_permission_rejects_service_for_admin_perm(self) -> None:
        """SERVICE lacks MANAGE_SETTINGS, so permission guard must reject it."""
        from fastapi import HTTPException

        guard = require_permission(Permission.MANAGE_SETTINGS)
        with pytest.raises(HTTPException) as exc_info:
            guard(role=Role.SERVICE)
        assert exc_info.value.status_code == 403

    def test_service_cannot_approve_plans(self) -> None:
        """SERVICE must not be able to approve plans (safety-critical)."""
        assert not role_has_permission(Role.SERVICE, Permission.APPROVE_PLANS)

    def test_service_cannot_manage_webhooks(self) -> None:
        """SERVICE must not manage webhooks."""
        assert not role_has_permission(Role.SERVICE, Permission.MANAGE_WEBHOOKS)


# ===================================================================
# 5. TestCSRFProtection
# ===================================================================


class TestCSRFProtection:
    """Verify CSRF middleware enforces the double-submit cookie pattern.

    CSRF validation is only enforced when the request carries a
    ``refresh_token`` cookie (browser sessions).  API-key and Bearer-only
    requests bypass CSRF.
    """

    @pytest.fixture()
    def _csrf_app(self) -> Any:
        """Create a minimal FastAPI app with CSRF middleware."""
        from fastapi import FastAPI
        from starlette.responses import JSONResponse

        from api.middleware.csrf import CSRFMiddleware

        app = FastAPI()
        app.add_middleware(CSRFMiddleware)

        @app.post("/api/v1/test-endpoint")
        async def test_endpoint() -> JSONResponse:
            return JSONResponse({"status": "ok"})

        @app.get("/api/v1/test-get")
        async def test_get() -> JSONResponse:
            return JSONResponse({"status": "ok"})

        return app

    @pytest.mark.asyncio
    async def test_post_with_refresh_cookie_but_no_csrf_returns_403(
        self,
        _csrf_app: Any,
    ) -> None:
        """POST with refresh_token cookie but no CSRF token -> 403."""
        transport = ASGITransport(app=_csrf_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/test-endpoint",
                cookies={"refresh_token": "some-refresh-token"},
            )
        assert resp.status_code == 403
        assert "CSRF" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_post_with_refresh_cookie_and_valid_csrf_passes(
        self,
        _csrf_app: Any,
    ) -> None:
        """POST with refresh_token cookie and matching CSRF token -> passes."""
        csrf_value = secrets.token_hex(32)
        transport = ASGITransport(app=_csrf_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/test-endpoint",
                cookies={
                    "refresh_token": "some-refresh-token",
                    "csrf_token": csrf_value,
                },
                headers={"X-CSRF-Token": csrf_value},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_post_without_refresh_cookie_no_csrf_needed(
        self,
        _csrf_app: Any,
    ) -> None:
        """POST without refresh_token cookie (API key auth) -> no CSRF needed."""
        transport = ASGITransport(app=_csrf_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/v1/test-endpoint")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_csrf_mismatch_returns_403(
        self,
        _csrf_app: Any,
    ) -> None:
        """Mismatched CSRF cookie and header values -> 403."""
        transport = ASGITransport(app=_csrf_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/test-endpoint",
                cookies={
                    "refresh_token": "some-refresh-token",
                    "csrf_token": "cookie-value",
                },
                headers={"X-CSRF-Token": "different-header-value"},
            )
        assert resp.status_code == 403
        assert "mismatch" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_get_request_sets_csrf_cookie_on_first_visit(
        self,
        _csrf_app: Any,
    ) -> None:
        """GET request without existing CSRF cookie should set one."""
        transport = ASGITransport(app=_csrf_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/test-get")
        assert resp.status_code == 200
        # The middleware should have set the csrf_token cookie.
        assert "csrf_token" in resp.cookies

    @pytest.mark.asyncio
    async def test_put_with_refresh_cookie_but_no_csrf_returns_403(
        self,
        _csrf_app: Any,
    ) -> None:
        """PUT is also a state-changing method and needs CSRF protection."""
        from fastapi import FastAPI
        from starlette.responses import JSONResponse

        from api.middleware.csrf import CSRFMiddleware

        app = FastAPI()
        app.add_middleware(CSRFMiddleware)

        @app.put("/api/v1/test-put")
        async def test_put() -> JSONResponse:
            return JSONResponse({"status": "ok"})

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.put(
                "/api/v1/test-put",
                cookies={"refresh_token": "some-refresh-token"},
            )
        assert resp.status_code == 403


# ===================================================================
# 6. TestLLMKeyRedaction
# ===================================================================


class TestLLMKeyRedaction:
    """Verify API keys are redacted from error messages.

    The ``_redact_key()`` function in tenant_config.py must remove all
    occurrences of the key (full and last-8-char suffix) from any
    error message before it reaches HTTP responses or logs.
    """

    def test_full_key_redacted_from_message(self) -> None:
        """The full API key value must be replaced with [REDACTED]."""
        key = "sk-ant-api03-abcdef1234567890"
        message = f"AuthenticationError: Invalid API key: {key}"
        result = _redact_key(message, key)
        assert key not in result
        assert "[REDACTED]" in result

    def test_partial_key_suffix_also_redacted(self) -> None:
        """Some error messages include the last 8 chars of the key."""
        key = "sk-ant-api03-abcdef1234567890"
        suffix = key[-8:]
        message = f"Invalid key ending in ...{suffix}"
        result = _redact_key(message, key)
        assert suffix not in result
        assert "[REDACTED]" in result

    def test_empty_key_returns_message_unchanged(self) -> None:
        """If the key is empty, the message should be returned as-is."""
        message = "Some error occurred"
        result = _redact_key(message, "")
        assert result == message

    def test_short_key_no_suffix_redaction(self) -> None:
        """Keys with 8 or fewer chars skip the suffix redaction path."""
        key = "12345678"
        message = f"Error with key {key} here"
        result = _redact_key(message, key)
        assert key not in result
        assert "[REDACTED]" in result

    def test_multiple_occurrences_all_redacted(self) -> None:
        """If the key appears multiple times, all occurrences are redacted."""
        key = "sk-secret-key-value-1234"
        message = f"First: {key}, second: {key}, suffix: {key[-8:]}"
        result = _redact_key(message, key)
        assert key not in result
        assert key[-8:] not in result

    def test_key_in_json_error_redacted(self) -> None:
        """Key embedded in a JSON-like error string gets redacted."""
        key = "sk-ant-api03-xyzxyzxyz12345678"
        error_dict = {"error": {"message": f"Invalid API Key {key}", "type": "authentication_error"}}
        message = json.dumps(error_dict)
        result = _redact_key(message, key)
        assert key not in result
        assert "[REDACTED]" in result

    def test_message_without_key_unchanged(self) -> None:
        """A message that doesn't contain the key should be returned as-is."""
        key = "sk-secret-12345678901234"
        message = "Connection timeout after 10 seconds"
        result = _redact_key(message, key)
        assert result == message
