"""Authentication middleware that extracts and validates JWT tokens.

Extracts ``Authorization: Bearer <token>`` from every request, validates
via :class:`TokenManager`, and populates ``request.state`` with
``tenant_id``, ``sub`` (user identity), ``scopes``, and ``role``.

The ``role`` claim is read from the JWT payload and stored as a string
on ``request.state.role`` for downstream RBAC enforcement.  When the
token omits a role claim, the default ``"viewer"`` (least-privilege)
is applied.

Endpoints explicitly listed in ``_PUBLIC_PATHS`` bypass authentication.
"""

from __future__ import annotations

import logging
import os
import secrets
import time
from collections.abc import Callable
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from api.security import AuthMode, TokenConfig, TokenManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Revocation cache
# ---------------------------------------------------------------------------


class _RevocationCache:
    """TTL cache for token revocation lookups.

    Caches both positive (revoked) and negative (not revoked) results
    for a short TTL to reduce database pressure and survive brief
    outages.  When the database is unreachable and no cached result
    exists, the caller should fail closed (reject the request).

    The cache is intentionally simple (dict + monotonic timestamps)
    with no external dependencies.  It is safe for single-process
    use; for multi-replica deployments, consider a shared Redis cache.

    Parameters
    ----------
    ttl_seconds:
        How long each cache entry remains valid (default: 30 seconds).
    max_entries:
        Hard cap on cache size to prevent unbounded memory growth.
        When reached, stale entries are cleaned before inserting.
    """

    def __init__(
        self,
        ttl_seconds: float = 30.0,
        max_entries: int = 10_000,
    ) -> None:
        self._cache: dict[str, tuple[bool, float]] = {}
        self._ttl = ttl_seconds
        self._max_entries = max_entries

    def get(self, token_jti: str) -> bool | None:
        """Return the cached revocation status, or ``None`` on miss/expiry."""
        entry = self._cache.get(token_jti)
        if entry is None:
            return None
        is_revoked, cached_at = entry
        if time.monotonic() - cached_at > self._ttl:
            del self._cache[token_jti]
            return None
        return is_revoked

    def set(self, token_jti: str, is_revoked: bool) -> None:
        """Store a revocation result with the current timestamp."""
        if len(self._cache) >= self._max_entries:
            self.cleanup()
        self._cache[token_jti] = (is_revoked, time.monotonic())

    def cleanup(self) -> None:
        """Remove all entries whose TTL has expired."""
        now = time.monotonic()
        stale = [k for k, (_, t) in self._cache.items() if now - t > self._ttl]
        for k in stale:
            del self._cache[k]


# Module-level singleton used by the revocation checker.
_revocation_cache = _RevocationCache()

# Revocation checker: injected at startup via init_revocation_checker().
_check_revocation: Callable[[str], Any] | None = None


def init_revocation_checker(session_factory: Any) -> None:
    """Wire the token revocation checker into the auth middleware.

    Called once at application startup with the async session factory.
    The checker queries the ``token_revocations`` table for the given jti,
    with a short TTL cache to reduce database load and survive brief
    outages.

    Behaviour on database failure:

    - If a recent cached result exists, the cached value is returned.
      This means a token revoked less than 30 seconds ago may still
      be accepted during an outage (bounded staleness window).
    - If no cached result exists AND the database is unreachable, the
      request is rejected (fail-closed) and an ERROR-level log is
      emitted so monitoring can alert on sustained DB outages.
    """
    global _check_revocation  # noqa: PLW0603

    async def _checker(jti: str) -> bool:
        # Check the TTL cache first.
        cached = _revocation_cache.get(jti)
        if cached is not None:
            return cached

        from core_engine.state.repository import TokenRevocationRepository

        try:
            async with session_factory() as session:
                repo = TokenRevocationRepository(session)
                is_revoked = await repo.is_revoked(jti)
                _revocation_cache.set(jti, is_revoked)
                return is_revoked
        except Exception:
            # Database is unreachable.  Try the cache one more time in
            # case the entry expired between the initial check and now
            # (unlikely but harmless).
            stale = _revocation_cache.get(jti)
            if stale is not None:
                logger.warning(
                    "Revocation DB check failed for jti=%s; using cached result (is_revoked=%s)",
                    jti,
                    stale,
                )
                return stale

            # No cache entry available -- fail closed to prevent a
            # revoked token from being accepted during a DB outage.
            logger.error(
                "Revocation DB check failed for jti=%s and no cached result "
                "available -- failing closed (rejecting request).  This "
                "indicates a database connectivity issue that needs immediate "
                "attention.",
                jti,
                exc_info=True,
            )
            return True

    _check_revocation = _checker


# Paths that do not require authentication.
_PUBLIC_PATHS: frozenset[str] = frozenset(
    {
        "/api/v1/health",
        "/ready",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/favicon.ico",
        "/api/v1/billing/webhooks",
        "/api/v1/webhooks/github",
        "/api/v1/auth/signup",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/session",
        "/api/v1/auth/logout",
        "/metrics",
    }
)

# Prefixes that skip auth (e.g. static docs assets).
_PUBLIC_PREFIXES: tuple[str, ...] = (
    "/docs",
    "/redoc",
)


def _build_token_config() -> TokenConfig:
    """Construct a :class:`TokenConfig` from environment variables.

    Reads the following env vars with sensible defaults:

    - ``AUTH_MODE`` -- one of ``jwt``, ``kms_exchange``, ``oidc_onprem``,
      ``development``.  Defaults to ``development``.
    - ``JWT_SECRET`` -- signing/verification secret.
    - ``JWT_ALGORITHM`` -- e.g. ``HS256``.
    - ``TOKEN_TTL_SECONDS`` -- default token lifetime.
    - ``MAX_TOKEN_TTL_SECONDS`` -- hard cap on token lifetime.
    - ``OIDC_ISSUER_URL`` -- required when ``AUTH_MODE=oidc_onprem``.
    - ``OIDC_AUDIENCE`` -- audience claim for OIDC validation.
    - ``KMS_KEY_ARN`` -- required when ``AUTH_MODE=kms_exchange``.
    """
    from pydantic import SecretStr

    auth_mode_raw = os.environ.get("AUTH_MODE", "development").lower()
    try:
        auth_mode = AuthMode(auth_mode_raw)
    except ValueError:
        logger.warning("Unknown AUTH_MODE '%s'; falling back to development", auth_mode_raw)
        auth_mode = AuthMode.DEVELOPMENT

    jwt_secret_value = os.environ.get("JWT_SECRET", "")
    if not jwt_secret_value:
        if auth_mode == AuthMode.DEVELOPMENT:
            jwt_secret_value = f"dev-{secrets.token_hex(32)}"
            logger.warning(
                "JWT_SECRET not set — generated random per-process dev secret. "
                "Tokens will not survive process restarts."
            )
        else:
            raise RuntimeError(
                f"JWT_SECRET environment variable must be set when AUTH_MODE={auth_mode.value}. "
                "Refusing to start with an insecure default secret."
            )
    jwt_algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
    token_ttl = int(os.environ.get("TOKEN_TTL_SECONDS", "3600"))
    max_token_ttl = int(os.environ.get("MAX_TOKEN_TTL_SECONDS", "86400"))
    refresh_ttl = int(os.environ.get("REFRESH_TOKEN_TTL_SECONDS", "86400"))
    kms_key_arn = os.environ.get("KMS_KEY_ARN")
    oidc_issuer_url = os.environ.get("OIDC_ISSUER_URL")
    oidc_audience = os.environ.get("OIDC_AUDIENCE")

    return TokenConfig(
        auth_mode=auth_mode,
        jwt_secret=SecretStr(jwt_secret_value),
        jwt_algorithm=jwt_algorithm,
        token_ttl_seconds=token_ttl,
        max_token_ttl_seconds=max_token_ttl,
        refresh_token_ttl_seconds=refresh_ttl,
        kms_key_arn=kms_key_arn,
        oidc_issuer_url=oidc_issuer_url,
        oidc_audience=oidc_audience,
    )


def _is_public_path(path: str) -> bool:
    """Return ``True`` if the path should bypass authentication."""
    if path in _PUBLIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in _PUBLIC_PREFIXES)


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that enforces Bearer token authentication.

    On each request the middleware:

    1. Checks whether the path is public (health, docs) and skips auth.
    2. Extracts the ``Authorization: Bearer <token>`` header.
    3. Validates the token via :class:`TokenManager`.
    4. Stores ``tenant_id``, ``sub``, and ``scopes`` on ``request.state``.
    5. Returns a 401/403 JSON response on failure.
    """

    def __init__(self, app: Any) -> None:
        super().__init__(app)
        config = _build_token_config()
        self._token_manager = TokenManager(config)
        self._auth_mode = config.auth_mode
        logger.info("AuthenticationMiddleware initialised (mode=%s)", config.auth_mode.value)

    @staticmethod
    def _extract_role_from_claims(claims: Any) -> str | None:
        """Extract the ``role`` claim from already-validated token claims.

        Uses the validated claims object rather than re-decoding the raw token,
        avoiding redundant base64 parsing and potential inconsistencies.
        """
        # Check direct attribute first.
        role = getattr(claims, "role", None)
        if role:
            return str(role)

        # Fall back to model_dump() for extra fields that may not be
        # defined on the TokenClaims schema.
        try:
            dumped = claims.model_dump()
            return dumped.get("role")
        except AttributeError:
            pass
        return None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Allow public endpoints through without authentication.
        if _is_public_path(path):
            return await call_next(request)

        # Extract the Authorization header.
        auth_header = request.headers.get("authorization")
        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing Authorization header"},
            )

        # Expect "Bearer <token>" format.
        parts = auth_header.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization header must use Bearer scheme"},
            )

        token = parts[1]

        # --- API Key authentication (bmkey. prefix) -----------------------
        if token.startswith("bmkey."):
            return await self._authenticate_api_key(request, call_next, token)

        # --- JWT / HMAC token authentication ------------------------------
        try:
            claims = self._token_manager.validate_token(token)
        except PermissionError as exc:
            error_msg = str(exc)
            # Distinguish expired tokens (403) from invalid tokens (401).
            if "expired" in error_msg.lower():
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Token has expired"},
                )
            return JSONResponse(
                status_code=401,
                content={"detail": f"Invalid token: {error_msg}"},
            )

        # Check token revocation (jti-based replay protection).
        jti = getattr(claims, "jti", None)
        if jti and _check_revocation is not None:
            is_revoked = await _check_revocation(jti)
            if is_revoked:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Token has been revoked"},
                )

        # Populate request.state for downstream dependencies and handlers.
        request.state.tenant_id = claims.tenant_id
        request.state.sub = claims.sub
        request.state.scopes = claims.scopes

        # Identity kind (user vs service account).
        identity_kind = getattr(claims, "identity_kind", "user")
        request.state.identity_kind = identity_kind

        # Extract the role claim for RBAC enforcement.  The claim may be
        # present as a top-level field on TokenClaims or as an extra field
        # in the raw payload.  Default depends on identity_kind: service
        # accounts default to "service", human users to "viewer".
        role_value = getattr(claims, "role", None)
        if role_value is None:
            # The role may have been decoded as part of the JWT payload but
            # not modelled on TokenClaims — check the validated claims
            # object directly (avoids redundant base64 re-decoding).
            role_value = self._extract_role_from_claims(claims)
        if not role_value:
            role_value = "service" if identity_kind == "service" else "viewer"
        request.state.role = role_value

        return await call_next(request)

    async def _authenticate_api_key(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
        api_key: str,
    ) -> Response:
        """Validate an API key (``bmkey.`` prefix) and populate request.state.

        API keys are validated via the :class:`APIKeyRepository` which checks
        the SHA-256 hash, expiration, and revocation status.
        """
        from api.dependencies import get_session_factory

        try:
            session_factory = get_session_factory()
        except RuntimeError:
            logger.error("Cannot validate API key: database not initialised")
            return JSONResponse(
                status_code=503,
                content={"detail": "Service unavailable"},
            )

        try:
            async with session_factory() as session:
                from core_engine.state.repository import APIKeyRepository, UserRepository

                repo = APIKeyRepository(session)
                key_row = await repo.validate_key(api_key)

                if key_row is None:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Invalid or expired API key"},
                    )

                # Look up the owning user to get their role.
                user_repo = UserRepository(session, tenant_id=key_row.tenant_id)
                user = await user_repo.get_by_id(key_row.user_id)

                if user is None or not user.is_active:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "API key owner account not found or deactivated"},
                    )

                # Populate request.state.
                request.state.tenant_id = key_row.tenant_id
                request.state.sub = key_row.user_id
                request.state.scopes = key_row.scopes or ["read", "write"]
                request.state.identity_kind = "service"
                request.state.role = user.role

                await session.commit()

        except Exception:
            logger.warning("API key validation failed", exc_info=True)
            return JSONResponse(
                status_code=401,
                content={"detail": "API key validation failed"},
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# License enforcement middleware
# ---------------------------------------------------------------------------

# Global license manager, initialised at startup via init_license_manager().
_license_manager: Any = None


def init_license_manager(license_path: str | None = None) -> None:
    """Initialise the global license manager.

    Called once at application startup.  If ``license_path`` is provided,
    loads and verifies the license file.  Otherwise, the system operates
    at Community tier (free, limited features).

    Parameters
    ----------
    license_path:
        Path to the JSON license file, or ``None`` for Community mode.
    """
    global _license_manager  # noqa: PLW0603
    from pathlib import Path as _Path

    from core_engine.license.license_manager import LicenseExpiredError, LicenseManager

    _license_manager = LicenseManager(public_key_bytes=None)  # No embedded key yet.

    if license_path:
        try:
            _license_manager.load_license(_Path(license_path))
            logger.info("License loaded from %s", license_path)
        except LicenseExpiredError:
            logger.warning("License at %s has expired -- falling back to Community", license_path)
        except FileNotFoundError:
            logger.warning("License file not found: %s -- using Community tier", license_path)
        except Exception:
            logger.warning("Failed to load license: %s -- using Community tier", license_path, exc_info=True)


def get_license_manager() -> Any:
    """Return the global license manager instance."""
    return _license_manager


# Mapping from API path prefixes to the Feature required.
_GATED_ENDPOINTS: dict[str, str] = {
    "/api/v1/audit": "audit_log",
    "/api/v1/reconciliation": "reconciliation",
    "/api/v1/tenant/config": "multi_tenant",
}


class LicenseMiddleware(BaseHTTPMiddleware):
    """Enforce license-tier feature gating on API endpoints.

    Returns HTTP 402 (Payment Required) when an endpoint requires a
    feature that is not available at the current license tier.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if _license_manager is None:
            return await call_next(request)

        path = request.url.path

        for prefix, feature_name in _GATED_ENDPOINTS.items():
            if path.startswith(prefix):
                from core_engine.license.feature_flags import Feature

                try:
                    feature = Feature(feature_name)
                except ValueError:
                    break

                if not _license_manager.check_entitlement(feature):
                    return JSONResponse(
                        status_code=402,
                        content={
                            "detail": (
                                f"Feature '{feature_name}' requires a higher license tier. "
                                f"Current tier: {_license_manager.effective_tier.value}"
                            ),
                        },
                    )
                break

        return await call_next(request)
