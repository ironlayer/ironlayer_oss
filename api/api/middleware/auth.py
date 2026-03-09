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
    with no external dependencies.  It is safe for single-process use.

    **Multi-replica behaviour:** Revocation state is process-local. A token
    revoked on one replica may still be accepted by other replicas for up to
    ``ttl_seconds`` (default 30s) until their local cache expires or they
    query the database. For strict revocation across all replicas, use a
    shared store (e.g. Redis) for revocation state instead of this cache.

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

    def get_batch(self, jtis: list[str]) -> dict[str, bool | None]:
        """Return cached revocation status for multiple JTIs in one call.

        Returns a mapping of ``jti -> bool | None`` where ``None`` indicates
        a cache miss or an expired entry for that JTI.
        """
        result: dict[str, bool | None] = {}
        for jti in jtis:
            result[jti] = self.get(jti)
        return result

    def set_batch(self, statuses: dict[str, bool]) -> None:
        """Store multiple revocation results atomically."""
        for jti, is_revoked in statuses.items():
            self.set(jti, is_revoked)


# Module-level singleton used by the revocation checker.
_revocation_cache = _RevocationCache()

# Revocation checker: injected at startup via init_revocation_checker().
_check_revocation: Callable[[str], Any] | None = None

# Batch revocation checker: injected at startup alongside _check_revocation.
_check_revocation_batch: Callable[[list[str]], Any] | None = None


def init_revocation_checker(session_factory: Any, redis_url: str | None = None) -> None:
    """Wire the token revocation checker into the auth middleware.

    Called once at application startup with the async session factory.

    Revocation check layers (fastest → authoritative):
    1. **L1 in-process cache** (5s TTL) — eliminates most DB/Redis round-trips.
    2. **Redis shared cache** — propagates revocation across replicas within 5s.
       Fail-closed: if Redis is configured but unreachable, the request is
       rejected (status 401) rather than accepted with stale state.
    3. **PostgreSQL** — authoritative source of truth for JTI revocation.

    When no cached result is available and the database is unreachable, the
    request is rejected (fail-closed) and an ERROR-level log is emitted.

    Parameters
    ----------
    session_factory:
        Async session factory for querying the token_revocations table.
    redis_url:
        Optional Redis URL. When provided, Redis is used as a shared L2
        cache so that token revocation propagates across replicas within
        the L1 TTL window (default: 5 seconds).
    """
    global _check_revocation  # noqa: PLW0603
    global _check_revocation_batch  # noqa: PLW0603

    # Use a 5s TTL for L1 cache when Redis is available (L2 handles broader window).
    # When Redis is absent, keep 30s TTL for resilience.
    l1_ttl = 5.0 if redis_url else 30.0
    l1_cache = _RevocationCache(ttl_seconds=l1_ttl)

    _REDIS_KEY_PREFIX = "ironlayer:revoked:"

    async def _checker(jti: str) -> bool:
        # L1: in-process cache.
        cached = l1_cache.get(jti)
        if cached is not None:
            return cached

        # L2: Redis shared cache (if configured).
        if redis_url:
            from api.services.redis_client import get_redis_client

            redis = await get_redis_client()
            if redis is None:
                # Redis was configured but is now unreachable: fail-closed.
                logger.error(
                    "Revocation Redis check failed for jti=%s — Redis unavailable. "
                    "Failing closed (rejecting request). Configure REDIS_URL or "
                    "check Redis connectivity.",
                    jti,
                )
                return True  # fail-closed

            try:
                redis_result = await redis.get(f"{_REDIS_KEY_PREFIX}{jti}")
                if redis_result is not None:
                    is_revoked = redis_result == "1"
                    l1_cache.set(jti, is_revoked)
                    return is_revoked
            except Exception as exc:
                logger.warning(
                    "Revocation Redis GET failed for jti=%s: %s — falling through to DB",
                    jti,
                    exc,
                )

        # L3: database (authoritative).
        from core_engine.state.repository import TokenRevocationRepository

        try:
            async with session_factory() as session:
                repo = TokenRevocationRepository(session)
                is_revoked = await repo.is_revoked(jti)
                l1_cache.set(jti, is_revoked)

                # Populate Redis so other replicas benefit from this lookup.
                if redis_url and is_revoked:
                    try:
                        from api.services.redis_client import get_redis_client

                        redis = await get_redis_client()
                        if redis is not None:
                            # Keep Redis entry for 24h (tokens can't exceed max TTL).
                            await redis.set(
                                f"{_REDIS_KEY_PREFIX}{jti}",
                                "1" if is_revoked else "0",
                                ex=86400,
                            )
                    except Exception:
                        pass  # Redis write failure is non-fatal; DB is authoritative

                return is_revoked
        except Exception:
            # Database unreachable — try stale L1 entry.
            stale = l1_cache.get(jti)
            if stale is not None:
                logger.warning(
                    "Revocation DB check failed for jti=%s; using stale L1 result (is_revoked=%s)",
                    jti,
                    stale,
                )
                return stale

            # No cached result — fail closed.
            logger.error(
                "Revocation DB check failed for jti=%s and no cached result "
                "available -- failing closed (rejecting request).",
                jti,
                exc_info=True,
            )
            return True

    async def _batch_checker(jtis: list[str]) -> dict[str, bool]:
        """Check multiple JTIs for revocation using Redis MGET when available.

        Returns a ``{jti: is_revoked}`` mapping.  For each JTI:
        1. Checked against L1 in-process cache.
        2. Remaining misses resolved in a single Redis MGET call (L2).
        3. Remaining misses resolved via individual DB lookups (L3).

        This is significantly more efficient than calling ``_checker`` in a
        loop when processing a batch of tokens simultaneously (BL-095).
        """
        result: dict[str, bool] = {}
        misses: list[str] = []

        # L1: check in-process cache for all JTIs in one pass.
        cached = l1_cache.get_batch(jtis)
        for jti, status in cached.items():
            if status is not None:
                result[jti] = status
            else:
                misses.append(jti)

        if not misses:
            return result

        # L2: Redis MGET for all remaining misses in a single round-trip.
        redis_misses: list[str] = list(misses)
        if redis_url and misses:
            from api.services.redis_client import get_redis_client

            redis = await get_redis_client()
            if redis is None:
                # Redis configured but unreachable — fail-closed for all misses.
                logger.error(
                    "Batch revocation Redis MGET failed — Redis unavailable. "
                    "Failing closed for %d JTIs.",
                    len(misses),
                )
                for jti in misses:
                    result[jti] = True  # fail-closed
                return result

            try:
                redis_keys = [f"{_REDIS_KEY_PREFIX}{jti}" for jti in misses]
                redis_values = await redis.mget(*redis_keys)
                redis_misses = []
                for jti, val in zip(misses, redis_values):
                    if val is not None:
                        is_revoked = val == "1"
                        l1_cache.set(jti, is_revoked)
                        result[jti] = is_revoked
                    else:
                        redis_misses.append(jti)
            except Exception as exc:
                logger.warning(
                    "Batch revocation Redis MGET failed: %s — falling through to DB",
                    exc,
                )
                redis_misses = list(misses)

        # L3: DB lookups for any JTIs still unresolved.
        if redis_misses:
            from core_engine.state.repository import TokenRevocationRepository

            try:
                async with session_factory() as session:
                    repo = TokenRevocationRepository(session)
                    for jti in redis_misses:
                        is_revoked = await repo.is_revoked(jti)
                        l1_cache.set(jti, is_revoked)
                        result[jti] = is_revoked

                        # Populate Redis for other replicas.
                        if redis_url and is_revoked:
                            try:
                                from api.services.redis_client import get_redis_client

                                redis = await get_redis_client()
                                if redis is not None:
                                    await redis.set(
                                        f"{_REDIS_KEY_PREFIX}{jti}",
                                        "1",
                                        ex=86400,
                                    )
                            except Exception:
                                pass
            except Exception:
                for jti in redis_misses:
                    stale = l1_cache.get(jti)
                    if stale is not None:
                        result[jti] = stale
                        logger.warning(
                            "Revocation DB batch check failed for jti=%s; using stale L1 result",
                            jti,
                        )
                    else:
                        logger.error(
                            "Revocation DB batch check failed for jti=%s and no cached result "
                            "— failing closed.",
                            jti,
                        )
                        result[jti] = True  # fail-closed

        return result

    _check_revocation = _checker
    _check_revocation_batch = _batch_checker


# Paths that do not require authentication.
# OpenAPI docs paths (/docs, /redoc, /openapi.json) are only included when
# running in development mode — in staging/production the FastAPI application
# is configured with docs_url=None, so those paths return 404.  Omitting them
# from _PUBLIC_PATHS in non-dev prevents auth bypass for those paths if docs
# are accidentally re-enabled.
def _build_public_paths() -> frozenset[str]:
    """Compute the public-path set based on the current runtime environment."""
    _platform_env = os.environ.get("PLATFORM_ENV", "development")
    _auth_mode_raw = os.environ.get("AUTH_MODE", "development").lower()
    _is_dev = _platform_env == "development" and _auth_mode_raw == "development"

    paths: set[str] = {
        "/api/v1/health",
        "/ready",
        "/favicon.ico",
        "/api/v1/billing/webhooks",
        "/api/v1/webhooks/github",
        "/api/v1/auth/signup",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/session",
        "/api/v1/auth/logout",
    }
    if _is_dev:
        # Interactive docs are only served and publicly accessible in dev.
        paths |= {"/docs", "/openapi.json", "/redoc"}
        # Metrics are public in dev; in non-dev Prometheus must use a bearer token.
        paths.add("/metrics")
    elif os.environ.get("METRICS_PUBLIC", "false").lower() == "true":
        # Operators may opt-in to public metrics for internally-only deployments
        # where the API port is not reachable from the public internet.
        paths.add("/metrics")
    return frozenset(paths)


def _build_public_prefixes() -> tuple[str, ...]:
    """Compute the public-prefix tuple based on the current runtime environment."""
    _platform_env = os.environ.get("PLATFORM_ENV", "development")
    _auth_mode_raw = os.environ.get("AUTH_MODE", "development").lower()
    _is_dev = _platform_env == "development" and _auth_mode_raw == "development"
    # /docs and /redoc prefixes cover static assets served by FastAPI's Swagger UI.
    return ("/docs", "/redoc") if _is_dev else ()


_PUBLIC_PATHS: frozenset[str] = _build_public_paths()

# Prefixes that skip auth (e.g. static docs assets — dev only).
_PUBLIC_PREFIXES: tuple[str, ...] = _build_public_prefixes()

# Allowlist of valid role claim values.  JWT tokens containing any other
# role value are normalised to "viewer" (least privilege) to prevent RBAC
# bypass via crafted tokens (BL-061).
_KNOWN_ROLES: frozenset[str] = frozenset({"admin", "engineer", "viewer", "service", "operator"})


def _build_token_config_from_settings(settings: Any) -> TokenConfig:
    """Build :class:`TokenConfig` from :class:`APISettings` (app.state.settings)."""
    from pydantic import SecretStr

    auth_mode_raw = os.environ.get("AUTH_MODE", "development").lower()
    try:
        auth_mode = AuthMode(auth_mode_raw)
    except ValueError:
        logger.warning("Unknown AUTH_MODE '%s'; falling back to development", auth_mode_raw)
        auth_mode = AuthMode.DEVELOPMENT

    jwt_secret_value = settings.jwt_secret.get_secret_value() if settings.jwt_secret else ""
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
    jwt_secret_previous_raw = os.environ.get("JWT_SECRET_PREVIOUS")
    jwt_secret_previous = SecretStr(jwt_secret_previous_raw) if jwt_secret_previous_raw else None

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
        jwt_secret_previous=jwt_secret_previous,
    )


def _build_token_config() -> TokenConfig:
    """Build TokenConfig from env (legacy). Prefer _build_token_config_from_settings(settings)."""
    from api.config import load_api_settings
    return _build_token_config_from_settings(load_api_settings())


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
        self._token_manager: TokenManager | None = None
        self._auth_mode: AuthMode | None = None

    def _ensure_token_manager(self, request: Request) -> None:
        """Build token manager from app.state.settings on first request."""
        if self._token_manager is not None:
            return
        settings = getattr(request.app.state, "settings", None)
        if settings is None:
            raise RuntimeError("app.state.settings not set; ensure lifespan has run")
        config = _build_token_config_from_settings(settings)
        self._token_manager = TokenManager(config)
        self._auth_mode = config.auth_mode
        logger.info("AuthenticationMiddleware token config ready (mode=%s)", config.auth_mode.value)

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
            return None  # Pydantic v1 or missing model_dump
        return None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Allow public endpoints through without authentication.
        if _is_public_path(path):
            return await call_next(request)

        self._ensure_token_manager(request)

        # Extract the Authorization header.
        auth_header = request.headers.get("authorization")
        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing Authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Expect "Bearer <token>" format.
        parts = auth_header.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization header must use Bearer scheme"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = parts[1]

        # --- API Key authentication (bmkey. prefix) -----------------------
        if token.startswith("bmkey."):
            return await self._authenticate_api_key(request, call_next, token)

        # --- JWT / HMAC token authentication ------------------------------
        if self._token_manager is None:
            logger.error("TokenManager not initialized — rejecting request")
            return JSONResponse(
                {"detail": "Authentication service unavailable"},
                status_code=503,
            )

        try:
            claims = await self._token_manager.validate_token_async(token)
        except PermissionError as exc:
            # BL-069: Use a single generic message for all token failures.
            # Distinguishing "expired" (403) from "invalid" (401) reveals
            # the validity state of a captured token to attackers, enabling
            # timing-attack and replay-strategy reconnaissance.
            # Log the specific reason server-side only for debugging.
            logger.info(
                "Token validation failed for %s: %s",
                request.url.path,
                exc,
            )
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Check token revocation (jti-based replay protection).
        jti = getattr(claims, "jti", None)
        if jti and _check_revocation is not None:
            is_revoked = await _check_revocation(jti)
            if is_revoked:
                # Use the same generic message as expired/invalid tokens so
                # attackers cannot distinguish revocation from expiry.
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid or expired token"},
                    headers={"WWW-Authenticate": "Bearer"},
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

        # Validate the role claim against the allowlist of known roles.
        # A crafted JWT with an unrecognised role claim must not bypass RBAC —
        # normalise to the least-privilege default instead of propagating it.
        if role_value not in _KNOWN_ROLES:
            logger.warning(
                "JWT contained unrecognised role claim %r for sub=%s; "
                "defaulting to 'viewer' (least privilege).",
                role_value,
                getattr(claims, "sub", "unknown"),
            )
            role_value = "viewer"

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
        session_factory = getattr(request.app.state, "session_factory", None)
        if session_factory is None:
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
                        headers={"WWW-Authenticate": "Bearer"},
                    )

                # Look up the owning user to get their role.
                user_repo = UserRepository(session, tenant_id=key_row.tenant_id)
                user = await user_repo.get_by_id(key_row.user_id)

                if user is None or not user.is_active:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Invalid or expired API key"},
                        headers={"WWW-Authenticate": "Bearer"},
                    )

                # Populate request.state.
                request.state.tenant_id = key_row.tenant_id
                request.state.sub = key_row.user_id
                request.state.scopes = key_row.scopes or ["read", "write"]
                request.state.identity_kind = "service"
                request.state.role = user.role

                await session.commit()

        except Exception:  # Intentional: do not leak DB/session errors; return 401
            logger.warning("API key validation failed", exc_info=True)
            return JSONResponse(
                status_code=401,
                content={"detail": "API key validation failed"},
                headers={"WWW-Authenticate": "Bearer"},
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
        except Exception:  # Intentional: any load/parse error → fall back to Community tier
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
