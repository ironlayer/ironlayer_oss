"""Authentication, rate limiting, and request size middleware for the AI engine."""

from __future__ import annotations

import logging
import os
import secrets
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Paths that bypass authentication and rate limiting (health checks, readiness probes).
_PUBLIC_PATHS: frozenset[str] = frozenset({"/health", "/healthz", "/readyz"})


class SharedSecretMiddleware(BaseHTTPMiddleware):
    """Validates a shared secret on every non-public request.

    In development mode (AI_ENGINE_SHARED_SECRET not set and
    PLATFORM_ENV == "development"), generates a random per-process
    secret and logs a warning.  In all other environments, the
    AI_ENGINE_SHARED_SECRET env var is mandatory -- the middleware
    raises RuntimeError at init time if it is missing.
    """

    def __init__(self, app, *, platform_env: str = "development") -> None:
        super().__init__(app)
        raw_secret = os.environ.get("AI_ENGINE_SHARED_SECRET", "")
        if raw_secret:
            self._secret = raw_secret
        elif platform_env.lower() in ("development", "dev", "local"):
            self._secret = f"dev-{secrets.token_hex(32)}"
            logger.warning(
                "AI_ENGINE_SHARED_SECRET not set -- generated random per-process "
                "dev secret. Cross-service calls will fail unless the API uses "
                "the same secret."
            )
        else:
            raise RuntimeError(
                f"AI_ENGINE_SHARED_SECRET is required in {platform_env} mode. "
                "Refusing to start AI engine without authentication."
            )

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Allow health checks without auth.
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"detail": "Missing or malformed Authorization header. Expected: Bearer <token>"},
                status_code=401,
            )

        token = auth_header[7:]  # Strip "Bearer " prefix
        if not secrets.compare_digest(token, self._secret):
            logger.warning(
                "AI engine auth failed: invalid shared secret from %s",
                request.client.host if request.client else "unknown",
            )
            return JSONResponse(
                {"detail": "Invalid shared secret"},
                status_code=401,
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------


class _TenantBucket:
    """Token bucket rate limiter per tenant.

    Tracks a sliding window of request timestamps and rejects requests
    once the window fills.  Thread safety is not required because
    Starlette middlewares execute inside a single async event loop.
    """

    __slots__ = ("max_requests", "window", "requests")

    def __init__(self, max_requests: int = 60, window_seconds: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests: list[float] = []

    def check(self) -> tuple[bool, int]:
        """Return ``(allowed, retry_after_seconds)``.

        If the request is allowed, appends the current timestamp and
        returns ``(True, 0)``.  If rejected, returns ``(False, N)``
        where *N* is the number of seconds the caller should wait.
        """
        now = time.monotonic()
        cutoff = now - self.window
        # Prune expired timestamps.
        self.requests = [t for t in self.requests if t > cutoff]
        if len(self.requests) >= self.max_requests:
            oldest_in_window = self.requests[0] if self.requests else now
            retry_after = int(oldest_in_window + self.window - now) + 1
            return False, max(retry_after, 1)
        self.requests.append(now)
        return True, 0


class AIRateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limits AI engine requests by tenant.

    Uses the ``X-Tenant-Id`` header (set by the API service) to
    identify tenants.  Requests without a tenant header are tracked
    under the ``__unknown__`` bucket.

    Default: 60 requests per minute per tenant.
    """

    def __init__(
        self,
        app,
        *,
        max_requests: int = 60,
        window_seconds: float = 60.0,
    ) -> None:
        super().__init__(app)
        self._max_requests = max_requests
        self._window = window_seconds
        self._buckets: dict[str, _TenantBucket] = defaultdict(lambda: _TenantBucket(self._max_requests, self._window))

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Skip rate limiting for health checks.
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        tenant_id = request.headers.get("x-tenant-id", "__unknown__")
        bucket = self._buckets[tenant_id]
        allowed, retry_after = bucket.check()

        if not allowed:
            logger.warning("AI engine rate limit exceeded for tenant %s", tenant_id)
            return JSONResponse(
                {"detail": f"Rate limit exceeded. Try again in {retry_after} seconds."},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# Request body size guard
# ---------------------------------------------------------------------------


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests whose ``Content-Length`` exceeds a configurable limit.

    This is a fast, pre-read guard: it inspects the ``Content-Length``
    header before any body is consumed.  Requests that omit the header
    (chunked transfer-encoding) are allowed through; downstream
    Pydantic validators enforce field-level limits.

    Default limit: 1 MiB (1 048 576 bytes).
    """

    def __init__(self, app, *, max_body_size: int = 1_048_576) -> None:
        super().__init__(app)
        self._max_size = max_body_size

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                length = int(content_length)
            except (ValueError, OverflowError):
                return JSONResponse(
                    {"detail": "Invalid Content-Length header"},
                    status_code=400,
                )
            if length > self._max_size:
                logger.warning(
                    "Rejected request to %s: Content-Length %d exceeds limit %d",
                    request.url.path,
                    length,
                    self._max_size,
                )
                return JSONResponse(
                    {"detail": (f"Request body too large. Maximum: {self._max_size} bytes")},
                    status_code=413,
                )
        return await call_next(request)
