"""Rate-limiting middleware -- sliding-window, per-tenant and per-IP.

Implements a sliding window counter with two backends:

* :class:`InProcessRateLimitBackend` — in-process deque counter.  Zero
  external dependencies; suitable for single-replica deployments.
* :class:`RedisRateLimitBackend` — Redis sorted-set counter.  Enforces
  globally consistent limits across *all* replicas.

The backend is selected by :func:`build_rate_limit_backend`:
- When ``REDIS_URL`` is configured and reachable, Redis is used.
- Otherwise the in-process backend is used with a logged warning.

Features:
- Per-tenant rate limiting (keyed by ``tenant_id`` from auth middleware).
- Per-IP fallback for unauthenticated endpoints.
- Endpoint-specific lower limits for expensive operations.
- Burst allowance via a configurable multiplier.
- Automatic cleanup of stale in-process tracking entries.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import random
import time
import uuid
from collections import deque
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

try:
    from api.middleware.prometheus import RATE_LIMIT_REJECTED_TOTAL, _normalise_path

    _RL_METRICS = True
except ImportError:
    _RL_METRICS = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class RateLimitConfig(BaseModel):
    """Rate-limiting configuration parameters.

    Attributes:
        enabled: Master toggle.  When ``False`` the middleware becomes a
            no-op pass-through.
        default_requests_per_minute: Baseline request budget per client
            within the sliding window.
        burst_multiplier: Multiplier applied to the per-minute limit that
            allows short traffic spikes.  For example, a value of ``1.5``
            with 60 rpm yields a burst cap of 90 in any given window.
        expensive_endpoints: Mapping of path patterns (supporting
            ``fnmatch`` glob syntax) to per-minute limits that are lower
            than the default.
        auth_endpoints_per_minute: Per-minute cap applied to paths
            starting with ``/api/auth``.
        exempt_paths: Paths that bypass rate limiting entirely (e.g.
            health checks).
    """

    enabled: bool = True
    default_requests_per_minute: int = 60
    burst_multiplier: float = 1.5
    expensive_endpoints: dict[str, int] = {
        "/api/v1/plans/generate": 10,
        "/api/v1/plans/*/apply": 5,
        "/api/v1/plans/*/augment": 10,
        "/api/v1/plans/*/simulate": 10,
        "/api/v1/backfills": 10,
        "/api/v1/simulation/*": 10,
    }
    auth_endpoints_per_minute: int = 20
    exempt_paths: set[str] = {"/api/health", "/api/v1/health", "/ready", "/metrics"}


# ---------------------------------------------------------------------------
# Sliding window counter
# ---------------------------------------------------------------------------

_WINDOW_SECONDS: float = 60.0
_CLEANUP_INTERVAL_SECONDS: float = 60.0


class SlidingWindowCounter:
    """Thread-safe (asyncio-safe) sliding window request counter.

    Each client key maps to a :class:`~collections.deque` of UNIX
    timestamps.  Calling :meth:`hit` prunes entries older than
    ``window_seconds`` before appending the current timestamp.

    A background task periodically removes keys that have been idle for
    longer than the window to prevent unbounded memory growth.
    """

    def __init__(self, window_seconds: float = _WINDOW_SECONDS) -> None:
        self._window: float = window_seconds
        self._buckets: dict[str, deque[float]] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._running: bool = False

    # -- Lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Launch the periodic cleanup coroutine."""
        if self._running:
            return
        self._running = True
        self._cleanup_task = asyncio.ensure_future(self._cleanup_loop())

    async def stop(self) -> None:
        """Cancel the cleanup loop and wait for it to finish."""
        self._running = False
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    # -- Core API ------------------------------------------------------------

    async def hit(self, key: str) -> int:
        """Record a request for *key* and return the current count.

        The count reflects only timestamps within the sliding window
        (i.e. the last ``window_seconds``).
        """
        now = time.monotonic()
        cutoff = now - self._window

        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = deque()
                self._buckets[key] = bucket

            # Prune expired entries.
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            bucket.append(now)
            return len(bucket)

    async def count(self, key: str) -> int:
        """Return the current request count without recording a new hit."""
        now = time.monotonic()
        cutoff = now - self._window

        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                return 0

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            return len(bucket)

    async def time_until_reset(self, key: str) -> float:
        """Seconds until the oldest entry in *key*'s window expires.

        Returns ``0.0`` if the key has no recorded timestamps.
        """
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.get(key)
            if not bucket:
                return 0.0
            oldest = bucket[0]
            remaining = (oldest + self._window) - now
            return max(remaining, 0.0)

    # -- Housekeeping --------------------------------------------------------

    async def _cleanup_loop(self) -> None:
        """Remove keys whose entries have all expired."""
        while self._running:
            await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)
            now = time.monotonic()
            cutoff = now - self._window

            async with self._lock:
                stale_keys: list[str] = []
                for key, bucket in self._buckets.items():
                    # Prune expired entries.
                    while bucket and bucket[0] <= cutoff:
                        bucket.popleft()
                    if not bucket:
                        stale_keys.append(key)
                for key in stale_keys:
                    del self._buckets[key]

                if stale_keys:
                    logger.debug("Rate-limit cleanup removed %d stale keys", len(stale_keys))


# ---------------------------------------------------------------------------
# Backend protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RateLimitBackend(Protocol):
    """Async sliding-window counter backend.

    Both :class:`InProcessRateLimitBackend` and
    :class:`RedisRateLimitBackend` implement this protocol so that
    :class:`RateLimitMiddleware` is decoupled from the storage layer.
    """

    async def hit(self, key: str) -> int:
        """Record a hit for *key* and return the new window count."""
        ...

    async def count(self, key: str) -> int:
        """Return the current count without recording a hit."""
        ...

    async def time_until_reset(self, key: str) -> float:
        """Seconds until the oldest entry in *key*'s window expires."""
        ...


class InProcessRateLimitBackend:
    """In-process :class:`RateLimitBackend` backed by :class:`SlidingWindowCounter`.

    Wraps the existing counter so that the middleware can treat both
    backends uniformly.
    """

    def __init__(self, window_seconds: float = _WINDOW_SECONDS) -> None:
        self._counter = SlidingWindowCounter(window_seconds)

    def start(self) -> None:
        """Start the background cleanup task."""
        self._counter.start()

    async def stop(self) -> None:
        """Stop the background cleanup task."""
        await self._counter.stop()

    async def hit(self, key: str) -> int:
        return await self._counter.hit(key)

    async def count(self, key: str) -> int:
        return await self._counter.count(key)

    async def time_until_reset(self, key: str) -> float:
        return await self._counter.time_until_reset(key)


class RedisRateLimitBackend:
    """Redis-backed :class:`RateLimitBackend` using sorted-set sliding windows.

    Uses a Redis sorted set per counter key where each member is a unique
    ID scored by its creation timestamp.  The ZREMRANGEBYSCORE +
    ZADD + ZCARD pipeline is executed atomically, giving globally
    consistent counts across all replicas.

    Key format: ``rl:<counter_key>`` (e.g. ``rl:tenant:abc123:60``)
    """

    def __init__(self, redis_client: Any, window_seconds: float = _WINDOW_SECONDS) -> None:
        self._redis = redis_client
        self._window = window_seconds

    async def hit(self, key: str) -> int:
        now = time.time()
        cutoff = now - self._window
        redis_key = f"rl:{key}"
        member = f"{now}:{uuid.uuid4().hex}"

        pipe = self._redis.pipeline(transaction=True)
        pipe.zremrangebyscore(redis_key, "-inf", cutoff)
        pipe.zadd(redis_key, {member: now})
        pipe.zcard(redis_key)
        pipe.expire(redis_key, int(self._window) + 2)
        results = await pipe.execute()
        return int(results[2])  # ZCARD result

    async def count(self, key: str) -> int:
        now = time.time()
        cutoff = now - self._window
        redis_key = f"rl:{key}"

        pipe = self._redis.pipeline()
        pipe.zremrangebyscore(redis_key, "-inf", cutoff)
        pipe.zcard(redis_key)
        results = await pipe.execute()
        return int(results[1])

    async def time_until_reset(self, key: str) -> float:
        redis_key = f"rl:{key}"
        oldest = await self._redis.zrange(redis_key, 0, 0, withscores=True)
        if not oldest:
            return 0.0
        oldest_score = float(oldest[0][1])
        remaining = (oldest_score + self._window) - time.time()
        return max(remaining, 0.0)


async def build_rate_limit_backend(
    redis_url: str | None,
    window_seconds: float = _WINDOW_SECONDS,
) -> InProcessRateLimitBackend | RedisRateLimitBackend:
    """Return the best available backend for the current deployment.

    When ``redis_url`` is provided and the connection succeeds, returns a
    :class:`RedisRateLimitBackend`.  Otherwise logs a warning and returns
    an :class:`InProcessRateLimitBackend`.

    Parameters
    ----------
    redis_url:
        Redis connection URL (e.g. ``redis://localhost:6379/0``).
        ``None`` means Redis is not configured → always use in-process.
    window_seconds:
        Sliding window duration in seconds.
    """
    if redis_url:
        from api.services.redis_client import get_redis_client

        client = await get_redis_client()
        if client is not None:
            logger.info("RateLimitMiddleware: using Redis backend (%s)", redis_url.split("@")[-1])
            return RedisRateLimitBackend(client, window_seconds)
        logger.warning(
            "RateLimitMiddleware: Redis unavailable — falling back to in-process backend. "
            "Rate limits will NOT be enforced across replicas."
        )

    backend = InProcessRateLimitBackend(window_seconds)
    backend.start()
    return backend


# ---------------------------------------------------------------------------
# BL-099: Module-level reference to the active in-process backend so that the
# application lifespan can cancel its background cleanup task on graceful shutdown.
# Without cancellation, asyncio emits "Task was destroyed but it is pending\!" warnings
# that obscure real errors in logs.
# ---------------------------------------------------------------------------

_active_inprocess_backend: InProcessRateLimitBackend | None = None


def get_inprocess_rate_limit_backend() -> InProcessRateLimitBackend | None:
    """Return the active in-process rate-limit backend, or None if Redis is used."""
    return _active_inprocess_backend


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Starlette middleware enforcing per-client sliding-window rate limits.

    Client identification strategy:

    1. If ``request.state.tenant_id`` is set (populated by
       :class:`AuthenticationMiddleware`), the tenant ID is used as the
       rate-limit key.  This ensures fair per-tenant quotas.
    2. Otherwise the client IP address is used as a fallback for
       unauthenticated endpoints.

    The middleware decorates every response with standard rate-limit headers
    (``X-RateLimit-Limit``, ``X-RateLimit-Remaining``, ``X-RateLimit-Reset``)
    and returns a ``429 Too Many Requests`` response with a ``Retry-After``
    header when a client exceeds its budget.
    """

    def __init__(
        self,
        app: Any,
        config: RateLimitConfig | None = None,
        backend: InProcessRateLimitBackend | RedisRateLimitBackend | None = None,
        redis_url: str | None = None,
    ) -> None:
        super().__init__(app)
        self._config: RateLimitConfig = config or RateLimitConfig()
        self._redis_url = redis_url
        self._backend_upgraded = False  # True once we've tried to upgrade to Redis

        if backend is not None:
            self._backend: InProcessRateLimitBackend | RedisRateLimitBackend = backend
            self._backend_upgraded = True
        else:
            # Default: in-process (started immediately so tests work without async setup)
            self._backend = InProcessRateLimitBackend()
            if self._config.enabled:
                self._backend.start()
            # BL-099: Expose to lifespan shutdown hook.
            global _active_inprocess_backend
            _active_inprocess_backend = self._backend
        logger.info(
            "RateLimitMiddleware initialised (enabled=%s, rpm=%d, burst=%.1fx, backend=%s)",
            self._config.enabled,
            self._config.default_requests_per_minute,
            self._config.burst_multiplier,
            type(self._backend).__name__,
        )

    async def _maybe_upgrade_backend(self) -> None:
        """On the first request after startup, attempt to upgrade to Redis if configured.

        Called at the start of each ``dispatch()`` until the upgrade succeeds
        or is determined to be unavailable.  After the first attempt the
        ``_backend_upgraded`` flag is set so this becomes a no-op.
        """
        if self._backend_upgraded or not self._redis_url:
            return
        self._backend_upgraded = True  # Only try once regardless of outcome

        from api.services.redis_client import get_redis_client

        client = await get_redis_client()
        if client is not None:
            self._backend = RedisRateLimitBackend(client)
            logger.info("RateLimitMiddleware: upgraded to Redis backend")
        else:
            logger.warning(
                "RateLimitMiddleware: Redis not available — using in-process backend. "
                "Rate limits will NOT be enforced across replicas."
            )

    # -- Helpers -------------------------------------------------------------

    def _client_key(self, request: Request) -> str:
        """Derive a rate-limit key from the request.

        Prefers tenant_id (set by the auth middleware) so that all requests
        from the same tenant share a quota regardless of source IP.  Falls
        back to the client IP for unauthenticated endpoints.
        """
        tenant_id: str | None = getattr(request.state, "tenant_id", None)
        if tenant_id:
            return f"tenant:{tenant_id}"
        ip = request.client.host if request.client else "unknown"
        return f"ip:{ip}"

    def _limit_for_path(self, path: str) -> int:
        """Return the effective per-minute limit for *path*.

        Evaluation order:
        1. Exempt paths -- returns ``0`` (sentinel for *unlimited*).
        2. Auth endpoints (``/api/auth*``).
        3. Expensive endpoint patterns (``fnmatch`` glob).
        4. Default limit.
        """
        if path in self._config.exempt_paths:
            return 0  # Sentinel: unlimited.

        if path.startswith("/api/auth") or path.startswith("/api/v1/auth"):
            return self._config.auth_endpoints_per_minute

        for pattern, limit in self._config.expensive_endpoints.items():
            if fnmatch.fnmatch(path, pattern) or path == pattern:
                return limit

        return self._config.default_requests_per_minute

    def _effective_burst_limit(self, base_limit: int) -> int:
        """Apply the burst multiplier to a base limit."""
        return int(base_limit * self._config.burst_multiplier)

    # -- Dispatch ------------------------------------------------------------

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not self._config.enabled:
            return await call_next(request)

        # Lazily upgrade to Redis backend after startup (no-op after first attempt).
        await self._maybe_upgrade_backend()

        path = request.url.path
        base_limit = self._limit_for_path(path)

        # Sentinel 0 means exempt.
        if base_limit == 0:
            return await call_next(request)

        burst_limit = self._effective_burst_limit(base_limit)
        client_key = self._client_key(request)

        # Scope the counter key to the specific limit tier so that a
        # tenant's general requests and their expensive-endpoint requests
        # are tracked independently.
        counter_key = f"{client_key}:{base_limit}"

        current_count = await self._backend.hit(counter_key)

        if current_count > burst_limit:
            retry_after = await self._backend.time_until_reset(counter_key)
            retry_after_base = max(int(retry_after) + 1, 1)

            # BL-082: Add ±10% jitter to Retry-After to prevent timing
            # reconnaissance.  Exact values reveal bucket size and refill
            # rate, enabling attackers to space requests optimally.
            jitter = random.uniform(-0.10, 0.10)
            retry_after_int = max(int(retry_after_base * (1 + jitter)), 1)

            logger.warning(
                "Rate limit exceeded: key=%s path=%s count=%d limit=%d",
                client_key,
                path,
                current_count,
                burst_limit,
            )

            if _RL_METRICS:
                RATE_LIMIT_REJECTED_TOTAL.labels(endpoint=_normalise_path(path)).inc()

            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded. Try again later.",
                },
                headers={
                    "Retry-After": str(retry_after_int),
                    "X-RateLimit-Limit": str(burst_limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(retry_after_int),
                },
            )

        # Request is within budget -- forward downstream.
        response = await call_next(request)

        remaining = max(burst_limit - current_count, 0)
        reset_seconds = await self._backend.time_until_reset(counter_key)
        reset_int = max(int(reset_seconds) + 1, 1)

        response.headers["X-RateLimit-Limit"] = str(burst_limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_int)

        return response
