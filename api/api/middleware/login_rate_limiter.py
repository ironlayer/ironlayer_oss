"""Login brute-force protection middleware.

Tracks failed login attempts by (email, IP) pair and enforces exponential
backoff.  Two backends are supported:

* In-memory (default) — zero external dependencies; suitable for single-replica
  deployments.
* Redis-backed — stores state in a Redis hash with a 1-hour TTL so that
  brute-force attempt counts are shared across all replicas in a multi-replica
  deployment.

The backend is selected lazily.  Call :func:`configure_redis` (or use the
:meth:`LoginRateLimiter.configure_redis` classmethod) after the Redis client
has been initialised during application startup to upgrade the singleton
to the Redis backend.  If Redis is not configured, or if any Redis operation
raises an exception, the limiter falls back to the in-memory store
transparently and logs a warning.

Public interface
----------------
All three mutating methods are coroutines so that they can be ``await``-ed
directly from async route handlers:

* ``await limiter.check_rate_limit(email, ip)``  → ``(allowed, retry_after)``
* ``await limiter.record_failure(email, ip)``
* ``await limiter.record_success(email, ip)``
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Backoff schedule in seconds: 30s, 60s, 120s, 240s, 900s max
_BACKOFF_SCHEDULE = [30, 60, 120, 240, 900]
_MAX_ATTEMPTS_BEFORE_LOCKOUT = 5

# Redis key prefix and TTL.
_REDIS_KEY_PREFIX = "login_rl"
_REDIS_TTL = 3600  # 1 hour


@dataclass
class _AttemptRecord:
    failed_count: int = 0
    last_failed_at: float = 0.0
    locked_until: float = 0.0


class LoginRateLimiter:
    """Login rate limiter with Redis-backed distributed state and in-memory fallback.

    Tracks failed attempts by (email_lower, client_ip) tuples.
    After MAX_ATTEMPTS failed logins, enforces exponential backoff.
    Resets on successful login.

    In-memory mode is used when Redis is not configured.  Redis mode is
    activated by calling :meth:`configure_redis` (or the module-level
    :func:`configure_redis` helper) after the Redis client is ready.
    """

    # Class-level Redis client — shared across all instances of LoginRateLimiter.
    # None means "use in-memory fallback".
    _redis_client: Any | None = None

    def __init__(self) -> None:
        # In-memory fallback store.
        self._attempts: dict[tuple[str, str], _AttemptRecord] = defaultdict(_AttemptRecord)

    @classmethod
    def configure_redis(cls, redis_client: Any) -> None:
        """Configure the Redis client used by all LoginRateLimiter instances.

        Call this once during application startup, after the Redis connection
        has been established.  The client must be a ``redis.asyncio.Redis``
        instance (or compatible async Redis client) with ``decode_responses=True``.

        Parameters
        ----------
        redis_client:
            An initialised async Redis client.  Pass ``None`` to revert to
            in-memory mode (useful in tests).
        """
        cls._redis_client = redis_client
        if redis_client is not None:
            logger.info("LoginRateLimiter: Redis backend configured")
        else:
            logger.info("LoginRateLimiter: reverted to in-memory backend")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _redis_key(email: str, client_ip: str) -> str:
        """Build the Redis hash key for a given (email, ip) pair."""
        return f"{_REDIS_KEY_PREFIX}:{email.lower().strip()}:{client_ip}"

    # ------------------------------------------------------------------
    # Public async interface
    # ------------------------------------------------------------------

    async def check_rate_limit(self, email: str, client_ip: str) -> tuple[bool, int]:
        """Check if a login attempt is allowed.

        Returns
        -------
        (allowed, retry_after_seconds)
            *allowed* is ``True`` when the request may proceed.
            *retry_after_seconds* is ``> 0`` when the caller is locked out.
        """
        if self._redis_client is not None:
            try:
                return await self._redis_check(email, client_ip)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "LoginRateLimiter: Redis check_rate_limit failed, falling back to in-memory: %s",
                    exc,
                )

        return self._mem_check(email, client_ip)

    async def record_failure(self, email: str, client_ip: str) -> None:
        """Record a failed login attempt. May trigger a lockout."""
        if self._redis_client is not None:
            try:
                await self._redis_record_failure(email, client_ip)
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "LoginRateLimiter: Redis record_failure failed, falling back to in-memory: %s",
                    exc,
                )

        self._mem_record_failure(email, client_ip)

    async def record_success(self, email: str, client_ip: str) -> None:
        """Reset the failure counter on successful login."""
        if self._redis_client is not None:
            try:
                await self._redis_record_success(email, client_ip)
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "LoginRateLimiter: Redis record_success failed, falling back to in-memory: %s",
                    exc,
                )

        self._mem_record_success(email, client_ip)

    # ------------------------------------------------------------------
    # Redis backend
    # ------------------------------------------------------------------

    async def _redis_check(self, email: str, client_ip: str) -> tuple[bool, int]:
        """Check rate limit against the Redis store."""
        key = self._redis_key(email, client_ip)
        data = await self._redis_client.hgetall(key)  # type: ignore[union-attr]
        if not data:
            return True, 0

        locked_until = float(data.get("locked_until", 0.0))
        now = time.time()
        if locked_until > now:
            retry_after = int(locked_until - now) + 1
            return False, retry_after

        return True, 0

    async def _redis_record_failure(self, email: str, client_ip: str) -> None:
        """Increment the failure counter in Redis, applying a lockout if needed."""
        key = self._redis_key(email, client_ip)

        # Read current state.
        data = await self._redis_client.hgetall(key)  # type: ignore[union-attr]
        failed_count = int(data.get("failed_count", 0)) + 1
        now = time.time()
        locked_until = float(data.get("locked_until", 0.0))

        if failed_count >= _MAX_ATTEMPTS_BEFORE_LOCKOUT:
            excess = failed_count - _MAX_ATTEMPTS_BEFORE_LOCKOUT
            tier = min(excess, len(_BACKOFF_SCHEDULE) - 1)
            backoff = _BACKOFF_SCHEDULE[tier]
            locked_until = now + backoff
            logger.warning(
                "Login rate limit: %s from %s locked for %ds after %d failures",
                email,
                client_ip,
                backoff,
                failed_count,
            )

        mapping = {
            "failed_count": str(failed_count),
            "last_failed_at": str(now),
            "locked_until": str(locked_until),
        }
        await self._redis_client.hset(key, mapping=mapping)  # type: ignore[union-attr]
        await self._redis_client.expire(key, _REDIS_TTL)  # type: ignore[union-attr]

    async def _redis_record_success(self, email: str, client_ip: str) -> None:
        """Delete the Redis key on a successful login, resetting the counter."""
        key = self._redis_key(email, client_ip)
        await self._redis_client.delete(key)  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # In-memory backend
    # ------------------------------------------------------------------

    def _mem_check(self, email: str, client_ip: str) -> tuple[bool, int]:
        """Check rate limit against the in-memory store (uses time.monotonic)."""
        key = (email.lower().strip(), client_ip)
        record = self._attempts[key]

        now = time.monotonic()
        if record.locked_until > now:
            retry_after = int(record.locked_until - now) + 1
            return False, retry_after

        return True, 0

    def _mem_record_failure(self, email: str, client_ip: str) -> None:
        """Increment the failure counter in the in-memory store."""
        key = (email.lower().strip(), client_ip)
        record = self._attempts[key]
        record.failed_count += 1
        record.last_failed_at = time.monotonic()

        if record.failed_count >= _MAX_ATTEMPTS_BEFORE_LOCKOUT:
            excess = record.failed_count - _MAX_ATTEMPTS_BEFORE_LOCKOUT
            tier = min(excess, len(_BACKOFF_SCHEDULE) - 1)
            backoff = _BACKOFF_SCHEDULE[tier]
            record.locked_until = time.monotonic() + backoff
            logger.warning(
                "Login rate limit: %s from %s locked for %ds after %d failures",
                email,
                client_ip,
                backoff,
                record.failed_count,
            )

    def _mem_record_success(self, email: str, client_ip: str) -> None:
        """Reset the in-memory failure counter."""
        key = (email.lower().strip(), client_ip)
        if key in self._attempts:
            del self._attempts[key]

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def cleanup_stale(self, max_age_seconds: float = 3600) -> None:
        """Remove in-memory records older than *max_age_seconds*.

        This method operates only on the in-memory store and is a no-op
        when Redis is configured (Redis TTLs handle expiry automatically).
        """
        now = time.monotonic()
        stale_keys = [
            k
            for k, v in self._attempts.items()
            if (now - v.last_failed_at) > max_age_seconds and v.locked_until < now
        ]
        for k in stale_keys:
            del self._attempts[k]


# ---------------------------------------------------------------------------
# Module-level helper — mirrors the class method for convenience
# ---------------------------------------------------------------------------


def configure_redis(redis_client: Any) -> None:
    """Configure the Redis client used by all :class:`LoginRateLimiter` instances.

    Delegates to :meth:`LoginRateLimiter.configure_redis`.  Call this once
    during application startup after the Redis connection is established.

    Parameters
    ----------
    redis_client:
        An initialised ``redis.asyncio.Redis`` instance, or ``None`` to
        revert to in-memory mode.
    """
    LoginRateLimiter.configure_redis(redis_client)
