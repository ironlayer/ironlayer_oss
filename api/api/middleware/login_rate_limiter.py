"""Login brute-force protection middleware.

Tracks failed login attempts by (email, IP) pair and enforces exponential
backoff.  This implementation uses an in-memory store, suitable for a
single-replica deployment.  For multi-replica deployments, migrate to a
Redis-backed store.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Backoff schedule in seconds: 30s, 60s, 120s, 240s, 900s max
_BACKOFF_SCHEDULE = [30, 60, 120, 240, 900]
_MAX_ATTEMPTS_BEFORE_LOCKOUT = 5


@dataclass
class _AttemptRecord:
    failed_count: int = 0
    last_failed_at: float = 0.0
    locked_until: float = 0.0


class LoginRateLimiter:
    """In-memory login rate limiter.

    Tracks failed attempts by (email_lower, client_ip) tuples.
    After MAX_ATTEMPTS failed logins, enforces exponential backoff.
    Resets on successful login.

    Note: For multi-replica deployments, this should be migrated to
    a Redis-backed store to share state across replicas.
    """

    def __init__(self) -> None:
        self._attempts: dict[tuple[str, str], _AttemptRecord] = defaultdict(_AttemptRecord)

    def check_rate_limit(self, email: str, client_ip: str) -> tuple[bool, int]:
        """Check if login is allowed.

        Returns:
            (allowed, retry_after_seconds) -- if not allowed, retry_after > 0
        """
        key = (email.lower().strip(), client_ip)
        record = self._attempts[key]

        now = time.monotonic()
        if record.locked_until > now:
            retry_after = int(record.locked_until - now) + 1
            return False, retry_after

        return True, 0

    def record_failure(self, email: str, client_ip: str) -> None:
        """Record a failed login attempt. May trigger lockout."""
        key = (email.lower().strip(), client_ip)
        record = self._attempts[key]
        record.failed_count += 1
        record.last_failed_at = time.monotonic()

        if record.failed_count >= _MAX_ATTEMPTS_BEFORE_LOCKOUT:
            # Calculate backoff tier
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

    def record_success(self, email: str, client_ip: str) -> None:
        """Reset the failure counter on successful login."""
        key = (email.lower().strip(), client_ip)
        if key in self._attempts:
            del self._attempts[key]

    def cleanup_stale(self, max_age_seconds: float = 3600) -> None:
        """Remove records older than max_age to prevent memory growth."""
        now = time.monotonic()
        stale_keys = [
            k for k, v in self._attempts.items() if (now - v.last_failed_at) > max_age_seconds and v.locked_until < now
        ]
        for k in stale_keys:
            del self._attempts[k]
