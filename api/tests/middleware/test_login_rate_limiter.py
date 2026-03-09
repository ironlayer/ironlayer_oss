"""Tests for api/api/middleware/login_rate_limiter.py

Covers:
1. In-memory backend — normal flow, lockout, backoff tiers, success reset.
2. In-memory backend — cleanup_stale() maintenance helper.
3. Redis backend (mocked) — check, record_failure, record_success.
4. Redis failure → graceful in-memory fallback with a warning log.
5. configure_redis() class/module-level wiring.
6. Module-level configure_redis() helper.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from api.middleware.login_rate_limiter import (
    LoginRateLimiter,
    _BACKOFF_SCHEDULE,
    _MAX_ATTEMPTS_BEFORE_LOCKOUT,
    _REDIS_KEY_PREFIX,
    _REDIS_TTL,
    configure_redis,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def reset_redis_client():
    """Ensure every test starts with the in-memory backend.

    The LoginRateLimiter._redis_client is a class attribute, so we must
    reset it between tests to prevent cross-test pollution.
    """
    original = LoginRateLimiter._redis_client
    LoginRateLimiter._redis_client = None
    yield
    LoginRateLimiter._redis_client = original


def _make_limiter() -> LoginRateLimiter:
    """Return a fresh LoginRateLimiter with a clean in-memory store."""
    return LoginRateLimiter()


def _make_redis_mock(*, hgetall_data: dict | None = None) -> AsyncMock:
    """Return a mock async Redis client.

    Parameters
    ----------
    hgetall_data:
        Mapping returned by ``hgetall``.  Defaults to ``{}`` (no existing key).
    """
    mock = AsyncMock()
    mock.hgetall = AsyncMock(return_value=hgetall_data or {})
    mock.hset = AsyncMock()
    mock.expire = AsyncMock()
    mock.delete = AsyncMock()
    return mock


# ---------------------------------------------------------------------------
# 1. In-memory backend — basic flow
# ---------------------------------------------------------------------------


class TestInMemoryBackend:
    """Tests for the in-memory fallback path (no Redis configured)."""

    @pytest.mark.asyncio
    async def test_new_user_is_allowed(self):
        """A brand-new (email, ip) pair is allowed with retry_after=0."""
        limiter = _make_limiter()
        allowed, retry_after = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
        assert allowed is True
        assert retry_after == 0

    @pytest.mark.asyncio
    async def test_record_failure_does_not_lock_before_threshold(self):
        """Failures below the lockout threshold do not cause a lockout."""
        limiter = _make_limiter()
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT - 1):
            await limiter.record_failure("user@example.com", "1.2.3.4")

        allowed, retry_after = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
        assert allowed is True
        assert retry_after == 0

    @pytest.mark.asyncio
    async def test_lockout_triggers_at_threshold(self):
        """Exactly MAX_ATTEMPTS_BEFORE_LOCKOUT failures triggers a lockout."""
        limiter = _make_limiter()
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
            await limiter.record_failure("user@example.com", "1.2.3.4")

        allowed, retry_after = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
        assert allowed is False
        assert retry_after > 0

    @pytest.mark.asyncio
    async def test_first_lockout_uses_first_backoff_tier(self):
        """The first lockout uses the first entry in _BACKOFF_SCHEDULE."""
        limiter = _make_limiter()

        now_mono = 1_000_000.0

        with patch("api.middleware.login_rate_limiter.time") as mock_time:
            mock_time.monotonic.return_value = now_mono

            for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
                await limiter.record_failure("user@example.com", "1.2.3.4")

            # Move time forward but not past the lockout window.
            check_time = now_mono + _BACKOFF_SCHEDULE[0] - 1
            mock_time.monotonic.return_value = check_time

            allowed, retry_after = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
            assert allowed is False
            assert retry_after >= 1

    @pytest.mark.asyncio
    async def test_subsequent_failures_advance_backoff_tier(self):
        """Each additional failure beyond the threshold advances the backoff tier."""
        limiter = _make_limiter()
        # Reach the lockout threshold.
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
            await limiter.record_failure("locked@example.com", "5.6.7.8")

        # One more failure should advance to tier 1.
        with patch("api.middleware.login_rate_limiter.time") as mock_time:
            # Simulate time during the lockout so the check still shows locked.
            mono = time.monotonic()
            mock_time.monotonic.return_value = mono
            await limiter.record_failure("locked@example.com", "5.6.7.8")

            key = ("locked@example.com", "5.6.7.8")
            record = limiter._attempts[key]
            # locked_until should reflect tier-1 backoff from now.
            expected_backoff = _BACKOFF_SCHEDULE[1]
            assert record.locked_until >= mono + expected_backoff - 1

    @pytest.mark.asyncio
    async def test_backoff_caps_at_max_tier(self):
        """Excessive failures cap at the last backoff schedule entry."""
        limiter = _make_limiter()
        # Exceed the threshold by more than the number of tiers.
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT + len(_BACKOFF_SCHEDULE) + 5):
            await limiter.record_failure("heavy@example.com", "9.9.9.9")

        key = ("heavy@example.com", "9.9.9.9")
        record = limiter._attempts[key]
        max_backoff = _BACKOFF_SCHEDULE[-1]
        # locked_until should not exceed now + max_backoff by more than a small delta.
        assert record.locked_until <= time.monotonic() + max_backoff + 2

    @pytest.mark.asyncio
    async def test_success_resets_counter(self):
        """record_success clears the failure record so the user can log in again."""
        limiter = _make_limiter()
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
            await limiter.record_failure("user@example.com", "1.2.3.4")

        allowed, _ = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
        assert allowed is False

        await limiter.record_success("user@example.com", "1.2.3.4")

        allowed, retry_after = await limiter.check_rate_limit("user@example.com", "1.2.3.4")
        assert allowed is True
        assert retry_after == 0

    @pytest.mark.asyncio
    async def test_success_on_unknown_key_is_noop(self):
        """record_success for a key that has never failed is a safe no-op."""
        limiter = _make_limiter()
        await limiter.record_success("nobody@example.com", "0.0.0.0")
        allowed, _ = await limiter.check_rate_limit("nobody@example.com", "0.0.0.0")
        assert allowed is True

    @pytest.mark.asyncio
    async def test_email_is_case_normalised(self):
        """Emails are lower-cased so 'User@Example.COM' matches 'user@example.com'."""
        limiter = _make_limiter()
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
            await limiter.record_failure("User@Example.COM", "1.1.1.1")

        # Check with a different casing.
        allowed, _ = await limiter.check_rate_limit("user@example.com", "1.1.1.1")
        assert allowed is False

    @pytest.mark.asyncio
    async def test_different_ips_are_isolated(self):
        """Two different IPs for the same email are tracked independently."""
        limiter = _make_limiter()
        for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
            await limiter.record_failure("user@example.com", "1.1.1.1")

        # A different IP must not be locked.
        allowed, _ = await limiter.check_rate_limit("user@example.com", "2.2.2.2")
        assert allowed is True

    @pytest.mark.asyncio
    async def test_lockout_expires_after_backoff(self):
        """After the backoff period elapses, the user is allowed again."""
        limiter = _make_limiter()

        base_mono = 1_000_000.0
        with patch("api.middleware.login_rate_limiter.time") as mock_time:
            mock_time.monotonic.return_value = base_mono
            for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
                await limiter.record_failure("expiry@example.com", "3.3.3.3")

            # Fast-forward past the lockout.
            mock_time.monotonic.return_value = base_mono + _BACKOFF_SCHEDULE[0] + 2
            allowed, retry_after = await limiter.check_rate_limit("expiry@example.com", "3.3.3.3")
            assert allowed is True
            assert retry_after == 0


# ---------------------------------------------------------------------------
# 2. In-memory cleanup_stale
# ---------------------------------------------------------------------------


class TestCleanupStale:
    """Tests for the cleanup_stale() maintenance helper."""

    def test_cleanup_removes_old_records(self):
        """Stale unlocked records older than max_age are removed."""
        limiter = _make_limiter()
        key = ("stale@example.com", "1.1.1.1")
        from api.middleware.login_rate_limiter import _AttemptRecord

        old_time = time.monotonic() - 7200
        limiter._attempts[key] = _AttemptRecord(
            failed_count=3,
            last_failed_at=old_time,
            locked_until=0.0,
        )

        limiter.cleanup_stale(max_age_seconds=3600)
        assert key not in limiter._attempts

    def test_cleanup_retains_recent_records(self):
        """Records updated recently are NOT cleaned up."""
        limiter = _make_limiter()
        key = ("recent@example.com", "2.2.2.2")
        from api.middleware.login_rate_limiter import _AttemptRecord

        recent_time = time.monotonic() - 60
        limiter._attempts[key] = _AttemptRecord(
            failed_count=2,
            last_failed_at=recent_time,
            locked_until=0.0,
        )

        limiter.cleanup_stale(max_age_seconds=3600)
        assert key in limiter._attempts

    def test_cleanup_retains_locked_records(self):
        """Records that are still locked are NOT cleaned up even if old."""
        limiter = _make_limiter()
        key = ("locked@example.com", "3.3.3.3")
        from api.middleware.login_rate_limiter import _AttemptRecord

        now = time.monotonic()
        limiter._attempts[key] = _AttemptRecord(
            failed_count=5,
            last_failed_at=now - 7200,
            locked_until=now + 60,  # still locked
        )

        limiter.cleanup_stale(max_age_seconds=3600)
        assert key in limiter._attempts


# ---------------------------------------------------------------------------
# 3. Redis backend (mocked)
# ---------------------------------------------------------------------------


class TestRedisBackend:
    """Tests for the Redis-backed path."""

    @pytest.mark.asyncio
    async def test_redis_check_allows_new_key(self):
        """hgetall returning {} means no existing record — allow login."""
        redis = _make_redis_mock(hgetall_data={})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        allowed, retry_after = await limiter.check_rate_limit("new@example.com", "1.1.1.1")
        assert allowed is True
        assert retry_after == 0
        redis.hgetall.assert_awaited_once_with(f"{_REDIS_KEY_PREFIX}:new@example.com:1.1.1.1")

    @pytest.mark.asyncio
    async def test_redis_check_returns_locked_when_locked_until_in_future(self):
        """When locked_until > now, check returns (False, retry_after > 0)."""
        future_locked_until = time.time() + 120
        redis = _make_redis_mock(
            hgetall_data={
                "failed_count": "7",
                "last_failed_at": str(time.time() - 10),
                "locked_until": str(future_locked_until),
            }
        )
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        allowed, retry_after = await limiter.check_rate_limit("locked@example.com", "2.2.2.2")
        assert allowed is False
        assert retry_after >= 1

    @pytest.mark.asyncio
    async def test_redis_check_allows_when_lockout_expired(self):
        """When locked_until is in the past, the key is unlocked."""
        past_locked_until = time.time() - 10
        redis = _make_redis_mock(
            hgetall_data={
                "failed_count": "5",
                "last_failed_at": str(time.time() - 40),
                "locked_until": str(past_locked_until),
            }
        )
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        allowed, retry_after = await limiter.check_rate_limit("old@example.com", "3.3.3.3")
        assert allowed is True
        assert retry_after == 0

    @pytest.mark.asyncio
    async def test_redis_record_failure_below_threshold_no_lockout(self):
        """Failures below MAX_ATTEMPTS_BEFORE_LOCKOUT set count without locking."""
        redis = _make_redis_mock(hgetall_data={"failed_count": "2"})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        await limiter.record_failure("user@example.com", "4.4.4.4")

        redis.hset.assert_awaited_once()
        # Unpack the mapping argument.
        call_kwargs = redis.hset.call_args
        mapping = call_kwargs.kwargs.get("mapping") or call_kwargs.args[1]
        assert int(mapping["failed_count"]) == 3
        # locked_until should be 0.0 (no lock).
        assert float(mapping["locked_until"]) == 0.0

    @pytest.mark.asyncio
    async def test_redis_record_failure_at_threshold_sets_lockout(self):
        """At MAX_ATTEMPTS_BEFORE_LOCKOUT failures, locked_until is set."""
        # failed_count in Redis is already MAX-1, so the new failure hits threshold.
        initial_count = _MAX_ATTEMPTS_BEFORE_LOCKOUT - 1
        redis = _make_redis_mock(hgetall_data={"failed_count": str(initial_count)})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        before = time.time()
        await limiter.record_failure("soon@example.com", "5.5.5.5")
        after = time.time()

        redis.hset.assert_awaited_once()
        mapping = redis.hset.call_args.kwargs.get("mapping") or redis.hset.call_args.args[1]
        assert int(mapping["failed_count"]) == _MAX_ATTEMPTS_BEFORE_LOCKOUT
        locked_until = float(mapping["locked_until"])
        # locked_until should be in the range [now + first_backoff - 1, now + first_backoff + 2].
        expected_backoff = _BACKOFF_SCHEDULE[0]
        assert locked_until >= before + expected_backoff - 1
        assert locked_until <= after + expected_backoff + 2

    @pytest.mark.asyncio
    async def test_redis_record_failure_sets_ttl(self):
        """expire() is called with _REDIS_TTL after every write."""
        redis = _make_redis_mock(hgetall_data={})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        await limiter.record_failure("ttl@example.com", "6.6.6.6")

        redis.expire.assert_awaited_once_with(
            f"{_REDIS_KEY_PREFIX}:ttl@example.com:6.6.6.6", _REDIS_TTL
        )

    @pytest.mark.asyncio
    async def test_redis_record_success_deletes_key(self):
        """record_success calls delete() to clear the Redis key."""
        redis = _make_redis_mock()
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        await limiter.record_success("user@example.com", "7.7.7.7")

        redis.delete.assert_awaited_once_with(
            f"{_REDIS_KEY_PREFIX}:user@example.com:7.7.7.7"
        )

    @pytest.mark.asyncio
    async def test_redis_key_email_is_lowercased(self):
        """Redis keys normalise the email to lower-case."""
        redis = _make_redis_mock(hgetall_data={})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        await limiter.check_rate_limit("UPPER@Example.COM", "8.8.8.8")

        redis.hgetall.assert_awaited_once_with(
            f"{_REDIS_KEY_PREFIX}:upper@example.com:8.8.8.8"
        )

    @pytest.mark.asyncio
    async def test_redis_backoff_advances_with_extra_failures(self):
        """Additional failures past the threshold advance the backoff tier in Redis."""
        # Start at MAX failures (locked out at tier 0), add one more → tier 1.
        initial_count = _MAX_ATTEMPTS_BEFORE_LOCKOUT
        redis = _make_redis_mock(hgetall_data={"failed_count": str(initial_count)})
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        before = time.time()
        await limiter.record_failure("extra@example.com", "9.9.9.9")
        after = time.time()

        mapping = redis.hset.call_args.kwargs.get("mapping") or redis.hset.call_args.args[1]
        locked_until = float(mapping["locked_until"])
        expected_backoff = _BACKOFF_SCHEDULE[1]
        assert locked_until >= before + expected_backoff - 1
        assert locked_until <= after + expected_backoff + 2


# ---------------------------------------------------------------------------
# 4. Redis failure → in-memory fallback
# ---------------------------------------------------------------------------


class TestRedisFallback:
    """Tests that any Redis error falls back to in-memory gracefully."""

    @pytest.mark.asyncio
    async def test_check_falls_back_on_redis_error(self, caplog):
        """Redis hgetall raising an exception causes fallback to in-memory."""
        redis = AsyncMock()
        redis.hgetall = AsyncMock(side_effect=ConnectionError("Redis down"))
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        import logging

        with caplog.at_level(logging.WARNING):
            allowed, retry_after = await limiter.check_rate_limit("fallback@example.com", "1.1.1.1")

        # In-memory store is empty → should be allowed.
        assert allowed is True
        assert retry_after == 0
        assert any("falling back to in-memory" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_record_failure_falls_back_on_redis_error(self, caplog):
        """Redis error during record_failure writes to in-memory store instead."""
        redis = AsyncMock()
        redis.hgetall = AsyncMock(side_effect=ConnectionError("Redis down"))
        redis.hset = AsyncMock(side_effect=ConnectionError("Redis down"))
        redis.expire = AsyncMock()
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        import logging

        with caplog.at_level(logging.WARNING):
            for _ in range(_MAX_ATTEMPTS_BEFORE_LOCKOUT):
                await limiter.record_failure("fallback@example.com", "2.2.2.2")

        # The in-memory store should have the record.
        key = ("fallback@example.com", "2.2.2.2")
        assert key in limiter._attempts
        assert limiter._attempts[key].failed_count == _MAX_ATTEMPTS_BEFORE_LOCKOUT

    @pytest.mark.asyncio
    async def test_record_success_falls_back_on_redis_error(self, caplog):
        """Redis error during record_success falls back to in-memory delete."""
        redis = AsyncMock()
        redis.delete = AsyncMock(side_effect=ConnectionError("Redis down"))
        LoginRateLimiter.configure_redis(redis)
        limiter = _make_limiter()

        # Seed the in-memory store.
        from api.middleware.login_rate_limiter import _AttemptRecord

        key = ("fallback@example.com", "3.3.3.3")
        limiter._attempts[key] = _AttemptRecord(failed_count=3)

        import logging

        with caplog.at_level(logging.WARNING):
            await limiter.record_success("fallback@example.com", "3.3.3.3")

        # In-memory entry should have been cleared.
        assert key not in limiter._attempts


# ---------------------------------------------------------------------------
# 5. configure_redis classmethod / module-level helper
# ---------------------------------------------------------------------------


class TestConfigureRedis:
    """Tests for the configure_redis classmethod and module-level function."""

    def test_configure_redis_sets_class_attribute(self):
        """configure_redis() sets the class-level _redis_client."""
        mock_redis = MagicMock()
        LoginRateLimiter.configure_redis(mock_redis)
        assert LoginRateLimiter._redis_client is mock_redis

    def test_configure_redis_none_reverts_to_memory(self):
        """Passing None resets the class to in-memory mode."""
        LoginRateLimiter.configure_redis(MagicMock())
        LoginRateLimiter.configure_redis(None)
        assert LoginRateLimiter._redis_client is None

    def test_module_level_configure_redis(self):
        """The module-level configure_redis() is a thin wrapper around the classmethod."""
        mock_redis = MagicMock()
        configure_redis(mock_redis)
        assert LoginRateLimiter._redis_client is mock_redis

    @pytest.mark.asyncio
    async def test_new_instance_uses_class_redis(self):
        """Instances created after configure_redis() use the shared Redis client."""
        redis = _make_redis_mock(hgetall_data={})
        LoginRateLimiter.configure_redis(redis)

        limiter_a = _make_limiter()
        limiter_b = _make_limiter()

        # Both instances share the class-level Redis client.
        await limiter_a.check_rate_limit("a@example.com", "1.1.1.1")
        await limiter_b.check_rate_limit("b@example.com", "2.2.2.2")

        assert redis.hgetall.await_count == 2
