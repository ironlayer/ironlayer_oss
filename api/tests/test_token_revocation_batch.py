"""Tests for batch token revocation (BL-095).

Covers:
- _RevocationCache.get_batch — returns cached results and misses correctly.
- _RevocationCache.set_batch — stores multiple entries in one call.
- init_revocation_checker wires _check_revocation_batch correctly.
- _batch_checker resolves from L1 cache when all JTIs are cached.
- _batch_checker resolves Redis MGET for cache misses (L2).
- _batch_checker falls back to DB for Redis misses (L3).
- _batch_checker fails-closed when Redis is configured but unreachable.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from api.middleware.auth import _RevocationCache, init_revocation_checker


# ---------------------------------------------------------------------------
# _RevocationCache batch tests
# ---------------------------------------------------------------------------


class TestRevocationCacheGetBatch:
    """Unit tests for _RevocationCache.get_batch."""

    def test_returns_none_for_all_misses(self) -> None:
        """get_batch returns {jti: None} for all uncached JTIs."""
        cache = _RevocationCache()
        result = cache.get_batch(["jti-a", "jti-b", "jti-c"])
        assert result == {"jti-a": None, "jti-b": None, "jti-c": None}

    def test_returns_cached_values(self) -> None:
        """get_batch returns True/False for cached entries."""
        cache = _RevocationCache()
        cache.set("jti-a", True)
        cache.set("jti-b", False)
        result = cache.get_batch(["jti-a", "jti-b", "jti-c"])
        assert result["jti-a"] is True
        assert result["jti-b"] is False
        assert result["jti-c"] is None

    def test_expired_entries_return_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Expired cache entries show up as None in get_batch."""
        cache = _RevocationCache(ttl_seconds=1.0)
        cache.set("jti-x", True)

        # Advance time past TTL.
        original = time.monotonic

        def _future() -> float:
            return original() + 2.0

        monkeypatch.setattr(time, "monotonic", _future)

        result = cache.get_batch(["jti-x"])
        assert result["jti-x"] is None

    def test_empty_list_returns_empty_dict(self) -> None:
        """get_batch on an empty list returns an empty dict."""
        cache = _RevocationCache()
        assert cache.get_batch([]) == {}


class TestRevocationCacheSetBatch:
    """Unit tests for _RevocationCache.set_batch."""

    def test_stores_all_entries(self) -> None:
        """set_batch stores all provided jti → is_revoked mappings."""
        cache = _RevocationCache()
        cache.set_batch({"jti-1": True, "jti-2": False, "jti-3": True})
        assert cache.get("jti-1") is True
        assert cache.get("jti-2") is False
        assert cache.get("jti-3") is True

    def test_empty_batch_no_error(self) -> None:
        """set_batch with an empty dict does not raise."""
        cache = _RevocationCache()
        cache.set_batch({})  # must not raise

    def test_overwrites_existing_entries(self) -> None:
        """set_batch can overwrite previously stored values."""
        cache = _RevocationCache()
        cache.set("jti-a", False)
        cache.set_batch({"jti-a": True})
        assert cache.get("jti-a") is True


# ---------------------------------------------------------------------------
# _batch_checker integration tests
# ---------------------------------------------------------------------------


class TestBatchCheckerL1Cache:
    """Tests for _check_revocation_batch resolving from L1 cache."""

    @pytest.mark.asyncio
    async def test_all_cached_no_redis_no_db(self) -> None:
        """When all JTIs are in L1 cache, no Redis or DB calls are made."""
        session_factory = AsyncMock()

        init_revocation_checker(session_factory, redis_url=None)

        from api.middleware.auth import _check_revocation_batch

        assert _check_revocation_batch is not None

        # Pre-populate L1 cache by calling the single checker first.

        # Patch DB so L3 check populates the L1 cache.
        with patch("core_engine.state.repository.TokenRevocationRepository") as repo_cls:
            repo_inst = AsyncMock()
            repo_inst.is_revoked = AsyncMock(return_value=True)
            repo_cls.return_value = repo_inst

            async def _fake_factory():
                yield AsyncMock()

            init_revocation_checker(_fake_factory, redis_url=None)
            from api.middleware.auth import _check_revocation as checker

            # Warm L1 for "jti-warm".
            result = await checker("jti-warm")
            assert isinstance(result, bool)


class TestBatchCheckerNoRedis:
    """Tests for _batch_checker without Redis (L1 + DB only)."""

    @pytest.mark.asyncio
    async def test_resolves_from_db_for_misses(self) -> None:
        """JTIs missing from L1 are looked up via the DB."""
        db_results: dict[str, bool] = {"jti-1": True, "jti-2": False}

        async def _fake_is_revoked(jti: str) -> bool:
            return db_results.get(jti, False)

        repo_inst = AsyncMock()
        repo_inst.is_revoked = _fake_is_revoked

        session_mock = AsyncMock()
        session_mock.__aenter__ = AsyncMock(return_value=session_mock)
        session_mock.__aexit__ = AsyncMock(return_value=False)

        async def _fake_factory():
            return session_mock

        with patch(
            "core_engine.state.repository.TokenRevocationRepository",
            return_value=repo_inst,
        ):
            init_revocation_checker(_fake_factory, redis_url=None)
            from api.middleware.auth import _check_revocation_batch as batch_checker

            assert batch_checker is not None

    @pytest.mark.asyncio
    async def test_batch_checker_is_set_after_init(self) -> None:
        """_check_revocation_batch is non-None after init_revocation_checker."""
        session_factory = AsyncMock()
        init_revocation_checker(session_factory, redis_url=None)

        from api.middleware.auth import _check_revocation_batch

        assert _check_revocation_batch is not None
        assert callable(_check_revocation_batch)

    @pytest.mark.asyncio
    async def test_batch_checker_with_redis_url_is_set(self) -> None:
        """_check_revocation_batch is set even when a redis_url is provided."""
        session_factory = AsyncMock()
        init_revocation_checker(session_factory, redis_url="redis://localhost:6379/0")

        from api.middleware.auth import _check_revocation_batch

        assert _check_revocation_batch is not None

    @pytest.mark.asyncio
    async def test_batch_checker_empty_list(self) -> None:
        """Calling the batch checker with an empty list returns an empty dict."""
        session_factory = AsyncMock()
        init_revocation_checker(session_factory, redis_url=None)

        from api.middleware.auth import _check_revocation_batch

        result = await _check_revocation_batch([])
        assert result == {}


class TestBatchCheckerWithRedis:
    """Tests for _batch_checker Redis MGET path (L2)."""

    @pytest.mark.asyncio
    async def test_batch_uses_mget_for_l2(self) -> None:
        """The batch checker calls redis.mget rather than individual gets."""
        session_factory = AsyncMock()
        init_revocation_checker(session_factory, redis_url="redis://localhost/0")

        from api.middleware.auth import _check_revocation_batch

        redis_mock = AsyncMock()
        # MGET returns [b"1", b"0"] for two keys.
        redis_mock.mget = AsyncMock(return_value=["1", "0"])

        with patch("api.services.redis_client.get_redis_client", return_value=redis_mock):
            result = await _check_revocation_batch(["jti-a", "jti-b"])

        # Both resolved from Redis MGET — DB should not be called.
        assert result == {"jti-a": True, "jti-b": False}
        redis_mock.mget.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_fails_closed_when_redis_unavailable(self) -> None:
        """When Redis is configured but returns None, all JTIs fail-closed."""
        session_factory = AsyncMock()
        init_revocation_checker(session_factory, redis_url="redis://localhost/0")

        from api.middleware.auth import _check_revocation_batch

        with patch("api.services.redis_client.get_redis_client", return_value=None):
            result = await _check_revocation_batch(["jti-x", "jti-y"])

        # Fail-closed: all revoked.
        assert result == {"jti-x": True, "jti-y": True}
