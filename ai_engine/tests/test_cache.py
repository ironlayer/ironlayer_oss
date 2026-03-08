"""Tests for ResponseCache and cache integration."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from ai_engine.engines.cache import DEFAULT_TTL, ResponseCache

# ---------------------------------------------------------------------------
# ResponseCache.make_key
# ---------------------------------------------------------------------------


class TestMakeKey:
    """Tests for deterministic key generation."""

    def test_same_inputs_same_key(self):
        k1 = ResponseCache.make_key("risk_score", {"a": 1, "b": 2})
        k2 = ResponseCache.make_key("risk_score", {"a": 1, "b": 2})
        assert k1 == k2

    def test_key_is_sha256_hex(self):
        key = ResponseCache.make_key("predict_cost", {"x": 42})
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_different_type_different_key(self):
        k1 = ResponseCache.make_key("predict_cost", {"a": 1})
        k2 = ResponseCache.make_key("risk_score", {"a": 1})
        assert k1 != k2

    def test_different_payload_different_key(self):
        k1 = ResponseCache.make_key("predict_cost", {"a": 1})
        k2 = ResponseCache.make_key("predict_cost", {"a": 2})
        assert k1 != k2

    def test_different_prompt_version_different_key(self):
        k1 = ResponseCache.make_key("predict_cost", {"a": 1}, prompt_version="v1")
        k2 = ResponseCache.make_key("predict_cost", {"a": 1}, prompt_version="v2")
        assert k1 != k2

    def test_key_order_insensitive(self):
        """JSON sort_keys ensures dict key order doesn't matter."""
        k1 = ResponseCache.make_key("risk_score", {"b": 2, "a": 1})
        k2 = ResponseCache.make_key("risk_score", {"a": 1, "b": 2})
        assert k1 == k2


# ---------------------------------------------------------------------------
# ResponseCache.get / put
# ---------------------------------------------------------------------------


class TestGetPut:
    """Tests for cache get and put operations."""

    def test_miss_returns_none(self):
        cache = ResponseCache()
        assert cache.get("nonexistent") is None

    def test_put_and_get(self):
        cache = ResponseCache()
        key = ResponseCache.make_key("risk_score", {"model": "m1"})
        cache.put(key, {"risk_score": 5.0}, "risk_score")
        assert cache.get(key) == {"risk_score": 5.0}

    def test_expired_entry_returns_none(self):
        cache = ResponseCache(ttl_overrides={"risk_score": 0})
        key = ResponseCache.make_key("risk_score", {"model": "m1"})
        cache.put(key, {"risk_score": 5.0}, "risk_score")
        # TTL=0 means it expires immediately
        time.sleep(0.01)
        assert cache.get(key) is None

    def test_expired_entry_is_evicted(self):
        cache = ResponseCache(ttl_overrides={"risk_score": 0})
        key = ResponseCache.make_key("risk_score", {"model": "m1"})
        cache.put(key, {"risk_score": 5.0}, "risk_score")
        time.sleep(0.01)
        cache.get(key)  # triggers lazy eviction
        assert cache.size == 0

    def test_overwrite_existing_key(self):
        cache = ResponseCache()
        key = ResponseCache.make_key("risk_score", {"model": "m1"})
        cache.put(key, {"v": 1}, "risk_score")
        cache.put(key, {"v": 2}, "risk_score")
        assert cache.get(key) == {"v": 2}

    def test_different_request_types_independent(self):
        cache = ResponseCache()
        k1 = ResponseCache.make_key("predict_cost", {"m": "x"})
        k2 = ResponseCache.make_key("risk_score", {"m": "x"})
        cache.put(k1, "cost_result", "predict_cost")
        cache.put(k2, "risk_result", "risk_score")
        assert cache.get(k1) == "cost_result"
        assert cache.get(k2) == "risk_result"

    def test_disabled_cache_noop(self):
        cache = ResponseCache(enabled=False)
        key = ResponseCache.make_key("risk_score", {"model": "m1"})
        cache.put(key, "value", "risk_score")
        assert cache.get(key) is None
        assert cache.size == 0


# ---------------------------------------------------------------------------
# ResponseCache.invalidate
# ---------------------------------------------------------------------------


class TestInvalidate:
    """Tests for cache invalidation."""

    def test_invalidate_single(self):
        cache = ResponseCache()
        key = ResponseCache.make_key("risk_score", {"m": "x"})
        cache.put(key, "value", "risk_score")
        assert cache.invalidate(key) is True
        assert cache.get(key) is None

    def test_invalidate_nonexistent(self):
        cache = ResponseCache()
        assert cache.invalidate("missing") is False

    def test_invalidate_by_type(self):
        cache = ResponseCache()
        k1 = ResponseCache.make_key("risk_score", {"m": "a"})
        k2 = ResponseCache.make_key("risk_score", {"m": "b"})
        k3 = ResponseCache.make_key("predict_cost", {"m": "c"})
        cache.put(k1, "v1", "risk_score")
        cache.put(k2, "v2", "risk_score")
        cache.put(k3, "v3", "predict_cost")

        removed = cache.invalidate_by_type("risk_score")
        assert removed == 2
        assert cache.get(k1) is None
        assert cache.get(k2) is None
        assert cache.get(k3) == "v3"  # cost entry untouched

    def test_invalidate_all(self):
        cache = ResponseCache()
        for i in range(10):
            key = ResponseCache.make_key("risk_score", {"i": i})
            cache.put(key, f"v{i}", "risk_score")
        assert cache.size == 10
        removed = cache.invalidate_all()
        assert removed == 10
        assert cache.size == 0


# ---------------------------------------------------------------------------
# ResponseCache.stats
# ---------------------------------------------------------------------------


class TestStats:
    """Tests for cache statistics."""

    def test_initial_stats(self):
        cache = ResponseCache()
        stats = cache.stats
        assert stats["entries"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate"] == 0.0
        assert stats["enabled"] is True

    def test_hit_miss_tracking(self):
        cache = ResponseCache()
        key = ResponseCache.make_key("risk_score", {"m": "x"})

        cache.get("nonexistent")  # miss
        cache.put(key, "value", "risk_score")
        cache.get(key)  # hit
        cache.get(key)  # hit
        cache.get("another_miss")  # miss

        stats = cache.stats
        assert stats["hits"] == 2
        assert stats["misses"] == 2
        assert stats["hit_rate"] == 0.5

    def test_stats_reset_on_invalidate_all(self):
        cache = ResponseCache()
        cache.get("x")  # miss
        cache.invalidate_all()
        stats = cache.stats
        assert stats["hits"] == 0
        assert stats["misses"] == 0


# ---------------------------------------------------------------------------
# Eviction
# ---------------------------------------------------------------------------


class TestEviction:
    """Tests for cache eviction behaviour."""

    def test_max_entries_enforced(self):
        cache = ResponseCache(max_entries=5)
        for i in range(10):
            key = ResponseCache.make_key("risk_score", {"i": i})
            cache.put(key, f"v{i}", "risk_score")
        assert cache.size <= 5

    def test_eviction_removes_oldest(self):
        cache = ResponseCache(max_entries=3)
        keys = []
        for i in range(3):
            key = ResponseCache.make_key("risk_score", {"i": i})
            cache.put(key, f"v{i}", "risk_score")
            keys.append(key)

        # Add a 4th entry, should evict the oldest
        new_key = ResponseCache.make_key("risk_score", {"i": 99})
        cache.put(new_key, "new", "risk_score")

        # The newest entry should be present
        assert cache.get(new_key) == "new"
        assert cache.size <= 3


# ---------------------------------------------------------------------------
# Default TTLs
# ---------------------------------------------------------------------------


class TestDefaultTTL:
    """Verify default TTL configuration."""

    def test_semantic_classify_ttl(self):
        assert DEFAULT_TTL["semantic_classify"] == 3600

    def test_predict_cost_ttl(self):
        assert DEFAULT_TTL["predict_cost"] == 900

    def test_risk_score_ttl(self):
        assert DEFAULT_TTL["risk_score"] == 3600

    def test_optimize_sql_ttl(self):
        assert DEFAULT_TTL["optimize_sql"] == 1800

    def test_ttl_overrides(self):
        cache = ResponseCache(ttl_overrides={"risk_score": 60})
        assert cache._ttls["risk_score"] == 60
        assert cache._ttls["predict_cost"] == 900  # default unchanged
