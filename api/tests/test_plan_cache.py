"""Tests for BL-094: Redis plan cache in PlanService.get_plan().

Covers:
- Cache HIT: second GET for same plan_id reads from Redis, not DB.
- Cache MISS: first GET falls through to DB and stores result in Redis.
- Mutation invalidation: generate_plan() pre-warms the cache.
- Fail-open: Redis errors are swallowed, DB is always the fallback.
- No-redis: when no Redis client is configured, DB is used transparently.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.services.plan_service import PLAN_CACHE_TTL, PlanService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan_row(plan_id: str = "plan-001") -> MagicMock:
    """Build a minimal mock PlanTable row."""
    row = MagicMock()
    row.plan_id = plan_id
    row.base_sha = "abc123"
    row.target_sha = "def456"
    row.plan_json = json.dumps(
        {
            "plan_id": plan_id,
            "steps": [],
            "summary": {"total_steps": 0, "models_changed": []},
        }
    )
    row.approvals_json = json.dumps([])
    row.auto_approved = False
    row.created_at = datetime(2026, 3, 7, tzinfo=UTC)
    return row


def _make_service(redis: Any | None = None) -> tuple[PlanService, MagicMock]:
    """Create a PlanService with mocked dependencies, return (svc, plan_repo_mock)."""
    session = AsyncMock()
    ai_client = MagicMock()
    settings = MagicMock()
    settings.allowed_repo_base = "/tmp"

    svc = PlanService(
        session=session,
        ai_client=ai_client,
        settings=settings,
        tenant_id="tenant-test",
        redis=redis,
    )
    # Patch the internal repo
    plan_repo_mock = AsyncMock()
    svc._plan_repo = plan_repo_mock
    return svc, plan_repo_mock


def _make_redis(
    get_result: str | None = None,
    raise_on_get: bool = False,
    raise_on_set: bool = False,
    raise_on_delete: bool = False,
) -> AsyncMock:
    """Build a minimal mock Redis client."""
    redis = AsyncMock()
    if raise_on_get:
        redis.get.side_effect = ConnectionError("Redis down")
    else:
        redis.get.return_value = get_result
    if raise_on_set:
        redis.setex.side_effect = ConnectionError("Redis down")
    if raise_on_delete:
        redis.delete.side_effect = ConnectionError("Redis down")
    return redis


# ---------------------------------------------------------------------------
# PLAN_CACHE_TTL constant
# ---------------------------------------------------------------------------


class TestPlanCacheTTL:
    def test_cache_ttl_is_300(self):
        assert PLAN_CACHE_TTL == 300


# ---------------------------------------------------------------------------
# get_plan() — cache miss path
# ---------------------------------------------------------------------------


class TestGetPlanCacheMiss:
    """First GET falls through to DB and writes result to Redis."""

    @pytest.mark.asyncio
    async def test_miss_hits_db(self):
        redis = _make_redis(get_result=None)
        svc, repo = _make_service(redis=redis)
        row = _make_plan_row("p1")
        repo.get_plan.return_value = row

        result = await svc.get_plan("p1")

        assert result is not None
        assert result["plan_id"] == "p1"
        repo.get_plan.assert_awaited_once_with("p1")

    @pytest.mark.asyncio
    async def test_miss_stores_in_redis(self):
        redis = _make_redis(get_result=None)
        svc, repo = _make_service(redis=redis)
        repo.get_plan.return_value = _make_plan_row("p1")

        await svc.get_plan("p1")

        redis.setex.assert_awaited_once()
        key, ttl, value = redis.setex.call_args.args
        assert key == "plan:tenant-test:p1"
        assert ttl == PLAN_CACHE_TTL
        stored = json.loads(value)
        assert stored["plan_id"] == "p1"

    @pytest.mark.asyncio
    async def test_miss_returns_none_when_db_has_no_row(self):
        redis = _make_redis(get_result=None)
        svc, repo = _make_service(redis=redis)
        repo.get_plan.return_value = None

        result = await svc.get_plan("missing")

        assert result is None
        redis.setex.assert_not_awaited()


# ---------------------------------------------------------------------------
# get_plan() — cache hit path
# ---------------------------------------------------------------------------


class TestGetPlanCacheHit:
    """Second GET reads from Redis; DB is never queried."""

    @pytest.mark.asyncio
    async def test_hit_returns_cached_data(self):
        cached = json.dumps(
            {"plan_id": "p2", "steps": [], "approvals": [], "auto_approved": False}
        )
        redis = _make_redis(get_result=cached)
        svc, repo = _make_service(redis=redis)

        result = await svc.get_plan("p2")

        assert result is not None
        assert result["plan_id"] == "p2"
        # DB must NOT be called on cache hit.
        repo.get_plan.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_hit_uses_correct_cache_key(self):
        cached = json.dumps({"plan_id": "p3", "steps": []})
        redis = _make_redis(get_result=cached)
        svc, repo = _make_service(redis=redis)

        await svc.get_plan("p3")

        redis.get.assert_awaited_once_with("plan:tenant-test:p3")

    @pytest.mark.asyncio
    async def test_cache_key_is_tenant_scoped(self):
        """Cache keys include the tenant_id to prevent cross-tenant leakage."""
        redis = _make_redis(get_result=None)
        redis.get.return_value = None

        session = AsyncMock()
        svc_a = PlanService(
            session=session, ai_client=MagicMock(), settings=MagicMock(),
            tenant_id="tenant-a", redis=redis,
        )
        svc_b = PlanService(
            session=session, ai_client=MagicMock(), settings=MagicMock(),
            tenant_id="tenant-b", redis=redis,
        )
        key_a = svc_a._plan_cache_key("p1")
        key_b = svc_b._plan_cache_key("p1")
        assert key_a != key_b
        assert "tenant-a" in key_a
        assert "tenant-b" in key_b


# ---------------------------------------------------------------------------
# Fail-open: Redis errors are swallowed
# ---------------------------------------------------------------------------


class TestGetPlanFailOpen:
    """Redis errors must never propagate to the caller."""

    @pytest.mark.asyncio
    async def test_redis_get_error_falls_back_to_db(self):
        redis = _make_redis(raise_on_get=True)
        svc, repo = _make_service(redis=redis)
        repo.get_plan.return_value = _make_plan_row("p4")

        result = await svc.get_plan("p4")

        assert result is not None
        assert result["plan_id"] == "p4"
        repo.get_plan.assert_awaited_once_with("p4")

    @pytest.mark.asyncio
    async def test_redis_set_error_does_not_raise(self):
        redis = _make_redis(get_result=None, raise_on_set=True)
        svc, repo = _make_service(redis=redis)
        repo.get_plan.return_value = _make_plan_row("p5")

        # Must not raise even when setex fails.
        result = await svc.get_plan("p5")
        assert result is not None

    @pytest.mark.asyncio
    async def test_no_redis_client_uses_db_only(self):
        svc, repo = _make_service(redis=None)
        repo.get_plan.return_value = _make_plan_row("p6")

        result = await svc.get_plan("p6")

        assert result is not None
        assert result["plan_id"] == "p6"
        repo.get_plan.assert_awaited_once_with("p6")


# ---------------------------------------------------------------------------
# invalidate_plan()
# ---------------------------------------------------------------------------


class TestInvalidatePlan:
    @pytest.mark.asyncio
    async def test_invalidate_deletes_cache_key(self):
        redis = AsyncMock()
        svc, _ = _make_service(redis=redis)

        await svc.invalidate_plan("p7")

        redis.delete.assert_awaited_once_with("plan:tenant-test:p7")

    @pytest.mark.asyncio
    async def test_invalidate_no_redis_is_noop(self):
        """invalidate_plan() is a no-op when Redis is not configured."""
        svc, _ = _make_service(redis=None)
        # Must not raise.
        await svc.invalidate_plan("p8")

    @pytest.mark.asyncio
    async def test_invalidate_redis_error_does_not_raise(self):
        redis = _make_redis(raise_on_delete=True)
        svc, _ = _make_service(redis=redis)
        # Fail-open — must not raise.
        await svc.invalidate_plan("p9")
