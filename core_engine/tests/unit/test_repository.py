"""Unit tests for RunRepository cost-tracking functionality.

These tests use an in-memory SQLite database via aiosqlite so they can
run without a PostgreSQL instance.  Because JSONB is Postgres-specific,
the test fixtures substitute a plain JSON column type via event listeners.

Covers:
- Storing and retrieving cost_usd on a run record
- Computing avg_cost_usd in historical stats
- Updating cost after run creation via update_cost()
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.types import TypeDecorator

from core_engine.state.repository import RunRepository
from core_engine.state.tables import Base, RunTable

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _patch_columns_for_sqlite() -> None:
    """Substitute Postgres-specific column types for SQLite compatibility.

    * ``JSONB`` -> ``JSON`` (SQLite has no JSONB type compiler).
    * ``DateTime(timezone=True)`` -> ``DateTime()`` with an explicit
      :class:`~sqlalchemy.TypeDecorator` that coerces naive datetimes
      returned by SQLite back to UTC-aware.
    """

    class _UTCAwareDateTime(TypeDecorator):
        """SQLAlchemy TypeDecorator that ensures datetimes are always UTC-aware."""

        impl = DateTime
        cache_ok = True

        def process_result_value(self, value, dialect):  # type: ignore[override]
            if value is not None and value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, JSONB):
                column.type = JSON()
            elif isinstance(column.type, DateTime) and getattr(column.type, "timezone", False):
                column.type = _UTCAwareDateTime()


# Perform patching once at import time so every test session shares the
# same metadata state.  This is safe because the test suite never runs
# against Postgres.
_patch_columns_for_sqlite()


@pytest_asyncio.fixture
async def async_session():
    """Provide an async session backed by an in-memory SQLite database."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session

    await engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXECUTOR_VERSION = "test-0.1.0"
_TENANT = "test-tenant"


def _make_run_record(
    *,
    plan_id: str = "plan-1",
    step_id: str | None = None,
    model_name: str = "analytics.orders",
    status: str = "COMPLETED",
    cost_usd: float | None = None,
    external_run_id: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> dict:
    """Build a minimal run record dict for test insertion."""
    now = datetime.now(timezone.utc)
    return {
        "run_id": uuid4().hex,
        "plan_id": plan_id,
        "step_id": step_id or uuid4().hex,
        "model_name": model_name,
        "status": status,
        "started_at": started_at or now - timedelta(seconds=120),
        "finished_at": finished_at or now,
        "input_range_start": None,
        "input_range_end": None,
        "error_message": None,
        "logs_uri": None,
        "cluster_used": None,
        "executor_version": _EXECUTOR_VERSION,
        "retry_count": 0,
        "cost_usd": cost_usd,
        "external_run_id": external_run_id,
    }


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestRunCostStoredAndRetrieved:
    """Verify that cost_usd and external_run_id are persisted and readable."""

    @pytest.mark.asyncio
    async def test_run_cost_stored_and_retrieved(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        record = _make_run_record(cost_usd=1.2345, external_run_id="ext-abc-123")
        await repo.create_run(record)

        row = await repo.get_by_id(record["run_id"])
        assert row is not None
        assert float(row.cost_usd) == pytest.approx(1.2345)
        assert row.external_run_id == "ext-abc-123"

    @pytest.mark.asyncio
    async def test_run_without_cost_has_null(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        record = _make_run_record(cost_usd=None, external_run_id=None)
        await repo.create_run(record)

        row = await repo.get_by_id(record["run_id"])
        assert row is not None
        assert row.cost_usd is None
        assert row.external_run_id is None


class TestHistoricalStatsWithCost:
    """Verify that get_historical_stats computes avg_cost_usd correctly."""

    @pytest.mark.asyncio
    async def test_historical_stats_with_cost(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        model = "analytics.revenue"

        # Create three runs with known costs: 1.0, 2.0, 3.0 -> avg = 2.0.
        for cost in [1.0, 2.0, 3.0]:
            now = datetime.now(timezone.utc)
            record = _make_run_record(
                model_name=model,
                cost_usd=cost,
                started_at=now - timedelta(seconds=60),
                finished_at=now,
            )
            await repo.create_run(record)

        stats = await repo.get_historical_stats(model)
        assert stats["run_count"] == 3
        assert stats["avg_cost_usd"] is not None
        assert stats["avg_cost_usd"] == pytest.approx(2.0)
        assert stats["avg_runtime_seconds"] is not None
        assert stats["avg_runtime_seconds"] == pytest.approx(60.0, abs=1.0)

    @pytest.mark.asyncio
    async def test_historical_stats_with_no_cost(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        model = "analytics.sessions"

        # Create runs without cost.
        for _ in range(2):
            now = datetime.now(timezone.utc)
            record = _make_run_record(
                model_name=model,
                cost_usd=None,
                started_at=now - timedelta(seconds=30),
                finished_at=now,
            )
            await repo.create_run(record)

        stats = await repo.get_historical_stats(model)
        assert stats["run_count"] == 2
        assert stats["avg_cost_usd"] is None

    @pytest.mark.asyncio
    async def test_historical_stats_mixed_cost(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        model = "analytics.mixed"

        # One run with cost, one without.  avg should be over the non-null value only.
        now = datetime.now(timezone.utc)
        record_with = _make_run_record(
            model_name=model,
            cost_usd=4.0,
            started_at=now - timedelta(seconds=60),
            finished_at=now,
        )
        record_without = _make_run_record(
            model_name=model,
            cost_usd=None,
            started_at=now - timedelta(seconds=60),
            finished_at=now,
        )
        await repo.create_run(record_with)
        await repo.create_run(record_without)

        stats = await repo.get_historical_stats(model)
        assert stats["run_count"] == 2
        # SQL AVG ignores NULLs, so avg of [4.0] = 4.0.
        assert stats["avg_cost_usd"] == pytest.approx(4.0)

    @pytest.mark.asyncio
    async def test_historical_stats_empty(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        stats = await repo.get_historical_stats("nonexistent.model")
        assert stats["run_count"] == 0
        assert stats["avg_cost_usd"] is None
        assert stats["avg_runtime_seconds"] is None


class TestUpdateCost:
    """Verify that update_cost sets cost on an existing run."""

    @pytest.mark.asyncio
    async def test_update_cost(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        record = _make_run_record(cost_usd=None)
        await repo.create_run(record)

        # Verify initially null.
        row = await repo.get_by_id(record["run_id"])
        assert row is not None
        assert row.cost_usd is None

        # Update.
        await repo.update_cost(record["run_id"], 0.5678)

        # Verify updated.
        row = await repo.get_by_id(record["run_id"])
        assert row is not None
        assert float(row.cost_usd) == pytest.approx(0.5678)

    @pytest.mark.asyncio
    async def test_update_cost_overwrites_existing(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        record = _make_run_record(cost_usd=1.0)
        await repo.create_run(record)

        await repo.update_cost(record["run_id"], 2.5)

        row = await repo.get_by_id(record["run_id"])
        assert row is not None
        assert float(row.cost_usd) == pytest.approx(2.5)

    @pytest.mark.asyncio
    async def test_update_cost_tenant_isolation(self, async_session: AsyncSession):
        repo_t1 = RunRepository(async_session, tenant_id="tenant-a")
        repo_t2 = RunRepository(async_session, tenant_id="tenant-b")

        record = _make_run_record(cost_usd=None)
        await repo_t1.create_run(record)

        # Updating from a different tenant should not affect the run.
        await repo_t2.update_cost(record["run_id"], 99.99)

        row = await repo_t1.get_by_id(record["run_id"])
        assert row is not None
        assert row.cost_usd is None  # unchanged
