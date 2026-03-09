"""Unit tests for RunRepository cost-tracking functionality.

These tests use an in-memory SQLite database via aiosqlite so they can
run without a PostgreSQL instance.  Because JSONB is Postgres-specific,
the test fixtures substitute a plain JSON column type via event listeners.

Covers:
- Storing and retrieving cost_usd on a run record
- Computing avg_cost_usd in historical stats
- Updating cost after run creation via update_cost()
- Batch methods: get_historical_stats_batch, get_failure_rates_batch (BL-062)
- WatermarkRepository.get_watermarks_batch (BL-062)
- ModelRepository.get_models_batch (BL-062)
- PlanRepository.list_recent with SQL-level offset (BL-063)
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from core_engine.state.plan_repository import PlanRepository
from core_engine.state.repository import ModelRepository, RunRepository, WatermarkRepository
from core_engine.state.tables import Base, ModelTable, PlanTable, WatermarkTable
from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.types import TypeDecorator

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
                return value.replace(tzinfo=UTC)
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
    now = datetime.now(UTC)
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
            now = datetime.now(UTC)
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
            now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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


# ---------------------------------------------------------------------------
# BL-062: RunRepository.get_historical_stats_batch
# ---------------------------------------------------------------------------


class TestGetHistoricalStatsBatch:
    """Verify that get_historical_stats_batch returns correct aggregates."""

    @pytest.mark.asyncio
    async def test_batch_returns_stats_for_all_models(self, async_session: AsyncSession):
        """Two models with completed runs appear in the batch result."""
        repo = RunRepository(async_session, tenant_id=_TENANT)
        now = datetime.now(UTC)

        for model in ("analytics.orders", "analytics.users"):
            for cost in [1.0, 2.0]:
                record = _make_run_record(
                    model_name=model,
                    cost_usd=cost,
                    started_at=now - timedelta(seconds=60),
                    finished_at=now,
                )
                await repo.create_run(record)

        result = await repo.get_historical_stats_batch(["analytics.orders", "analytics.users"])

        assert "analytics.orders" in result
        assert "analytics.users" in result
        assert result["analytics.orders"]["run_count"] == 2
        assert result["analytics.orders"]["avg_cost_usd"] == pytest.approx(1.5)
        assert result["analytics.users"]["run_count"] == 2

    @pytest.mark.asyncio
    async def test_batch_absent_model_not_in_result(self, async_session: AsyncSession):
        """Models with no completed runs are absent from the batch result."""
        repo = RunRepository(async_session, tenant_id=_TENANT)

        result = await repo.get_historical_stats_batch(["nonexistent.model"])

        assert "nonexistent.model" not in result

    @pytest.mark.asyncio
    async def test_batch_empty_input_returns_empty(self, async_session: AsyncSession):
        """Empty model name list returns empty dict without a DB round trip."""
        repo = RunRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_historical_stats_batch([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_batch_tenant_isolation(self, async_session: AsyncSession):
        """Stats for tenant-A do not appear in tenant-B's batch result."""
        repo_a = RunRepository(async_session, tenant_id="tenant-a")
        repo_b = RunRepository(async_session, tenant_id="tenant-b")
        now = datetime.now(UTC)

        record = _make_run_record(
            model_name="shared.model",
            cost_usd=5.0,
            started_at=now - timedelta(seconds=30),
            finished_at=now,
        )
        await repo_a.create_run(record)

        result = await repo_b.get_historical_stats_batch(["shared.model"])
        assert "shared.model" not in result

    @pytest.mark.asyncio
    async def test_batch_caps_at_thirty_runs_per_model(self, async_session: AsyncSession):
        """Only the 30 most recent completed runs are used per model."""
        repo = RunRepository(async_session, tenant_id=_TENANT)
        base = datetime.now(UTC)

        # Insert 35 runs; the oldest 5 have cost=100, the recent 30 have cost=1.
        for i in range(35):
            cost = 100.0 if i < 5 else 1.0
            started = base - timedelta(seconds=3600 - i)
            finished = base - timedelta(seconds=3600 - i - 60)
            record = _make_run_record(
                model_name="analytics.capped",
                cost_usd=cost,
                started_at=started,
                finished_at=finished,
            )
            await repo.create_run(record)

        result = await repo.get_historical_stats_batch(["analytics.capped"])
        assert result["analytics.capped"]["run_count"] == 30
        # The 30 most recent all have cost=1.0
        assert result["analytics.capped"]["avg_cost_usd"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# BL-062: RunRepository.get_failure_rates_batch
# ---------------------------------------------------------------------------


class TestGetFailureRatesBatch:
    """Verify that get_failure_rates_batch computes failure rates correctly."""

    @pytest.mark.asyncio
    async def test_failure_rate_computed_correctly(self, async_session: AsyncSession):
        """Failure rate is failed / total across all runs for the model."""
        repo = RunRepository(async_session, tenant_id=_TENANT)
        model = "analytics.fr_test"

        for status in ("COMPLETED", "COMPLETED", "FAILED"):
            record = _make_run_record(model_name=model, status=status)
            await repo.create_run(record)

        result = await repo.get_failure_rates_batch([model])
        assert result[model] == pytest.approx(1 / 3)

    @pytest.mark.asyncio
    async def test_model_with_no_runs_gets_zero(self, async_session: AsyncSession):
        """Models with no runs get a 0.0 failure rate (not absent from result)."""
        repo = RunRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_failure_rates_batch(["absent.model"])
        assert result["absent.model"] == 0.0

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty(self, async_session: AsyncSession):
        repo = RunRepository(async_session, tenant_id=_TENANT)
        assert await repo.get_failure_rates_batch([]) == {}

    @pytest.mark.asyncio
    async def test_batch_multiple_models(self, async_session: AsyncSession):
        """Multiple models each get their own failure rate."""
        repo = RunRepository(async_session, tenant_id=_TENANT)

        for status in ("COMPLETED", "FAILED"):
            record = _make_run_record(model_name="m.a", status=status)
            await repo.create_run(record)

        record = _make_run_record(model_name="m.b", status="COMPLETED")
        await repo.create_run(record)

        result = await repo.get_failure_rates_batch(["m.a", "m.b"])
        assert result["m.a"] == pytest.approx(0.5)
        assert result["m.b"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# BL-062: WatermarkRepository.get_watermarks_batch
# ---------------------------------------------------------------------------


def _make_watermark_row(
    session,
    tenant_id: str,
    model_name: str,
    start: date,
    end: date,
) -> WatermarkTable:
    row = WatermarkTable(
        tenant_id=tenant_id,
        model_name=model_name,
        partition_start=start,
        partition_end=end,
        last_updated=datetime.now(UTC),
    )
    session.add(row)
    return row


class TestGetWatermarksBatch:
    """Verify that WatermarkRepository.get_watermarks_batch fetches all in one query."""

    @pytest.mark.asyncio
    async def test_batch_returns_latest_per_model(self, async_session: AsyncSession):
        """The most-recent watermark for each model is returned."""
        repo = WatermarkRepository(async_session, tenant_id=_TENANT)
        d1, d2, d3, d4 = date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 28)

        # Insert two watermarks for model-a (different timestamps).
        row_old = _make_watermark_row(async_session, _TENANT, "m.a", d1, d2)
        row_old.last_updated = datetime(2024, 1, 10, tzinfo=UTC)

        row_new = _make_watermark_row(async_session, _TENANT, "m.a", d3, d4)
        row_new.last_updated = datetime(2024, 2, 10, tzinfo=UTC)

        # One watermark for model-b.
        _make_watermark_row(async_session, _TENANT, "m.b", d1, d2)
        await async_session.flush()

        result = await repo.get_watermarks_batch(["m.a", "m.b"])

        assert set(result.keys()) == {"m.a", "m.b"}
        # m.a should give the newer partition range
        assert result["m.a"] == (d3, d4)

    @pytest.mark.asyncio
    async def test_batch_absent_model_not_included(self, async_session: AsyncSession):
        """Models without watermarks are absent from the result."""
        repo = WatermarkRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_watermarks_batch(["no.watermark"])
        assert "no.watermark" not in result

    @pytest.mark.asyncio
    async def test_batch_empty_input(self, async_session: AsyncSession):
        repo = WatermarkRepository(async_session, tenant_id=_TENANT)
        assert await repo.get_watermarks_batch([]) == {}

    @pytest.mark.asyncio
    async def test_batch_tenant_isolation(self, async_session: AsyncSession):
        """Watermarks from another tenant are excluded."""
        d1, d2 = date(2024, 1, 1), date(2024, 1, 31)
        _make_watermark_row(async_session, "other-tenant", "m.a", d1, d2)
        await async_session.flush()

        repo = WatermarkRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_watermarks_batch(["m.a"])
        assert "m.a" not in result


# ---------------------------------------------------------------------------
# BL-062: ModelRepository.get_models_batch
# ---------------------------------------------------------------------------


def _make_model_table_row(
    session,
    tenant_id: str,
    model_name: str,
    tags: str | None = None,
) -> ModelTable:
    row = ModelTable(
        model_name=model_name,
        tenant_id=tenant_id,
        repo_path=f"/repo/{model_name}.sql",
        current_version="abc123",
        kind="TABLE",
        materialization="incremental",
        tags=tags,
    )
    session.add(row)
    return row


class TestGetModelsBatch:
    """Verify that ModelRepository.get_models_batch fetches all in one query."""

    @pytest.mark.asyncio
    async def test_batch_returns_all_requested_models(self, async_session: AsyncSession):
        """All requested model names present in DB are returned."""
        repo = ModelRepository(async_session, tenant_id=_TENANT)
        _make_model_table_row(async_session, _TENANT, "m.alpha")
        _make_model_table_row(async_session, _TENANT, "m.beta")
        await async_session.flush()

        result = await repo.get_models_batch(["m.alpha", "m.beta"])

        assert set(result.keys()) == {"m.alpha", "m.beta"}
        assert result["m.alpha"].model_name == "m.alpha"
        assert result["m.beta"].model_name == "m.beta"

    @pytest.mark.asyncio
    async def test_batch_absent_model_not_included(self, async_session: AsyncSession):
        """Models not in the DB are absent from the result."""
        repo = ModelRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_models_batch(["not.exists"])
        assert "not.exists" not in result

    @pytest.mark.asyncio
    async def test_batch_empty_input(self, async_session: AsyncSession):
        repo = ModelRepository(async_session, tenant_id=_TENANT)
        assert await repo.get_models_batch([]) == {}

    @pytest.mark.asyncio
    async def test_batch_tenant_isolation(self, async_session: AsyncSession):
        """Models from another tenant are excluded."""
        _make_model_table_row(async_session, "other-tenant", "m.shared")
        await async_session.flush()

        repo = ModelRepository(async_session, tenant_id=_TENANT)
        result = await repo.get_models_batch(["m.shared"])
        assert "m.shared" not in result


# ---------------------------------------------------------------------------
# BL-063: PlanRepository.list_recent with SQL-level offset
# ---------------------------------------------------------------------------


def _make_plan_table_row(
    session,
    tenant_id: str,
    plan_id: str,
    plan_json: str = '{"plan_id": "x"}',
    base_sha: str = "aaa",
    target_sha: str = "bbb",
) -> PlanTable:
    row = PlanTable(
        plan_id=plan_id,
        tenant_id=tenant_id,
        base_sha=base_sha,
        target_sha=target_sha,
        plan_json=plan_json,
    )
    session.add(row)
    return row


class TestPlanRepositoryListRecentOffset:
    """Verify that PlanRepository.list_recent paginates at the SQL level."""

    @pytest.mark.asyncio
    async def test_list_recent_no_offset(self, async_session: AsyncSession):
        """Without offset, returns up to limit rows."""
        repo = PlanRepository(async_session, tenant_id=_TENANT)
        for i in range(5):
            _make_plan_table_row(async_session, _TENANT, f"plan-{i:02d}")
        await async_session.flush()

        result = await repo.list_recent(limit=3)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_list_recent_with_offset_skips_rows(self, async_session: AsyncSession):
        """With offset=2, the first two rows (most recent) are skipped."""
        repo = PlanRepository(async_session, tenant_id=_TENANT)
        # Insert 4 plans in order; they'll be returned newest-first by created_at.
        import asyncio as _asyncio

        for i in range(4):
            _make_plan_table_row(async_session, _TENANT, f"off-plan-{i:02d}")
            await async_session.flush()
            await _asyncio.sleep(0)  # allow clock to advance between flushes

        all_rows = await repo.list_recent(limit=10)
        assert len(all_rows) == 4

        # Offset 2 should skip the 2 most recent.
        offset_rows = await repo.list_recent(limit=10, offset=2)
        assert len(offset_rows) == 2
        for skipped in all_rows[:2]:
            assert all(r.plan_id != skipped.plan_id for r in offset_rows)

    @pytest.mark.asyncio
    async def test_list_recent_offset_beyond_total_returns_empty(self, async_session: AsyncSession):
        """An offset larger than the total row count returns an empty list."""
        repo = PlanRepository(async_session, tenant_id=_TENANT)
        _make_plan_table_row(async_session, _TENANT, "solo-plan")
        await async_session.flush()

        result = await repo.list_recent(limit=10, offset=100)
        assert result == []

    @pytest.mark.asyncio
    async def test_list_recent_tenant_isolation(self, async_session: AsyncSession):
        """Only plans belonging to the given tenant are returned."""
        repo_mine = PlanRepository(async_session, tenant_id=_TENANT)
        _make_plan_table_row(async_session, _TENANT, "mine-plan")
        _make_plan_table_row(async_session, "other-tenant", "other-plan")
        await async_session.flush()

        result = await repo_mine.list_recent(limit=10)
        assert len(result) == 1
        assert result[0].plan_id == "mine-plan"
