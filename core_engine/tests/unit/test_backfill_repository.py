"""Unit tests for BackfillCheckpointRepository and BackfillAuditRepository.

Uses an in-memory SQLite database via aiosqlite so tests run without
PostgreSQL.  JSONB columns are patched to plain JSON for SQLite
compatibility (same approach as test_repository.py).

Covers:
- BackfillCheckpointRepository:
    - create, get, update_progress, mark_completed, mark_failed
    - get_resumable (status filter), list_for_model
    - tenant isolation
- BackfillAuditRepository:
    - record_chunk, get_history, get_for_backfill
    - ordering guarantees
    - tenant isolation
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.types import TypeDecorator

from core_engine.state.repository import (
    BackfillAuditRepository,
    BackfillCheckpointRepository,
)
from core_engine.state.tables import Base

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _patch_columns_for_sqlite() -> None:
    """Substitute Postgres-specific column types for SQLite compatibility."""

    class _UTCAwareDateTime(TypeDecorator):
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


_patch_columns_for_sqlite()

_TENANT = "test-tenant"


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
# BackfillCheckpointRepository Tests
# ---------------------------------------------------------------------------


class TestBackfillCheckpointCreate:
    """Verify checkpoint creation and retrieval."""

    @pytest.mark.asyncio
    async def test_create_and_get(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        row = await repo.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 3, 31),
            chunk_size_days=7,
            total_chunks=13,
            cluster_size="medium",
            plan_id="plan-abc",
        )

        assert row.backfill_id == backfill_id
        assert row.model_name == "analytics.orders"
        assert row.overall_start == date(2024, 1, 1)
        assert row.overall_end == date(2024, 3, 31)
        assert row.chunk_size_days == 7
        assert row.status == "RUNNING"
        assert row.total_chunks == 13
        assert row.completed_chunks == 0
        assert row.completed_through is None
        assert row.cluster_size == "medium"
        assert row.plan_id == "plan-abc"
        assert row.error_message is None

        # Retrieve by ID.
        fetched = await repo.get(backfill_id)
        assert fetched is not None
        assert fetched.backfill_id == backfill_id

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        assert await repo.get("nonexistent-id") is None

    @pytest.mark.asyncio
    async def test_create_with_minimal_fields(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        row = await repo.create(
            backfill_id=backfill_id,
            model_name="staging.events",
            overall_start=date(2024, 6, 1),
            overall_end=date(2024, 6, 7),
            chunk_size_days=1,
            total_chunks=7,
        )

        assert row.cluster_size is None
        assert row.plan_id is None


class TestBackfillCheckpointUpdateProgress:
    """Verify checkpoint progress advancement."""

    @pytest.mark.asyncio
    async def test_update_progress(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        await repo.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
            chunk_size_days=7,
            total_chunks=3,
        )

        # Advance after first chunk.
        await repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 7),
            completed_chunks=1,
        )

        row = await repo.get(backfill_id)
        assert row is not None
        assert row.completed_through == date(2024, 1, 7)
        assert row.completed_chunks == 1
        assert row.status == "RUNNING"  # Status unchanged.

        # Advance after second chunk.
        await repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 14),
            completed_chunks=2,
        )

        row = await repo.get(backfill_id)
        assert row is not None
        assert row.completed_through == date(2024, 1, 14)
        assert row.completed_chunks == 2


class TestBackfillCheckpointMarkCompleted:
    """Verify marking a backfill as completed."""

    @pytest.mark.asyncio
    async def test_mark_completed(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        await repo.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )

        await repo.mark_completed(backfill_id)

        row = await repo.get(backfill_id)
        assert row is not None
        assert row.status == "COMPLETED"


class TestBackfillCheckpointMarkFailed:
    """Verify marking a backfill as failed with error recording."""

    @pytest.mark.asyncio
    async def test_mark_failed(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        await repo.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
            chunk_size_days=7,
            total_chunks=3,
        )

        # Advance one chunk then fail.
        await repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 7),
            completed_chunks=1,
        )

        await repo.mark_failed(backfill_id, "Connection timeout on chunk 2")

        row = await repo.get(backfill_id)
        assert row is not None
        assert row.status == "FAILED"
        assert row.error_message == "Connection timeout on chunk 2"
        assert row.completed_through == date(2024, 1, 7)
        assert row.completed_chunks == 1

    @pytest.mark.asyncio
    async def test_mark_failed_truncates_long_error(self, async_session: AsyncSession):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        await repo.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )

        long_error = "x" * 3000
        await repo.mark_failed(backfill_id, long_error)

        row = await repo.get(backfill_id)
        assert row is not None
        assert len(row.error_message) <= 2000


class TestBackfillCheckpointGetResumable:
    """Verify filtering for resumable (FAILED/RUNNING) checkpoints."""

    @pytest.mark.asyncio
    async def test_get_resumable_returns_failed_and_running(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)

        # Create checkpoints in various states.
        id_running = uuid4().hex
        id_failed = uuid4().hex
        id_completed = uuid4().hex

        for bid, model in [
            (id_running, "m.running"),
            (id_failed, "m.failed"),
            (id_completed, "m.completed"),
        ]:
            await repo.create(
                backfill_id=bid,
                model_name=model,
                overall_start=date(2024, 1, 1),
                overall_end=date(2024, 1, 7),
                chunk_size_days=7,
                total_chunks=1,
            )

        await repo.mark_failed(id_failed, "some error")
        await repo.mark_completed(id_completed)

        resumable = await repo.get_resumable()
        resumable_ids = {r.backfill_id for r in resumable}

        assert id_running in resumable_ids
        assert id_failed in resumable_ids
        assert id_completed not in resumable_ids

    @pytest.mark.asyncio
    async def test_get_resumable_filtered_by_model(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)

        id_a = uuid4().hex
        id_b = uuid4().hex

        await repo.create(
            backfill_id=id_a,
            model_name="model.alpha",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )
        await repo.create(
            backfill_id=id_b,
            model_name="model.beta",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )

        resumable_alpha = await repo.get_resumable(model_name="model.alpha")
        assert len(resumable_alpha) == 1
        assert resumable_alpha[0].backfill_id == id_a

        resumable_beta = await repo.get_resumable(model_name="model.beta")
        assert len(resumable_beta) == 1
        assert resumable_beta[0].backfill_id == id_b


class TestBackfillCheckpointListForModel:
    """Verify listing checkpoints for a model with ordering."""

    @pytest.mark.asyncio
    async def test_list_for_model_ordered_newest_first(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)

        ids = []
        for i in range(3):
            bid = uuid4().hex
            ids.append(bid)
            await repo.create(
                backfill_id=bid,
                model_name="analytics.events",
                overall_start=date(2024, 1 + i, 1),
                overall_end=date(2024, 1 + i, 28),
                chunk_size_days=7,
                total_chunks=4,
            )

        result = await repo.list_for_model("analytics.events")
        assert len(result) == 3
        # Newest first (created_at DESC).  Since they were created
        # sequentially the last-created has the latest timestamp.
        returned_ids = [r.backfill_id for r in result]
        assert returned_ids == list(reversed(ids))

    @pytest.mark.asyncio
    async def test_list_for_model_respects_limit(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)

        for _ in range(5):
            await repo.create(
                backfill_id=uuid4().hex,
                model_name="analytics.events",
                overall_start=date(2024, 1, 1),
                overall_end=date(2024, 1, 7),
                chunk_size_days=7,
                total_chunks=1,
            )

        result = await repo.list_for_model("analytics.events", limit=3)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_list_for_model_returns_empty_for_unknown(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        result = await repo.list_for_model("nonexistent.model")
        assert result == []


class TestBackfillCheckpointTenantIsolation:
    """Verify that checkpoint operations are scoped to the tenant."""

    @pytest.mark.asyncio
    async def test_tenant_isolation(self, async_session: AsyncSession):
        repo_a = BackfillCheckpointRepository(async_session, tenant_id="tenant-a")
        repo_b = BackfillCheckpointRepository(async_session, tenant_id="tenant-b")

        backfill_id = uuid4().hex

        await repo_a.create(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )

        # Tenant A can see it.
        assert await repo_a.get(backfill_id) is not None

        # Tenant B cannot.
        assert await repo_b.get(backfill_id) is None

    @pytest.mark.asyncio
    async def test_list_for_model_tenant_isolation(
        self,
        async_session: AsyncSession,
    ):
        repo_a = BackfillCheckpointRepository(async_session, tenant_id="tenant-a")
        repo_b = BackfillCheckpointRepository(async_session, tenant_id="tenant-b")

        await repo_a.create(
            backfill_id=uuid4().hex,
            model_name="shared.model",
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 7),
            chunk_size_days=7,
            total_chunks=1,
        )

        assert len(await repo_a.list_for_model("shared.model")) == 1
        assert len(await repo_b.list_for_model("shared.model")) == 0


# ---------------------------------------------------------------------------
# BackfillAuditRepository Tests
# ---------------------------------------------------------------------------


class TestBackfillAuditRecordChunk:
    """Verify recording chunk execution outcomes."""

    @pytest.mark.asyncio
    async def test_record_success_chunk(self, async_session: AsyncSession):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex
        run_id = uuid4().hex

        row = await repo.record_chunk(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            chunk_start=date(2024, 1, 1),
            chunk_end=date(2024, 1, 7),
            status="SUCCESS",
            run_id=run_id,
            duration_seconds=45.3,
        )

        assert row.backfill_id == backfill_id
        assert row.model_name == "analytics.orders"
        assert row.chunk_start == date(2024, 1, 1)
        assert row.chunk_end == date(2024, 1, 7)
        assert row.status == "SUCCESS"
        assert row.run_id == run_id
        assert row.error_message is None
        assert row.duration_seconds == pytest.approx(45.3)

    @pytest.mark.asyncio
    async def test_record_failed_chunk(self, async_session: AsyncSession):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex
        run_id = uuid4().hex

        row = await repo.record_chunk(
            backfill_id=backfill_id,
            model_name="analytics.orders",
            chunk_start=date(2024, 1, 8),
            chunk_end=date(2024, 1, 14),
            status="FAILED",
            run_id=run_id,
            error_message="Databricks cluster terminated",
            duration_seconds=120.0,
        )

        assert row.status == "FAILED"
        assert row.error_message == "Databricks cluster terminated"

    @pytest.mark.asyncio
    async def test_record_chunk_truncates_long_error(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)

        row = await repo.record_chunk(
            backfill_id=uuid4().hex,
            model_name="analytics.orders",
            chunk_start=date(2024, 1, 1),
            chunk_end=date(2024, 1, 7),
            status="FAILED",
            error_message="e" * 5000,
        )

        assert len(row.error_message) <= 2000


class TestBackfillAuditGetHistory:
    """Verify retrieving audit history for a model."""

    @pytest.mark.asyncio
    async def test_get_history_returns_newest_first(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        # Record three chunks sequentially.
        for day_offset in range(3):
            start = date(2024, 1, 1 + day_offset * 7)
            end = date(2024, 1, 7 + day_offset * 7)
            await repo.record_chunk(
                backfill_id=backfill_id,
                model_name="analytics.orders",
                chunk_start=start,
                chunk_end=end,
                status="SUCCESS",
                run_id=uuid4().hex,
                duration_seconds=30.0 + day_offset,
            )

        history = await repo.get_history("analytics.orders")
        assert len(history) == 3
        # Newest first (executed_at DESC).
        assert history[0].chunk_start >= history[-1].chunk_start

    @pytest.mark.asyncio
    async def test_get_history_respects_limit(self, async_session: AsyncSession):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex

        for day_offset in range(5):
            start = date(2024, 1, 1 + day_offset)
            await repo.record_chunk(
                backfill_id=backfill_id,
                model_name="analytics.orders",
                chunk_start=start,
                chunk_end=start,
                status="SUCCESS",
            )

        history = await repo.get_history("analytics.orders", limit=3)
        assert len(history) == 3

    @pytest.mark.asyncio
    async def test_get_history_returns_empty_for_unknown_model(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        history = await repo.get_history("nonexistent.model")
        assert history == []


class TestBackfillAuditGetForBackfill:
    """Verify retrieving audit records for a specific backfill."""

    @pytest.mark.asyncio
    async def test_get_for_backfill_ordered_by_chunk_start(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        backfill_id = uuid4().hex
        other_backfill_id = uuid4().hex

        # Record chunks in reverse order.
        for day_offset in [14, 7, 0]:
            start = date(2024, 1, 1) + __import__("datetime").timedelta(days=day_offset)
            end = start + __import__("datetime").timedelta(days=6)
            await repo.record_chunk(
                backfill_id=backfill_id,
                model_name="analytics.orders",
                chunk_start=start,
                chunk_end=end,
                status="SUCCESS",
                run_id=uuid4().hex,
            )

        # Record a chunk for a different backfill (should not be returned).
        await repo.record_chunk(
            backfill_id=other_backfill_id,
            model_name="analytics.orders",
            chunk_start=date(2024, 2, 1),
            chunk_end=date(2024, 2, 7),
            status="SUCCESS",
        )

        entries = await repo.get_for_backfill(backfill_id)
        assert len(entries) == 3

        # Ordered by chunk_start ASC.
        starts = [e.chunk_start for e in entries]
        assert starts == sorted(starts)

        # Does not include the other backfill's entry.
        for entry in entries:
            assert entry.backfill_id == backfill_id

    @pytest.mark.asyncio
    async def test_get_for_backfill_returns_empty_for_unknown(
        self,
        async_session: AsyncSession,
    ):
        repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)
        entries = await repo.get_for_backfill("nonexistent-backfill")
        assert entries == []


class TestBackfillAuditTenantIsolation:
    """Verify that audit operations are scoped to the tenant."""

    @pytest.mark.asyncio
    async def test_tenant_isolation_history(self, async_session: AsyncSession):
        repo_a = BackfillAuditRepository(async_session, tenant_id="tenant-a")
        repo_b = BackfillAuditRepository(async_session, tenant_id="tenant-b")

        backfill_id = uuid4().hex

        await repo_a.record_chunk(
            backfill_id=backfill_id,
            model_name="shared.model",
            chunk_start=date(2024, 1, 1),
            chunk_end=date(2024, 1, 7),
            status="SUCCESS",
        )

        # Tenant A sees the audit entry.
        assert len(await repo_a.get_history("shared.model")) == 1

        # Tenant B does not.
        assert len(await repo_b.get_history("shared.model")) == 0

    @pytest.mark.asyncio
    async def test_tenant_isolation_get_for_backfill(
        self,
        async_session: AsyncSession,
    ):
        repo_a = BackfillAuditRepository(async_session, tenant_id="tenant-a")
        repo_b = BackfillAuditRepository(async_session, tenant_id="tenant-b")

        backfill_id = uuid4().hex

        await repo_a.record_chunk(
            backfill_id=backfill_id,
            model_name="shared.model",
            chunk_start=date(2024, 1, 1),
            chunk_end=date(2024, 1, 7),
            status="SUCCESS",
        )

        assert len(await repo_a.get_for_backfill(backfill_id)) == 1
        assert len(await repo_b.get_for_backfill(backfill_id)) == 0


# ---------------------------------------------------------------------------
# Integration: Checkpoint + Audit workflow
# ---------------------------------------------------------------------------


class TestBackfillWorkflow:
    """End-to-end workflow: create checkpoint, record chunks, advance, fail, resume."""

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, async_session: AsyncSession):
        cp_repo = BackfillCheckpointRepository(async_session, tenant_id=_TENANT)
        audit_repo = BackfillAuditRepository(async_session, tenant_id=_TENANT)

        backfill_id = uuid4().hex
        model = "analytics.revenue"

        # 1. Create checkpoint: 3 chunks covering Jan 1-21.
        await cp_repo.create(
            backfill_id=backfill_id,
            model_name=model,
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
            chunk_size_days=7,
            total_chunks=3,
        )

        # 2. Chunk 1 succeeds (Jan 1-7).
        await audit_repo.record_chunk(
            backfill_id=backfill_id,
            model_name=model,
            chunk_start=date(2024, 1, 1),
            chunk_end=date(2024, 1, 7),
            status="SUCCESS",
            run_id="run-1",
            duration_seconds=30.0,
        )
        await cp_repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 7),
            completed_chunks=1,
        )

        # 3. Chunk 2 fails (Jan 8-14).
        await audit_repo.record_chunk(
            backfill_id=backfill_id,
            model_name=model,
            chunk_start=date(2024, 1, 8),
            chunk_end=date(2024, 1, 14),
            status="FAILED",
            run_id="run-2",
            error_message="OOM on worker node",
            duration_seconds=90.0,
        )
        await cp_repo.mark_failed(backfill_id, "OOM on worker node")

        # Verify checkpoint state after failure.
        cp = await cp_repo.get(backfill_id)
        assert cp is not None
        assert cp.status == "FAILED"
        assert cp.completed_through == date(2024, 1, 7)
        assert cp.completed_chunks == 1
        assert cp.error_message == "OOM on worker node"

        # Verify it appears in resumable list.
        resumable = await cp_repo.get_resumable(model_name=model)
        assert any(r.backfill_id == backfill_id for r in resumable)

        # Verify audit trail has 2 entries for this backfill.
        entries = await audit_repo.get_for_backfill(backfill_id)
        assert len(entries) == 2
        assert entries[0].status == "SUCCESS"
        assert entries[1].status == "FAILED"

        # 4. "Resume" — chunk 2 succeeds this time.
        await audit_repo.record_chunk(
            backfill_id=backfill_id,
            model_name=model,
            chunk_start=date(2024, 1, 8),
            chunk_end=date(2024, 1, 14),
            status="SUCCESS",
            run_id="run-2-retry",
            duration_seconds=60.0,
        )
        await cp_repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 14),
            completed_chunks=2,
        )

        # 5. Chunk 3 succeeds (Jan 15-21).
        await audit_repo.record_chunk(
            backfill_id=backfill_id,
            model_name=model,
            chunk_start=date(2024, 1, 15),
            chunk_end=date(2024, 1, 21),
            status="SUCCESS",
            run_id="run-3",
            duration_seconds=35.0,
        )
        await cp_repo.update_progress(
            backfill_id=backfill_id,
            completed_through=date(2024, 1, 21),
            completed_chunks=3,
        )
        await cp_repo.mark_completed(backfill_id)

        # 6. Verify final state.
        cp = await cp_repo.get(backfill_id)
        assert cp is not None
        assert cp.status == "COMPLETED"
        assert cp.completed_through == date(2024, 1, 21)
        assert cp.completed_chunks == 3

        # No longer resumable.
        resumable = await cp_repo.get_resumable(model_name=model)
        assert not any(r.backfill_id == backfill_id for r in resumable)

        # Full audit trail: 4 entries (1 success + 1 fail + 1 retry success + 1 success).
        all_entries = await audit_repo.get_for_backfill(backfill_id)
        assert len(all_entries) == 4

        # History for the model shows all entries.
        history = await audit_repo.get_history(model)
        assert len(history) == 4
