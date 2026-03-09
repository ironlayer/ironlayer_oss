"""Unit tests for AuditRepository hash-chaining and query logic.

These tests use an in-memory SQLite database via aiosqlite so they can
run without a PostgreSQL instance.  Because JSONB is Postgres-specific,
the test fixtures substitute a plain JSON column type via event listeners.

Covers:
- Writing and reading audit entries
- Hash chain integrity verification (valid chain)
- Hash chain verification detecting tamper
- Query filters (by action, entity_type, since)
- Pagination (limit / offset)
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import pytest_asyncio
from core_engine.state.repository import AuditRepository
from core_engine.state.tables import Base
from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.types import TypeDecorator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _patch_columns_for_sqlite() -> None:
    """Substitute Postgres-specific column types for SQLite compatibility.

    * ``JSONB`` → ``JSON`` (SQLite has no JSONB type compiler).
    * ``DateTime(timezone=True)`` → ``DateTime()`` with an explicit
      :class:`~sqlalchemy.TypeDecorator` that coerces naive datetimes
      returned by SQLite back to UTC-aware, keeping hash computation
      deterministic across backends.
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
# Test cases
# ---------------------------------------------------------------------------


class TestAuditRepositoryWriteAndRead:
    """Write and read audit entries."""

    @pytest.mark.asyncio
    async def test_log_creates_entry_and_returns_id(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        entry_id = await repo.log(
            actor="alice@example.com",
            action="PLAN_CREATED",
            entity_type="plan",
            entity_id="plan-abc",
            metadata={"base_sha": "aaa", "target_sha": "bbb"},
        )
        assert isinstance(entry_id, str)
        assert len(entry_id) == 32  # uuid4 hex

        entries = await repo.query()
        assert len(entries) == 1
        assert entries[0].id == entry_id
        assert entries[0].actor == "alice@example.com"
        assert entries[0].action == "PLAN_CREATED"
        assert entries[0].entity_type == "plan"
        assert entries[0].entity_id == "plan-abc"
        assert entries[0].entry_hash is not None
        assert entries[0].previous_hash is None  # first entry

    @pytest.mark.asyncio
    async def test_second_entry_chains_to_first(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        await repo.log(actor="alice", action="A1")
        await repo.log(actor="alice", action="A2")

        entries = await repo.query()
        assert len(entries) == 2
        # Most recent first (A2 then A1).
        second, first = entries[0], entries[1]
        assert first.previous_hash is None
        assert second.previous_hash == first.entry_hash

    @pytest.mark.asyncio
    async def test_get_latest_hash(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        assert await repo.get_latest_hash() is None

        await repo.log(actor="bob", action="TEST")
        latest = await repo.get_latest_hash()
        assert latest is not None
        assert len(latest) == 64  # SHA-256 hex


class TestAuditChainVerification:
    """Hash chain integrity verification."""

    @pytest.mark.asyncio
    async def test_valid_chain_passes(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(5):
            await repo.log(actor="alice", action=f"ACTION_{i}")

        is_valid, count = await repo.verify_chain()
        assert is_valid is True
        assert count == 5

    @pytest.mark.asyncio
    async def test_empty_chain_passes(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        is_valid, count = await repo.verify_chain()
        assert is_valid is True
        assert count == 0

    @pytest.mark.asyncio
    async def test_tampered_entry_hash_detected(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(3):
            await repo.log(actor="alice", action=f"ACTION_{i}")

        # Tamper with the second entry's hash.
        entries = await repo.query()
        # entries are most-recent-first; we want the middle one in chain order.
        # Chain order is oldest first, so entries[2] is oldest, entries[0] is newest.
        middle_entry = entries[1]
        middle_entry.entry_hash = "0" * 64
        await async_session.flush()

        is_valid, count = await repo.verify_chain()
        assert is_valid is False
        # Should detect the break at the tampered entry.
        assert count < 3

    @pytest.mark.asyncio
    async def test_tampered_previous_hash_detected(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(3):
            await repo.log(actor="alice", action=f"ACTION_{i}")

        # Tamper with the last entry's previous_hash.
        entries = await repo.query()
        newest = entries[0]
        newest.previous_hash = "f" * 64
        await async_session.flush()

        is_valid, count = await repo.verify_chain()
        assert is_valid is False


class TestAuditQueryFilters:
    """Query filters: by action, entity_type, since."""

    @pytest.mark.asyncio
    async def test_filter_by_action(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        await repo.log(actor="a", action="PLAN_CREATED")
        await repo.log(actor="a", action="PLAN_APPROVED")
        await repo.log(actor="a", action="PLAN_CREATED")

        results = await repo.query(action="PLAN_CREATED")
        assert len(results) == 2
        assert all(r.action == "PLAN_CREATED" for r in results)

    @pytest.mark.asyncio
    async def test_filter_by_entity_type(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        await repo.log(actor="a", action="A1", entity_type="plan")
        await repo.log(actor="a", action="A2", entity_type="model")
        await repo.log(actor="a", action="A3", entity_type="plan")

        results = await repo.query(entity_type="plan")
        assert len(results) == 2
        assert all(r.entity_type == "plan" for r in results)

    @pytest.mark.asyncio
    async def test_filter_by_since(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        await repo.log(actor="a", action="OLD")
        await repo.log(actor="a", action="NEW")

        entries = await repo.query()
        assert len(entries) == 2

        # Use the timestamp of the second (newer) entry as the since cutoff.
        # Since entries are returned newest-first, entries[0] is the newest.
        cutoff = entries[0].created_at
        results = await repo.query(since=cutoff)
        assert len(results) >= 1
        assert all(r.created_at >= cutoff for r in results)

    @pytest.mark.asyncio
    async def test_filter_by_entity_id(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        await repo.log(actor="a", action="A1", entity_type="plan", entity_id="p1")
        await repo.log(actor="a", action="A2", entity_type="plan", entity_id="p2")
        await repo.log(actor="a", action="A3", entity_type="plan", entity_id="p1")

        results = await repo.query(entity_id="p1")
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_tenant_isolation(self, async_session: AsyncSession):
        repo_t1 = AuditRepository(async_session, tenant_id="t1")
        repo_t2 = AuditRepository(async_session, tenant_id="t2")
        await repo_t1.log(actor="a", action="A1")
        await repo_t2.log(actor="b", action="A2")

        t1_entries = await repo_t1.query()
        t2_entries = await repo_t2.query()
        assert len(t1_entries) == 1
        assert len(t2_entries) == 1
        assert t1_entries[0].actor == "a"
        assert t2_entries[0].actor == "b"


class TestAuditPagination:
    """Pagination with limit and offset."""

    @pytest.mark.asyncio
    async def test_limit(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(10):
            await repo.log(actor="a", action=f"A{i}")

        results = await repo.query(limit=3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_offset(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(5):
            await repo.log(actor="a", action=f"A{i}")

        all_entries = await repo.query(limit=100)
        offset_entries = await repo.query(limit=100, offset=2)
        assert len(offset_entries) == len(all_entries) - 2

    @pytest.mark.asyncio
    async def test_limit_and_offset_combined(self, async_session: AsyncSession):
        repo = AuditRepository(async_session, tenant_id="t1")
        for i in range(10):
            await repo.log(actor="a", action=f"A{i}")

        page1 = await repo.query(limit=3, offset=0)
        page2 = await repo.query(limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        # No overlap between pages.
        page1_ids = {e.id for e in page1}
        page2_ids = {e.id for e in page2}
        assert page1_ids.isdisjoint(page2_ids)


class TestAuditHashComputation:
    """Verify the static hash computation is deterministic."""

    def test_deterministic_hash(self):
        now = datetime(2025, 1, 1, tzinfo=UTC)
        h1 = AuditRepository._compute_hash(
            tenant_id="t1",
            actor="alice",
            action="TEST",
            entity_type="plan",
            entity_id="p1",
            metadata={"key": "value"},
            previous_hash=None,
            created_at=now,
        )
        h2 = AuditRepository._compute_hash(
            tenant_id="t1",
            actor="alice",
            action="TEST",
            entity_type="plan",
            entity_id="p1",
            metadata={"key": "value"},
            previous_hash=None,
            created_at=now,
        )
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex length

    def test_different_inputs_different_hash(self):
        now = datetime(2025, 1, 1, tzinfo=UTC)
        h1 = AuditRepository._compute_hash(
            tenant_id="t1",
            actor="alice",
            action="A1",
            entity_type=None,
            entity_id=None,
            metadata=None,
            previous_hash=None,
            created_at=now,
        )
        h2 = AuditRepository._compute_hash(
            tenant_id="t1",
            actor="alice",
            action="A2",
            entity_type=None,
            entity_id=None,
            metadata=None,
            previous_hash=None,
            created_at=now,
        )
        assert h1 != h2

    def test_none_fields_handled(self):
        now = datetime(2025, 1, 1, tzinfo=UTC)
        h = AuditRepository._compute_hash(
            tenant_id="t1",
            actor="alice",
            action="TEST",
            entity_type=None,
            entity_id=None,
            metadata=None,
            previous_hash=None,
            created_at=now,
        )
        assert isinstance(h, str)
        assert len(h) == 64
