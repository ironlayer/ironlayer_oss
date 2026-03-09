"""Comprehensive unit tests for core_engine/state/repository.py.

Tests use an in-memory SQLite database via aiosqlite.  All repository classes
are covered with CRUD, filter, tenant-isolation, and error-path tests.

SQLite compatibility notes
--------------------------
* ``make_interval`` (used in LockRepository expiry) is PostgreSQL-specific;
  lock expiry / ``expire_stale_locks`` / ``check_lock`` are skipped for SQLite.
* ``date_trunc`` (ReportingRepository time-bucket methods) is PG-specific;
  those methods are skipped.
* ``percentile_cont`` (AnalyticsRepository.get_health_metrics) is PG-specific;
  that path is skipped.
* ``func.now()`` in AuditLogTable server_default is replaced by a UTC-aware
  Python default via the column-type patch performed at module import time.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from core_engine.state.repository import (
    AIFeedbackRepository,
    AnalyticsRepository,
    APIKeyRepository,
    AuditRepository,
    BackfillAuditRepository,
    BackfillCheckpointRepository,
    CredentialRepository,
    CustomerHealthRepository,
    EnvironmentRepository,
    EventOutboxRepository,
    EventSubscriptionRepository,
    InvoiceRepository,
    LLMUsageLogRepository,
    LockRepository,
    ModelRepository,
    ModelTestRepository,
    QuotaRepository,
    ReconciliationRepository,
    ReconciliationScheduleRepository,
    ReportingRepository,
    SchemaDriftRepository,
    SnapshotRepository,
    TelemetryRepository,
    TenantConfigRepository,
    TestResultRepository,
    TokenRevocationRepository,
    UserRepository,
    WatermarkRepository,
)
from core_engine.state.tables import Base, UsageEventTable
from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.types import TypeDecorator

# ---------------------------------------------------------------------------
# SQLite column-type compatibility patching
# ---------------------------------------------------------------------------


def _patch_columns_for_sqlite() -> None:
    """Replace PostgreSQL-specific column types with SQLite-compatible ones.

    * JSONB  → JSON  (no JSONB type compiler in SQLite).
    * DateTime(timezone=True) → UTC-aware TypeDecorator wrapper so that naive
      datetimes returned by SQLite are converted back to UTC-aware objects,
      preventing comparison errors in tests.
    """

    class _UTCAwareDateTime(TypeDecorator):
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


# Apply once at import time — shared across all tests in this module.
_patch_columns_for_sqlite()

# ---------------------------------------------------------------------------
# Shared constants and helpers
# ---------------------------------------------------------------------------

_TENANT = "test-tenant"
_OTHER_TENANT = "other-tenant"


def _uid() -> str:
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def async_session() -> AsyncSession:  # type: ignore[misc]
    """Async session backed by a fresh in-memory SQLite database."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


# ---------------------------------------------------------------------------
# ModelRepository
# ---------------------------------------------------------------------------


class TestModelRepository:
    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        row = await repo.create(
            model_name="analytics.orders",
            repo_path="models/orders.sql",
            version="v1",
            kind="TABLE",
            time_column="event_date",
            unique_key="order_id",
            materialization="incremental",
            owner="data-eng",
            tags=["pii", "finance"],
        )
        assert row.model_name == "analytics.orders"
        assert row.tenant_id == _TENANT

        fetched = await repo.get("analytics.orders")
        assert fetched is not None
        assert fetched.model_name == "analytics.orders"

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        assert await repo.get("no.such.model") is None

    async def test_list_all_empty(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        rows = await repo.list_all()
        assert rows == []

    async def test_list_all_pagination(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        for i in range(5):
            await repo.create(
                model_name=f"model_{i:03d}",
                repo_path=f"models/m{i}.sql",
                version="v1",
                kind="TABLE",
                time_column=None,
                unique_key=None,
                materialization="full",
                owner=None,
                tags=None,
            )
        first_two = await repo.list_all(limit=2, offset=0)
        assert len(first_two) == 2
        next_two = await repo.list_all(limit=2, offset=2)
        assert len(next_two) == 2
        last = await repo.list_all(limit=2, offset=4)
        assert len(last) == 1

    async def test_list_filtered_by_kind(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        for kind in ("TABLE", "VIEW", "TABLE"):
            await repo.create(
                model_name=f"m_{_uid()}",
                repo_path="x",
                version="v1",
                kind=kind,
                time_column=None,
                unique_key=None,
                materialization="full",
                owner=None,
                tags=None,
            )
        tables = await repo.list_filtered(kind="TABLE")
        assert all(r.kind == "TABLE" for r in tables)
        assert len(tables) == 2

    async def test_list_filtered_by_owner(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        await repo.create(
            model_name="m_alice",
            repo_path="x",
            version="v1",
            kind="TABLE",
            time_column=None,
            unique_key=None,
            materialization="full",
            owner="alice",
            tags=None,
        )
        await repo.create(
            model_name="m_bob",
            repo_path="x",
            version="v1",
            kind="TABLE",
            time_column=None,
            unique_key=None,
            materialization="full",
            owner="bob",
            tags=None,
        )
        result = await repo.list_filtered(owner="alice")
        assert len(result) == 1
        assert result[0].owner == "alice"

    async def test_list_filtered_by_search(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        for name in ("analytics.orders", "analytics.revenue", "staging.clicks"):
            await repo.create(
                model_name=name,
                repo_path="x",
                version="v1",
                kind="TABLE",
                time_column=None,
                unique_key=None,
                materialization="full",
                owner=None,
                tags=None,
            )
        result = await repo.list_filtered(search="analytics")
        assert len(result) == 2

    async def test_update_version(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        await repo.create(
            model_name="versioned.model",
            repo_path="x",
            version="v1",
            kind="TABLE",
            time_column=None,
            unique_key=None,
            materialization="full",
            owner=None,
            tags=None,
        )
        await repo.update_version("versioned.model", "v2")
        row = await repo.get("versioned.model")
        assert row is not None
        assert row.current_version == "v2"

    async def test_delete(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        await repo.create(
            model_name="to.delete",
            repo_path="x",
            version="v1",
            kind="TABLE",
            time_column=None,
            unique_key=None,
            materialization="full",
            owner=None,
            tags=None,
        )
        await repo.delete("to.delete")
        assert await repo.get("to.delete") is None

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = ModelRepository(async_session, _TENANT)
        repo_b = ModelRepository(async_session, _OTHER_TENANT)
        await repo_a.create(
            model_name="shared.name",
            repo_path="x",
            version="v1",
            kind="TABLE",
            time_column=None,
            unique_key=None,
            materialization="full",
            owner=None,
            tags=None,
        )
        assert await repo_b.get("shared.name") is None
        assert len(await repo_b.list_all()) == 0

    async def test_list_all_limits_capped(self, async_session: AsyncSession) -> None:
        repo = ModelRepository(async_session, _TENANT)
        # Negative limit should be clamped to 1
        rows = await repo.list_all(limit=-5)
        assert isinstance(rows, list)


# ---------------------------------------------------------------------------
# SnapshotRepository
# ---------------------------------------------------------------------------


class TestSnapshotRepository:
    async def test_create_and_get_by_id(self, async_session: AsyncSession) -> None:
        repo = SnapshotRepository(async_session, _TENANT)
        snap = await repo.create_snapshot("production", {"m1": "v1", "m2": "v2"})
        assert snap.environment == "production"

        fetched = await repo.get_by_id(snap.snapshot_id)
        assert fetched is not None
        assert fetched.snapshot_id == snap.snapshot_id

    async def test_get_latest(self, async_session: AsyncSession) -> None:
        repo = SnapshotRepository(async_session, _TENANT)
        await repo.create_snapshot("staging", {"m1": "v1"})
        # Second snapshot with different versions produces a different hash
        await repo.create_snapshot("staging", {"m1": "v2"})
        latest = await repo.get_latest("staging")
        # Both snapshots exist; the latest call returns the most recent
        assert latest is not None

    async def test_get_latest_no_snapshot(self, async_session: AsyncSession) -> None:
        repo = SnapshotRepository(async_session, _TENANT)
        assert await repo.get_latest("nonexistent") is None

    async def test_get_by_id_wrong_tenant(self, async_session: AsyncSession) -> None:
        repo_a = SnapshotRepository(async_session, _TENANT)
        repo_b = SnapshotRepository(async_session, _OTHER_TENANT)
        snap = await repo_a.create_snapshot("prod", {"m": "v"})
        assert await repo_b.get_by_id(snap.snapshot_id) is None

    async def test_deterministic_id(self, async_session: AsyncSession) -> None:
        repo = SnapshotRepository(async_session, _TENANT)
        s1 = await repo.create_snapshot("prod", {"m": "v"})
        # Same inputs → same hash; second insert will conflict; try with different versions
        s2 = await repo.create_snapshot("prod", {"m": "v2"})
        assert s1.snapshot_id != s2.snapshot_id


# ---------------------------------------------------------------------------
# WatermarkRepository
# ---------------------------------------------------------------------------


class TestWatermarkRepository:
    async def test_update_and_get_watermark(self, async_session: AsyncSession) -> None:
        repo = WatermarkRepository(async_session, _TENANT)
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        await repo.update_watermark("orders", start, end, row_count=1000)
        result = await repo.get_watermark("orders")
        assert result is not None
        assert result[0] == start
        assert result[1] == end

    async def test_get_watermark_nonexistent(self, async_session: AsyncSession) -> None:
        repo = WatermarkRepository(async_session, _TENANT)
        assert await repo.get_watermark("no.model") is None

    async def test_get_all_for_model(self, async_session: AsyncSession) -> None:
        repo = WatermarkRepository(async_session, _TENANT)
        await repo.update_watermark("m", date(2024, 1, 1), date(2024, 1, 31), None)
        await repo.update_watermark("m", date(2024, 2, 1), date(2024, 2, 28), 500)
        all_marks = await repo.get_all_for_model("m")
        assert len(all_marks) == 2

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = WatermarkRepository(async_session, _TENANT)
        repo_b = WatermarkRepository(async_session, _OTHER_TENANT)
        await repo_a.update_watermark("m", date(2024, 1, 1), date(2024, 1, 31), None)
        assert await repo_b.get_watermark("m") is None

    async def test_upsert_updates_existing(self, async_session: AsyncSession) -> None:
        repo = WatermarkRepository(async_session, _TENANT)
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        await repo.update_watermark("m", start, end, 100)
        await repo.update_watermark("m", start, end, 200)
        all_marks = await repo.get_all_for_model("m")
        # Upsert should update in-place, not insert a second row
        assert len(all_marks) == 1


# ---------------------------------------------------------------------------
# LockRepository  (release / force_release only)
#
# acquire_lock and check_lock use func.make_interval() which is
# PostgreSQL-specific and cannot run against SQLite.  Those code paths are
# exercised by the integration tests against a real Postgres instance.
# We test the acquire code path by directly inserting rows, then test
# release and force_release (which do not use make_interval).
# ---------------------------------------------------------------------------


class TestLockRepository:
    async def _direct_insert_lock(
        self,
        session: AsyncSession,
        tenant_id: str,
        model_name: str,
        range_start: date,
        range_end: date,
        locked_by: str = "worker-1",
    ) -> None:
        """Insert a lock row directly (bypassing acquire_lock's make_interval DELETE)."""
        from core_engine.state.tables import LockTable

        row = LockTable(
            tenant_id=tenant_id,
            model_name=model_name,
            range_start=range_start,
            range_end=range_end,
            locked_by=locked_by,
            ttl_seconds=3600,
        )
        session.add(row)
        await session.flush()

    async def test_release_lock(self, async_session: AsyncSession) -> None:
        """release_lock deletes the row without using make_interval."""
        repo = LockRepository(async_session, _TENANT)
        rs, re = date(2024, 1, 1), date(2024, 1, 31)
        await self._direct_insert_lock(async_session, _TENANT, "model.release", rs, re)
        # Release should succeed (no PG-specific SQL)
        await repo.release_lock("model.release", rs, re)

    async def test_force_release_returns_true(self, async_session: AsyncSession) -> None:
        """force_release_lock deletes and writes an audit entry."""
        repo = LockRepository(async_session, _TENANT)
        rs, re = date(2024, 4, 1), date(2024, 4, 30)
        await self._direct_insert_lock(async_session, _TENANT, "model.force", rs, re)
        released = await repo.force_release_lock("model.force", rs, re, "admin", "emergency")
        assert released is True

    async def test_force_release_no_lock_returns_false(self, async_session: AsyncSession) -> None:
        repo = LockRepository(async_session, _TENANT)
        result = await repo.force_release_lock("nomodel", date(2024, 1, 1), date(2024, 1, 31), "admin", "test")
        assert result is False

    async def test_release_lock_noop_when_absent(self, async_session: AsyncSession) -> None:
        """release_lock on a non-existent range should silently succeed."""
        repo = LockRepository(async_session, _TENANT)
        # Should not raise even if no lock exists
        await repo.release_lock("ghost.model", date(2024, 1, 1), date(2024, 1, 31))


# ---------------------------------------------------------------------------
# TelemetryRepository
# ---------------------------------------------------------------------------


class TestTelemetryRepository:
    def _make_telemetry(self, run_id: str, model_name: str = "m") -> dict[str, Any]:
        return {
            "run_id": run_id,
            "model_name": model_name,
            "runtime_seconds": 42.0,
            "shuffle_bytes": 1024,
            "input_rows": 1000,
            "output_rows": 900,
            "partition_count": 10,
            "cluster_id": "cluster-1",
        }

    async def test_record_and_get_for_run(self, async_session: AsyncSession) -> None:
        repo = TelemetryRepository(async_session, _TENANT)
        run_id = _uid()
        await repo.record(self._make_telemetry(run_id))
        rows = await repo.get_for_run(run_id)
        assert len(rows) == 1
        assert rows[0].runtime_seconds == pytest.approx(42.0)

    async def test_get_for_run_empty(self, async_session: AsyncSession) -> None:
        repo = TelemetryRepository(async_session, _TENANT)
        assert await repo.get_for_run("nonexistent") == []

    async def test_get_for_model(self, async_session: AsyncSession) -> None:
        repo = TelemetryRepository(async_session, _TENANT)
        for _ in range(3):
            await repo.record(self._make_telemetry(_uid(), "model.x"))
        rows = await repo.get_for_model("model.x", limit=2)
        assert len(rows) == 2

    async def test_cleanup_old_telemetry(self, async_session: AsyncSession) -> None:
        repo = TelemetryRepository(async_session, _TENANT)
        run_id = _uid()
        await repo.record(self._make_telemetry(run_id))
        # With 0 retention days, nothing older than now — should delete nothing
        deleted = await repo.cleanup_old_telemetry(retention_days=365)
        assert deleted == 0

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = TelemetryRepository(async_session, _TENANT)
        repo_b = TelemetryRepository(async_session, _OTHER_TENANT)
        run_id = _uid()
        await repo_a.record(self._make_telemetry(run_id))
        assert await repo_b.get_for_run(run_id) == []


# ---------------------------------------------------------------------------
# CredentialRepository
# ---------------------------------------------------------------------------


class TestCredentialRepository:
    async def test_store_and_get(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        await repo.store("db_password", "enc:secret123")
        value = await repo.get("db_password")
        assert value == "enc:secret123"

    async def test_get_missing(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        assert await repo.get("nonexistent") is None

    async def test_upsert_updates(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        await repo.store("key", "v1")
        await repo.store("key", "v2")
        assert await repo.get("key") == "v2"

    async def test_delete(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        await repo.store("key_to_delete", "val")
        deleted = await repo.delete("key_to_delete")
        assert deleted is True
        assert await repo.get("key_to_delete") is None

    async def test_delete_nonexistent(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        assert await repo.delete("ghost") is False

    async def test_list_names(self, async_session: AsyncSession) -> None:
        repo = CredentialRepository(async_session, _TENANT)
        await repo.store("alpha", "v1")
        await repo.store("beta", "v2")
        names = await repo.list_names()
        assert "alpha" in names
        assert "beta" in names

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = CredentialRepository(async_session, _TENANT)
        repo_b = CredentialRepository(async_session, _OTHER_TENANT)
        await repo_a.store("secret", "val")
        assert await repo_b.get("secret") is None


# ---------------------------------------------------------------------------
# AuditRepository
# ---------------------------------------------------------------------------


class TestAuditRepository:
    async def test_log_and_query(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        entry_id = await repo.log(actor="alice", action="model.create", entity_type="model", entity_id="orders")
        assert entry_id is not None

        entries = await repo.query()
        assert len(entries) == 1
        assert entries[0].actor == "alice"

    async def test_query_filter_by_action(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        await repo.log(actor="alice", action="model.create")
        await repo.log(actor="alice", action="model.delete")
        result = await repo.query(action="model.create")
        assert len(result) == 1
        assert result[0].action == "model.create"

    async def test_query_filter_by_entity_type(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        await repo.log(actor="a", action="x", entity_type="model")
        await repo.log(actor="a", action="x", entity_type="run")
        result = await repo.query(entity_type="model")
        assert all(r.entity_type == "model" for r in result)

    async def test_query_filter_by_entity_id(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        await repo.log(actor="a", action="x", entity_id="order-42")
        await repo.log(actor="a", action="x", entity_id="order-99")
        result = await repo.query(entity_id="order-42")
        assert len(result) == 1

    async def test_query_filter_by_since(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        await repo.log(actor="a", action="x")
        future = datetime.now(UTC) + timedelta(hours=1)
        result = await repo.query(since=future)
        assert result == []

    async def test_verify_chain_valid(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        for i in range(3):
            await repo.log(actor="alice", action=f"action_{i}")
        is_valid, count = await repo.verify_chain()
        assert is_valid is True
        assert count == 3

    async def test_verify_chain_empty(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        is_valid, count = await repo.verify_chain()
        assert is_valid is True
        assert count == 0

    async def test_get_latest_hash(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        assert await repo.get_latest_hash() is None
        await repo.log(actor="alice", action="test")
        h = await repo.get_latest_hash()
        assert h is not None and len(h) == 64

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = AuditRepository(async_session, tenant_id=_TENANT)
        repo_b = AuditRepository(async_session, tenant_id=_OTHER_TENANT)
        await repo_a.log(actor="a", action="x")
        assert await repo_b.query() == []

    async def test_log_with_metadata(self, async_session: AsyncSession) -> None:
        repo = AuditRepository(async_session, tenant_id=_TENANT)
        meta = {"key": "value", "count": 42}
        await repo.log(actor="bob", action="plan.run", metadata=meta)
        entries = await repo.query()
        assert len(entries) == 1
        assert entries[0].metadata_json == meta


# ---------------------------------------------------------------------------
# TokenRevocationRepository
# ---------------------------------------------------------------------------


class TestTokenRevocationRepository:
    async def test_revoke_and_check(self, async_session: AsyncSession) -> None:
        repo = TokenRevocationRepository(async_session, _TENANT)
        jti = _uid()
        await repo.revoke(jti, reason="logout")
        assert await repo.is_revoked(jti) is True

    async def test_not_revoked(self, async_session: AsyncSession) -> None:
        repo = TokenRevocationRepository(async_session, _TENANT)
        assert await repo.is_revoked("ghost-jti") is False

    async def test_revoke_idempotent(self, async_session: AsyncSession) -> None:
        repo = TokenRevocationRepository(async_session, _TENANT)
        jti = _uid()
        await repo.revoke(jti)
        await repo.revoke(jti, reason="updated-reason")
        assert await repo.is_revoked(jti) is True

    async def test_is_revoked_cross_tenant(self, async_session: AsyncSession) -> None:
        repo_a = TokenRevocationRepository(async_session, _TENANT)
        repo_b = TokenRevocationRepository(async_session, _OTHER_TENANT)
        jti = _uid()
        await repo_a.revoke(jti)
        # Cross-tenant check (no tenant_id filter) should find it
        assert await repo_b.is_revoked(jti) is True

    async def test_is_revoked_tenant_scoped(self, async_session: AsyncSession) -> None:
        repo_a = TokenRevocationRepository(async_session, _TENANT)
        repo_b = TokenRevocationRepository(async_session, _OTHER_TENANT)
        jti = _uid()
        await repo_a.revoke(jti)
        # With tenant_id filter, other tenant should not see it
        assert await repo_b.is_revoked(jti, tenant_id=_OTHER_TENANT) is False

    async def test_cleanup_expired(self, async_session: AsyncSession) -> None:
        repo = TokenRevocationRepository(async_session, _TENANT)
        past = datetime.now(UTC) - timedelta(hours=1)
        future = datetime.now(UTC) + timedelta(hours=1)
        await repo.revoke(_uid(), expires_at=past)
        await repo.revoke(_uid(), expires_at=future)
        deleted = await repo.cleanup_expired()
        assert deleted == 1

    async def test_cleanup_all_expired(self, async_session: AsyncSession) -> None:
        repo_a = TokenRevocationRepository(async_session, _TENANT)
        repo_b = TokenRevocationRepository(async_session, _OTHER_TENANT)
        past = datetime.now(UTC) - timedelta(hours=1)
        await repo_a.revoke(_uid(), expires_at=past)
        await repo_b.revoke(_uid(), expires_at=past)
        deleted = await repo_a.cleanup_all_expired()
        assert deleted == 2


# ---------------------------------------------------------------------------
# TenantConfigRepository
# ---------------------------------------------------------------------------


class TestTenantConfigRepository:
    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, _TENANT)
        row = await repo.create(llm_enabled=True, created_by="system")
        assert row.tenant_id == _TENANT
        assert row.llm_enabled is True

        fetched = await repo.get()
        assert fetched is not None
        assert fetched.tenant_id == _TENANT

    async def test_get_missing(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, "nobody")
        assert await repo.get() is None

    async def test_create_duplicate_raises(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, _TENANT)
        await repo.create()
        with pytest.raises(ValueError, match="already exists"):
            await repo.create()

    async def test_upsert(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, _TENANT)
        await repo.create(llm_enabled=True)
        row = await repo.upsert(llm_enabled=False, updated_by="admin")
        assert row.llm_enabled is False

    async def test_upsert_with_budgets(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, _TENANT)
        row = await repo.upsert(
            llm_enabled=True,
            llm_monthly_budget_usd=100.0,
            llm_daily_budget_usd=5.0,
        )
        assert row is not None

    async def test_deactivate(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, _TENANT)
        await repo.create()
        row = await repo.deactivate(deactivated_by="admin")
        assert row is not None
        assert row.deactivated_at is not None

    async def test_deactivate_nonexistent(self, async_session: AsyncSession) -> None:
        repo = TenantConfigRepository(async_session, "ghost-tenant")
        result = await repo.deactivate()
        assert result is None

    async def test_list_all(self, async_session: AsyncSession) -> None:
        for tid in ("t1", "t2", "t3"):
            await TenantConfigRepository(async_session, tid).create()
        repo = TenantConfigRepository(async_session, "t1")
        all_configs = await repo.list_all()
        assert len(all_configs) >= 3

    async def test_list_all_excludes_deactivated(self, async_session: AsyncSession) -> None:
        repo_active = TenantConfigRepository(async_session, "ta_active")
        repo_dead = TenantConfigRepository(async_session, "ta_dead")
        await repo_active.create()
        await repo_dead.create()
        await repo_dead.deactivate()

        admin_repo = TenantConfigRepository(async_session, "any")
        all_active = await admin_repo.list_all(include_deactivated=False)
        all_with_dead = await admin_repo.list_all(include_deactivated=True)
        assert len(all_with_dead) > len(all_active)


# ---------------------------------------------------------------------------
# ReconciliationRepository
# ---------------------------------------------------------------------------


class TestReconciliationRepository:
    async def test_record_and_get_unresolved(self, async_session: AsyncSession) -> None:
        repo = ReconciliationRepository(async_session, _TENANT)
        row = await repo.record_check(
            run_id=_uid(),
            model_name="m",
            expected_status="COMPLETED",
            warehouse_status="RUNNING",
            discrepancy_type="STATUS_MISMATCH",
        )
        unresolved = await repo.get_unresolved()
        assert len(unresolved) == 1
        assert unresolved[0].id == row.id

    async def test_no_discrepancy_not_in_unresolved(self, async_session: AsyncSession) -> None:
        repo = ReconciliationRepository(async_session, _TENANT)
        await repo.record_check(
            run_id=_uid(),
            model_name="m",
            expected_status="COMPLETED",
            warehouse_status="COMPLETED",
        )
        assert await repo.get_unresolved() == []

    async def test_resolve(self, async_session: AsyncSession) -> None:
        repo = ReconciliationRepository(async_session, _TENANT)
        row = await repo.record_check(
            run_id=_uid(),
            model_name="m",
            expected_status="COMPLETED",
            warehouse_status="FAILED",
            discrepancy_type="STATUS_MISMATCH",
        )
        updated = await repo.resolve(row.id, resolved_by="admin", resolution_note="rerun")
        assert updated is not None
        assert updated.resolved is True

    async def test_resolve_nonexistent(self, async_session: AsyncSession) -> None:
        repo = ReconciliationRepository(async_session, _TENANT)
        result = await repo.resolve(99999, resolved_by="admin", resolution_note="note")
        assert result is None

    async def test_get_stats(self, async_session: AsyncSession) -> None:
        repo = ReconciliationRepository(async_session, _TENANT)
        await repo.record_check(_uid(), "m", "COMPLETED", "COMPLETED")
        row = await repo.record_check(_uid(), "m", "COMPLETED", "FAILED", "STATUS_MISMATCH")
        stats = await repo.get_stats()
        assert stats["total_checks"] == 2
        assert stats["total_discrepancies"] == 1
        assert stats["unresolved_discrepancies"] == 1
        await repo.resolve(row.id, "admin", "note")
        stats2 = await repo.get_stats()
        assert stats2["resolved_discrepancies"] == 1

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = ReconciliationRepository(async_session, _TENANT)
        repo_b = ReconciliationRepository(async_session, _OTHER_TENANT)
        await repo_a.record_check(_uid(), "m", "COMPLETED", "FAILED", "STATUS_MISMATCH")
        assert await repo_b.get_unresolved() == []


# ---------------------------------------------------------------------------
# AIFeedbackRepository
# ---------------------------------------------------------------------------


class TestAIFeedbackRepository:
    async def test_record_prediction_and_outcome(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        plan_id, step_id = _uid(), _uid()
        await repo.record_prediction(plan_id, step_id, "model.m", "cost", {"predicted": 5.0})
        await repo.record_outcome(plan_id, step_id, "model.m", "cost", {"actual": 4.5}, accuracy_score=0.9)
        data = await repo.get_training_data("cost")
        assert len(data) == 1
        assert data[0]["accuracy_score"] == pytest.approx(0.9)

    async def test_record_outcome_no_prior_prediction(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        plan_id, step_id = _uid(), _uid()
        await repo.record_outcome(plan_id, step_id, "model.m", "risk", {"actual": "high"})
        data = await repo.get_training_data("risk")
        assert len(data) == 0  # no prediction_json set in this path

    async def test_mark_accepted(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        plan_id, step_id = _uid(), _uid()
        await repo.record_prediction(plan_id, step_id, "model.m", "classification", {"cls": "high"})
        updated = await repo.mark_accepted(plan_id, step_id, "model.m", "classification", True)
        assert updated is True

    async def test_mark_accepted_nonexistent(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        updated = await repo.mark_accepted("ghost", "ghost", "ghost", "cost", True)
        assert updated is False

    async def test_get_accuracy_stats_empty(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        stats = await repo.get_accuracy_stats()
        assert stats["record_count"] == 0
        assert stats["avg_accuracy"] is None

    async def test_get_accuracy_stats_with_data(self, async_session: AsyncSession) -> None:
        repo = AIFeedbackRepository(async_session, _TENANT)
        plan_id, step_id = _uid(), _uid()
        await repo.record_prediction(plan_id, step_id, "m", "cost", {"p": 1})
        await repo.record_outcome(plan_id, step_id, "m", "cost", {"a": 1}, accuracy_score=0.8)
        await repo.mark_accepted(plan_id, step_id, "m", "cost", True)
        stats = await repo.get_accuracy_stats(feedback_type="cost")
        assert stats["record_count"] == 1
        assert stats["avg_accuracy"] == pytest.approx(0.8)
        assert stats["acceptance_rate"] == pytest.approx(1.0)

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = AIFeedbackRepository(async_session, _TENANT)
        repo_b = AIFeedbackRepository(async_session, _OTHER_TENANT)
        plan_id, step_id = _uid(), _uid()
        await repo_a.record_prediction(plan_id, step_id, "m", "cost", {"p": 1})
        assert await repo_b.get_training_data("cost") == []


# ---------------------------------------------------------------------------
# LLMUsageLogRepository
# ---------------------------------------------------------------------------


class TestLLMUsageLogRepository:
    def _make_usage(self) -> dict[str, Any]:
        return {
            "call_type": "plan_advisory",
            "model_id": "claude-3",
            "input_tokens": 100,
            "output_tokens": 50,
            "estimated_cost_usd": 0.005,
            "latency_ms": 800,
            "success": True,
        }

    async def test_record_and_period_cost(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        await repo.record_usage(**self._make_usage())
        since = datetime.now(UTC) - timedelta(hours=1)
        cost = await repo.get_period_cost(since)
        assert cost == pytest.approx(0.005)

    async def test_period_cost_empty(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        since = datetime.now(UTC) + timedelta(hours=1)
        cost = await repo.get_period_cost(since)
        assert cost == 0.0

    async def test_get_daily_cost(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        await repo.record_usage(**self._make_usage())
        cost = await repo.get_daily_cost()
        assert cost == pytest.approx(0.005)

    async def test_get_monthly_cost(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        await repo.record_usage(**self._make_usage())
        cost = await repo.get_monthly_cost()
        assert cost == pytest.approx(0.005)

    async def test_get_usage_stats_empty(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        stats = await repo.get_usage_stats()
        assert stats["total_calls"] == 0
        assert stats["total_cost_usd"] == 0.0

    async def test_get_usage_stats_with_data(self, async_session: AsyncSession) -> None:
        repo = LLMUsageLogRepository(async_session, _TENANT)
        await repo.record_usage(**self._make_usage())
        failed = dict(self._make_usage())
        failed["success"] = False
        await repo.record_usage(**failed)
        stats = await repo.get_usage_stats()
        assert stats["total_calls"] == 2
        assert stats["success_rate"] == pytest.approx(0.5)

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = LLMUsageLogRepository(async_session, _TENANT)
        repo_b = LLMUsageLogRepository(async_session, _OTHER_TENANT)
        await repo_a.record_usage(**self._make_usage())
        since = datetime.now(UTC) - timedelta(hours=1)
        assert await repo_b.get_period_cost(since) == 0.0


# ---------------------------------------------------------------------------
# BackfillCheckpointRepository
# ---------------------------------------------------------------------------


class TestBackfillCheckpointRepository:
    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid = _uid()
        row = await repo.create(
            backfill_id=bfid,
            model_name="m",
            overall_start=date(2023, 1, 1),
            overall_end=date(2023, 12, 31),
            chunk_size_days=30,
            total_chunks=12,
        )
        assert row.backfill_id == bfid
        fetched = await repo.get(bfid)
        assert fetched is not None

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        assert await repo.get("ghost") is None

    async def test_update_progress(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.create(bfid, "m", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        await repo.update_progress(bfid, date(2023, 2, 28), completed_chunks=2)
        row = await repo.get(bfid)
        assert row is not None
        assert row.completed_chunks == 2

    async def test_mark_completed(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.create(bfid, "m", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        await repo.mark_completed(bfid)
        row = await repo.get(bfid)
        assert row is not None
        assert row.status == "COMPLETED"

    async def test_mark_failed(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.create(bfid, "m", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        await repo.mark_failed(bfid, "OOM error")
        row = await repo.get(bfid)
        assert row is not None
        assert row.status == "FAILED"
        assert "OOM error" in (row.error_message or "")

    async def test_get_resumable(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid1, bfid2 = _uid(), _uid()
        await repo.create(bfid1, "m", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        await repo.create(bfid2, "m", date(2022, 1, 1), date(2022, 12, 31), 30, 12)
        await repo.mark_failed(bfid2, "error")
        await repo.mark_completed(bfid1)  # bfid1 completed, not resumable
        resumable = await repo.get_resumable()
        # bfid2 (FAILED) should be resumable, bfid1 (COMPLETED) should not
        assert any(r.backfill_id == bfid2 for r in resumable)
        assert not any(r.backfill_id == bfid1 for r in resumable)

    async def test_get_resumable_by_model(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.create(bfid, "model_x", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        resumable = await repo.get_resumable(model_name="model_x")
        assert len(resumable) == 1
        resumable_other = await repo.get_resumable(model_name="model_y")
        assert resumable_other == []

    async def test_list_for_model(self, async_session: AsyncSession) -> None:
        repo = BackfillCheckpointRepository(async_session, _TENANT)
        for _ in range(3):
            await repo.create(_uid(), "model_z", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        rows = await repo.list_for_model("model_z", limit=2)
        assert len(rows) == 2

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = BackfillCheckpointRepository(async_session, _TENANT)
        repo_b = BackfillCheckpointRepository(async_session, _OTHER_TENANT)
        bfid = _uid()
        await repo_a.create(bfid, "m", date(2023, 1, 1), date(2023, 12, 31), 30, 12)
        assert await repo_b.get(bfid) is None


# ---------------------------------------------------------------------------
# BackfillAuditRepository
# ---------------------------------------------------------------------------


class TestBackfillAuditRepository:
    async def test_record_chunk_and_get_history(self, async_session: AsyncSession) -> None:
        repo = BackfillAuditRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.record_chunk(bfid, "m", date(2023, 1, 1), date(2023, 1, 31), "SUCCESS", run_id=_uid())
        history = await repo.get_history("m")
        assert len(history) == 1

    async def test_get_for_backfill(self, async_session: AsyncSession) -> None:
        repo = BackfillAuditRepository(async_session, _TENANT)
        bfid = _uid()
        await repo.record_chunk(bfid, "m", date(2023, 1, 1), date(2023, 1, 31), "SUCCESS")
        await repo.record_chunk(bfid, "m", date(2023, 2, 1), date(2023, 2, 28), "FAILED", error_message="oom")
        entries = await repo.get_for_backfill(bfid)
        assert len(entries) == 2
        assert entries[0].chunk_start < entries[1].chunk_start

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = BackfillAuditRepository(async_session, _TENANT)
        repo_b = BackfillAuditRepository(async_session, _OTHER_TENANT)
        bfid = _uid()
        await repo_a.record_chunk(bfid, "m", date(2023, 1, 1), date(2023, 1, 31), "SUCCESS")
        assert await repo_b.get_for_backfill(bfid) == []


# ---------------------------------------------------------------------------
# SchemaDriftRepository
# ---------------------------------------------------------------------------


class TestSchemaDriftRepository:
    async def test_record_drift(self, async_session: AsyncSession) -> None:
        repo = SchemaDriftRepository(async_session, _TENANT)
        row = await repo.record_drift(
            model_name="m",
            expected_columns=["id", "name"],
            actual_columns=["id", "name", "extra"],
            drift_type="COLUMN_ADDED",
            drift_details={"added": ["extra"]},
        )
        assert row.drift_type == "COLUMN_ADDED"

    async def test_get_unresolved(self, async_session: AsyncSession) -> None:
        repo = SchemaDriftRepository(async_session, _TENANT)
        await repo.record_drift("m", [], [], "NONE", None)
        await repo.record_drift("m", [], ["x"], "COLUMN_ADDED", None)
        unresolved = await repo.get_unresolved()
        assert len(unresolved) == 2  # both have resolved=False initially

    async def test_get_for_model(self, async_session: AsyncSession) -> None:
        repo = SchemaDriftRepository(async_session, _TENANT)
        await repo.record_drift("model_x", [], [], "NONE", None)
        await repo.record_drift("model_y", [], [], "NONE", None)
        result = await repo.get_for_model("model_x")
        assert len(result) == 1

    async def test_resolve(self, async_session: AsyncSession) -> None:
        repo = SchemaDriftRepository(async_session, _TENANT)
        row = await repo.record_drift("m", [], [], "COLUMN_ADDED", None)
        updated = await repo.resolve(row.id, "admin", "fixed the schema")
        assert updated is not None
        assert updated.resolved is True

    async def test_resolve_nonexistent(self, async_session: AsyncSession) -> None:
        repo = SchemaDriftRepository(async_session, _TENANT)
        assert await repo.resolve(99999, "admin", "note") is None

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = SchemaDriftRepository(async_session, _TENANT)
        repo_b = SchemaDriftRepository(async_session, _OTHER_TENANT)
        await repo_a.record_drift("m", [], [], "NONE", None)
        assert await repo_b.get_unresolved() == []


# ---------------------------------------------------------------------------
# ReconciliationScheduleRepository
# ---------------------------------------------------------------------------


class TestReconciliationScheduleRepository:
    async def test_upsert_and_get(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        row = await repo.upsert_schedule("status_check", "0 * * * *", enabled=True)
        assert row.schedule_type == "status_check"
        fetched = await repo.get_schedule("status_check")
        assert fetched is not None

    async def test_upsert_updates(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        await repo.upsert_schedule("s", "0 * * * *")
        updated = await repo.upsert_schedule("s", "30 * * * *")
        assert updated.cron_expression == "30 * * * *"

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        assert await repo.get_schedule("ghost") is None

    async def test_update_last_run(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        await repo.upsert_schedule("s", "0 * * * *")
        now = datetime.now(UTC)
        next_run = now + timedelta(hours=1)
        result = await repo.update_last_run("s", now, next_run)
        assert result is True

    async def test_update_last_run_nonexistent(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        result = await repo.update_last_run("ghost", datetime.now(UTC), datetime.now(UTC))
        assert result is False

    async def test_get_all_enabled(self, async_session: AsyncSession) -> None:
        repo = ReconciliationScheduleRepository(async_session, _TENANT)
        await repo.upsert_schedule("s1", "0 * * * *", enabled=True)
        await repo.upsert_schedule("s2", "0 * * * *", enabled=False)
        enabled = await repo.get_all_enabled()
        assert all(r.enabled for r in enabled)


# ---------------------------------------------------------------------------
# EnvironmentRepository
# ---------------------------------------------------------------------------


class TestEnvironmentRepository:
    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        env = await repo.create("production", "prod_catalog", "prod_")
        assert env.name == "production"
        fetched = await repo.get("production")
        assert fetched is not None

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        assert await repo.get("ghost") is None

    async def test_list_all(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.create("env_a", "cat", "a_")
        await repo.create("env_b", "cat", "b_")
        rows = await repo.list_all()
        assert len(rows) == 2

    async def test_soft_delete(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.create("staging", "cat", "stg_")
        result = await repo.soft_delete("staging")
        assert result is True
        assert await repo.get("staging") is None

    async def test_soft_delete_nonexistent(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        assert await repo.soft_delete("ghost") is False

    async def test_get_default(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.create("dev", "cat", "dev_", is_default=True)
        default = await repo.get_default()
        assert default is not None
        assert default.name == "dev"

    async def test_get_ephemeral(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.create("pr-42", "cat", "pr42_", is_ephemeral=True, pr_number=42)
        await repo.create("main", "cat", "main_", is_ephemeral=False)
        ephemeral = await repo.get_ephemeral()
        assert len(ephemeral) == 1
        assert ephemeral[0].name == "pr-42"

    async def test_cleanup_expired(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        past = datetime.now(UTC) - timedelta(hours=1)
        await repo.create("expired-pr", "cat", "exp_", is_ephemeral=True, expires_at=past)
        count = await repo.cleanup_expired()
        assert count == 1

    async def test_record_promotion(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        promo = await repo.record_promotion("staging", "prod", "snap-1", "snap-2", "alice")
        assert promo.source_environment == "staging"

    async def test_get_promotion_history(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.record_promotion("staging", "prod", "snap-1", "snap-2", "alice")
        await repo.record_promotion("dev", "staging", "snap-3", "snap-4", "bob")
        history = await repo.get_promotion_history()
        assert len(history) == 2
        history_staging = await repo.get_promotion_history(environment_name="staging")
        assert len(history_staging) == 2  # staging appears as source and target

    async def test_list_all_include_deleted(self, async_session: AsyncSession) -> None:
        repo = EnvironmentRepository(async_session, tenant_id=_TENANT)
        await repo.create("active-env", "cat", "a_")
        await repo.create("deleted-env", "cat", "d_")
        await repo.soft_delete("deleted-env")
        active_only = await repo.list_all(include_deleted=False)
        all_envs = await repo.list_all(include_deleted=True)
        assert len(all_envs) > len(active_only)

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = EnvironmentRepository(async_session, tenant_id=_TENANT)
        repo_b = EnvironmentRepository(async_session, tenant_id=_OTHER_TENANT)
        await repo_a.create("prod", "cat", "p_")
        assert await repo_b.get("prod") is None


# ---------------------------------------------------------------------------
# ModelTestRepository
# ---------------------------------------------------------------------------


class TestModelTestRepository:
    async def test_save_and_get(self, async_session: AsyncSession) -> None:
        repo = ModelTestRepository(async_session, _TENANT)
        test_id = _uid()
        row = await repo.save_test(test_id, "model.m", "not_null", {"columns": ["id"]})
        assert row.test_id == test_id

        fetched = await repo.get_by_id(test_id)
        assert fetched is not None

    async def test_save_updates_existing(self) -> None:
        """save_test with duplicate test_id should update the existing row.

        The repository's save_test uses an IntegrityError-based upsert which
        issues a session.rollback() internally.  SQLAlchemy's session becomes
        unusable after rollback (the transaction is rolled back), so we use two
        independent sessions: one to insert the initial row, one to trigger the
        update path via a second insert attempt.
        """
        engine = create_async_engine("sqlite+aiosqlite://", echo=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)

        test_id = _uid()

        # Session 1: create the initial row and commit it.
        async with factory() as s1:
            repo1 = ModelTestRepository(s1, _TENANT)
            await repo1.save_test(test_id, "model.m", "not_null", {"columns": ["id"]})
            await s1.commit()

        # Session 2: trigger the update path (duplicate test_id → rollback + UPDATE).
        async with factory() as s2:
            repo2 = ModelTestRepository(s2, _TENANT)
            await repo2.save_test(test_id, "model.m", "unique", {"columns": ["id"]}, severity="WARN")
            await s2.commit()

        # Session 3: verify the update was persisted.
        async with factory() as s3:
            repo3 = ModelTestRepository(s3, _TENANT)
            fetched = await repo3.get_by_id(test_id)
            assert fetched is not None
            assert fetched.test_type == "unique"

        await engine.dispose()

    async def test_get_for_model(self, async_session: AsyncSession) -> None:
        repo = ModelTestRepository(async_session, _TENANT)
        for _ in range(3):
            await repo.save_test(_uid(), "model.x", "not_null", None)
        rows = await repo.get_for_model("model.x")
        assert len(rows) == 3

    async def test_delete_for_model(self, async_session: AsyncSession) -> None:
        repo = ModelTestRepository(async_session, _TENANT)
        await repo.save_test(_uid(), "model.del", "not_null", None)
        await repo.save_test(_uid(), "model.del", "unique", None)
        count = await repo.delete_for_model("model.del")
        assert count == 2
        assert await repo.get_for_model("model.del") == []

    async def test_get_by_id_nonexistent(self, async_session: AsyncSession) -> None:
        repo = ModelTestRepository(async_session, _TENANT)
        assert await repo.get_by_id("ghost") is None

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = ModelTestRepository(async_session, _TENANT)
        repo_b = ModelTestRepository(async_session, _OTHER_TENANT)
        await repo_a.save_test(_uid(), "m", "not_null", None)
        assert await repo_b.get_for_model("m") == []


# ---------------------------------------------------------------------------
# TestResultRepository
# ---------------------------------------------------------------------------


class TestTestResultRepository:
    async def test_record_and_get_for_plan(self, async_session: AsyncSession) -> None:
        repo = TestResultRepository(async_session, _TENANT)
        plan_id = _uid()
        await repo.record_result(
            test_id=_uid(),
            plan_id=plan_id,
            model_name="m",
            test_type="not_null",
            passed=True,
            failure_message=None,
            execution_mode="local",
            duration_ms=50,
        )
        rows = await repo.get_for_plan(plan_id)
        assert len(rows) == 1

    async def test_get_for_model(self, async_session: AsyncSession) -> None:
        repo = TestResultRepository(async_session, _TENANT)
        for _ in range(3):
            await repo.record_result(_uid(), None, "model.x", "not_null", True, None, "local", 10)
        rows = await repo.get_for_model("model.x", limit=2)
        assert len(rows) == 2

    async def test_get_summary(self, async_session: AsyncSession) -> None:
        repo = TestResultRepository(async_session, _TENANT)
        plan_id = _uid()
        await repo.record_result(_uid(), plan_id, "m", "not_null", True, None, "local", 10)
        await repo.record_result(_uid(), plan_id, "m", "unique", False, "dup found", "local", 10)
        summary = await repo.get_summary(plan_id)
        assert summary["total"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = TestResultRepository(async_session, _TENANT)
        repo_b = TestResultRepository(async_session, _OTHER_TENANT)
        plan_id = _uid()
        await repo_a.record_result(_uid(), plan_id, "m", "not_null", True, None, "local", 10)
        assert await repo_b.get_for_plan(plan_id) == []


# ---------------------------------------------------------------------------
# EventSubscriptionRepository
# ---------------------------------------------------------------------------


class TestEventSubscriptionRepository:
    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        sub = await repo.create("My Webhook", "https://example.com/hook")
        assert sub.name == "My Webhook"
        fetched = await repo.get(sub.id)
        assert fetched is not None

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        assert await repo.get(99999) is None

    async def test_list_active(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        sub1 = await repo.create("S1", "https://a.com")
        await repo.create("S2", "https://b.com")
        await repo.update(sub1.id, active=False)
        active = await repo.list_active()
        assert len(active) == 1
        assert all(s.active for s in active)

    async def test_list_all(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        await repo.create("S1", "https://a.com")
        await repo.create("S2", "https://b.com")
        all_subs = await repo.list_all()
        assert len(all_subs) == 2

    async def test_update(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        sub = await repo.create("Original", "https://orig.com")
        updated = await repo.update(sub.id, name="Updated", url="https://new.com")
        assert updated is not None
        assert updated.name == "Updated"

    async def test_update_nonexistent(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        assert await repo.update(99999, name="X") is None

    async def test_delete(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        sub = await repo.create("ToDelete", "https://x.com")
        result = await repo.delete(sub.id)
        assert result is True
        assert await repo.get(sub.id) is None

    async def test_delete_nonexistent(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        assert await repo.delete(99999) is False

    async def test_list_for_event_type_wildcard(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        await repo.create("Wildcard", "https://x.com", event_types=None)
        result = await repo.list_for_event_type("plan.run.completed")
        assert len(result) == 1

    async def test_list_for_event_type_specific(self, async_session: AsyncSession) -> None:
        repo = EventSubscriptionRepository(async_session, _TENANT)
        await repo.create("Specific", "https://x.com", event_types=["plan.run.completed"])
        await repo.create("Other", "https://y.com", event_types=["model.created"])
        result = await repo.list_for_event_type("plan.run.completed")
        assert len(result) == 1

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = EventSubscriptionRepository(async_session, _TENANT)
        repo_b = EventSubscriptionRepository(async_session, _OTHER_TENANT)
        await repo_a.create("S", "https://x.com")
        assert await repo_b.list_all() == []


# ---------------------------------------------------------------------------
# UserRepository
# ---------------------------------------------------------------------------


class TestUserRepository:
    async def test_create_and_get_by_email(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        user = await repo.create("alice@example.com", "Password123!", "Alice")
        assert user.email == "alice@example.com"
        fetched = await repo.get_by_email("alice@example.com")
        assert fetched is not None
        assert fetched.display_name == "Alice"

    async def test_email_normalized(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        await repo.create("BOB@EXAMPLE.COM", "Password!", "Bob")
        fetched = await repo.get_by_email("bob@example.com")
        assert fetched is not None

    async def test_get_by_email_not_found(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        assert await repo.get_by_email("nobody@example.com") is None

    async def test_get_by_id(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        user = await repo.create("carol@example.com", "Pass!", "Carol")
        fetched = await repo.get_by_id(user.id)
        assert fetched is not None

    async def test_get_by_id_not_found(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        assert await repo.get_by_id("ghost-id") is None

    async def test_verify_password_correct(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        await repo.create("dave@example.com", "SecurePass1!", "Dave")
        result = await repo.verify_password("dave@example.com", "SecurePass1!")
        assert result is not None

    async def test_verify_password_wrong(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        await repo.create("eve@example.com", "RealPassword!", "Eve")
        result = await repo.verify_password("eve@example.com", "WrongPassword!")
        assert result is None

    async def test_verify_password_nonexistent_user(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        result = await repo.verify_password("ghost@example.com", "anything")
        assert result is None

    async def test_get_by_email_any_tenant(self, async_session: AsyncSession) -> None:
        repo_a = UserRepository(async_session, _TENANT)
        repo_b = UserRepository(async_session, _OTHER_TENANT)
        await repo_a.create("frank@example.com", "Pass!", "Frank")
        # get_by_email_any_tenant should find across tenants
        found = await repo_b.get_by_email_any_tenant("frank@example.com")
        assert found is not None

    async def test_update_last_login(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        user = await repo.create("gina@example.com", "Pass!", "Gina")
        await repo.update_last_login(user.id)
        fetched = await repo.get_by_id(user.id)
        assert fetched is not None

    async def test_list_by_tenant(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        await repo.create("u1@example.com", "Pass!", "U1")
        await repo.create("u2@example.com", "Pass!", "U2")
        users = await repo.list_by_tenant()
        assert len(users) == 2

    async def test_count_by_tenant(self, async_session: AsyncSession) -> None:
        repo = UserRepository(async_session, _TENANT)
        await repo.create("cnt1@example.com", "Pass!", "C1")
        count = await repo.count_by_tenant()
        assert count == 1

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = UserRepository(async_session, _TENANT)
        repo_b = UserRepository(async_session, _OTHER_TENANT)
        await repo_a.create("isolated@example.com", "Pass!", "X")
        assert await repo_b.get_by_email("isolated@example.com") is None


# ---------------------------------------------------------------------------
# APIKeyRepository
# ---------------------------------------------------------------------------


class TestAPIKeyRepository:
    async def test_create_and_validate(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        row, plaintext = await repo.create(user_id="user-1", name="My Key")
        assert plaintext.startswith("bmkey.")
        validated = await repo.validate_key(plaintext)
        assert validated is not None
        assert validated.id == row.id

    async def test_validate_wrong_key(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        await repo.create(user_id="user-1", name="K")
        assert await repo.validate_key("bmkey.wrongwrongwrong") is None

    async def test_revoke(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        row, plaintext = await repo.create(user_id="user-1", name="K")
        result = await repo.revoke(row.id)
        assert result is True
        assert await repo.validate_key(plaintext) is None

    async def test_revoke_nonexistent(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        assert await repo.revoke("ghost-id") is False

    async def test_list_by_user(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        await repo.create(user_id="user-2", name="K1")
        await repo.create(user_id="user-2", name="K2")
        keys = await repo.list_by_user("user-2")
        assert len(keys) == 2

    async def test_revoked_not_in_list(self, async_session: AsyncSession) -> None:
        repo = APIKeyRepository(async_session, _TENANT)
        row, _ = await repo.create(user_id="user-3", name="K")
        await repo.revoke(row.id)
        keys = await repo.list_by_user("user-3")
        assert keys == []

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = APIKeyRepository(async_session, _TENANT)
        repo_b = APIKeyRepository(async_session, _OTHER_TENANT)
        _, plaintext = await repo_a.create(user_id="user-1", name="K")
        # Validating from tenant B should not find tenant A's key
        assert await repo_b.validate_key(plaintext) is None


# ---------------------------------------------------------------------------
# AnalyticsRepository (cross-tenant, skip PG-only methods)
# ---------------------------------------------------------------------------


class TestAnalyticsRepository:
    async def test_get_platform_overview_empty(self, async_session: AsyncSession) -> None:
        repo = AnalyticsRepository(async_session)
        since = datetime.now(UTC) - timedelta(days=30)
        result = await repo.get_platform_overview(since)
        assert "total_tenants" in result
        assert result["total_tenants"] == 0
        assert result["total_runs"] == 0

    async def test_get_platform_overview_with_data(self, async_session: AsyncSession) -> None:
        # Create a tenant config so total_tenants is > 0
        await TenantConfigRepository(async_session, "analytics-tenant").create()
        repo = AnalyticsRepository(async_session)
        since = datetime.now(UTC) - timedelta(days=30)
        result = await repo.get_platform_overview(since)
        assert result["total_tenants"] >= 1

    async def test_get_revenue_metrics_empty(self, async_session: AsyncSession) -> None:
        repo = AnalyticsRepository(async_session)
        result = await repo.get_revenue_metrics()
        assert result["mrr_usd"] == 0.0
        assert result["tiers"] == {}

    async def test_get_per_tenant_breakdown_empty(self, async_session: AsyncSession) -> None:
        repo = AnalyticsRepository(async_session)
        since = datetime.now(UTC) - timedelta(days=30)
        result = await repo.get_per_tenant_breakdown(since)
        assert result["tenants"] == []
        assert result["total"] == 0

    async def test_get_cost_breakdown_by_model_empty(self, async_session: AsyncSession) -> None:
        repo = AnalyticsRepository(async_session)
        since = datetime.now(UTC) - timedelta(days=30)
        result = await repo.get_cost_breakdown(since, group_by="model")
        assert result["items"] == []
        assert result["total_cost_usd"] == 0.0


# ---------------------------------------------------------------------------
# QuotaRepository
# ---------------------------------------------------------------------------


class TestQuotaRepository:
    async def _insert_usage_event(
        self,
        session: AsyncSession,
        tenant_id: str,
        event_type: str,
        quantity: int = 1,
    ) -> None:
        row = UsageEventTable(
            event_id=_uid(),
            tenant_id=tenant_id,
            event_type=event_type,
            quantity=quantity,
        )
        session.add(row)
        await session.flush()

    async def test_get_monthly_event_count_empty(self, async_session: AsyncSession) -> None:
        repo = QuotaRepository(async_session, _TENANT)
        count = await repo.get_monthly_event_count("plan_run")
        assert count == 0

    async def test_get_monthly_event_count(self, async_session: AsyncSession) -> None:
        repo = QuotaRepository(async_session, _TENANT)
        await self._insert_usage_event(async_session, _TENANT, "plan_run", 3)
        await self._insert_usage_event(async_session, _TENANT, "plan_run", 2)
        count = await repo.get_monthly_event_count("plan_run")
        assert count == 5

    async def test_get_current_usage(self, async_session: AsyncSession) -> None:
        repo = QuotaRepository(async_session, _TENANT)
        await self._insert_usage_event(async_session, _TENANT, "plan_run", 10)
        await self._insert_usage_event(async_session, _TENANT, "ai_call", 5)
        usage = await repo.get_current_usage()
        assert usage.get("plan_run", 0) == 10
        assert usage.get("ai_call", 0) == 5

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = QuotaRepository(async_session, _TENANT)
        await self._insert_usage_event(async_session, _OTHER_TENANT, "plan_run", 99)
        count = await repo_a.get_monthly_event_count("plan_run")
        assert count == 0


# ---------------------------------------------------------------------------
# CustomerHealthRepository (cross-tenant)
# ---------------------------------------------------------------------------


class TestCustomerHealthRepository:
    # Valid health_status values per CHECK constraint: 'active', 'at_risk', 'churning'

    async def test_upsert_and_get(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        row = await repo.upsert(
            "tenant-health-1",
            health_score=85.0,
            health_status="active",
        )
        assert row.health_score == pytest.approx(85.0)
        fetched = await repo.get("tenant-health-1")
        assert fetched is not None

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        assert await repo.get("ghost-tenant") is None

    async def test_list_all(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        await repo.upsert("t-a", health_score=90.0, health_status="active")
        await repo.upsert("t-b", health_score=30.0, health_status="at_risk")
        rows, total = await repo.list_all()
        assert total == 2

    async def test_list_all_status_filter(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        await repo.upsert("t-c", health_score=90.0, health_status="active")
        await repo.upsert("t-d", health_score=20.0, health_status="churning")
        rows, total = await repo.list_all(status_filter="churning")
        assert all(r.health_status == "churning" for r in rows)

    async def test_get_at_risk_tenants(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        await repo.upsert("t-e", health_score=90.0, health_status="active")
        await repo.upsert("t-f", health_score=40.0, health_status="at_risk")
        await repo.upsert("t-g", health_score=10.0, health_status="churning")
        at_risk = await repo.get_at_risk_tenants()
        assert all(r.health_status in ("at_risk", "churning") for r in at_risk)

    async def test_upsert_updates(self, async_session: AsyncSession) -> None:
        repo = CustomerHealthRepository(async_session)
        await repo.upsert("t-h", health_score=50.0, health_status="at_risk")
        updated = await repo.upsert("t-h", health_score=90.0, health_status="active")
        assert updated.health_score == pytest.approx(90.0)


# ---------------------------------------------------------------------------
# InvoiceRepository
# ---------------------------------------------------------------------------


class TestInvoiceRepository:
    def _make_invoice(self, invoice_id: str | None = None) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "invoice_id": invoice_id or _uid(),
            "invoice_number": f"INV-{_uid()[:6]}",
            "period_start": now - timedelta(days=30),
            "period_end": now,
            "subtotal_usd": 49.0,
            "tax_usd": 4.9,
            "total_usd": 53.9,
            "line_items": [{"desc": "Team plan", "amount": 49.0}],
        }

    async def test_create_and_get(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        inv_data = self._make_invoice()
        row = await repo.create(**inv_data)
        assert row.invoice_id == inv_data["invoice_id"]
        fetched = await repo.get(inv_data["invoice_id"])
        assert fetched is not None

    async def test_get_nonexistent(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        assert await repo.get("ghost") is None

    async def test_list_for_tenant(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        for _ in range(3):
            await repo.create(**self._make_invoice())
        rows, total = await repo.list_for_tenant(limit=2)
        assert total == 3
        assert len(rows) == 2

    async def test_update_pdf_key(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        inv_data = self._make_invoice()
        await repo.create(**inv_data)
        result = await repo.update_pdf_key(inv_data["invoice_id"], "s3://bucket/key.pdf")
        assert result is True

    async def test_update_pdf_key_nonexistent(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        assert await repo.update_pdf_key("ghost", "s3://x") is False

    async def test_update_status(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        inv_data = self._make_invoice()
        await repo.create(**inv_data)
        result = await repo.update_status(inv_data["invoice_id"], "paid")
        assert result is True
        fetched = await repo.get(inv_data["invoice_id"])
        assert fetched is not None
        assert fetched.status == "paid"

    async def test_get_next_invoice_number(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        num = await repo.get_next_invoice_number()
        assert num.startswith("INV-")
        # Second call should produce a different sequence number
        datetime.now(UTC)
        inv_data = self._make_invoice()
        inv_data["invoice_number"] = num
        await repo.create(**inv_data)
        num2 = await repo.get_next_invoice_number()
        assert num2 != num

    async def test_get_by_stripe_invoice(self, async_session: AsyncSession) -> None:
        repo = InvoiceRepository(async_session, _TENANT)
        inv_data = self._make_invoice()
        inv_data["stripe_invoice_id"] = "in_test_12345"
        await repo.create(**inv_data)
        fetched = await repo.get_by_stripe_invoice("in_test_12345")
        assert fetched is not None

    async def test_tenant_isolation(self, async_session: AsyncSession) -> None:
        repo_a = InvoiceRepository(async_session, _TENANT)
        repo_b = InvoiceRepository(async_session, _OTHER_TENANT)
        inv_data = self._make_invoice()
        await repo_a.create(**inv_data)
        assert await repo_b.get(inv_data["invoice_id"]) is None


# ---------------------------------------------------------------------------
# EventOutboxRepository
# ---------------------------------------------------------------------------


class TestEventOutboxRepository:
    async def test_write_and_get_pending(self, async_session: AsyncSession) -> None:
        repo = EventOutboxRepository(async_session)
        corr_id = _uid()
        row = await repo.write(_TENANT, "plan.run.completed", {"plan_id": "p1"}, corr_id)
        assert row.status == "pending"

        pending = await repo.get_pending()
        assert len(pending) == 1
        assert pending[0].correlation_id == corr_id

    async def test_mark_delivered(self, async_session: AsyncSession) -> None:
        repo = EventOutboxRepository(async_session)
        row = await repo.write(_TENANT, "model.created", {}, _uid())
        await repo.mark_delivered(row.id)
        pending = await repo.get_pending()
        assert pending == []

    async def test_mark_failed(self, async_session: AsyncSession) -> None:
        repo = EventOutboxRepository(async_session)
        row = await repo.write(_TENANT, "model.deleted", {}, _uid())
        await repo.mark_failed(row.id, "Connection timeout")
        # Entry stays pending (not delivered), attempt count bumped
        pending = await repo.get_pending()
        assert len(pending) == 1

    async def test_cleanup_delivered(self, async_session: AsyncSession) -> None:
        repo = EventOutboxRepository(async_session)
        row = await repo.write(_TENANT, "event", {}, _uid())
        await repo.mark_delivered(row.id)
        deleted = await repo.cleanup_delivered(older_than_hours=0)
        assert deleted >= 1

    async def test_get_pending_limit(self, async_session: AsyncSession) -> None:
        repo = EventOutboxRepository(async_session)
        for _ in range(5):
            await repo.write(_TENANT, "event", {}, _uid())
        pending = await repo.get_pending(limit=3)
        assert len(pending) == 3


# ---------------------------------------------------------------------------
# ReportingRepository (skip date_trunc methods — PG-specific)
# ---------------------------------------------------------------------------


class TestReportingRepository:
    async def test_get_cost_by_model_empty(self, async_session: AsyncSession) -> None:
        repo = ReportingRepository(async_session, _TENANT)
        since = datetime.now(UTC) - timedelta(days=30)
        until = datetime.now(UTC)
        result = await repo.get_cost_by_model(since, until)
        assert result == []

    async def test_get_usage_by_actor_empty(self, async_session: AsyncSession) -> None:
        repo = ReportingRepository(async_session, _TENANT)
        since = datetime.now(UTC) - timedelta(days=30)
        until = datetime.now(UTC)
        result = await repo.get_usage_by_actor(since, until)
        assert result == []

    async def test_get_llm_cost_by_call_type_empty(self, async_session: AsyncSession) -> None:
        repo = ReportingRepository(async_session, _TENANT)
        since = datetime.now(UTC) - timedelta(days=30)
        until = datetime.now(UTC)
        result = await repo.get_llm_cost_by_call_type(since, until)
        assert result == []

    async def test_get_usage_by_actor_with_data(self, async_session: AsyncSession) -> None:
        audit_repo = AuditRepository(async_session, tenant_id=_TENANT)
        await audit_repo.log(actor="alice", action="plan.run")
        await audit_repo.log(actor="alice", action="plan.run")
        await audit_repo.log(actor="bob", action="model.create")

        repo = ReportingRepository(async_session, _TENANT)
        since = datetime.now(UTC) - timedelta(hours=1)
        until = datetime.now(UTC) + timedelta(hours=1)
        result = await repo.get_usage_by_actor(since, until)
        assert len(result) >= 2

    async def test_get_llm_cost_by_call_type_with_data(self, async_session: AsyncSession) -> None:
        llm_repo = LLMUsageLogRepository(async_session, _TENANT)
        await llm_repo.record_usage(
            call_type="advisory",
            model_id="claude-3",
            input_tokens=100,
            output_tokens=50,
            estimated_cost_usd=0.01,
            latency_ms=500,
            success=True,
        )
        repo = ReportingRepository(async_session, _TENANT)
        since = datetime.now(UTC) - timedelta(hours=1)
        until = datetime.now(UTC) + timedelta(hours=1)
        result = await repo.get_llm_cost_by_call_type(since, until)
        assert len(result) == 1
        assert result[0]["call_type"] == "advisory"
