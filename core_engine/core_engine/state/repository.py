"""Repository classes providing CRUD access to the IronLayer state store.

Each repository takes an ``AsyncSession`` at construction time and operates
within the caller's transaction boundary.  All writes call ``session.flush()``
so that generated defaults are populated; the caller is responsible for calling
``session.commit()`` (or relying on the ``get_session`` context manager).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core_engine.state._repository_utils import (
    _dialect_upsert,
    _dialect_upsert_nothing,
    _escape_like,
)
from core_engine.state.plan_repository import PlanRepository as PlanRepository  # noqa: F401
from core_engine.state.run_repository import RunRepository as RunRepository  # noqa: F401
from core_engine.state.tables import (
    AIFeedbackTable,
    APIKeyTable,
    AuditLogTable,
    BackfillAuditTable,
    BackfillCheckpointTable,
    BillingCustomerTable,
    CredentialTable,
    CustomerHealthTable,
    EnvironmentPromotionTable,
    EnvironmentTable,
    EventOutboxTable,
    EventSubscriptionTable,
    InvoiceTable,
    LLMUsageLogTable,
    LockTable,
    ModelTable,
    ModelTestTable,
    ReconciliationCheckTable,
    ReconciliationScheduleTable,
    RunTable,
    SchemaDriftCheckTable,
    SnapshotTable,
    TelemetryTable,
    TenantConfigTable,
    TestResultTable,
    TokenRevocationTable,
    UsageEventTable,
    UserTable,
    WatermarkTable,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ModelRepository
# ---------------------------------------------------------------------------

_MAX_MODEL_PAGE_SIZE = 500


class ModelRepository:
    """CRUD operations for the ``models`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create(
        self,
        model_name: str,
        repo_path: str,
        version: str,
        kind: str,
        time_column: str | None,
        unique_key: str | None,
        materialization: str,
        owner: str | None,
        tags: list[str] | None,
    ) -> ModelTable:
        """Insert a new model record and return the persisted row."""
        row = ModelTable(
            model_name=model_name,
            tenant_id=self._tenant_id,
            repo_path=repo_path,
            current_version=version,
            kind=kind,
            time_column=time_column,
            unique_key=unique_key,
            materialization=materialization,
            owner=owner,
            tags=json.dumps(tags) if tags else None,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get(self, model_name: str) -> ModelTable | None:
        """Fetch a single model by its canonical name."""
        stmt = select(ModelTable).where(
            ModelTable.tenant_id == self._tenant_id,
            ModelTable.model_name == model_name,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ModelTable]:
        """Return registered models, ordered by name, with pagination.

        Parameters
        ----------
        limit:
            Maximum number of rows to return (capped at ``_MAX_MODEL_PAGE_SIZE``).
        offset:
            Number of rows to skip before returning results.
        """
        limit = max(1, min(limit, _MAX_MODEL_PAGE_SIZE))
        offset = max(offset, 0)
        stmt = (
            select(ModelTable)
            .where(ModelTable.tenant_id == self._tenant_id)
            .order_by(ModelTable.model_name)
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_filtered(
        self,
        kind: str | None = None,
        owner: str | None = None,
        search: str | None = None,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ModelTable]:
        """Return models matching the given filters, with pagination.

        Parameters
        ----------
        kind:
            Filter by model kind (e.g. ``"VIEW"``, ``"TABLE"``).
        owner:
            Filter by owner.
        search:
            Case-insensitive substring match on model_name.
        limit:
            Maximum number of rows to return (capped at ``_MAX_MODEL_PAGE_SIZE``).
        offset:
            Number of rows to skip before returning results.
        """
        limit = max(1, min(limit, _MAX_MODEL_PAGE_SIZE))
        offset = max(offset, 0)
        stmt = select(ModelTable).where(ModelTable.tenant_id == self._tenant_id)
        if kind:
            stmt = stmt.where(ModelTable.kind == kind)
        if owner:
            stmt = stmt.where(ModelTable.owner == owner)
        if search:
            stmt = stmt.where(ModelTable.model_name.ilike(f"%{_escape_like(search)}%"))
        stmt = stmt.order_by(ModelTable.model_name).limit(limit).offset(offset)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def update_version(self, model_name: str, new_version: str) -> None:
        """Set *current_version* for an existing model."""
        stmt = (
            update(ModelTable)
            .where(
                ModelTable.tenant_id == self._tenant_id,
                ModelTable.model_name == model_name,
            )
            .values(current_version=new_version)
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def delete(self, model_name: str) -> None:
        """Remove a model and cascade to related version rows."""
        stmt = delete(ModelTable).where(
            ModelTable.tenant_id == self._tenant_id,
            ModelTable.model_name == model_name,
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def get_models_batch(self, model_names: list[str]) -> dict[str, Any]:
        """Fetch multiple model rows by name in a single ``WHERE name IN (...)`` query.

        Returns a mapping of ``{model_name: ModelTable}`` for only those models
        that exist in the database.  Absent names are not included.
        """
        if not model_names:
            return {}
        stmt = select(ModelTable).where(
            ModelTable.tenant_id == self._tenant_id,
            ModelTable.model_name.in_(model_names),
        )
        result = await self._session.execute(stmt)
        return {row.model_name: row for row in result.scalars().all()}


# ---------------------------------------------------------------------------
# SnapshotRepository
# ---------------------------------------------------------------------------


class SnapshotRepository:
    """CRUD operations for the ``snapshots`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create_snapshot(
        self,
        environment: str,
        model_versions: dict[str, str],
    ) -> SnapshotTable:
        """Persist a new snapshot mapping model names to version IDs.

        Parameters
        ----------
        environment:
            Target environment name (e.g. ``"production"``).
        model_versions:
            Mapping of ``{model_name: version_id}``.
        """
        import hashlib

        # Derive a deterministic snapshot ID from tenant, environment, and sorted model versions.
        hasher = hashlib.sha256()
        hasher.update(self._tenant_id.encode("utf-8"))
        hasher.update(environment.encode("utf-8"))
        for name in sorted(model_versions):
            hasher.update(name.encode("utf-8"))
            hasher.update(model_versions[name].encode("utf-8"))
        snapshot_id = hasher.hexdigest()

        row = SnapshotTable(
            snapshot_id=snapshot_id,
            tenant_id=self._tenant_id,
            environment=environment,
            model_versions_json=json.dumps(model_versions, sort_keys=True),
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_latest(self, environment: str) -> SnapshotTable | None:
        """Return the most recent snapshot for *environment*."""
        stmt = (
            select(SnapshotTable)
            .where(
                SnapshotTable.tenant_id == self._tenant_id,
                SnapshotTable.environment == environment,
            )
            .order_by(SnapshotTable.created_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, snapshot_id: str) -> SnapshotTable | None:
        """Fetch a snapshot by its unique identifier."""
        stmt = select(SnapshotTable).where(
            SnapshotTable.tenant_id == self._tenant_id,
            SnapshotTable.snapshot_id == snapshot_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# WatermarkRepository
# ---------------------------------------------------------------------------


class WatermarkRepository:
    """CRUD operations for the ``watermarks`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def get_watermark(self, model_name: str) -> tuple[date, date] | None:
        """Return the latest partition range for *model_name*, or ``None``.

        "Latest" is determined by the most recent ``last_updated`` timestamp.
        """
        stmt = (
            select(WatermarkTable.partition_start, WatermarkTable.partition_end)
            .where(
                WatermarkTable.tenant_id == self._tenant_id,
                WatermarkTable.model_name == model_name,
            )
            .order_by(WatermarkTable.last_updated.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.one_or_none()
        if row is None:
            return None
        return (row.partition_start, row.partition_end)

    async def update_watermark(
        self,
        model_name: str,
        partition_start: date,
        partition_end: date,
        row_count: int | None,
    ) -> None:
        """Upsert a watermark for the given model and partition range.

        Uses dialect-aware upsert keyed on
        ``(tenant_id, model_name, partition_start, partition_end)``.
        """
        now = datetime.now(UTC)
        await _dialect_upsert(
            self._session,
            WatermarkTable,
            values={
                "tenant_id": self._tenant_id,
                "model_name": model_name,
                "partition_start": partition_start,
                "partition_end": partition_end,
                "row_count": row_count,
                "last_updated": now,
            },
            index_elements=["tenant_id", "model_name", "partition_start", "partition_end"],
            update_columns=["row_count", "last_updated"],
        )
        await self._session.flush()

    async def get_all_for_model(self, model_name: str) -> list[WatermarkTable]:
        """Return all watermark records for *model_name*, ordered by partition start."""
        stmt = (
            select(WatermarkTable)
            .where(
                WatermarkTable.tenant_id == self._tenant_id,
                WatermarkTable.model_name == model_name,
            )
            .order_by(WatermarkTable.partition_start)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_watermarks_batch(self, model_names: list[str]) -> dict[str, Any]:
        """Return the latest watermark for each of the given model names in one query.

        Uses ``WHERE model_name IN (...)`` to avoid N+1 per-model round trips.
        Returns a mapping of ``{model_name: (partition_start, partition_end)}``.
        Models that have no watermark are absent from the result.
        """
        if not model_names:
            return {}

        # Subquery: rank watermarks per model by last_updated desc so we can
        # pick the most-recent row for each model name.
        ranked = (
            select(
                WatermarkTable.model_name,
                WatermarkTable.partition_start,
                WatermarkTable.partition_end,
                func.row_number()
                .over(
                    partition_by=WatermarkTable.model_name,
                    order_by=WatermarkTable.last_updated.desc(),
                )
                .label("rn"),
            )
            .where(
                WatermarkTable.tenant_id == self._tenant_id,
                WatermarkTable.model_name.in_(model_names),
            )
            .subquery()
        )

        stmt = select(
            ranked.c.model_name,
            ranked.c.partition_start,
            ranked.c.partition_end,
        ).where(ranked.c.rn == 1)

        result = await self._session.execute(stmt)
        return {row.model_name: (row.partition_start, row.partition_end) for row in result.all()}


# ---------------------------------------------------------------------------
# LockRepository
# ---------------------------------------------------------------------------


class LockRepository:
    """Advisory lock management for model partition ranges.

    Locks are row-based with a TTL.  ``acquire_lock`` performs an atomic
    check-and-insert: if a non-expired lock already exists the acquisition
    fails; expired locks are reaped transparently.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def acquire_lock(
        self,
        model_name: str,
        range_start: date,
        range_end: date,
        locked_by: str,
        ttl_seconds: int = 3600,
    ) -> bool:
        """Attempt to acquire an advisory lock on a partition range.

        Returns ``True`` if the lock was acquired, ``False`` if a non-expired
        lock already exists for the same ``(model_name, range_start, range_end)``.
        Expired locks are deleted automatically before the insert attempt.
        """
        now = datetime.now(UTC)

        # 1. Delete any expired lock for this exact range.
        expire_stmt = delete(LockTable).where(
            LockTable.tenant_id == self._tenant_id,
            LockTable.model_name == model_name,
            LockTable.range_start == range_start,
            LockTable.range_end == range_end,
            LockTable.locked_at + func.make_interval(0, 0, 0, 0, 0, 0, LockTable.ttl_seconds) < now,
        )
        await self._session.execute(expire_stmt)

        # 2. Atomically insert the lock — ON CONFLICT DO NOTHING eliminates
        #    the TOCTOU race between a SELECT check and a subsequent INSERT.
        result = await _dialect_upsert_nothing(
            self._session,
            LockTable,
            values={
                "tenant_id": self._tenant_id,
                "model_name": model_name,
                "range_start": range_start,
                "range_end": range_end,
                "locked_by": locked_by,
                "locked_at": now,
                "ttl_seconds": ttl_seconds,
            },
            index_elements=["tenant_id", "model_name", "range_start", "range_end"],
        )
        await self._session.flush()
        return (result.rowcount or 0) > 0  # type: ignore[attr-defined]

    async def release_lock(
        self,
        model_name: str,
        range_start: date,
        range_end: date,
    ) -> None:
        """Release (delete) a lock on a specific partition range."""
        stmt = delete(LockTable).where(
            LockTable.tenant_id == self._tenant_id,
            LockTable.model_name == model_name,
            LockTable.range_start == range_start,
            LockTable.range_end == range_end,
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def check_lock(
        self,
        model_name: str,
        range_start: date,
        range_end: date,
    ) -> bool:
        """Return ``True`` if a non-expired lock exists for the given range."""
        now = datetime.now(UTC)
        stmt = select(func.count()).where(
            LockTable.tenant_id == self._tenant_id,
            LockTable.model_name == model_name,
            LockTable.range_start == range_start,
            LockTable.range_end == range_end,
            LockTable.locked_at + func.make_interval(0, 0, 0, 0, 0, 0, LockTable.ttl_seconds) >= now,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one() > 0

    async def expire_stale_locks(self, batch_size: int = 1000) -> int:
        """Delete expired locks in batches to avoid long table locks.

        An unbounded ``DELETE … WHERE expires_at < now()`` can hold a
        write lock on every expired row simultaneously, which becomes a
        multi-minute stall after an extended downtime.  Chunked deletion
        caps the lock duration to the time needed to delete at most
        ``batch_size`` rows per iteration and yields the event loop
        between batches so other coroutines are not starved.

        Returns the total number of expired locks removed across all batches.
        """
        now = datetime.now(UTC)
        total = 0
        while True:
            stmt = (
                delete(LockTable)
                .where(
                    LockTable.tenant_id == self._tenant_id,
                    LockTable.locked_at + func.make_interval(0, 0, 0, 0, 0, 0, LockTable.ttl_seconds) < now,
                )
                .limit(batch_size)
            )
            result = await self._session.execute(stmt)
            await self._session.flush()
            total += result.rowcount  # type: ignore[attr-defined]
            if result.rowcount < batch_size:  # type: ignore[attr-defined]
                break
            await asyncio.sleep(0)  # yield to event loop between batches
        return total

    async def force_release_lock(
        self,
        model_name: str,
        range_start: date,
        range_end: date,
        released_by: str,
        reason: str,
    ) -> bool:
        """Forcibly release a lock with audit trail. Returns True if a lock was released.

        Before deleting the lock row, an audit log entry is created to
        preserve who originally owned the lock and why it was force-released.
        """
        # Read the lock row to capture the original owner for audit.
        lock_stmt = select(LockTable).where(
            LockTable.tenant_id == self._tenant_id,
            LockTable.model_name == model_name,
            LockTable.range_start == range_start,
            LockTable.range_end == range_end,
        )
        lock_result = await self._session.execute(lock_stmt)
        lock_row = lock_result.scalar_one_or_none()

        if lock_row is None:
            return False

        # Create audit log entry before releasing, preserving who owned
        # the lock and why it was force-released.
        audit_metadata = {
            "action": "lock.force_release",
            "original_owner": lock_row.locked_by,
            "reason": reason,
            "released_by": released_by,
            "model_name": model_name,
            "range": f"{range_start}:{range_end}",
        }
        audit_row = AuditLogTable(
            id=uuid.uuid4().hex,
            tenant_id=self._tenant_id,
            actor=released_by,
            action="lock.force_release",
            entity_type="lock",
            entity_id=f"{model_name}:{range_start}:{range_end}",
            metadata_json=audit_metadata,
            previous_hash=None,
            entry_hash=hashlib.sha256(json.dumps(audit_metadata, sort_keys=True).encode("utf-8")).hexdigest(),
            created_at=datetime.now(UTC),
        )
        self._session.add(audit_row)
        await self._session.flush()

        # Now delete the lock.
        del_stmt = delete(LockTable).where(
            LockTable.tenant_id == self._tenant_id,
            LockTable.model_name == model_name,
            LockTable.range_start == range_start,
            LockTable.range_end == range_end,
        )
        await self._session.execute(del_stmt)
        await self._session.flush()
        return True


# ---------------------------------------------------------------------------
# TelemetryRepository
# ---------------------------------------------------------------------------


class TelemetryRepository:
    """CRUD operations for the ``telemetry`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record(self, telemetry: dict[str, Any]) -> TelemetryTable:
        """Persist a telemetry record from a dictionary of field values.

        Expected keys: ``run_id``, ``model_name``, ``runtime_seconds``,
        ``shuffle_bytes``, ``input_rows``, ``output_rows``, ``partition_count``,
        and optionally ``cluster_id``.
        """
        row = TelemetryTable(
            tenant_id=self._tenant_id,
            run_id=telemetry["run_id"],
            model_name=telemetry["model_name"],
            runtime_seconds=telemetry["runtime_seconds"],
            shuffle_bytes=telemetry["shuffle_bytes"],
            input_rows=telemetry["input_rows"],
            output_rows=telemetry["output_rows"],
            partition_count=telemetry["partition_count"],
            cluster_id=telemetry.get("cluster_id"),
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_for_run(self, run_id: str) -> list[TelemetryTable]:
        """Return all telemetry entries for a given run."""
        stmt = (
            select(TelemetryTable)
            .where(
                TelemetryTable.tenant_id == self._tenant_id,
                TelemetryTable.run_id == run_id,
            )
            .order_by(TelemetryTable.captured_at)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_model(
        self,
        model_name: str,
        limit: int = 100,
    ) -> list[TelemetryTable]:
        """Return the most recent telemetry entries for a model."""
        stmt = (
            select(TelemetryTable)
            .where(
                TelemetryTable.tenant_id == self._tenant_id,
                TelemetryTable.model_name == model_name,
            )
            .order_by(TelemetryTable.captured_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def cleanup_old_telemetry(self, retention_days: int = 30) -> int:
        """Delete telemetry records older than retention_days. Returns count deleted."""
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        stmt = delete(TelemetryTable).where(
            TelemetryTable.tenant_id == self._tenant_id,
            TelemetryTable.captured_at < cutoff,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# CredentialRepository
# ---------------------------------------------------------------------------


class CredentialRepository:
    """CRUD operations for the ``credentials`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def store(self, credential_name: str, encrypted_value: str) -> None:
        """Upsert an encrypted credential."""
        await _dialect_upsert(
            self._session,
            CredentialTable,
            values={
                "tenant_id": self._tenant_id,
                "credential_name": credential_name,
                "encrypted_value": encrypted_value,
                "last_rotated_at": datetime.now(UTC),
            },
            index_elements=["tenant_id", "credential_name"],
            update_columns=["encrypted_value", "last_rotated_at"],
        )
        await self._session.flush()

    async def get(self, credential_name: str) -> str | None:
        """Retrieve the encrypted value for a credential. Returns None if not found."""
        stmt = select(CredentialTable.encrypted_value).where(
            CredentialTable.tenant_id == self._tenant_id,
            CredentialTable.credential_name == credential_name,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return row

    async def delete(self, credential_name: str) -> bool:
        """Delete a credential. Returns True if it existed."""
        stmt = delete(CredentialTable).where(
            CredentialTable.tenant_id == self._tenant_id,
            CredentialTable.credential_name == credential_name,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def list_names(self) -> list[str]:
        """List credential names (not values) for the tenant."""
        stmt = (
            select(CredentialTable.credential_name)
            .where(CredentialTable.tenant_id == self._tenant_id)
            .order_by(CredentialTable.credential_name)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# AuditRepository
# ---------------------------------------------------------------------------


class AuditRepository:
    """Append-only audit log repository with hash-chaining for tamper evidence.

    Each audit entry is linked to its predecessor via ``previous_hash``, forming
    a per-tenant tamper-evident chain.  ``entry_hash`` is a SHA-256 digest of
    the entry's content fields concatenated with the previous hash, so any
    modification to an existing row will break the chain for all subsequent
    entries.
    """

    def __init__(self, session: AsyncSession, *, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    @staticmethod
    def _compute_hash(
        tenant_id: str,
        actor: str,
        action: str,
        entity_type: str | None,
        entity_id: str | None,
        metadata: dict | None,
        previous_hash: str | None,
        created_at: datetime,
    ) -> str:
        """Compute SHA-256 hash over entry content fields.

        The hash is computed over the concatenation of all content fields
        separated by ``|``.  ``None`` values are represented as the empty
        string in the hash input.
        """
        parts = [
            tenant_id,
            actor,
            action,
            entity_type or "",
            entity_id or "",
            json.dumps(metadata, sort_keys=True, default=str) if metadata else "",
            previous_hash or "",
            created_at.isoformat(),
        ]
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        return digest

    async def get_latest_hash(self) -> str | None:
        """Get the entry_hash of the most recent audit log entry for this tenant."""
        stmt = (
            select(AuditLogTable.entry_hash)
            .where(AuditLogTable.tenant_id == self._tenant_id)
            .order_by(AuditLogTable.created_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def log(
        self,
        *,
        actor: str,
        action: str,
        entity_type: str | None = None,
        entity_id: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Write an audit entry. Returns the entry ID.

        Computes ``entry_hash`` as SHA-256 of the entry's content fields
        concatenated with the ``previous_hash`` from the last entry in
        this tenant's chain.
        """
        entry_id = uuid.uuid4().hex
        now = datetime.now(UTC)

        # Acquire advisory lock to prevent hash chain race (TOCTOU).
        # Two concurrent inserts could both read the same previous_hash,
        # creating a fork in the hash chain.
        lock_id = hash(f"audit_chain_{self._tenant_id}") & 0x7FFFFFFF
        bind = self._session.get_bind()
        dialect_name = getattr(getattr(bind, "dialect", None), "name", "")
        if "postgresql" in str(dialect_name):
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
        # For SQLite: single-writer semantics, no advisory lock needed.

        previous_hash = await self.get_latest_hash()

        entry_hash = self._compute_hash(
            tenant_id=self._tenant_id,
            actor=actor,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            metadata=metadata,
            previous_hash=previous_hash,
            created_at=now,
        )

        row = AuditLogTable(
            id=entry_id,
            tenant_id=self._tenant_id,
            actor=actor,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            metadata_json=metadata,
            previous_hash=previous_hash,
            entry_hash=entry_hash,
            created_at=now,
        )
        self._session.add(row)
        await self._session.flush()

        logger.info(
            "Audit: tenant=%s actor=%s action=%s entity=%s/%s",
            self._tenant_id,
            actor,
            action,
            entity_type or "-",
            entity_id or "-",
        )
        return entry_id

    async def query(
        self,
        *,
        action: str | None = None,
        entity_type: str | None = None,
        entity_id: str | None = None,
        since: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[AuditLogTable]:
        """Query audit log entries with filters.

        All filters are optional; when omitted the corresponding predicate
        is not applied.  Results are ordered by ``created_at`` descending
        (most recent first).
        """
        stmt = select(AuditLogTable).where(AuditLogTable.tenant_id == self._tenant_id)

        if action is not None:
            stmt = stmt.where(AuditLogTable.action == action)
        if entity_type is not None:
            stmt = stmt.where(AuditLogTable.entity_type == entity_type)
        if entity_id is not None:
            stmt = stmt.where(AuditLogTable.entity_id == entity_id)
        if since is not None:
            stmt = stmt.where(AuditLogTable.created_at >= since)

        stmt = stmt.order_by(AuditLogTable.created_at.desc()).limit(limit).offset(offset)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def verify_chain(self, *, limit: int = 1000) -> tuple[bool, int]:
        """Verify the hash chain integrity for this tenant.

        Reads the most recent *limit* entries (oldest first) and recomputes
        each entry's hash, comparing it against the stored ``entry_hash``
        and verifying that ``previous_hash`` matches the preceding entry's
        ``entry_hash``.

        GDPR anonymization handling
        ---------------------------
        Entries marked ``is_anonymized=True`` have had their ``actor`` and
        ``metadata_json`` fields redacted after the original ``entry_hash``
        was computed.  Recomputing the hash from current field values would
        produce a different result, causing a spurious mismatch.  For these
        entries the hash recomputation step is skipped — the stored
        ``entry_hash`` advances the chain link so all surrounding
        non-anonymized entries remain fully verifiable.

        Returns
        -------
        tuple[bool, int]
            ``(is_valid, entries_checked)`` where ``is_valid`` is ``True``
            only if every non-anonymized entry's hash and chain link are
            intact.  ``entries_checked`` counts only entries whose hash was
            actively verified (anonymized entries do not contribute).
        """
        stmt = (
            select(AuditLogTable)
            .where(AuditLogTable.tenant_id == self._tenant_id)
            .order_by(AuditLogTable.created_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        entries = list(result.scalars().all())

        if not entries:
            return (True, 0)

        checked = 0
        anonymized_skipped = 0
        previous_hash: str | None = None

        for entry in entries:
            # Verify the chain link: this entry's previous_hash must match
            # the prior entry's entry_hash (or None for the first entry).
            if entry.previous_hash != previous_hash:
                logger.warning(
                    "Audit chain break at entry %s: expected previous_hash=%s, got=%s",
                    entry.id,
                    previous_hash,
                    entry.previous_hash,
                )
                return (False, checked)

            # Anonymized entries: hash was computed from original (now erased)
            # data — recomputation is not possible.  Use the stored entry_hash
            # to advance the chain and continue verifying surrounding entries.
            if entry.is_anonymized:
                previous_hash = entry.entry_hash
                anonymized_skipped += 1
                continue

            # Recompute the entry hash and compare.
            expected_hash = self._compute_hash(
                tenant_id=entry.tenant_id,
                actor=entry.actor,
                action=entry.action,
                entity_type=entry.entity_type,
                entity_id=entry.entity_id,
                metadata=entry.metadata_json,
                previous_hash=entry.previous_hash,
                created_at=entry.created_at,
            )

            if entry.entry_hash != expected_hash:
                logger.warning(
                    "Audit hash mismatch at entry %s: stored=%s, computed=%s",
                    entry.id,
                    entry.entry_hash,
                    expected_hash,
                )
                return (False, checked)

            previous_hash = entry.entry_hash
            checked += 1

        if anonymized_skipped:
            logger.info(
                "verify_chain: %d entries verified, %d anonymized entries skipped (GDPR erasure)",
                checked,
                anonymized_skipped,
            )

        return (True, checked)

    async def cleanup_old_entries(self, retention_days: int) -> int:
        """Delete audit log entries older than *retention_days* for this tenant.

        Returns the number of rows deleted.

        Note: this deletes the oldest entries in the chain.  Because
        ``verify_chain`` queries entries oldest-first up to a *limit*, deleting
        the bottom of the chain does not affect verification of recent entries.
        """
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        result = await self._session.execute(
            delete(AuditLogTable)
            .where(AuditLogTable.tenant_id == self._tenant_id)
            .where(AuditLogTable.created_at < cutoff)
        )
        return result.rowcount

    async def anonymize_user_entries(self, user_id: str) -> int:
        """GDPR right-to-erasure: replace user PII in audit logs with ``[REDACTED]``.

        Sets ``actor`` to ``"[REDACTED]"``, clears ``metadata_json``, and
        marks ``is_anonymized=True`` so :meth:`verify_chain` can advance the
        hash-chain through these entries without treating them as tampering.

        Returns the number of rows updated.
        """
        result = await self._session.execute(
            update(AuditLogTable)
            .where(AuditLogTable.tenant_id == self._tenant_id)
            .where(AuditLogTable.actor == user_id)
            .values(actor="[REDACTED]", metadata_json=None, is_anonymized=True)
        )
        return result.rowcount


# ---------------------------------------------------------------------------
# TokenRevocationRepository
# ---------------------------------------------------------------------------


class TokenRevocationRepository:
    """CRUD operations for the ``token_revocations`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def revoke(
        self,
        jti: str,
        reason: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        """Record a token revocation by jti. Idempotent via upsert."""
        await _dialect_upsert(
            self._session,
            TokenRevocationTable,
            values={
                "tenant_id": self._tenant_id,
                "jti": jti,
                "reason": reason,
                "expires_at": expires_at,
                "revoked_at": datetime.now(UTC),
            },
            index_elements=["tenant_id", "jti"],
            update_columns=["reason", "revoked_at"],
        )
        await self._session.flush()

    async def is_revoked(self, jti: str, *, tenant_id: str | None = None) -> bool:
        """Check whether a token with the given jti has been revoked.

        By default this performs a **cross-tenant** lookup so that the
        authentication middleware can reject revoked tokens regardless of
        which tenant issued them.  This is intentional: token revocation
        must be globally effective to prevent a revoked token from being
        replayed against a different tenant's endpoints.

        Pass ``tenant_id`` to restrict the check to a single tenant
        (useful for tenant-scoped revocation queries in admin UIs).
        """
        stmt = select(func.count()).where(
            TokenRevocationTable.jti == jti,
        )
        if tenant_id is not None:
            stmt = stmt.where(TokenRevocationTable.tenant_id == tenant_id)
        result = await self._session.execute(stmt)
        return result.scalar_one() > 0

    async def cleanup_expired(self) -> int:
        """Remove revocation entries whose tokens have already expired for this tenant.

        Scoped to ``self._tenant_id`` so that each tenant's cleanup job
        only deletes its own expired revocations.

        Returns the number of records deleted.
        """
        now = datetime.now(UTC)
        stmt = delete(TokenRevocationTable).where(
            TokenRevocationTable.tenant_id == self._tenant_id,
            TokenRevocationTable.expires_at.is_not(None),
            TokenRevocationTable.expires_at < now,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[attr-defined]

    async def cleanup_all_expired(self) -> int:
        """Remove expired revocation entries across **all** tenants.

        This is an admin-only operation intended for background maintenance
        jobs that run outside any single tenant context.  It should only be
        called from admin endpoints guarded by appropriate permissions
        (e.g. ``require_permission(Permission.MANAGE_SETTINGS)``).

        Returns the number of records deleted.
        """
        now = datetime.now(UTC)
        stmt = delete(TokenRevocationTable).where(
            TokenRevocationTable.expires_at.is_not(None),
            TokenRevocationTable.expires_at < now,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# TenantConfigRepository
# ---------------------------------------------------------------------------


class TenantConfigRepository:
    """CRUD operations for the ``tenant_config`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def get(self) -> TenantConfigTable | None:
        """Fetch config for this tenant. Returns None if no row exists."""
        stmt = select(TenantConfigTable).where(
            TenantConfigTable.tenant_id == self._tenant_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert(
        self,
        *,
        llm_enabled: bool,
        updated_by: str = "system",
        llm_monthly_budget_usd: float | None = None,
        llm_daily_budget_usd: float | None = None,
        plan_quota_monthly: int | None = ...,  # type: ignore[assignment]
        api_quota_monthly: int | None = ...,  # type: ignore[assignment]
        ai_quota_monthly: int | None = ...,  # type: ignore[assignment]
    ) -> TenantConfigTable:
        """Create or update tenant configuration.

        Quota parameters use sentinel ``...`` to distinguish between "not
        provided" (leave unchanged) and explicit ``None`` (clear the quota,
        meaning unlimited).
        """
        values: dict[str, Any] = {
            "tenant_id": self._tenant_id,
            "llm_enabled": llm_enabled,
            "updated_by": updated_by,
        }
        if llm_monthly_budget_usd is not None:
            values["llm_monthly_budget_usd"] = llm_monthly_budget_usd
        if llm_daily_budget_usd is not None:
            values["llm_daily_budget_usd"] = llm_daily_budget_usd
        if plan_quota_monthly is not ...:
            values["plan_quota_monthly"] = plan_quota_monthly
        if api_quota_monthly is not ...:
            values["api_quota_monthly"] = api_quota_monthly
        if ai_quota_monthly is not ...:
            values["ai_quota_monthly"] = ai_quota_monthly

        values["updated_at"] = datetime.now(UTC)
        update_cols = ["llm_enabled", "updated_by", "updated_at"]
        if llm_monthly_budget_usd is not None:
            update_cols.append("llm_monthly_budget_usd")
        if llm_daily_budget_usd is not None:
            update_cols.append("llm_daily_budget_usd")
        if plan_quota_monthly is not ...:
            update_cols.append("plan_quota_monthly")
        if api_quota_monthly is not ...:
            update_cols.append("api_quota_monthly")
        if ai_quota_monthly is not ...:
            update_cols.append("ai_quota_monthly")

        await _dialect_upsert(
            self._session,
            TenantConfigTable,
            values=values,
            index_elements=["tenant_id"],
            update_columns=update_cols,
        )
        await self._session.flush()

        # Return the row.
        return await self.get()  # type: ignore[return-value]

    async def create(
        self,
        *,
        llm_enabled: bool = True,
        created_by: str = "system",
    ) -> TenantConfigTable:
        """Provision a new tenant with default configuration.

        Raises
        ------
        ValueError
            If the tenant already exists.
        """
        row = TenantConfigTable(
            tenant_id=self._tenant_id,
            llm_enabled=llm_enabled,
            updated_by=created_by,
        )
        try:
            self._session.add(row)
            await self._session.flush()
        except IntegrityError:
            await self._session.rollback()
            raise ValueError(f"Tenant '{self._tenant_id}' already exists")
        return row

    async def deactivate(self, *, deactivated_by: str = "system") -> TenantConfigTable | None:
        """Soft-delete a tenant by setting ``deactivated_at``.

        Returns the updated row, or ``None`` if the tenant does not exist.
        """
        row = await self.get()
        if row is None:
            return None
        row.deactivated_at = datetime.now(UTC)
        row.updated_by = deactivated_by
        await self._session.flush()
        return row

    async def list_all(self, *, include_deactivated: bool = False) -> list[TenantConfigTable]:
        """List all tenant configurations (**cross-tenant**).

        .. warning:: **Intentionally cross-tenant**

           This method does NOT filter by ``self._tenant_id``.  It returns
           rows for **all** tenants because its callers need a global view:

           - ``tenant_config.py:list_tenants`` -- admin endpoint protected
             by ``require_permission(Permission.MANAGE_SETTINGS)``
           - ``customer_health_service.py:compute_all`` -- admin-invoked
             health recomputation over all tenants

           Always call from admin contexts with appropriate permission guards.

        By default only active tenants are returned.  Pass
        ``include_deactivated=True`` to include soft-deleted tenants.
        """
        stmt = select(TenantConfigTable).order_by(TenantConfigTable.tenant_id)
        if not include_deactivated:
            stmt = stmt.where(TenantConfigTable.deactivated_at.is_(None))
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# ReconciliationRepository
# ---------------------------------------------------------------------------


class ReconciliationRepository:
    """CRUD operations for the ``reconciliation_checks`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_check(
        self,
        run_id: str,
        model_name: str,
        expected_status: str,
        warehouse_status: str,
        discrepancy_type: str | None = None,
    ) -> ReconciliationCheckTable:
        """Record a reconciliation check result."""
        row = ReconciliationCheckTable(
            tenant_id=self._tenant_id,
            run_id=run_id,
            model_name=model_name,
            expected_status=expected_status,
            warehouse_status=warehouse_status,
            discrepancy_type=discrepancy_type,
            resolved=discrepancy_type is None,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_unresolved(self, limit: int = 100) -> list[ReconciliationCheckTable]:
        """Return unresolved discrepancies for this tenant."""
        stmt = (
            select(ReconciliationCheckTable)
            .where(
                ReconciliationCheckTable.tenant_id == self._tenant_id,
                ReconciliationCheckTable.resolved.is_(False),
                ReconciliationCheckTable.discrepancy_type.is_not(None),
            )
            .order_by(ReconciliationCheckTable.checked_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def resolve(
        self,
        check_id: int,
        resolved_by: str,
        resolution_note: str,
    ) -> ReconciliationCheckTable | None:
        """Mark a discrepancy as resolved."""
        stmt = select(ReconciliationCheckTable).where(
            ReconciliationCheckTable.tenant_id == self._tenant_id,
            ReconciliationCheckTable.id == check_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        row.resolved = True
        row.resolved_by = resolved_by
        row.resolved_at = datetime.now(UTC)
        row.resolution_note = resolution_note
        await self._session.flush()
        return row

    async def get_stats(self) -> dict[str, Any]:
        """Return summary statistics for reconciliation checks."""
        total_stmt = select(func.count()).where(
            ReconciliationCheckTable.tenant_id == self._tenant_id,
        )
        total_result = await self._session.execute(total_stmt)
        total = total_result.scalar_one()

        discrepancy_stmt = select(func.count()).where(
            ReconciliationCheckTable.tenant_id == self._tenant_id,
            ReconciliationCheckTable.discrepancy_type.is_not(None),
        )
        disc_result = await self._session.execute(discrepancy_stmt)
        discrepancies = disc_result.scalar_one()

        unresolved_stmt = select(func.count()).where(
            ReconciliationCheckTable.tenant_id == self._tenant_id,
            ReconciliationCheckTable.discrepancy_type.is_not(None),
            ReconciliationCheckTable.resolved.is_(False),
        )
        unresolved_result = await self._session.execute(unresolved_stmt)
        unresolved = unresolved_result.scalar_one()

        return {
            "total_checks": total,
            "total_discrepancies": discrepancies,
            "unresolved_discrepancies": unresolved,
            "resolved_discrepancies": discrepancies - unresolved,
        }


# ---------------------------------------------------------------------------
# AIFeedbackRepository
# ---------------------------------------------------------------------------


class AIFeedbackRepository:
    """CRUD operations for the ``ai_feedback`` table.

    Records AI predictions (cost, risk, classification) and their actual
    outcomes after execution, enabling accuracy tracking and model retraining.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_prediction(
        self,
        plan_id: str,
        step_id: str,
        model_name: str,
        feedback_type: str,
        prediction: dict,
    ) -> AIFeedbackTable:
        """Record an AI prediction before execution."""
        row = AIFeedbackTable(
            tenant_id=self._tenant_id,
            plan_id=plan_id,
            step_id=step_id,
            model_name=model_name,
            feedback_type=feedback_type,
            prediction_json=prediction,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def record_outcome(
        self,
        plan_id: str,
        step_id: str,
        model_name: str,
        feedback_type: str,
        outcome: dict,
        accuracy_score: float | None = None,
    ) -> None:
        """Record the actual outcome and compute accuracy for a prediction.

        Looks up the most recent matching prediction and attaches the outcome.
        If no matching prediction exists, creates an outcome-only entry.
        """
        stmt = (
            select(AIFeedbackTable)
            .where(
                AIFeedbackTable.tenant_id == self._tenant_id,
                AIFeedbackTable.plan_id == plan_id,
                AIFeedbackTable.step_id == step_id,
                AIFeedbackTable.model_name == model_name,
                AIFeedbackTable.feedback_type == feedback_type,
            )
            .order_by(AIFeedbackTable.created_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is not None:
            row.outcome_json = outcome
            row.accuracy_score = accuracy_score
            await self._session.flush()
        else:
            new_row = AIFeedbackTable(
                tenant_id=self._tenant_id,
                plan_id=plan_id,
                step_id=step_id,
                model_name=model_name,
                feedback_type=feedback_type,
                outcome_json=outcome,
                accuracy_score=accuracy_score,
            )
            self._session.add(new_row)
            await self._session.flush()

    async def mark_accepted(
        self,
        plan_id: str,
        step_id: str,
        model_name: str,
        feedback_type: str,
        accepted: bool,
    ) -> bool:
        """Mark whether an AI suggestion was accepted or rejected.

        Returns ``True`` if at least one row was updated.
        """
        stmt = (
            update(AIFeedbackTable)
            .where(
                AIFeedbackTable.tenant_id == self._tenant_id,
                AIFeedbackTable.plan_id == plan_id,
                AIFeedbackTable.step_id == step_id,
                AIFeedbackTable.model_name == model_name,
                AIFeedbackTable.feedback_type == feedback_type,
            )
            .values(accepted=accepted)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def get_accuracy_stats(
        self,
        feedback_type: str | None = None,
        model_name: str | None = None,
    ) -> dict[str, Any]:
        """Compute aggregate accuracy and acceptance statistics.

        Returns
        -------
        dict
            ``{"avg_accuracy": float | None,
               "acceptance_rate": float | None,
               "record_count": int}``
        """
        # --- record count & average accuracy ---
        base = select(AIFeedbackTable).where(
            AIFeedbackTable.tenant_id == self._tenant_id,
            AIFeedbackTable.accuracy_score.is_not(None),
        )
        if feedback_type:
            base = base.where(AIFeedbackTable.feedback_type == feedback_type)
        if model_name:
            base = base.where(AIFeedbackTable.model_name == model_name)

        sub = base.subquery()

        count_result = await self._session.execute(select(func.count()).select_from(sub))
        count = count_result.scalar_one()

        if count == 0:
            return {
                "avg_accuracy": None,
                "acceptance_rate": None,
                "record_count": 0,
            }

        avg_result = await self._session.execute(select(func.avg(sub.c.accuracy_score)).select_from(sub))
        avg_accuracy = avg_result.scalar_one()

        # --- acceptance rate ---
        decided_filters = [
            AIFeedbackTable.tenant_id == self._tenant_id,
            AIFeedbackTable.accepted.is_not(None),
        ]
        if feedback_type:
            decided_filters.append(AIFeedbackTable.feedback_type == feedback_type)
        if model_name:
            decided_filters.append(AIFeedbackTable.model_name == model_name)

        decided_result = await self._session.execute(select(func.count()).where(*decided_filters))
        total_decided = decided_result.scalar_one()

        accepted_filters = decided_filters + [AIFeedbackTable.accepted == True]  # noqa: E712
        accepted_result = await self._session.execute(select(func.count()).where(*accepted_filters))
        accepted_count = accepted_result.scalar_one()

        acceptance_rate = round(float(accepted_count) / float(total_decided), 4) if total_decided > 0 else None

        return {
            "avg_accuracy": float(avg_accuracy) if avg_accuracy is not None else None,
            "acceptance_rate": acceptance_rate,
            "record_count": count,
        }

    async def get_training_data(
        self,
        feedback_type: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return prediction/outcome pairs suitable for model retraining."""
        stmt = (
            select(AIFeedbackTable)
            .where(
                AIFeedbackTable.tenant_id == self._tenant_id,
                AIFeedbackTable.feedback_type == feedback_type,
                AIFeedbackTable.outcome_json.is_not(None),
                AIFeedbackTable.prediction_json.is_not(None),
            )
            .order_by(AIFeedbackTable.created_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [
            {
                "plan_id": r.plan_id,
                "step_id": r.step_id,
                "model_name": r.model_name,
                "prediction": r.prediction_json,
                "outcome": r.outcome_json,
                "accuracy_score": r.accuracy_score,
                "accepted": r.accepted,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]


# ---------------------------------------------------------------------------
# LLMUsageLogRepository
# ---------------------------------------------------------------------------


class LLMUsageLogRepository:
    """CRUD operations for the ``llm_usage_log`` table.

    Records per-call LLM usage for budget enforcement and analytics.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_usage(
        self,
        call_type: str,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
        estimated_cost_usd: float,
        latency_ms: int,
        success: bool,
        error_type: str | None = None,
    ) -> LLMUsageLogTable:
        """Persist a single LLM API call record."""
        row = LLMUsageLogTable(
            tenant_id=self._tenant_id,
            call_type=call_type,
            model_id=model_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_usd=estimated_cost_usd,
            latency_ms=latency_ms,
            success=success,
            error_type=error_type,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_period_cost(
        self,
        since: datetime,
    ) -> float:
        """Sum estimated cost for this tenant since the given timestamp.

        Returns 0.0 if no records exist in the period.
        """
        stmt = select(func.coalesce(func.sum(LLMUsageLogTable.estimated_cost_usd), 0.0)).where(
            LLMUsageLogTable.tenant_id == self._tenant_id,
            LLMUsageLogTable.created_at >= since,
        )
        result = await self._session.execute(stmt)
        return float(result.scalar_one())

    async def get_daily_cost(self) -> float:
        """Sum estimated cost for this tenant for the current UTC day.

        Returns 0.0 if no records exist today.
        """
        today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        return await self.get_period_cost(today_start)

    async def get_monthly_cost(self) -> float:
        """Sum estimated cost for this tenant for the current calendar month.

        Returns 0.0 if no records exist this month.
        """
        now = datetime.now(UTC)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return await self.get_period_cost(month_start)

    async def get_usage_stats(
        self,
        since: datetime | None = None,
    ) -> dict[str, Any]:
        """Return aggregate usage statistics for this tenant.

        Returns
        -------
        dict
            ``{"total_calls": int, "total_cost_usd": float,
               "total_input_tokens": int, "total_output_tokens": int,
               "avg_latency_ms": float | None, "success_rate": float | None}``
        """
        filters = [LLMUsageLogTable.tenant_id == self._tenant_id]
        if since:
            filters.append(LLMUsageLogTable.created_at >= since)

        count_result = await self._session.execute(select(func.count()).where(*filters))
        total_calls = count_result.scalar_one()

        if total_calls == 0:
            return {
                "total_calls": 0,
                "total_cost_usd": 0.0,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "avg_latency_ms": None,
                "success_rate": None,
            }

        agg_result = await self._session.execute(
            select(
                func.sum(LLMUsageLogTable.estimated_cost_usd).label("total_cost"),
                func.sum(LLMUsageLogTable.input_tokens).label("total_input"),
                func.sum(LLMUsageLogTable.output_tokens).label("total_output"),
                func.avg(LLMUsageLogTable.latency_ms).label("avg_latency"),
            ).where(*filters)
        )
        row = agg_result.one()

        success_filters = filters + [LLMUsageLogTable.success == True]  # noqa: E712
        success_result = await self._session.execute(select(func.count()).where(*success_filters))
        success_count = success_result.scalar_one()

        return {
            "total_calls": total_calls,
            "total_cost_usd": round(float(row.total_cost or 0), 6),
            "total_input_tokens": int(row.total_input or 0),
            "total_output_tokens": int(row.total_output or 0),
            "avg_latency_ms": round(float(row.avg_latency), 1) if row.avg_latency else None,
            "success_rate": round(float(success_count) / float(total_calls), 4),
        }


# ---------------------------------------------------------------------------
# BackfillCheckpointRepository
# ---------------------------------------------------------------------------


class BackfillCheckpointRepository:
    """CRUD operations for the ``backfill_checkpoints`` table.

    Tracks overall progress of chunked backfill operations, enabling
    checkpoint-based resume on failure.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create(
        self,
        backfill_id: str,
        model_name: str,
        overall_start: date,
        overall_end: date,
        chunk_size_days: int,
        total_chunks: int,
        cluster_size: str | None = None,
        plan_id: str | None = None,
    ) -> BackfillCheckpointTable:
        """Create a new backfill checkpoint record."""
        row = BackfillCheckpointTable(
            backfill_id=backfill_id,
            tenant_id=self._tenant_id,
            model_name=model_name,
            overall_start=overall_start,
            overall_end=overall_end,
            chunk_size_days=chunk_size_days,
            status="RUNNING",
            total_chunks=total_chunks,
            completed_chunks=0,
            cluster_size=cluster_size,
            plan_id=plan_id,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get(self, backfill_id: str) -> BackfillCheckpointTable | None:
        """Fetch a checkpoint by backfill_id."""
        stmt = select(BackfillCheckpointTable).where(
            BackfillCheckpointTable.tenant_id == self._tenant_id,
            BackfillCheckpointTable.backfill_id == backfill_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_progress(
        self,
        backfill_id: str,
        completed_through: date,
        completed_chunks: int,
    ) -> None:
        """Advance the checkpoint after a successful chunk execution."""
        stmt = (
            update(BackfillCheckpointTable)
            .where(
                BackfillCheckpointTable.tenant_id == self._tenant_id,
                BackfillCheckpointTable.backfill_id == backfill_id,
            )
            .values(
                completed_through=completed_through,
                completed_chunks=completed_chunks,
                updated_at=datetime.now(UTC),
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_completed(self, backfill_id: str) -> None:
        """Mark the backfill as successfully completed."""
        stmt = (
            update(BackfillCheckpointTable)
            .where(
                BackfillCheckpointTable.tenant_id == self._tenant_id,
                BackfillCheckpointTable.backfill_id == backfill_id,
            )
            .values(
                status="COMPLETED",
                updated_at=datetime.now(UTC),
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_failed(
        self,
        backfill_id: str,
        error_message: str,
    ) -> None:
        """Mark the backfill as failed, recording the error."""
        stmt = (
            update(BackfillCheckpointTable)
            .where(
                BackfillCheckpointTable.tenant_id == self._tenant_id,
                BackfillCheckpointTable.backfill_id == backfill_id,
            )
            .values(
                status="FAILED",
                error_message=error_message[:2000],
                updated_at=datetime.now(UTC),
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def get_resumable(
        self,
        model_name: str | None = None,
    ) -> list[BackfillCheckpointTable]:
        """Return backfill checkpoints in FAILED or RUNNING status (i.e. resumable).

        Optionally filter by model name.  Results are ordered by creation
        time descending (most recent first).
        """
        filters = [
            BackfillCheckpointTable.tenant_id == self._tenant_id,
            BackfillCheckpointTable.status.in_(["FAILED", "RUNNING"]),
        ]
        if model_name is not None:
            filters.append(BackfillCheckpointTable.model_name == model_name)

        stmt = select(BackfillCheckpointTable).where(*filters).order_by(BackfillCheckpointTable.created_at.desc())
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_for_model(
        self,
        model_name: str,
        limit: int = 20,
    ) -> list[BackfillCheckpointTable]:
        """Return all backfill checkpoints for a model, newest first."""
        stmt = (
            select(BackfillCheckpointTable)
            .where(
                BackfillCheckpointTable.tenant_id == self._tenant_id,
                BackfillCheckpointTable.model_name == model_name,
            )
            .order_by(BackfillCheckpointTable.created_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# BackfillAuditRepository
# ---------------------------------------------------------------------------


class BackfillAuditRepository:
    """CRUD operations for the ``backfill_audit`` table.

    Records per-chunk execution outcomes for backfill operations,
    providing a detailed audit trail.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_chunk(
        self,
        backfill_id: str,
        model_name: str,
        chunk_start: date,
        chunk_end: date,
        status: str,
        run_id: str | None = None,
        error_message: str | None = None,
        duration_seconds: float | None = None,
    ) -> BackfillAuditTable:
        """Record the outcome of a single backfill chunk."""
        row = BackfillAuditTable(
            tenant_id=self._tenant_id,
            backfill_id=backfill_id,
            model_name=model_name,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            status=status,
            run_id=run_id,
            error_message=error_message[:2000] if error_message else None,
            duration_seconds=duration_seconds,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_history(
        self,
        model_name: str,
        limit: int = 100,
    ) -> list[BackfillAuditTable]:
        """Return backfill audit records for a model, newest first."""
        stmt = (
            select(BackfillAuditTable)
            .where(
                BackfillAuditTable.tenant_id == self._tenant_id,
                BackfillAuditTable.model_name == model_name,
            )
            .order_by(BackfillAuditTable.executed_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_backfill(
        self,
        backfill_id: str,
    ) -> list[BackfillAuditTable]:
        """Return all audit records for a specific backfill, ordered by chunk start."""
        stmt = (
            select(BackfillAuditTable)
            .where(
                BackfillAuditTable.tenant_id == self._tenant_id,
                BackfillAuditTable.backfill_id == backfill_id,
            )
            .order_by(BackfillAuditTable.chunk_start.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# SchemaDriftRepository
# ---------------------------------------------------------------------------


class SchemaDriftRepository:
    """CRUD operations for the ``schema_drift_checks`` table.

    Records schema comparison results between expected and actual table
    schemas, tracks drift detections, and manages their resolution.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_drift(
        self,
        model_name: str,
        expected_columns: Any,
        actual_columns: Any,
        drift_type: str,
        drift_details: dict | None,
    ) -> SchemaDriftCheckTable:
        """Record a schema drift detection for a model.

        Parameters
        ----------
        model_name:
            Canonical model name where drift was detected.
        expected_columns:
            JSON-serialisable representation of the expected schema.
        actual_columns:
            JSON-serialisable representation of the actual schema.
        drift_type:
            Classification of drift: ``COLUMN_ADDED``, ``COLUMN_REMOVED``,
            ``TYPE_CHANGED``, or ``NONE``.
        drift_details:
            Detailed information about the specific drifts found.
        """
        row = SchemaDriftCheckTable(
            tenant_id=self._tenant_id,
            model_name=model_name,
            expected_columns_json=expected_columns,
            actual_columns_json=actual_columns,
            drift_type=drift_type,
            drift_details_json=drift_details,
            resolved=False,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_unresolved(self, limit: int = 100) -> list[SchemaDriftCheckTable]:
        """Return unresolved schema drift checks for this tenant.

        Results are ordered by ``checked_at`` descending (most recent first).
        """
        stmt = (
            select(SchemaDriftCheckTable)
            .where(
                SchemaDriftCheckTable.tenant_id == self._tenant_id,
                SchemaDriftCheckTable.resolved == False,  # noqa: E712
            )
            .order_by(SchemaDriftCheckTable.checked_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_model(
        self,
        model_name: str,
        limit: int = 20,
    ) -> list[SchemaDriftCheckTable]:
        """Return schema drift checks for a specific model, newest first."""
        stmt = (
            select(SchemaDriftCheckTable)
            .where(
                SchemaDriftCheckTable.tenant_id == self._tenant_id,
                SchemaDriftCheckTable.model_name == model_name,
            )
            .order_by(SchemaDriftCheckTable.checked_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def resolve(
        self,
        check_id: int,
        resolved_by: str,
        resolution_note: str,
    ) -> SchemaDriftCheckTable | None:
        """Mark a schema drift check as resolved.

        Returns the updated row, or ``None`` if no matching unresolved
        check was found.
        """
        stmt = select(SchemaDriftCheckTable).where(
            SchemaDriftCheckTable.tenant_id == self._tenant_id,
            SchemaDriftCheckTable.id == check_id,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        row.resolved = True
        row.resolved_by = resolved_by
        row.resolved_at = datetime.now(UTC)
        row.resolution_note = resolution_note
        await self._session.flush()
        return row


# ---------------------------------------------------------------------------
# ReconciliationScheduleRepository
# ---------------------------------------------------------------------------


class ReconciliationScheduleRepository:
    """CRUD operations for the ``reconciliation_schedules`` table.

    Manages configurable background reconciliation schedules including
    creation, updates, and execution tracking.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def get_schedule(
        self,
        schedule_type: str,
    ) -> ReconciliationScheduleTable | None:
        """Fetch a schedule by its type for this tenant."""
        stmt = select(ReconciliationScheduleTable).where(
            ReconciliationScheduleTable.tenant_id == self._tenant_id,
            ReconciliationScheduleTable.schedule_type == schedule_type,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert_schedule(
        self,
        schedule_type: str,
        cron_expression: str,
        enabled: bool = True,
    ) -> ReconciliationScheduleTable:
        """Create or update a reconciliation schedule.

        Uses PostgreSQL ``INSERT ... ON CONFLICT ... DO UPDATE`` for an
        atomic upsert keyed on ``(tenant_id, schedule_type)``.
        """
        now = datetime.now(UTC)
        await _dialect_upsert(
            self._session,
            ReconciliationScheduleTable,
            values={
                "tenant_id": self._tenant_id,
                "schedule_type": schedule_type,
                "cron_expression": cron_expression,
                "enabled": enabled,
                "created_at": now,
                "updated_at": now,
            },
            index_elements=["tenant_id", "schedule_type"],
            update_columns=["cron_expression", "enabled", "updated_at"],
        )
        await self._session.flush()

        # Return the row after upsert.
        return await self.get_schedule(schedule_type)  # type: ignore[return-value]

    async def update_last_run(
        self,
        schedule_type: str,
        last_run_at: datetime,
        next_run_at: datetime,
    ) -> bool:
        """Update the last_run_at and next_run_at for a schedule.

        Returns ``True`` if the schedule was found and updated, ``False``
        otherwise.
        """
        stmt = (
            update(ReconciliationScheduleTable)
            .where(
                ReconciliationScheduleTable.tenant_id == self._tenant_id,
                ReconciliationScheduleTable.schedule_type == schedule_type,
            )
            .values(
                last_run_at=last_run_at,
                next_run_at=next_run_at,
                updated_at=datetime.now(UTC),
            )
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def get_all_enabled(self) -> list[ReconciliationScheduleTable]:
        """Return all enabled schedules for this tenant, ordered by schedule type."""
        stmt = (
            select(ReconciliationScheduleTable)
            .where(
                ReconciliationScheduleTable.tenant_id == self._tenant_id,
                ReconciliationScheduleTable.enabled == True,  # noqa: E712
            )
            .order_by(ReconciliationScheduleTable.schedule_type)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# EnvironmentRepository
# ---------------------------------------------------------------------------


class EnvironmentRepository:
    """CRUD operations for the ``environments`` and ``environment_promotions`` tables.

    Manages first-class environments with catalog/schema mapping for SQL
    rewriting, ephemeral PR environments with auto-expiry, and promotion
    tracking between environments.
    """

    def __init__(self, session: AsyncSession, *, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create(
        self,
        name: str,
        catalog: str,
        schema_prefix: str,
        *,
        is_default: bool = False,
        is_production: bool = False,
        is_ephemeral: bool = False,
        pr_number: int | None = None,
        branch_name: str | None = None,
        expires_at: datetime | None = None,
        created_by: str = "system",
    ) -> EnvironmentTable:
        """Create a new environment record.

        Raises
        ------
        ValueError
            If an active (non-deleted) environment with the same name already exists.
        """
        row = EnvironmentTable(
            tenant_id=self._tenant_id,
            name=name,
            catalog=catalog,
            schema_prefix=schema_prefix,
            is_default=is_default,
            is_production=is_production,
            is_ephemeral=is_ephemeral,
            pr_number=pr_number,
            branch_name=branch_name,
            expires_at=expires_at,
            created_by=created_by,
        )
        try:
            self._session.add(row)
            await self._session.flush()
        except IntegrityError:
            await self._session.rollback()
            raise ValueError(f"Environment '{name}' already exists for tenant '{self._tenant_id}'")
        return row

    async def get(self, name: str) -> EnvironmentTable | None:
        """Get an active (non-deleted) environment by name."""
        stmt = select(EnvironmentTable).where(
            EnvironmentTable.tenant_id == self._tenant_id,
            EnvironmentTable.name == name,
            EnvironmentTable.deleted_at.is_(None),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(self, include_deleted: bool = False) -> list[EnvironmentTable]:
        """List all environments, sorted deterministically by name.

        By default only active (non-deleted) environments are returned.
        Pass ``include_deleted=True`` to include soft-deleted environments.
        """
        stmt = select(EnvironmentTable).where(EnvironmentTable.tenant_id == self._tenant_id)
        if not include_deleted:
            stmt = stmt.where(EnvironmentTable.deleted_at.is_(None))
        stmt = stmt.order_by(EnvironmentTable.name)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def soft_delete(self, name: str) -> bool:
        """Soft-delete an environment by setting ``deleted_at``.

        Returns ``False`` if no active environment with the given name exists.
        """
        env = await self.get(name)
        if env is None:
            return False
        stmt = (
            update(EnvironmentTable)
            .where(
                EnvironmentTable.tenant_id == self._tenant_id,
                EnvironmentTable.name == name,
                EnvironmentTable.deleted_at.is_(None),
            )
            .values(deleted_at=datetime.now(UTC))
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def get_default(self) -> EnvironmentTable | None:
        """Get the default environment for this tenant."""
        stmt = select(EnvironmentTable).where(
            EnvironmentTable.tenant_id == self._tenant_id,
            EnvironmentTable.is_default == True,  # noqa: E712
            EnvironmentTable.deleted_at.is_(None),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_ephemeral(self) -> list[EnvironmentTable]:
        """Get all active ephemeral environments, sorted by name."""
        stmt = (
            select(EnvironmentTable)
            .where(
                EnvironmentTable.tenant_id == self._tenant_id,
                EnvironmentTable.is_ephemeral == True,  # noqa: E712
                EnvironmentTable.deleted_at.is_(None),
            )
            .order_by(EnvironmentTable.name)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def cleanup_expired(self) -> int:
        """Soft-delete expired ephemeral environments.

        Targets environments where ``is_ephemeral`` is True, ``expires_at``
        is in the past, and the environment has not already been deleted.

        Returns the number of environments that were soft-deleted.
        """
        now = datetime.now(UTC)
        stmt = (
            update(EnvironmentTable)
            .where(
                EnvironmentTable.tenant_id == self._tenant_id,
                EnvironmentTable.is_ephemeral == True,  # noqa: E712
                EnvironmentTable.expires_at.is_not(None),
                EnvironmentTable.expires_at < now,
                EnvironmentTable.deleted_at.is_(None),
            )
            .values(deleted_at=now)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[attr-defined]

    async def record_promotion(
        self,
        source_env: str,
        target_env: str,
        source_snapshot_id: str,
        target_snapshot_id: str,
        promoted_by: str,
        metadata: dict | None = None,
    ) -> EnvironmentPromotionTable:
        """Record a promotion event (snapshot reference copy).

        This does not copy data -- it records the fact that a snapshot
        reference was promoted from one environment to another.
        """
        row = EnvironmentPromotionTable(
            tenant_id=self._tenant_id,
            source_environment=source_env,
            target_environment=target_env,
            source_snapshot_id=source_snapshot_id,
            target_snapshot_id=target_snapshot_id,
            promoted_by=promoted_by,
            metadata_json=metadata,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_promotion_history(
        self,
        environment_name: str | None = None,
        limit: int = 20,
    ) -> list[EnvironmentPromotionTable]:
        """Get promotion history, sorted by promoted_at descending.

        If *environment_name* is given, only returns promotions where the
        environment appears as either source or target.
        """
        stmt = select(EnvironmentPromotionTable).where(EnvironmentPromotionTable.tenant_id == self._tenant_id)
        if environment_name is not None:
            from sqlalchemy import or_

            stmt = stmt.where(
                or_(
                    EnvironmentPromotionTable.source_environment == environment_name,
                    EnvironmentPromotionTable.target_environment == environment_name,
                )
            )
        stmt = stmt.order_by(EnvironmentPromotionTable.promoted_at.desc()).limit(limit)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# ModelTestRepository
# ---------------------------------------------------------------------------


class ModelTestRepository:
    """CRUD operations for the ``model_tests`` table.

    Stores declarative test definitions that can be synced from model
    headers or created via API.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def save_test(
        self,
        test_id: str,
        model_name: str,
        test_type: str,
        test_config: dict | None,
        severity: str = "BLOCK",
    ) -> ModelTestTable:
        """Insert or update a test definition.

        Uses INSERT-first with IntegrityError fallback to UPDATE,
        eliminating the TOCTOU race of SELECT-then-INSERT.
        """
        row = ModelTestTable(
            tenant_id=self._tenant_id,
            test_id=test_id,
            model_name=model_name,
            test_type=test_type,
            test_config_json=test_config,
            severity=severity,
        )
        try:
            self._session.add(row)
            await self._session.flush()
            return row
        except IntegrityError:
            await self._session.rollback()
            # Row already exists -- update it instead.
            stmt = (
                update(ModelTestTable)
                .where(
                    ModelTestTable.tenant_id == self._tenant_id,
                    ModelTestTable.test_id == test_id,
                )
                .values(
                    model_name=model_name,
                    test_type=test_type,
                    test_config_json=test_config,
                    severity=severity,
                )
            )
            await self._session.execute(stmt)
            await self._session.flush()
            return await self.get_by_id(test_id)  # type: ignore[return-value]

    async def get_for_model(self, model_name: str) -> list[ModelTestTable]:
        """Return all test definitions for a model, ordered by test type and ID."""
        stmt = (
            select(ModelTestTable)
            .where(
                ModelTestTable.tenant_id == self._tenant_id,
                ModelTestTable.model_name == model_name,
            )
            .order_by(ModelTestTable.test_type, ModelTestTable.test_id)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, test_id: str) -> ModelTestTable | None:
        """Fetch a single test definition by its ID."""
        stmt = select(ModelTestTable).where(
            ModelTestTable.tenant_id == self._tenant_id,
            ModelTestTable.test_id == test_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def delete_for_model(self, model_name: str) -> int:
        """Delete all test definitions for a model. Returns count deleted."""
        stmt = delete(ModelTestTable).where(
            ModelTestTable.tenant_id == self._tenant_id,
            ModelTestTable.model_name == model_name,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# TestResultRepository
# ---------------------------------------------------------------------------


class TestResultRepository:
    """CRUD operations for the ``test_results`` table.

    Records the outcome of test executions for audit and quality-gate checks.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def record_result(
        self,
        test_id: str,
        plan_id: str | None,
        model_name: str,
        test_type: str,
        passed: bool,
        failure_message: str | None,
        execution_mode: str,
        duration_ms: int,
    ) -> TestResultTable:
        """Persist a test execution result."""
        row = TestResultTable(
            tenant_id=self._tenant_id,
            test_id=test_id,
            plan_id=plan_id,
            model_name=model_name,
            test_type=test_type,
            passed=passed,
            failure_message=failure_message,
            execution_mode=execution_mode,
            duration_ms=duration_ms,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_for_plan(self, plan_id: str) -> list[TestResultTable]:
        """Return all test results for a plan, ordered by model name and test type."""
        stmt = (
            select(TestResultTable)
            .where(
                TestResultTable.tenant_id == self._tenant_id,
                TestResultTable.plan_id == plan_id,
            )
            .order_by(TestResultTable.model_name, TestResultTable.test_type)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_model(
        self,
        model_name: str,
        limit: int = 50,
    ) -> list[TestResultTable]:
        """Return recent test results for a model, newest first."""
        stmt = (
            select(TestResultTable)
            .where(
                TestResultTable.tenant_id == self._tenant_id,
                TestResultTable.model_name == model_name,
            )
            .order_by(TestResultTable.executed_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_summary(self, plan_id: str) -> dict[str, int]:
        """Return aggregate pass/fail/blocked counts for a plan's tests.

        Uses a single LEFT JOIN query to fetch test results with their
        severity in one round-trip, avoiding the N+1 per-failed-test lookup.

        Returns
        -------
        dict
            ``{"total": int, "passed": int, "failed": int, "blocked": int}``
            where ``blocked`` is the count of BLOCK-severity failures.
        """
        stmt = (
            select(TestResultTable, ModelTestTable.severity)
            .outerjoin(
                ModelTestTable,
                and_(
                    TestResultTable.test_id == ModelTestTable.test_id,
                    TestResultTable.tenant_id == ModelTestTable.tenant_id,
                ),
            )
            .where(
                TestResultTable.plan_id == plan_id,
                TestResultTable.tenant_id == self._tenant_id,
            )
        )
        result = await self._session.execute(stmt)
        rows = result.all()

        total = len(rows)
        passed = 0
        blocked = 0
        for test_result_row, severity in rows:
            if test_result_row.passed:
                passed += 1
            else:
                # If no matching test definition, default severity to BLOCK.
                effective_severity = severity if severity is not None else "BLOCK"
                if effective_severity == "BLOCK":
                    blocked += 1
        failed = total - passed

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "blocked": blocked,
        }


# ---------------------------------------------------------------------------
# EventSubscriptionRepository
# ---------------------------------------------------------------------------


class EventSubscriptionRepository:
    """CRUD operations for the ``event_subscriptions`` table.

    Manages webhook event subscriptions that receive HTTP POST
    notifications when lifecycle events occur in IronLayer.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create(
        self,
        name: str,
        url: str,
        *,
        secret_hash: str | None = None,
        event_types: list[str] | None = None,
        description: str | None = None,
    ) -> EventSubscriptionTable:
        """Create a new event subscription.

        Parameters
        ----------
        name:
            Human-readable subscription name.
        url:
            The webhook endpoint URL.
        secret_hash:
            Bcrypt hash of the webhook signing secret.
        event_types:
            List of event type strings to subscribe to.  ``None`` means all events.
        description:
            Optional description of the subscription's purpose.
        """
        row = EventSubscriptionTable(
            tenant_id=self._tenant_id,
            name=name,
            url=url,
            secret_hash=secret_hash,
            event_types=event_types,
            description=description,
            active=True,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get(self, subscription_id: int) -> EventSubscriptionTable | None:
        """Fetch a subscription by ID, scoped to the current tenant."""
        stmt = select(EventSubscriptionTable).where(
            EventSubscriptionTable.tenant_id == self._tenant_id,
            EventSubscriptionTable.id == subscription_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_active(self) -> list[EventSubscriptionTable]:
        """Return all active subscriptions for this tenant, ordered by ID."""
        stmt = (
            select(EventSubscriptionTable)
            .where(
                EventSubscriptionTable.tenant_id == self._tenant_id,
                EventSubscriptionTable.active == True,  # noqa: E712
            )
            .order_by(EventSubscriptionTable.id)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_all(self) -> list[EventSubscriptionTable]:
        """Return all subscriptions for this tenant (active and inactive)."""
        stmt = (
            select(EventSubscriptionTable)
            .where(EventSubscriptionTable.tenant_id == self._tenant_id)
            .order_by(EventSubscriptionTable.id)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def update(
        self,
        subscription_id: int,
        *,
        name: str | None = None,
        url: str | None = None,
        secret_hash: str | None = None,
        event_types: list[str] | None = ...,  # type: ignore[assignment]
        active: bool | None = None,
        description: str | None = ...,  # type: ignore[assignment]
    ) -> EventSubscriptionTable | None:
        """Update fields on an existing subscription.

        Only provided fields are updated; ``None``-sentinel ``...`` values
        for ``event_types`` and ``description`` mean 'leave unchanged'.

        Returns the updated row, or ``None`` if the subscription was not found.
        """
        row = await self.get(subscription_id)
        if row is None:
            return None

        if name is not None:
            row.name = name
        if url is not None:
            row.url = url
        if secret_hash is not None:
            row.secret_hash = secret_hash
        if event_types is not ...:
            row.event_types = event_types
        if active is not None:
            row.active = active
        if description is not ...:
            row.description = description

        row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return row

    async def delete(self, subscription_id: int) -> bool:
        """Delete a subscription by ID.  Returns True if a row was removed."""
        stmt = delete(EventSubscriptionTable).where(
            EventSubscriptionTable.tenant_id == self._tenant_id,
            EventSubscriptionTable.id == subscription_id,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def list_for_event_type(self, event_type: str) -> list[EventSubscriptionTable]:
        """Return active subscriptions that match a specific event type.

        Subscriptions with ``event_types`` set to ``None`` or an empty list
        are treated as wildcard (match all event types).

        Fetches active subscriptions via SQL and filters by event_type in
        Python for cross-dialect compatibility (JSONB containment is
        PostgreSQL-specific).
        """
        stmt = select(EventSubscriptionTable).where(
            EventSubscriptionTable.tenant_id == self._tenant_id,
            EventSubscriptionTable.active.is_(True),
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        return [r for r in rows if r.event_types is None or event_type in (r.event_types or [])]


# ---------------------------------------------------------------------------
# UserRepository
# ---------------------------------------------------------------------------


class UserRepository:
    """CRUD operations for the ``users`` table.

    Handles user creation, lookup, password verification, and profile
    updates.  Password hashing uses bcrypt via passlib.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    @staticmethod
    def _hash_password(plaintext: str) -> str:
        """Hash a plaintext password with bcrypt."""
        import bcrypt

        return bcrypt.hashpw(plaintext.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    @staticmethod
    def _verify_password(plaintext: str, hashed: str) -> bool:
        """Verify a plaintext password against a bcrypt hash."""
        import bcrypt

        return bcrypt.checkpw(plaintext.encode("utf-8"), hashed.encode("utf-8"))

    async def create(
        self,
        email: str,
        password: str,
        display_name: str,
        *,
        role: str = "viewer",
    ) -> UserTable:
        """Create a new user with a hashed password."""
        row = UserTable(
            id=uuid.uuid4().hex,
            tenant_id=self._tenant_id,
            email=email.lower().strip(),
            password_hash=self._hash_password(password),
            display_name=display_name.strip(),
            role=role,
            is_active=True,
            email_verified=False,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_by_email(self, email: str) -> UserTable | None:
        """Fetch a user by email address (case-insensitive)."""
        stmt = select(UserTable).where(
            UserTable.tenant_id == self._tenant_id,
            UserTable.email == email.lower().strip(),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_email_any_tenant(self, email: str) -> UserTable | None:
        """Fetch a user by email across all tenants (for login).

        Login doesn't know the tenant_id yet — the email determines it.
        """
        stmt = select(UserTable).where(
            UserTable.email == email.lower().strip(),
            UserTable.is_active == True,  # noqa: E712
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: str) -> UserTable | None:
        """Fetch a user by their primary key within the current tenant."""
        stmt = select(UserTable).where(
            UserTable.tenant_id == self._tenant_id,
            UserTable.id == user_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def verify_password(self, email: str, password: str) -> UserTable | None:
        """Validate credentials and return the user if correct.

        Returns ``None`` if the email is not found or the password does not match.
        Uses constant-time comparison via bcrypt to prevent timing attacks.
        """
        user = await self.get_by_email_any_tenant(email)
        if user is None:
            # Prevent timing-based user enumeration by still performing
            # a bcrypt hash comparison against a dummy value.
            self._hash_password("dummy-password-for-timing")
            return None
        if not self._verify_password(password, user.password_hash):
            return None
        return user

    async def update_last_login(self, user_id: str) -> None:
        """Record the current time as the user's last login."""
        stmt = (
            update(UserTable)
            .where(
                UserTable.tenant_id == self._tenant_id,
                UserTable.id == user_id,
            )
            .values(last_login_at=datetime.now(UTC))
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def list_by_tenant(self) -> list[UserTable]:
        """Return all users in the current tenant."""
        stmt = select(UserTable).where(UserTable.tenant_id == self._tenant_id).order_by(UserTable.created_at)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def count_by_tenant(self) -> int:
        """Return the number of users in the current tenant."""
        stmt = (
            select(func.count())
            .select_from(UserTable)
            .where(
                UserTable.tenant_id == self._tenant_id,
            )
        )
        result = await self._session.execute(stmt)
        return result.scalar_one()


# ---------------------------------------------------------------------------
# APIKeyRepository
# ---------------------------------------------------------------------------


class APIKeyRepository:
    """CRUD operations for the ``api_keys`` table.

    API keys use a ``bmkey.`` prefix convention for identification.  Only
    the SHA-256 hash of the key is stored; the plaintext is returned
    exactly once at creation time.
    """

    _KEY_PREFIX_CONVENTION = "bmkey."

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    @staticmethod
    def _hash_key(plaintext: str) -> str:
        """SHA-256 hash of the plaintext key."""
        return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()

    def _generate_key(self) -> tuple[str, str, str]:
        """Generate a new API key.

        Returns ``(plaintext, prefix, hash)`` where:
        - ``plaintext`` is ``bmkey.<random-hex>`` (shown once to user)
        - ``prefix`` is the first 16 characters of the random part
        - ``hash`` is the SHA-256 hash of the full plaintext
        """
        random_part = uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex chars
        plaintext = f"{self._KEY_PREFIX_CONVENTION}{random_part}"
        prefix = random_part[:16]
        key_hash = self._hash_key(plaintext)
        return plaintext, prefix, key_hash

    async def create(
        self,
        user_id: str,
        name: str,
        *,
        scopes: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> tuple[APIKeyTable, str]:
        """Create a new API key.

        Returns ``(row, plaintext_key)`` — the plaintext key is shown to the
        user exactly once and is never stored.
        """
        plaintext, prefix, key_hash = self._generate_key()

        row = APIKeyTable(
            id=uuid.uuid4().hex,
            tenant_id=self._tenant_id,
            user_id=user_id,
            name=name.strip(),
            key_prefix=prefix,
            key_hash=key_hash,
            scopes=scopes,
            expires_at=expires_at,
        )
        self._session.add(row)
        await self._session.flush()
        return row, plaintext

    async def validate_key(self, plaintext_key: str) -> APIKeyTable | None:
        """Look up an API key by its SHA-256 hash and validate it.

        Returns the API key row if valid, or ``None`` if not found, revoked,
        or expired.  Updates ``last_used_at`` on successful validation.

        All validation criteria are enforced in SQL to guarantee tenant
        isolation and prevent timing-based information leaks:

        - ``tenant_id`` must match the repository's tenant context
        - ``revoked_at`` must be NULL (key is not revoked)
        - ``expires_at`` must be NULL or in the future (key is not expired)
        """
        key_hash = self._hash_key(plaintext_key)
        stmt = select(APIKeyTable).where(
            APIKeyTable.key_hash == key_hash,
            APIKeyTable.tenant_id == self._tenant_id,
            APIKeyTable.revoked_at.is_(None),
            (APIKeyTable.expires_at.is_(None)) | (APIKeyTable.expires_at > func.now()),
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()

        if row is None:
            return None

        # Update last_used_at
        row.last_used_at = datetime.now(UTC)
        await self._session.flush()
        return row

    async def list_by_user(self, user_id: str) -> list[APIKeyTable]:
        """Return all non-revoked API keys for a user in this tenant."""
        stmt = (
            select(APIKeyTable)
            .where(
                APIKeyTable.tenant_id == self._tenant_id,
                APIKeyTable.user_id == user_id,
                APIKeyTable.revoked_at == None,  # noqa: E711
            )
            .order_by(APIKeyTable.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def revoke(self, key_id: str) -> bool:
        """Revoke an API key.  Returns ``True`` if a row was updated."""
        stmt = (
            update(APIKeyTable)
            .where(
                APIKeyTable.tenant_id == self._tenant_id,
                APIKeyTable.id == key_id,
                APIKeyTable.revoked_at == None,  # noqa: E711
            )
            .values(revoked_at=datetime.now(UTC))
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# AnalyticsRepository (cross-tenant admin view)
# ---------------------------------------------------------------------------


class AnalyticsRepository:
    """Cross-tenant analytics queries for the admin dashboard.

    Unlike tenant-scoped repositories, this class does NOT filter by
    ``tenant_id`` — it aggregates data across all tenants for platform
    operators.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_platform_overview(self, since: datetime) -> dict[str, Any]:
        """Return platform-wide aggregate metrics since *since*.

        Returns
        -------
        dict
            ``{"total_tenants": int, "active_tenants": int,
               "total_events": int, "total_runs": int, "total_cost_usd": float}``
        """
        total_tenants_r = await self._session.execute(
            select(func.count())
            .select_from(TenantConfigTable)
            .where(
                TenantConfigTable.deactivated_at.is_(None),
            )
        )
        total_tenants = total_tenants_r.scalar_one()

        # Active tenants = those with at least one usage event since the cutoff.
        active_tenants_r = await self._session.execute(
            select(func.count(func.distinct(UsageEventTable.tenant_id))).where(
                UsageEventTable.created_at >= since,
            )
        )
        active_tenants = active_tenants_r.scalar_one()

        total_events_r = await self._session.execute(
            select(func.count())
            .select_from(UsageEventTable)
            .where(
                UsageEventTable.created_at >= since,
            )
        )
        total_events = total_events_r.scalar_one()

        runs_r = await self._session.execute(
            select(
                func.count().label("cnt"),
                func.coalesce(func.sum(RunTable.cost_usd), 0.0).label("cost"),
            ).where(RunTable.started_at >= since)
        )
        runs_row = runs_r.one()

        return {
            "total_tenants": total_tenants,
            "active_tenants": active_tenants,
            "total_events": total_events,
            "total_runs": int(runs_row.cnt),
            "total_cost_usd": round(float(runs_row.cost), 2),
        }

    async def get_revenue_metrics(self) -> dict[str, Any]:
        """Return MRR and subscription counts grouped by plan tier.

        Tier pricing: community=$0, team=$49, enterprise=$199.

        Returns
        -------
        dict
            ``{"mrr_usd": float, "tiers": {tier: {"count": int, "mrr": float}}}``
        """
        tier_prices = {"community": 0.0, "team": 49.0, "enterprise": 199.0}
        result = await self._session.execute(
            select(
                BillingCustomerTable.plan_tier,
                func.count().label("cnt"),
            ).group_by(BillingCustomerTable.plan_tier)
        )
        rows = result.all()

        tiers: dict[str, dict[str, Any]] = {}
        total_mrr = 0.0
        for row in rows:
            tier = row.plan_tier
            count = int(row.cnt)
            price = tier_prices.get(tier, 0.0)
            tier_mrr = count * price
            tiers[tier] = {"count": count, "mrr": tier_mrr}
            total_mrr += tier_mrr

        return {"mrr_usd": round(total_mrr, 2), "tiers": tiers}

    async def get_per_tenant_breakdown(
        self,
        since: datetime,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Return a per-tenant breakdown of usage, cost, and billing info.

        Uses 5 total queries (1 count + 1 config page + 3 bulk aggregations)
        instead of 4N+1 to avoid the N+1 problem.

        Returns
        -------
        dict
            ``{"tenants": [...], "total": int}``
        """
        # 1. Count total active tenants for pagination.
        total_r = await self._session.execute(
            select(func.count())
            .select_from(TenantConfigTable)
            .where(
                TenantConfigTable.deactivated_at.is_(None),
            )
        )
        total = total_r.scalar_one()

        # 2. Fetch tenant configs with pagination.
        configs_r = await self._session.execute(
            select(TenantConfigTable)
            .where(TenantConfigTable.deactivated_at.is_(None))
            .order_by(TenantConfigTable.tenant_id)
            .limit(limit)
            .offset(offset)
        )
        configs = list(configs_r.scalars().all())

        if not configs:
            return {"tenants": [], "total": total}

        tenant_ids = [c.tenant_id for c in configs]

        # 3. Bulk billing info.
        billing_r = await self._session.execute(
            select(
                BillingCustomerTable.tenant_id,
                BillingCustomerTable.plan_tier,
            ).where(BillingCustomerTable.tenant_id.in_(tenant_ids))
        )
        billing_map: dict[str, str] = {row.tenant_id: row.plan_tier for row in billing_r.all()}

        # 4. Bulk usage events since cutoff (grouped by tenant_id and event_type).
        events_r = await self._session.execute(
            select(
                UsageEventTable.tenant_id,
                UsageEventTable.event_type,
                func.sum(UsageEventTable.quantity).label("total"),
            )
            .where(
                UsageEventTable.tenant_id.in_(tenant_ids),
                UsageEventTable.created_at >= since,
            )
            .group_by(UsageEventTable.tenant_id, UsageEventTable.event_type)
        )
        usage_map: dict[str, dict[str, int]] = {}
        for row in events_r.all():
            usage_map.setdefault(row.tenant_id, {})[row.event_type] = int(row.total)

        # 5. Bulk run cost.
        cost_r = await self._session.execute(
            select(
                RunTable.tenant_id,
                func.coalesce(func.sum(RunTable.cost_usd), 0.0).label("total_cost"),
            )
            .where(
                RunTable.tenant_id.in_(tenant_ids),
                RunTable.started_at >= since,
            )
            .group_by(RunTable.tenant_id)
        )
        cost_map: dict[str, float] = {row.tenant_id: float(row.total_cost) for row in cost_r.all()}

        # 6. Bulk LLM cost.
        llm_r = await self._session.execute(
            select(
                LLMUsageLogTable.tenant_id,
                func.coalesce(func.sum(LLMUsageLogTable.estimated_cost_usd), 0.0).label("llm_cost"),
            )
            .where(
                LLMUsageLogTable.tenant_id.in_(tenant_ids),
                LLMUsageLogTable.created_at >= since,
            )
            .group_by(LLMUsageLogTable.tenant_id)
        )
        llm_map: dict[str, float] = {row.tenant_id: float(row.llm_cost) for row in llm_r.all()}

        # Merge results in Python.
        tenants = []
        for config in configs:
            tid = config.tenant_id
            usage = usage_map.get(tid, {})
            run_cost = cost_map.get(tid, 0.0)
            llm_cost = llm_map.get(tid, 0.0)

            tenants.append(
                {
                    "tenant_id": tid,
                    "plan_tier": billing_map.get(tid, "community"),
                    "plan_runs": usage.get("plan_run", 0),
                    "ai_calls": usage.get("ai_call", 0),
                    "api_requests": usage.get("api_request", 0),
                    "run_cost_usd": round(run_cost, 4),
                    "llm_cost_usd": round(llm_cost, 4),
                    "total_cost_usd": round(run_cost + llm_cost, 4),
                    "llm_enabled": config.llm_enabled,
                    "created_at": config.created_at.isoformat() if config.created_at else None,
                }
            )

        return {"tenants": tenants, "total": total}

    async def get_cost_breakdown(
        self,
        since: datetime,
        group_by: str = "model",
    ) -> dict[str, Any]:
        """Return cost breakdown grouped by model or date.

        Parameters
        ----------
        group_by:
            ``"model"`` groups by model_name; ``"day"`` / ``"week"`` / ``"month"``
            groups by time bucket using ``date_trunc``.

        Returns
        -------
        dict
            ``{"items": [{"group": str, "cost_usd": float, "run_count": int}],
               "total_cost_usd": float}``
        """
        if group_by == "model":
            stmt = (
                select(
                    RunTable.model_name.label("grp"),
                    func.sum(RunTable.cost_usd).label("cost"),
                    func.count().label("cnt"),
                )
                .where(RunTable.started_at >= since, RunTable.cost_usd.is_not(None))
                .group_by(RunTable.model_name)
                .order_by(func.sum(RunTable.cost_usd).desc())
            )
        else:
            bucket = group_by if group_by in ("day", "week", "month") else "day"
            stmt = (
                select(
                    func.date_trunc(bucket, RunTable.started_at).label("grp"),
                    func.sum(RunTable.cost_usd).label("cost"),
                    func.count().label("cnt"),
                )
                .where(RunTable.started_at >= since, RunTable.cost_usd.is_not(None))
                .group_by(func.date_trunc(bucket, RunTable.started_at))
                .order_by(func.date_trunc(bucket, RunTable.started_at))
            )

        result = await self._session.execute(stmt)
        rows = result.all()

        items = []
        total_cost = 0.0
        for row in rows:
            cost = round(float(row.cost or 0), 4)
            total_cost += cost
            grp_str = str(row.grp) if row.grp else "unknown"
            items.append(
                {
                    "group": grp_str,
                    "cost_usd": cost,
                    "run_count": int(row.cnt),
                }
            )

        return {"items": items, "total_cost_usd": round(total_cost, 2)}

    async def get_health_metrics(self, since: datetime) -> dict[str, Any]:
        """Return platform health metrics: error rate, P95 runtime, AI stats.

        Returns
        -------
        dict
            ``{"error_rate": float | None, "p95_runtime_seconds": float | None,
               "total_runs": int, "failed_runs": int,
               "ai_acceptance_rate": float | None, "ai_avg_accuracy": float | None}``
        """
        # Run error rate.
        total_runs_r = await self._session.execute(
            select(func.count()).select_from(RunTable).where(RunTable.started_at >= since)
        )
        total_runs = total_runs_r.scalar_one()

        failed_runs_r = await self._session.execute(
            select(func.count())
            .select_from(RunTable)
            .where(
                RunTable.started_at >= since,
                RunTable.status == "FAILED",
            )
        )
        failed_runs = failed_runs_r.scalar_one()

        error_rate = round(failed_runs / total_runs, 4) if total_runs > 0 else None

        # P95 runtime from telemetry.
        p95_r = await self._session.execute(
            select(func.percentile_cont(0.95).within_group(TelemetryTable.runtime_seconds)).where(
                TelemetryTable.captured_at >= since
            )
        )
        p95 = p95_r.scalar_one_or_none()

        # AI feedback stats.
        ai_total_r = await self._session.execute(
            select(func.count())
            .select_from(AIFeedbackTable)
            .where(
                AIFeedbackTable.created_at >= since,
            )
        )
        ai_total = ai_total_r.scalar_one()

        ai_accepted_r = await self._session.execute(
            select(func.count())
            .select_from(AIFeedbackTable)
            .where(
                AIFeedbackTable.created_at >= since,
                AIFeedbackTable.accepted == True,  # noqa: E712
            )
        )
        ai_accepted = ai_accepted_r.scalar_one()

        ai_avg_acc_r = await self._session.execute(
            select(func.avg(AIFeedbackTable.accuracy_score)).where(
                AIFeedbackTable.created_at >= since,
                AIFeedbackTable.accuracy_score.is_not(None),
            )
        )
        ai_avg_acc = ai_avg_acc_r.scalar_one_or_none()

        return {
            "error_rate": error_rate,
            "p95_runtime_seconds": round(float(p95), 3) if p95 is not None else None,
            "total_runs": total_runs,
            "failed_runs": failed_runs,
            "ai_acceptance_rate": round(ai_accepted / ai_total, 4) if ai_total > 0 else None,
            "ai_avg_accuracy": round(float(ai_avg_acc), 4) if ai_avg_acc is not None else None,
        }


# ---------------------------------------------------------------------------
# QuotaRepository (tenant-scoped)
# ---------------------------------------------------------------------------


class QuotaRepository:
    """Queries for quota enforcement and usage display.

    Reads from ``usage_events`` to determine current-month usage counts
    for comparison against per-tenant or tier-default quotas.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def get_monthly_event_count(self, event_type: str) -> int:
        """Count usage events of *event_type* in the current calendar month.

        Returns 0 if no matching events exist.
        """
        now = datetime.now(UTC)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        stmt = select(func.coalesce(func.sum(UsageEventTable.quantity), 0)).where(
            UsageEventTable.tenant_id == self._tenant_id,
            UsageEventTable.event_type == event_type,
            UsageEventTable.created_at >= month_start,
        )
        result = await self._session.execute(stmt)
        return int(result.scalar_one() or 0)

    async def get_current_usage(self) -> dict[str, int]:
        """Return current-month usage counts for all tracked event types.

        Returns
        -------
        dict
            ``{"plan_run": int, "ai_call": int, "api_request": int, ...}``
        """
        now = datetime.now(UTC)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        stmt = (
            select(
                UsageEventTable.event_type,
                func.coalesce(func.sum(UsageEventTable.quantity), 0).label("total"),
            )
            .where(
                UsageEventTable.tenant_id == self._tenant_id,
                UsageEventTable.created_at >= month_start,
            )
            .group_by(UsageEventTable.event_type)
        )
        result = await self._session.execute(stmt)
        rows = result.all()
        usage: dict[str, int] = {}
        for row in rows:
            usage[row.event_type] = int(row.total)
        return usage


# ---------------------------------------------------------------------------
# ReportingRepository (tenant-scoped)
# ---------------------------------------------------------------------------


class ReportingRepository:
    """Tenant-scoped reporting queries for cost, usage, and LLM analytics.

    Designed for the per-tenant reports page — all queries filter by
    ``tenant_id`` and support configurable date ranges and grouping.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def get_cost_by_model(
        self,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        """Aggregate ``runs.cost_usd`` by ``model_name`` for the period."""
        stmt = (
            select(
                RunTable.model_name,
                func.sum(RunTable.cost_usd).label("cost"),
                func.count().label("run_count"),
            )
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.started_at >= since,
                RunTable.started_at < until,
                RunTable.cost_usd.is_not(None),
            )
            .group_by(RunTable.model_name)
            .order_by(func.sum(RunTable.cost_usd).desc())
        )
        result = await self._session.execute(stmt)
        return [
            {"model_name": r.model_name, "cost_usd": round(float(r.cost or 0), 4), "run_count": int(r.run_count)}
            for r in result.all()
        ]

    async def get_cost_by_time(
        self,
        since: datetime,
        until: datetime,
        bucket: str = "day",
    ) -> list[dict[str, Any]]:
        """Aggregate ``runs.cost_usd`` by time bucket for the period."""
        bucket = bucket if bucket in ("day", "week", "month") else "day"
        stmt = (
            select(
                func.date_trunc(bucket, RunTable.started_at).label("period"),
                func.sum(RunTable.cost_usd).label("cost"),
                func.count().label("run_count"),
            )
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.started_at >= since,
                RunTable.started_at < until,
                RunTable.cost_usd.is_not(None),
            )
            .group_by(func.date_trunc(bucket, RunTable.started_at))
            .order_by(func.date_trunc(bucket, RunTable.started_at))
        )
        result = await self._session.execute(stmt)
        return [
            {"period": str(r.period), "cost_usd": round(float(r.cost or 0), 4), "run_count": int(r.run_count)}
            for r in result.all()
        ]

    async def get_usage_by_actor(
        self,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        """Aggregate audit log actions by actor for the period."""
        stmt = (
            select(
                AuditLogTable.actor,
                AuditLogTable.action,
                func.count().label("cnt"),
            )
            .where(
                AuditLogTable.tenant_id == self._tenant_id,
                AuditLogTable.created_at >= since,
                AuditLogTable.created_at < until,
            )
            .group_by(AuditLogTable.actor, AuditLogTable.action)
            .order_by(func.count().desc())
        )
        result = await self._session.execute(stmt)
        return [{"actor": r.actor, "action": r.action, "count": int(r.cnt)} for r in result.all()]

    async def get_llm_cost_by_call_type(
        self,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        """Aggregate LLM usage by call_type for the period."""
        stmt = (
            select(
                LLMUsageLogTable.call_type,
                func.sum(LLMUsageLogTable.estimated_cost_usd).label("cost"),
                func.sum(LLMUsageLogTable.input_tokens).label("input_tokens"),
                func.sum(LLMUsageLogTable.output_tokens).label("output_tokens"),
                func.count().label("call_count"),
            )
            .where(
                LLMUsageLogTable.tenant_id == self._tenant_id,
                LLMUsageLogTable.created_at >= since,
                LLMUsageLogTable.created_at < until,
            )
            .group_by(LLMUsageLogTable.call_type)
            .order_by(func.sum(LLMUsageLogTable.estimated_cost_usd).desc())
        )
        result = await self._session.execute(stmt)
        return [
            {
                "call_type": r.call_type,
                "cost_usd": round(float(r.cost or 0), 6),
                "input_tokens": int(r.input_tokens or 0),
                "output_tokens": int(r.output_tokens or 0),
                "call_count": int(r.call_count),
            }
            for r in result.all()
        ]

    async def get_llm_cost_by_time(
        self,
        since: datetime,
        until: datetime,
        bucket: str = "day",
    ) -> list[dict[str, Any]]:
        """Aggregate LLM cost by time bucket for the period."""
        bucket = bucket if bucket in ("day", "week", "month") else "day"
        stmt = (
            select(
                func.date_trunc(bucket, LLMUsageLogTable.created_at).label("period"),
                func.sum(LLMUsageLogTable.estimated_cost_usd).label("cost"),
                func.count().label("call_count"),
            )
            .where(
                LLMUsageLogTable.tenant_id == self._tenant_id,
                LLMUsageLogTable.created_at >= since,
                LLMUsageLogTable.created_at < until,
            )
            .group_by(func.date_trunc(bucket, LLMUsageLogTable.created_at))
            .order_by(func.date_trunc(bucket, LLMUsageLogTable.created_at))
        )
        result = await self._session.execute(stmt)
        return [
            {"period": str(r.period), "cost_usd": round(float(r.cost or 0), 6), "call_count": int(r.call_count)}
            for r in result.all()
        ]

    async def get_usage_by_type_over_time(
        self,
        since: datetime,
        until: datetime,
        bucket: str = "day",
    ) -> list[dict[str, Any]]:
        """Aggregate usage events by event_type per time bucket."""
        bucket = bucket if bucket in ("day", "week", "month") else "day"
        stmt = (
            select(
                func.date_trunc(bucket, UsageEventTable.created_at).label("period"),
                UsageEventTable.event_type,
                func.sum(UsageEventTable.quantity).label("total"),
            )
            .where(
                UsageEventTable.tenant_id == self._tenant_id,
                UsageEventTable.created_at >= since,
                UsageEventTable.created_at < until,
            )
            .group_by(
                func.date_trunc(bucket, UsageEventTable.created_at),
                UsageEventTable.event_type,
            )
            .order_by(func.date_trunc(bucket, UsageEventTable.created_at))
        )
        result = await self._session.execute(stmt)
        return [{"period": str(r.period), "event_type": r.event_type, "count": int(r.total)} for r in result.all()]


# ---------------------------------------------------------------------------
# CustomerHealthRepository (cross-tenant admin view)
# ---------------------------------------------------------------------------


class CustomerHealthRepository:
    """CRUD operations for the ``customer_health`` table.

    Cross-tenant view for admin dashboards — tracks engagement scores,
    churn signals, and health trends for all tenants.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        tenant_id: str,
        *,
        health_score: float,
        health_status: str,
        engagement_metrics: dict | None = None,
        trend_direction: str | None = None,
        previous_score: float | None = None,
        last_login_at: datetime | None = None,
        last_plan_run_at: datetime | None = None,
        last_ai_call_at: datetime | None = None,
    ) -> CustomerHealthTable:
        """Create or update a customer health record."""
        now = datetime.now(UTC)
        await _dialect_upsert(
            self._session,
            CustomerHealthTable,
            values={
                "tenant_id": tenant_id,
                "health_score": health_score,
                "health_status": health_status,
                "engagement_metrics_json": engagement_metrics,
                "trend_direction": trend_direction,
                "previous_score": previous_score,
                "last_login_at": last_login_at,
                "last_plan_run_at": last_plan_run_at,
                "last_ai_call_at": last_ai_call_at,
                "computed_at": now,
                "updated_at": now,
            },
            index_elements=["tenant_id"],
            update_columns=[
                "health_score",
                "health_status",
                "engagement_metrics_json",
                "trend_direction",
                "previous_score",
                "last_login_at",
                "last_plan_run_at",
                "last_ai_call_at",
                "computed_at",
                "updated_at",
            ],
        )
        await self._session.flush()
        return await self.get(tenant_id)  # type: ignore[return-value]

    async def get(self, tenant_id: str) -> CustomerHealthTable | None:
        """Fetch health record for a specific tenant."""
        stmt = select(CustomerHealthTable).where(
            CustomerHealthTable.tenant_id == tenant_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(
        self,
        *,
        status_filter: str | None = None,
        sort_by: str = "health_score",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[CustomerHealthTable], int]:
        """List customer health records with optional filtering and sorting.

        Returns
        -------
        tuple
            ``(rows, total_count)`` for pagination support.
        """
        filters = []
        if status_filter:
            filters.append(CustomerHealthTable.health_status == status_filter)

        # Count total.
        count_stmt = select(func.count()).select_from(CustomerHealthTable)
        if filters:
            count_stmt = count_stmt.where(*filters)
        total_r = await self._session.execute(count_stmt)
        total = total_r.scalar_one()

        # Fetch rows.
        stmt = select(CustomerHealthTable)
        if filters:
            stmt = stmt.where(*filters)

        if sort_by == "health_score":
            stmt = stmt.order_by(CustomerHealthTable.health_score.asc())
        elif sort_by == "health_score_desc":
            stmt = stmt.order_by(CustomerHealthTable.health_score.desc())
        elif sort_by == "updated_at":
            stmt = stmt.order_by(CustomerHealthTable.updated_at.desc())
        else:
            stmt = stmt.order_by(CustomerHealthTable.tenant_id)

        stmt = stmt.limit(limit).offset(offset)
        result = await self._session.execute(stmt)
        return list(result.scalars().all()), total

    async def get_at_risk_tenants(self) -> list[CustomerHealthTable]:
        """Return all tenants with status ``at_risk`` or ``churning``."""
        stmt = (
            select(CustomerHealthTable)
            .where(
                CustomerHealthTable.health_status.in_(["at_risk", "churning"]),
            )
            .order_by(CustomerHealthTable.health_score.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# InvoiceRepository (tenant-scoped)
# ---------------------------------------------------------------------------


class InvoiceRepository:
    """CRUD operations for the ``invoices`` table.

    Manages invoice creation, retrieval, and PDF storage for per-tenant
    billing records.
    """

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create(
        self,
        *,
        invoice_id: str,
        invoice_number: str,
        period_start: datetime,
        period_end: datetime,
        subtotal_usd: float,
        tax_usd: float,
        total_usd: float,
        line_items: list[dict[str, Any]],
        stripe_invoice_id: str | None = None,
        pdf_storage_key: str | None = None,
    ) -> InvoiceTable:
        """Create a new invoice record."""
        row = InvoiceTable(
            invoice_id=invoice_id,
            tenant_id=self._tenant_id,
            stripe_invoice_id=stripe_invoice_id,
            invoice_number=invoice_number,
            period_start=period_start,
            period_end=period_end,
            subtotal_usd=subtotal_usd,
            tax_usd=tax_usd,
            total_usd=total_usd,
            line_items_json=line_items,
            pdf_storage_key=pdf_storage_key,
            status="generated",
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get(self, invoice_id: str) -> InvoiceTable | None:
        """Fetch a single invoice by ID for this tenant."""
        stmt = select(InvoiceTable).where(
            InvoiceTable.tenant_id == self._tenant_id,
            InvoiceTable.invoice_id == invoice_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_for_tenant(
        self,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[InvoiceTable], int]:
        """List invoices for this tenant, newest first.

        Returns
        -------
        tuple
            ``(rows, total_count)`` for pagination support.
        """
        count_r = await self._session.execute(
            select(func.count())
            .select_from(InvoiceTable)
            .where(
                InvoiceTable.tenant_id == self._tenant_id,
            )
        )
        total = count_r.scalar_one()

        stmt = (
            select(InvoiceTable)
            .where(InvoiceTable.tenant_id == self._tenant_id)
            .order_by(InvoiceTable.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all()), total

    async def update_pdf_key(self, invoice_id: str, key: str) -> bool:
        """Set the PDF storage key for an invoice.  Returns True if updated."""
        stmt = (
            update(InvoiceTable)
            .where(
                InvoiceTable.tenant_id == self._tenant_id,
                InvoiceTable.invoice_id == invoice_id,
            )
            .values(pdf_storage_key=key)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def update_status(self, invoice_id: str, status: str) -> bool:
        """Update invoice status (generated, paid, void).  Returns True if updated."""
        stmt = (
            update(InvoiceTable)
            .where(
                InvoiceTable.tenant_id == self._tenant_id,
                InvoiceTable.invoice_id == invoice_id,
            )
            .values(status=status)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0  # type: ignore[attr-defined]

    async def get_next_invoice_number(self) -> str:
        """Generate the next sequential invoice number.

        Format: ``INV-YYYYMM-XXXX`` where XXXX is a zero-padded sequence
        number for the current month.

        Acquires an advisory lock (PostgreSQL) before the COUNT query to
        prevent two concurrent requests from generating the same number.
        """
        # Acquire advisory lock to serialise invoice number generation.
        lock_id = hash(f"invoice_number_{self._tenant_id}") & 0x7FFFFFFF
        bind = self._session.get_bind()
        dialect_name = getattr(getattr(bind, "dialect", None), "name", "")
        if "postgresql" in str(dialect_name):
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(:id)"),
                {"id": lock_id},
            )
        # For SQLite: single-writer semantics, no advisory lock needed.

        now = datetime.now(UTC)
        prefix = f"INV-{now.strftime('%Y%m')}-"
        stmt = (
            select(func.count())
            .select_from(InvoiceTable)
            .where(
                InvoiceTable.tenant_id == self._tenant_id,
                InvoiceTable.invoice_number.like(f"{_escape_like(prefix)}%"),
            )
        )
        result = await self._session.execute(stmt)
        count = result.scalar_one()
        return f"{prefix}{count + 1:04d}"

    async def get_by_stripe_invoice(self, stripe_invoice_id: str) -> InvoiceTable | None:
        """Fetch an invoice by its Stripe invoice ID."""
        stmt = select(InvoiceTable).where(
            InvoiceTable.tenant_id == self._tenant_id,
            InvoiceTable.stripe_invoice_id == stripe_invoice_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()


class EventOutboxRepository:
    """CRUD operations for the transactional event outbox.

    Used by :meth:`EventBus.emit_persistent` to write events within the
    caller's transaction, and by :class:`OutboxPoller` to poll and
    mark entries delivered.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def write(
        self,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: str,
    ) -> EventOutboxTable:
        """Insert a pending outbox entry within the current transaction.

        The caller is responsible for committing the transaction.
        """
        from core_engine.state.tables import EventOutboxTable

        row = EventOutboxTable(
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
            correlation_id=correlation_id,
            status="pending",
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_pending(self, limit: int = 100) -> list[EventOutboxTable]:
        """Return pending entries ordered by ``created_at`` (oldest first)."""
        from core_engine.state.tables import EventOutboxTable

        stmt = (
            select(EventOutboxTable)
            .where(EventOutboxTable.status == "pending")
            .order_by(EventOutboxTable.created_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def count_pending(self) -> int:
        """Return the number of pending outbox entries."""
        from core_engine.state.tables import EventOutboxTable

        stmt = select(func.count()).select_from(EventOutboxTable).where(
            EventOutboxTable.status == "pending"
        )
        result = await self._session.execute(stmt)
        return result.scalar_one()

    async def mark_delivered(self, entry_id: int) -> None:
        """Mark an outbox entry as successfully delivered."""
        from datetime import UTC, datetime

        from core_engine.state.tables import EventOutboxTable

        stmt = (
            update(EventOutboxTable)
            .where(EventOutboxTable.id == entry_id)
            .values(status="delivered", delivered_at=datetime.now(UTC))
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_delivered_batch(self, entry_ids: list[int]) -> None:
        """Mark multiple outbox entries as delivered in a single UPDATE.

        Parameters
        ----------
        entry_ids:
            List of outbox entry primary-key IDs to mark delivered.
        """
        if not entry_ids:
            return
        from datetime import UTC, datetime

        from core_engine.state.tables import EventOutboxTable

        stmt = (
            update(EventOutboxTable)
            .where(EventOutboxTable.id.in_(entry_ids))
            .values(status="delivered", delivered_at=datetime.now(UTC))
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_failed(
        self, entry_id: int, error: str, *, permanent: bool = False
    ) -> None:
        """Increment attempt count and record the last error message.

        Parameters
        ----------
        permanent:
            When True the entry status is set to 'failed' so it is no
            longer returned by get_pending or counted by count_pending.
            Use this when the entry has exceeded max_attempts.
        """
        from core_engine.state.tables import EventOutboxTable

        values: dict = {
            "attempts": EventOutboxTable.attempts + 1,
            "last_error": error[:1024],
        }
        if permanent:
            values["status"] = "failed"

        stmt = (
            update(EventOutboxTable)
            .where(EventOutboxTable.id == entry_id)
            .values(**values)
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def cleanup_delivered(self, older_than_hours: int = 24) -> int:
        """Remove delivered entries older than ``older_than_hours``.

        Returns the number of rows deleted.
        """
        from datetime import UTC, datetime, timedelta

        from core_engine.state.tables import EventOutboxTable

        cutoff = datetime.now(UTC) - timedelta(hours=older_than_hours)
        stmt = delete(EventOutboxTable).where(
            EventOutboxTable.status == "delivered",
            EventOutboxTable.delivered_at.is_not(None),
            EventOutboxTable.delivered_at < cutoff,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount

    async def cleanup_failed(self, older_than_hours: int = 24) -> int:
        """Remove permanently failed entries older than ``older_than_hours``.

        Without periodic cleanup, entries marked ``status='failed'`` by
        :meth:`mark_failed(permanent=True)` accumulate indefinitely.

        Returns the number of rows deleted.
        """
        from datetime import UTC, datetime, timedelta

        from core_engine.state.tables import EventOutboxTable

        cutoff = datetime.now(UTC) - timedelta(hours=older_than_hours)
        stmt = delete(EventOutboxTable).where(
            EventOutboxTable.status == "failed",
            EventOutboxTable.created_at < cutoff,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount
