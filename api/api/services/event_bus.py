"""Lightweight event bus for IronLayer lifecycle hooks.

Provides fire-and-forget event emission with registered handlers.
Handler errors are logged but never propagate to callers, ensuring
that event dispatch never disrupts the primary request path.

Usage::

    bus = get_event_bus()
    await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1", data={...})

Handlers are registered at startup via ``bus.register_handler()``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------


class EventType(str, Enum):
    """Lifecycle events emitted by the IronLayer control plane."""

    PLAN_GENERATED = "plan.generated"
    PLAN_APPROVED = "plan.approved"
    PLAN_REJECTED = "plan.rejected"
    PLAN_APPLY_STARTED = "plan.apply_started"
    PLAN_APPLY_COMPLETED = "plan.apply_completed"
    PLAN_APPLY_FAILED = "plan.apply_failed"
    RUN_STARTED = "run.started"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"
    SCHEMA_DRIFT_DETECTED = "schema.drift_detected"
    TEST_GATE_BLOCKED = "test.gate_blocked"


# ---------------------------------------------------------------------------
# Event payload
# ---------------------------------------------------------------------------


class EventPayload(BaseModel):
    """Structured event payload dispatched to handlers."""

    event_type: EventType
    tenant_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    data: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Handler type
# ---------------------------------------------------------------------------

EventHandler = Callable[[EventPayload], Awaitable[None]]

# BL-089: Global semaphore limiting concurrent handler calls across all emits.
_EMIT_SEMAPHORE = asyncio.Semaphore(5)


# ---------------------------------------------------------------------------
# Event bus
# ---------------------------------------------------------------------------


class EventBus:
    """In-process event bus with async handler dispatch.

    Handlers are called concurrently via ``asyncio.gather``.  Each handler
    runs inside the module-level ``_EMIT_SEMAPHORE`` (max 5 concurrent) and
    is subject to a 5-second per-handler timeout.  A single failing handler
    does not affect others or the caller.
    """

    def __init__(self) -> None:
        self._handlers: dict[EventType | None, list[EventHandler]] = {}

    def register_handler(
        self,
        handler: EventHandler,
        *,
        event_type: EventType | None = None,
    ) -> None:
        """Register a handler for a specific event type (or all events).

        Parameters
        ----------
        handler:
            Async callable that accepts an :class:`EventPayload`.
        event_type:
            If ``None``, the handler receives *all* events (wildcard).
        """
        self._handlers.setdefault(event_type, []).append(handler)
        logger.debug(
            "Registered event handler %s for %s",
            handler.__name__,
            event_type or "ALL",
        )

    async def emit(
        self,
        event_type: EventType,
        *,
        tenant_id: str,
        data: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Emit an event to all matching handlers.

        This is fire-and-forget: handler exceptions are logged, not raised.
        Each handler is bounded by a 5-second timeout and the module-level
        concurrency semaphore (max 5 simultaneous handlers).

        Parameters
        ----------
        event_type:
            The lifecycle event that occurred.
        tenant_id:
            The tenant that triggered the event.
        data:
            Arbitrary event payload.
        correlation_id:
            Optional correlation ID for distributed tracing.
        """
        payload = EventPayload(
            event_type=event_type,
            tenant_id=tenant_id,
            data=data or {},
            correlation_id=correlation_id or uuid.uuid4().hex,
        )

        handlers = list(self._handlers.get(event_type, []))
        handlers.extend(self._handlers.get(None, []))

        if not handlers:
            logger.debug("No handlers for event %s", event_type.value)
            return

        logger.info(
            "Emitting %s for tenant=%s corr=%s (%d handler(s))",
            event_type.value,
            tenant_id,
            payload.correlation_id[:8],
            len(handlers),
        )

        # BL-089: semaphore + per-handler timeout.
        async def _safe_call(handler: EventHandler) -> None:
            async with _EMIT_SEMAPHORE:
                try:
                    await asyncio.wait_for(handler(payload), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "Event handler timed out: %s", handler.__name__
                    )
                except Exception as exc:
                    logger.error(
                        "Handler failed: %s — %r", handler.__name__, exc
                    )

        await asyncio.gather(*[_safe_call(h) for h in handlers])

    async def emit_persistent(
        self,
        session: Any,
        event_type: "EventType",
        *,
        tenant_id: str,
        data: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Emit an event and persist it to the transactional outbox.

        The outbox write is part of the caller's open transaction, so the
        event is guaranteed to be persisted if and only if the transaction
        commits.  The in-memory handlers are also dispatched immediately
        (best-effort, fire-and-forget).

        Parameters
        ----------
        session:
            An open :class:`AsyncSession` to write the outbox entry into.
            The caller must commit (or roll back) the transaction.
        event_type:
            The lifecycle event to emit.
        tenant_id:
            Tenant that triggered the event.
        data:
            Arbitrary event payload.
        correlation_id:
            Optional correlation ID; auto-generated if omitted.
        """
        corr_id = correlation_id or uuid.uuid4().hex
        payload = EventPayload(
            event_type=event_type,
            tenant_id=tenant_id,
            data=data or {},
            correlation_id=corr_id,
        )

        # Write to outbox within the caller's transaction.
        from core_engine.state.repository import EventOutboxRepository

        outbox = EventOutboxRepository(session)
        await outbox.write(
            tenant_id=tenant_id,
            event_type=event_type.value,
            payload=payload.model_dump(mode="json"),
            correlation_id=corr_id,
        )

        # Also dispatch in-memory handlers immediately (best-effort).
        await self.emit(
            event_type,
            tenant_id=tenant_id,
            data=data,
            correlation_id=corr_id,
        )

        logger.info(
            "Persistent event emitted to outbox: %s tenant=%s corr=%s",
            event_type.value,
            tenant_id,
            corr_id[:8],
        )

    async def emit_persistent_batch(
        self,
        session: Any,
        events: list[tuple[EventType, str, dict[str, Any] | None, str | None]],
    ) -> None:
        """Persist multiple events to the outbox in a single bulk INSERT.

        All rows are inserted atomically within the caller's open transaction
        using SQLAlchemy's bulk ``insert()``.  In-memory handlers are also
        dispatched immediately (best-effort, fire-and-forget) for each event.

        Parameters
        ----------
        session:
            An open :class:`AsyncSession` to write the outbox entries into.
            The caller must commit (or roll back) the transaction.
        events:
            A list of ``(event_type, tenant_id, data, correlation_id)``
            tuples.  ``data`` and ``correlation_id`` may be ``None``.
        """
        if not events:
            return

        from sqlalchemy import insert as sa_insert

        from core_engine.state.tables import EventOutboxTable

        now = datetime.now(UTC)
        rows: list[dict[str, Any]] = []
        payloads: list[tuple[EventType, str, dict[str, Any], str]] = []

        for event_type, tenant_id, data, correlation_id in events:
            corr_id = correlation_id or uuid.uuid4().hex
            payload = EventPayload(
                event_type=event_type,
                tenant_id=tenant_id,
                data=data or {},
                correlation_id=corr_id,
                timestamp=now,
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "event_type": event_type.value,
                    "payload": payload.model_dump(mode="json"),
                    "correlation_id": corr_id,
                    "status": "pending",
                    "created_at": now,
                    "attempts": 0,
                }
            )
            payloads.append((event_type, tenant_id, data or {}, corr_id))

        await session.execute(sa_insert(EventOutboxTable), rows)
        await session.flush()

        logger.info(
            "Persistent batch of %d events inserted into outbox", len(rows)
        )

        # Dispatch in-memory handlers best-effort for each event.
        for event_type, tenant_id, data, corr_id in payloads:
            await self.emit(
                event_type,
                tenant_id=tenant_id,
                data=data,
                correlation_id=corr_id,
            )

    @property
    def handler_count(self) -> int:
        """Total number of registered handlers across all event types."""
        return sum(len(v) for v in self._handlers.values())


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------


def make_audit_log_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> EventHandler:
    """Create an audit log handler backed by the AuditRepository.

    Each event is persisted as an audit log entry with hash-chaining
    for tamper evidence.  A new database session is created per event
    to avoid coupling handler lifetime to the request session.

    Parameters
    ----------
    session_factory:
        Async session factory for creating per-event database sessions.

    Returns
    -------
    EventHandler
        Async handler suitable for ``EventBus.register_handler()``.
    """

    async def _persist_audit(payload: EventPayload) -> None:
        from core_engine.state.repository import AuditRepository

        async with session_factory() as session:
            repo = AuditRepository(session, tenant_id=payload.tenant_id)
            await repo.log(
                actor="system:event_bus",
                action=payload.event_type.value,
                entity_type="event",
                entity_id=payload.correlation_id,
                metadata=payload.data,
            )
            await session.commit()

        logger.info(
            "AUDIT: %s tenant=%s corr=%s data_keys=%s",
            payload.event_type.value,
            payload.tenant_id,
            payload.correlation_id[:8],
            sorted(payload.data.keys()),
        )

    _persist_audit.__name__ = "audit_log_handler"
    return _persist_audit


async def audit_log_handler(payload: EventPayload) -> None:
    """Fallback audit log handler that logs to stdout.

    Used when no database session factory is available (e.g. during
    testing or before the database has been initialised).
    """
    logger.info(
        "AUDIT: %s tenant=%s corr=%s data_keys=%s",
        payload.event_type.value,
        payload.tenant_id,
        payload.correlation_id[:8],
        sorted(payload.data.keys()),
    )


async def metrics_handler(payload: EventPayload) -> None:
    """Increment Prometheus counters for each event type.

    In production, this integrates with the PrometheusMiddleware's
    counter registry.  Here it logs at DEBUG level.
    """
    logger.debug(
        "METRIC: ironlayer_event_total{type=%s,tenant=%s} += 1",
        payload.event_type.value,
        payload.tenant_id,
    )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_event_bus: EventBus | None = None


def init_event_bus(
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> EventBus:
    """Create and configure the global event bus with built-in handlers.

    Parameters
    ----------
    session_factory:
        When provided, the audit log handler persists events to the
        database via :class:`AuditRepository`.  When ``None``, a
        fallback handler that only logs to stdout is used.
    """
    global _event_bus  # noqa: PLW0603
    _event_bus = EventBus()

    if session_factory is not None:
        _event_bus.register_handler(make_audit_log_handler(session_factory))
    else:
        _event_bus.register_handler(audit_log_handler)
    _event_bus.register_handler(metrics_handler)

    logger.info("Event bus initialised with %d handler(s)", _event_bus.handler_count)
    return _event_bus


def get_event_bus() -> EventBus:
    """Return the module-level event bus instance."""
    if _event_bus is None:
        return init_event_bus()
    return _event_bus


# ---------------------------------------------------------------------------
# Outbox poller
# ---------------------------------------------------------------------------


class OutboxPoller:
    """Background task that polls the event outbox and dispatches pending events.

    Runs as a long-lived asyncio ``Task``.  On each cycle it:

    1. Counts pending entries and computes an adaptive batch limit:
       ``limit = min(500, max(100, pending_count * 2))``.
    2. Fetches up to ``limit`` ``pending`` outbox entries (oldest first)
       using a single session for the full read phase.
    3. Dispatches each entry through the registered event bus handlers.
    4. Marks all successfully dispatched entries as ``delivered`` in a
       single batch UPDATE (``WHERE id IN (...)``).
    5. Increments ``attempts`` and records the error for failed entries.
    6. Periodically removes old ``delivered`` entries (default: every
       15 minutes; entries older than 12 hours are pruned).

    Parameters
    ----------
    session_factory:
        Async session factory for creating per-poll database sessions.
    event_bus:
        The :class:`EventBus` whose handlers receive dispatched events.
    poll_interval_seconds:
        Seconds between polling cycles (default: 5).
    max_attempts:
        Maximum delivery attempts before an entry is permanently failed (default: 3).
    cleanup_interval_seconds:
        Seconds between cleanup runs that prune old delivered entries
        (default: 900 — 15 minutes).
    cleanup_retention_hours:
        Delivered entries older than this many hours are removed during
        cleanup (default: 12).
    """

    def __init__(
        self,
        session_factory: Any,
        event_bus: EventBus,
        *,
        poll_interval_seconds: float = 5.0,
        max_attempts: int = 3,
        cleanup_interval_seconds: float = 900.0,
        cleanup_retention_hours: int = 12,
    ) -> None:
        self._session_factory = session_factory
        self._bus = event_bus
        self._poll_interval = poll_interval_seconds
        self._max_attempts = max_attempts
        self._cleanup_interval = cleanup_interval_seconds
        self._cleanup_retention_hours = cleanup_retention_hours
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start the background polling task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop(), name="outbox-poller")
            logger.info(
                "OutboxPoller started (interval=%.1fs, max_attempts=%d)",
                self._poll_interval,
                self._max_attempts,
            )

    def stop(self) -> None:
        """Cancel the background polling task."""
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("OutboxPoller stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop — runs until cancelled."""
        import time as _time

        last_cleanup = _time.monotonic()
        while True:
            try:
                await self._poll_once()
                # Periodic cleanup of old delivered entries.
                now = _time.monotonic()
                if now - last_cleanup > self._cleanup_interval:
                    await self._cleanup()
                    last_cleanup = now
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("OutboxPoller encountered an unexpected error")
            await asyncio.sleep(self._poll_interval)

    async def _poll_once(self) -> None:
        """Fetch and dispatch one adaptive batch of pending outbox entries.

        **Design**:

        Phase 1 — Read (single session): count pending entries to compute
        an adaptive batch limit (``min(500, max(100, pending_count * 2))``),
        then fetch up to that many rows.  All values are snapshotted into
        plain Python dicts before the session closes to avoid SQLAlchemy
        ``expire_on_commit`` issues.

        Phase 2 — Dispatch: each entry is dispatched through the registered
        handlers.  Successfully dispatched IDs are collected and then marked
        delivered in a **single batch UPDATE** (``WHERE id IN (...)``).
        Failed entries are updated individually so that partial failures do
        not prevent the batch commit for the successful ones.
        """
        from core_engine.state.repository import EventOutboxRepository

        # --- Phase 1: adaptive batch read (single session, read-only) --------
        async with self._session_factory() as session:
            repo = EventOutboxRepository(session)
            pending_count = await repo.count_pending()
            if pending_count == 0:
                return
            limit = min(500, max(100, pending_count * 2))
            rows = await repo.get_pending(limit=limit)
            snapshots = [
                {
                    "id": row.id,
                    "attempts": row.attempts,
                    "event_type": row.event_type,
                    "payload": dict(row.payload),
                }
                for row in rows
            ]
        # Session closed — no live ORM references below.

        # --- Phase 2: dispatch + batch-UPDATE in one session -----------------
        delivered_ids: list[int] = []
        failed_snaps: list[dict[str, Any]] = []

        for snap in snapshots:
            entry_id: int = snap["id"]
            attempts: int = snap["attempts"]

            if attempts >= self._max_attempts:
                failed_snaps.append({**snap, "_error": "max_attempts_exceeded"})
                continue

            try:
                event_type = EventType(snap["event_type"])
                payload = EventPayload(**snap["payload"])
                handlers = [
                    *self._bus._handlers.get(event_type, []),
                    *self._bus._handlers.get(None, []),
                ]
                all_ok = True
                for handler in handlers:
                    try:
                        async with _EMIT_SEMAPHORE:
                            await asyncio.wait_for(handler(payload), timeout=5.0)
                    except asyncio.TimeoutError:
                        all_ok = False
                        logger.warning(
                            "OutboxPoller handler %s timed out (5s) for entry id=%d",
                            getattr(handler, "__name__", str(handler)),
                            entry_id,
                        )
                    except Exception:
                        all_ok = False
                        logger.exception(
                            "OutboxPoller handler %s failed for entry id=%d",
                            getattr(handler, "__name__", str(handler)),
                            entry_id,
                        )
                if all_ok:
                    delivered_ids.append(entry_id)
                else:
                    failed_snaps.append({**snap, "_error": "handler_error"})
            except Exception as exc:
                failed_snaps.append({**snap, "_error": str(exc)[:1024]})
                logger.warning(
                    "OutboxPoller failed to dispatch outbox entry id=%d: %s",
                    entry_id,
                    exc,
                )

        async with self._session_factory() as session:
            repo = EventOutboxRepository(session)

            # Batch-UPDATE all delivered entries in one statement.
            if delivered_ids:
                await repo.mark_delivered_batch(delivered_ids)

            # Update each failed entry individually.  Entries that exceeded
            # max_attempts are permanently marked status='failed' so they
            # are no longer returned by get_pending().
            for snap in failed_snaps:
                is_permanent = snap["_error"] == "max_attempts_exceeded"
                await repo.mark_failed(
                    snap["id"], snap["_error"], permanent=is_permanent,
                )

            await session.commit()

    async def _cleanup(self) -> None:
        """Prune old delivered and permanently failed outbox entries.

        Removes entries older than ``cleanup_retention_hours`` (default: 12 h).
        Both ``status='delivered'`` and ``status='failed'`` rows are pruned
        so that neither accumulates indefinitely.
        """
        from core_engine.state.repository import EventOutboxRepository

        async with self._session_factory() as session:
            repo = EventOutboxRepository(session)
            n_delivered = await repo.cleanup_delivered(
                older_than_hours=self._cleanup_retention_hours
            )
            n_failed = await repo.cleanup_failed(
                older_than_hours=self._cleanup_retention_hours
            )
            await session.commit()
            logger.info(
                "OutboxPoller cleanup: deleted %d delivered + %d failed entries "
                "(retention=%dh)",
                n_delivered,
                n_failed,
                self._cleanup_retention_hours,
            )


# Module-level outbox poller instance (started at app startup).
_outbox_poller: OutboxPoller | None = None


def init_outbox_poller(
    session_factory: Any,
    event_bus: EventBus | None = None,
    *,
    poll_interval_seconds: float = 5.0,
) -> OutboxPoller:
    """Create and start the outbox poller background task.

    Should be called once at application startup, after the event bus and
    database session factory are initialised.

    Parameters
    ----------
    session_factory:
        Async session factory for the poller's database sessions.
    event_bus:
        The event bus to dispatch through.  Defaults to the module-level
        singleton returned by :func:`get_event_bus`.
    poll_interval_seconds:
        Seconds between polling cycles.
    """
    global _outbox_poller  # noqa: PLW0603

    bus = event_bus or get_event_bus()
    _outbox_poller = OutboxPoller(
        session_factory,
        bus,
        poll_interval_seconds=poll_interval_seconds,
    )
    _outbox_poller.start()
    logger.info("OutboxPoller initialised")
    return _outbox_poller


def get_outbox_poller() -> OutboxPoller | None:
    """Return the module-level outbox poller instance, or ``None`` if not started."""
    return _outbox_poller
