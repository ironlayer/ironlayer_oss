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


# ---------------------------------------------------------------------------
# Event bus
# ---------------------------------------------------------------------------


class EventBus:
    """In-process event bus with async handler dispatch.

    Handlers are called concurrently via ``asyncio.gather``.  Each handler
    runs in a ``try / except`` so that a single failing handler does not
    affect others or the caller.
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

        async def _safe_call(handler: EventHandler) -> None:
            try:
                await handler(payload)
            except Exception:
                logger.exception(
                    "Handler %s failed for event %s (tenant=%s)",
                    handler.__name__,
                    event_type.value,
                    tenant_id,
                )

        await asyncio.gather(*[_safe_call(h) for h in handlers])

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
