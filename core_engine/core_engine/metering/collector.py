"""Thread-safe in-memory usage event collector with configurable sinks.

The :class:`MeteringCollector` buffers events in memory and flushes them
to a :class:`MeteringSink` implementation at configurable intervals or
buffer thresholds.

Two sinks are provided:

* :class:`DatabaseSink` -- persists events to the ``usage_events`` table.
* :class:`FileSink` -- appends events as JSON lines to a local file
  (useful for ``platform dev`` local mode).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

from core_engine.metering.events import UsageEvent, UsageEventType

logger = logging.getLogger(__name__)


class MeteringSink(Protocol):
    """Protocol for metering event persistence."""

    def flush(self, events: Sequence[UsageEvent]) -> None:
        """Persist a batch of events."""
        ...


class FileSink:
    """Appends usage events as JSON lines to a local file.

    Parameters
    ----------
    path:
        Path to the JSON lines file.
    """

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def flush(self, events: Sequence[UsageEvent]) -> None:
        """Append events as newline-delimited JSON."""
        if not events:
            return
        with self._path.open("a", encoding="utf-8") as fh:
            for event in events:
                fh.write(event.model_dump_json() + "\n")
        logger.debug("Flushed %d events to %s", len(events), self._path)


class DatabaseSink:
    """Persists usage events to the ``usage_events`` database table.

    Parameters
    ----------
    session_factory:
        An async session factory for database access.

    Note: This sink requires an async context, so ``flush()`` spawns a
    background coroutine.  For synchronous callers, use :class:`FileSink`.
    """

    def __init__(self, session_factory: Any) -> None:
        self._session_factory = session_factory

    def flush(self, events: Sequence[UsageEvent]) -> None:
        """Persist events to the database (sync wrapper)."""
        if not events:
            return

        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._async_flush(events))
        except RuntimeError:
            # No running loop -- run synchronously.
            asyncio.run(self._async_flush(events))

    async def _async_flush(self, events: Sequence[UsageEvent]) -> None:
        """Persist events to the usage_events table."""
        from core_engine.state.tables import UsageEventTable

        try:
            async with self._session_factory() as session:
                for event in events:
                    row = UsageEventTable(
                        event_id=event.event_id,
                        tenant_id=event.tenant_id,
                        event_type=event.event_type.value,
                        quantity=event.quantity,
                        metadata_json=event.metadata,
                        created_at=event.timestamp,
                    )
                    session.add(row)
                await session.commit()
            logger.debug("Flushed %d events to database", len(events))
        except Exception:
            logger.warning("Failed to flush metering events to database", exc_info=True)


class MeteringCollector:
    """Thread-safe in-memory event collector with periodic flushing.

    Parameters
    ----------
    sink:
        The sink to flush events to.
    flush_interval_seconds:
        How often to flush (default: 60s).
    max_buffer_size:
        Maximum events before forcing a flush (default: 1000).
    """

    def __init__(
        self,
        sink: MeteringSink,
        flush_interval_seconds: float = 60.0,
        max_buffer_size: int = 1000,
    ) -> None:
        self._sink = sink
        self._flush_interval = flush_interval_seconds
        self._max_buffer_size = max_buffer_size
        self._buffer: list[UsageEvent] = []
        self._lock = threading.Lock()
        self._flush_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def record(self, event: UsageEvent) -> None:
        """Record a usage event.

        Thread-safe.  If the buffer exceeds ``max_buffer_size``, triggers
        an immediate flush.

        Parameters
        ----------
        event:
            The usage event to record.
        """
        with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= self._max_buffer_size:
                self._flush_locked()

    def record_event(
        self,
        *,
        tenant_id: str,
        event_type: UsageEventType,
        quantity: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> UsageEvent:
        """Create and record a usage event in one call.

        Parameters
        ----------
        tenant_id:
            The tenant generating the event.
        event_type:
            The type of metered event.
        quantity:
            Number of units consumed.
        metadata:
            Additional context.

        Returns
        -------
        UsageEvent
            The recorded event.
        """
        event = UsageEvent(
            tenant_id=tenant_id,
            event_type=event_type,
            quantity=quantity,
            metadata=metadata or {},
        )
        self.record(event)
        return event

    def flush(self) -> int:
        """Flush all buffered events to the sink.

        Returns
        -------
        int
            Number of events flushed.
        """
        with self._lock:
            return self._flush_locked()

    def _flush_locked(self) -> int:
        """Flush while already holding the lock."""
        if not self._buffer:
            return 0
        batch = list(self._buffer)
        self._buffer.clear()
        try:
            self._sink.flush(batch)
        except Exception:
            logger.warning("Metering flush failed; %d events lost", len(batch), exc_info=True)
        return len(batch)

    def start_background_flush(self) -> None:
        """Start a background thread that flushes at regular intervals."""
        if self._flush_thread is not None:
            return

        self._stop_event.clear()

        def _run() -> None:
            while not self._stop_event.wait(self._flush_interval):
                self.flush()
            # Final flush on stop.
            self.flush()

        self._flush_thread = threading.Thread(target=_run, name="metering-flush", daemon=True)
        self._flush_thread.start()
        logger.info(
            "Metering background flush started (interval=%.0fs)",
            self._flush_interval,
        )

    def stop_background_flush(self) -> None:
        """Stop the background flush thread and do a final flush."""
        self._stop_event.set()
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5.0)
            self._flush_thread = None
        self.flush()

    @property
    def pending_count(self) -> int:
        """Number of events currently buffered."""
        with self._lock:
            return len(self._buffer)

    def get_usage_summary(
        self,
        tenant_id: str | None = None,
    ) -> dict[str, int]:
        """Return a summary of buffered events by type.

        This queries the in-memory buffer only (not the sink).

        Parameters
        ----------
        tenant_id:
            If provided, filter events by tenant.

        Returns
        -------
        dict[str, int]
            Event counts by type.
        """
        with self._lock:
            summary: dict[str, int] = {}
            for event in self._buffer:
                if tenant_id and event.tenant_id != tenant_id:
                    continue
                key = event.event_type.value
                summary[key] = summary.get(key, 0) + event.quantity
            return summary
