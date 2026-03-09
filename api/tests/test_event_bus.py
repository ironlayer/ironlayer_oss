"""Tests for the event bus lifecycle hook system.

Validates handler registration, event emission, error isolation, and
the built-in audit log and metrics handlers.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.event_bus import (
    EventBus,
    EventPayload,
    EventType,
    OutboxPoller,
    _EMIT_SEMAPHORE,
    audit_log_handler,
    get_event_bus,
    init_event_bus,
    metrics_handler,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bus() -> EventBus:
    """Return a fresh EventBus instance (no built-in handlers)."""
    return EventBus()


@pytest.fixture
def payload() -> EventPayload:
    """Return a sample event payload."""
    return EventPayload(
        event_type=EventType.PLAN_GENERATED,
        tenant_id="tenant-test",
        data={"plan_id": "plan_abc123"},
    )


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------


class TestHandlerRegistration:
    """Tests for EventBus.register_handler()."""

    def test_register_handler_for_specific_event(self, bus: EventBus) -> None:
        async def handler(p: EventPayload) -> None:
            pass

        bus.register_handler(handler, event_type=EventType.PLAN_GENERATED)
        assert bus.handler_count == 1

    def test_register_handler_wildcard(self, bus: EventBus) -> None:
        async def handler(p: EventPayload) -> None:
            pass

        bus.register_handler(handler)
        assert bus.handler_count == 1

    def test_register_multiple_handlers(self, bus: EventBus) -> None:
        async def h1(p: EventPayload) -> None:
            pass

        async def h2(p: EventPayload) -> None:
            pass

        async def h3(p: EventPayload) -> None:
            pass

        bus.register_handler(h1, event_type=EventType.PLAN_GENERATED)
        bus.register_handler(h2, event_type=EventType.PLAN_APPROVED)
        bus.register_handler(h3)  # wildcard
        assert bus.handler_count == 3

    def test_handler_count_empty(self, bus: EventBus) -> None:
        assert bus.handler_count == 0


# ---------------------------------------------------------------------------
# Event emission
# ---------------------------------------------------------------------------


class TestEventEmission:
    """Tests for EventBus.emit()."""

    @pytest.mark.asyncio
    async def test_emit_dispatches_to_matching_handler(self, bus: EventBus) -> None:
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler, event_type=EventType.PLAN_GENERATED)
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1", data={"key": "val"})

        assert len(received) == 1
        assert received[0].event_type == EventType.PLAN_GENERATED
        assert received[0].tenant_id == "t1"
        assert received[0].data == {"key": "val"}

    @pytest.mark.asyncio
    async def test_emit_does_not_dispatch_to_unmatched_handler(self, bus: EventBus) -> None:
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler, event_type=EventType.PLAN_APPROVED)
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_emit_dispatches_to_wildcard_handler(self, bus: EventBus) -> None:
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler)  # wildcard
        await bus.emit(EventType.RUN_COMPLETED, tenant_id="t1")

        assert len(received) == 1
        assert received[0].event_type == EventType.RUN_COMPLETED

    @pytest.mark.asyncio
    async def test_emit_dispatches_to_both_specific_and_wildcard(self, bus: EventBus) -> None:
        specific_received: list[EventPayload] = []
        wildcard_received: list[EventPayload] = []

        async def specific(p: EventPayload) -> None:
            specific_received.append(p)

        async def wildcard(p: EventPayload) -> None:
            wildcard_received.append(p)

        bus.register_handler(specific, event_type=EventType.PLAN_GENERATED)
        bus.register_handler(wildcard)

        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert len(specific_received) == 1
        assert len(wildcard_received) == 1

    @pytest.mark.asyncio
    async def test_emit_generates_correlation_id_when_not_provided(self, bus: EventBus) -> None:
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler)
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert len(received) == 1
        assert received[0].correlation_id  # non-empty
        assert len(received[0].correlation_id) == 32  # uuid hex

    @pytest.mark.asyncio
    async def test_emit_uses_provided_correlation_id(self, bus: EventBus) -> None:
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler)
        await bus.emit(
            EventType.PLAN_GENERATED,
            tenant_id="t1",
            correlation_id="custom_corr_id",
        )

        assert received[0].correlation_id == "custom_corr_id"

    @pytest.mark.asyncio
    async def test_emit_with_no_handlers_succeeds_silently(self, bus: EventBus) -> None:
        # Should not raise.
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")


# ---------------------------------------------------------------------------
# Error isolation
# ---------------------------------------------------------------------------


class TestErrorIsolation:
    """Tests that handler failures do not propagate to the caller."""

    @pytest.mark.asyncio
    async def test_failing_handler_does_not_raise(self, bus: EventBus) -> None:
        async def bad_handler(p: EventPayload) -> None:
            raise RuntimeError("Handler broke!")

        bus.register_handler(bad_handler)

        # emit() should NOT raise even though the handler fails.
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

    @pytest.mark.asyncio
    async def test_failing_handler_does_not_block_others(self, bus: EventBus) -> None:
        received: list[str] = []

        async def bad_handler(p: EventPayload) -> None:
            raise RuntimeError("I fail!")

        async def good_handler(p: EventPayload) -> None:
            received.append("ok")

        bus.register_handler(bad_handler)
        bus.register_handler(good_handler)

        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")
        assert "ok" in received

    @pytest.mark.asyncio
    async def test_failing_handler_logged(self, bus: EventBus, caplog: pytest.LogCaptureFixture) -> None:
        async def bad_handler(p: EventPayload) -> None:
            raise ValueError("Test error")

        bus.register_handler(bad_handler)

        with caplog.at_level(logging.ERROR):
            await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert any("bad_handler" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# EventPayload
# ---------------------------------------------------------------------------


class TestEventPayload:
    """Tests for EventPayload defaults and serialisation."""

    def test_payload_defaults(self) -> None:
        p = EventPayload(
            event_type=EventType.PLAN_GENERATED,
            tenant_id="t1",
        )
        assert p.tenant_id == "t1"
        assert p.event_type == EventType.PLAN_GENERATED
        assert p.data == {}
        assert p.correlation_id  # auto-generated
        assert p.timestamp is not None

    def test_payload_custom_data(self) -> None:
        p = EventPayload(
            event_type=EventType.RUN_FAILED,
            tenant_id="t1",
            data={"error": "OOM"},
        )
        assert p.data["error"] == "OOM"

    def test_payload_model_dump_json(self) -> None:
        p = EventPayload(
            event_type=EventType.PLAN_APPROVED,
            tenant_id="t1",
        )
        json_str = p.model_dump_json()
        assert "plan.approved" in json_str


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------


class TestBuiltInHandlers:
    """Tests for the audit_log_handler and metrics_handler."""

    @pytest.mark.asyncio
    async def test_audit_log_handler_runs(self, payload: EventPayload, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            await audit_log_handler(payload)

        assert any("AUDIT" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_metrics_handler_runs(self, payload: EventPayload, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG):
            await metrics_handler(payload)

        assert any("METRIC" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------


class TestModuleSingleton:
    """Tests for init_event_bus() and get_event_bus()."""

    def test_init_event_bus_returns_bus(self) -> None:
        bus = init_event_bus()
        assert isinstance(bus, EventBus)
        # Built-in handlers: audit + metrics.
        assert bus.handler_count == 2

    def test_get_event_bus_returns_same_instance(self) -> None:
        bus1 = init_event_bus()
        bus2 = get_event_bus()
        assert bus1 is bus2

    def test_get_event_bus_auto_initialises(self) -> None:
        # Force reset by patching the module variable.
        import api.services.event_bus as mod

        original = mod._event_bus
        try:
            mod._event_bus = None
            bus = get_event_bus()
            assert isinstance(bus, EventBus)
        finally:
            mod._event_bus = original


# ---------------------------------------------------------------------------
# EventType enum
# ---------------------------------------------------------------------------


class TestEventType:
    """Tests for the EventType enum values."""

    def test_all_event_types_defined(self) -> None:
        expected = {
            "plan.generated",
            "plan.approved",
            "plan.rejected",
            "plan.apply_started",
            "plan.apply_completed",
            "plan.apply_failed",
            "run.started",
            "run.completed",
            "run.failed",
            "schema.drift_detected",
            "test.gate_blocked",
        }
        actual = {e.value for e in EventType}
        assert actual == expected

    def test_event_type_is_string_enum(self) -> None:
        assert isinstance(EventType.PLAN_GENERATED.value, str)
        assert EventType.PLAN_GENERATED == "plan.generated"


# ---------------------------------------------------------------------------
# BL-089: Semaphore + handler timeout
# ---------------------------------------------------------------------------


class TestEmitSemaphoreAndTimeout:
    """Tests for the _EMIT_SEMAPHORE concurrency limit and per-handler timeout."""

    def test_emit_semaphore_exists(self) -> None:
        """Module-level semaphore must exist with value 5."""
        assert isinstance(_EMIT_SEMAPHORE, asyncio.Semaphore)
        # Internal counter equals the initial value when idle.
        assert _EMIT_SEMAPHORE._value == 5  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_handler_timeout_logs_warning(
        self, bus: EventBus, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A handler that sleeps past the 5s timeout is cancelled and a warning logged."""

        async def slow_handler(p: EventPayload) -> None:
            await asyncio.sleep(100)  # never completes within timeout

        bus.register_handler(slow_handler)

        with patch("api.services.event_bus.asyncio.wait_for", side_effect=asyncio.TimeoutError):
            with caplog.at_level(logging.WARNING):
                await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert any("timed out" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_handler_error_logs_error(
        self, bus: EventBus, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Handler exceptions are caught and logged at ERROR level."""

        async def bad_handler(p: EventPayload) -> None:
            raise ValueError("boom")

        bus.register_handler(bad_handler)

        with caplog.at_level(logging.ERROR):
            await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert any("Handler failed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_timeout_does_not_affect_other_handlers(self, bus: EventBus) -> None:
        """A timed-out handler must not prevent subsequent handlers from running."""
        received: list[str] = []

        async def good_handler(p: EventPayload) -> None:
            received.append("ok")

        bus.register_handler(good_handler)

        # Patch wait_for so the first call raises TimeoutError, second succeeds.
        original_wait_for = asyncio.wait_for
        call_count = 0

        async def patched_wait_for(coro: Any, timeout: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Cancel the coroutine to avoid resource warnings.
                coro.close()
                raise asyncio.TimeoutError
            return await original_wait_for(coro, timeout)

        bus.register_handler(good_handler)  # register again → 2 handlers

        with patch("api.services.event_bus.asyncio.wait_for", side_effect=patched_wait_for):
            await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        # At least the second handler ran.
        assert "ok" in received

    @pytest.mark.asyncio
    async def test_semaphore_acquired_per_handler(self, bus: EventBus) -> None:
        """Handler executes correctly with semaphore in place."""
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler)
        await bus.emit(EventType.PLAN_GENERATED, tenant_id="t1")

        assert len(received) == 1
        assert received[0].tenant_id == "t1"


# ---------------------------------------------------------------------------
# BL-088: OutboxPoller adaptive batch + batch UPDATE
# ---------------------------------------------------------------------------


def _make_mock_session_factory(
    pending_count: int = 3,
    pending_rows: list[dict] | None = None,
) -> Any:
    """Build a mock async session factory suitable for OutboxPoller tests."""
    if pending_rows is None:
        pending_rows = [
            {"id": i + 1, "attempts": 0, "event_type": "plan.generated", "payload": {
                "event_type": "plan.generated",
                "tenant_id": "t1",
                "data": {},
                "correlation_id": f"corr{i}",
                "timestamp": "2026-03-07T00:00:00+00:00",
            }}
            for i in range(pending_count)
        ]

    # Build ORM-like row mocks.
    mock_rows = []
    for r in pending_rows:
        row = MagicMock()
        row.id = r["id"]
        row.attempts = r["attempts"]
        row.event_type = r["event_type"]
        row.payload = r["payload"]
        mock_rows.append(row)

    mock_repo = AsyncMock()
    mock_repo.count_pending = AsyncMock(return_value=pending_count)
    mock_repo.get_pending = AsyncMock(return_value=mock_rows)
    mock_repo.mark_delivered_batch = AsyncMock()
    mock_repo.mark_failed = AsyncMock()
    mock_repo.cleanup_delivered = AsyncMock(return_value=5)
    mock_repo.cleanup_failed = AsyncMock(return_value=0)

    mock_session = AsyncMock()
    mock_session.commit = AsyncMock()

    @asynccontextmanager
    async def _factory():
        yield mock_session

    # Patch EventOutboxRepository to return our mock_repo.
    return _factory, mock_repo, mock_session


class TestAdaptiveBatchSize:
    """Tests for BL-088: adaptive limit and batch UPDATE in OutboxPoller._poll_once."""

    @pytest.mark.asyncio
    async def test_adaptive_limit_scales_with_pending(self) -> None:
        """limit = min(500, max(100, pending_count * 2))."""
        # pending_count = 60 → limit = max(100, 120) = 120
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=60)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        mock_repo.get_pending.assert_called_once_with(limit=120)

    @pytest.mark.asyncio
    async def test_adaptive_limit_minimum_is_100(self) -> None:
        """pending_count = 10 → limit = max(100, 20) = 100."""
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=10)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        mock_repo.get_pending.assert_called_once_with(limit=100)

    @pytest.mark.asyncio
    async def test_adaptive_limit_capped_at_500(self) -> None:
        """pending_count = 400 → limit = min(500, 800) = 500."""
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=400)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        mock_repo.get_pending.assert_called_once_with(limit=500)

    @pytest.mark.asyncio
    async def test_batch_update_called_for_delivered(self) -> None:
        """mark_delivered_batch() is called with all successfully dispatched IDs."""
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=3)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        # All 3 entries should be delivered in one batch call.
        mock_repo.mark_delivered_batch.assert_called_once()
        ids_arg = mock_repo.mark_delivered_batch.call_args[0][0]
        assert sorted(ids_arg) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_poll_once_skips_when_no_pending(self) -> None:
        """_poll_once returns immediately without querying rows when count is 0."""
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=0, pending_rows=[])

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        mock_repo.get_pending.assert_not_called()
        mock_repo.mark_delivered_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_max_attempts_entries_are_marked_failed(self) -> None:
        """Entries at max_attempts are marked failed, not delivered."""
        rows = [
            {"id": 99, "attempts": 3, "event_type": "plan.generated", "payload": {
                "event_type": "plan.generated",
                "tenant_id": "t1",
                "data": {},
                "correlation_id": "corr99",
                "timestamp": "2026-03-07T00:00:00+00:00",
            }}
        ]
        factory, mock_repo, _ = _make_mock_session_factory(pending_count=1, pending_rows=rows)

        bus = EventBus()
        poller = OutboxPoller(factory, bus, max_attempts=3)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._poll_once()

        mock_repo.mark_failed.assert_called_once_with(
            99, "max_attempts_exceeded", permanent=True,
        )
        # Delivered batch should be called with empty list or not include id 99.
        if mock_repo.mark_delivered_batch.called:
            ids = mock_repo.mark_delivered_batch.call_args[0][0]
            assert 99 not in ids


# ---------------------------------------------------------------------------
# BL-098: Cleanup interval + retention
# ---------------------------------------------------------------------------


class TestCleanupIntervalAndRetention:
    """Tests for BL-098: 15-minute cleanup interval, 12h retention, row count logging."""

    def test_default_cleanup_interval_is_900_seconds(self) -> None:
        """OutboxPoller default cleanup interval must be 900 s (15 minutes)."""
        factory, _, _ = _make_mock_session_factory()
        bus = EventBus()
        poller = OutboxPoller(factory, bus)
        assert poller._cleanup_interval == 900.0

    def test_default_retention_hours_is_12(self) -> None:
        """OutboxPoller default retention must be 12 hours."""
        factory, _, _ = _make_mock_session_factory()
        bus = EventBus()
        poller = OutboxPoller(factory, bus)
        assert poller._cleanup_retention_hours == 12

    def test_cleanup_interval_configurable(self) -> None:
        """Custom cleanup_interval_seconds is stored correctly."""
        factory, _, _ = _make_mock_session_factory()
        bus = EventBus()
        poller = OutboxPoller(factory, bus, cleanup_interval_seconds=300.0)
        assert poller._cleanup_interval == 300.0

    def test_retention_hours_configurable(self) -> None:
        """Custom cleanup_retention_hours is stored correctly."""
        factory, _, _ = _make_mock_session_factory()
        bus = EventBus()
        poller = OutboxPoller(factory, bus, cleanup_retention_hours=6)
        assert poller._cleanup_retention_hours == 6

    @pytest.mark.asyncio
    async def test_cleanup_logs_deleted_count(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """_cleanup() always logs the number of deleted rows at INFO level."""
        factory, mock_repo, _ = _make_mock_session_factory()
        mock_repo.cleanup_delivered = AsyncMock(return_value=42)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            with caplog.at_level(logging.INFO):
                await poller._cleanup()

        log_messages = [r.message for r in caplog.records]
        assert any("42" in m for m in log_messages)

    @pytest.mark.asyncio
    async def test_cleanup_logs_zero_deleted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """_cleanup() logs even when no rows are deleted (n=0)."""
        factory, mock_repo, _ = _make_mock_session_factory()
        mock_repo.cleanup_delivered = AsyncMock(return_value=0)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            with caplog.at_level(logging.INFO):
                await poller._cleanup()

        log_messages = [r.message for r in caplog.records]
        # Message should still be logged with 0.
        assert any("cleanup" in m.lower() or "deleted" in m.lower() or "0" in m for m in log_messages)

    @pytest.mark.asyncio
    async def test_cleanup_uses_retention_hours(self) -> None:
        """_cleanup() passes cleanup_retention_hours to both cleanup methods."""
        factory, mock_repo, _ = _make_mock_session_factory()
        mock_repo.cleanup_delivered = AsyncMock(return_value=0)
        mock_repo.cleanup_failed = AsyncMock(return_value=0)

        bus = EventBus()
        poller = OutboxPoller(factory, bus, cleanup_retention_hours=6)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._cleanup()

        mock_repo.cleanup_delivered.assert_called_once_with(older_than_hours=6)
        mock_repo.cleanup_failed.assert_called_once_with(older_than_hours=6)

    @pytest.mark.asyncio
    async def test_cleanup_prunes_failed_entries(self) -> None:
        """_cleanup() also removes permanently failed entries, not just delivered."""
        factory, mock_repo, _ = _make_mock_session_factory()
        mock_repo.cleanup_delivered = AsyncMock(return_value=3)
        mock_repo.cleanup_failed = AsyncMock(return_value=7)

        bus = EventBus()
        poller = OutboxPoller(factory, bus)

        with patch("core_engine.state.repository.EventOutboxRepository", return_value=mock_repo):
            await poller._cleanup()

        mock_repo.cleanup_failed.assert_called_once()
        mock_repo.cleanup_delivered.assert_called_once()


# ---------------------------------------------------------------------------
# BL-102: emit_persistent_batch()
# ---------------------------------------------------------------------------


class TestEmitPersistentBatch:
    """Tests for BL-102: emit_persistent_batch() bulk insert."""

    def _make_batch_session(self) -> tuple[AsyncMock, list[dict]]:
        """Return a mock session that captures the rows passed to bulk INSERT."""
        captured_rows: list[dict] = []

        async def capture_execute(stmt: Any, rows: Any = None) -> None:
            if rows is not None:
                captured_rows.extend(rows)

        mock_session = AsyncMock()
        mock_session.execute.side_effect = capture_execute
        mock_session.flush = AsyncMock()
        return mock_session, captured_rows

    @pytest.mark.asyncio
    async def test_batch_inserts_multiple_events(self, bus: EventBus) -> None:
        """emit_persistent_batch() calls session.execute + flush for bulk INSERT."""
        mock_session, captured_rows = self._make_batch_session()

        events = [
            (EventType.PLAN_GENERATED, "tenant1", {"k": "v"}, None),
            (EventType.RUN_STARTED, "tenant2", None, "corr-abc"),
            (EventType.RUN_COMPLETED, "tenant1", {"result": "ok"}, None),
        ]

        with patch("sqlalchemy.insert", return_value=MagicMock()):
            await bus.emit_persistent_batch(mock_session, events)

        # session.execute called once for the bulk INSERT, flush called once.
        assert mock_session.execute.called
        assert mock_session.flush.called

    @pytest.mark.asyncio
    async def test_batch_empty_list_is_noop(self, bus: EventBus) -> None:
        """emit_persistent_batch() with an empty list makes no DB calls."""
        mock_session = AsyncMock()

        await bus.emit_persistent_batch(mock_session, [])

        mock_session.execute.assert_not_called()
        mock_session.flush.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_auto_generates_correlation_ids(self, bus: EventBus) -> None:
        """Events without correlation_id get auto-generated UUID correlation IDs."""
        mock_session, captured_rows = self._make_batch_session()

        events = [
            (EventType.PLAN_GENERATED, "t1", None, None),
            (EventType.PLAN_APPROVED, "t2", None, None),
        ]

        with patch("sqlalchemy.insert", return_value=MagicMock()):
            await bus.emit_persistent_batch(mock_session, events)

        assert len(captured_rows) == 2
        for row in captured_rows:
            assert row["correlation_id"]
            assert len(row["correlation_id"]) == 32  # uuid hex

    @pytest.mark.asyncio
    async def test_batch_explicit_correlation_id_preserved(self, bus: EventBus) -> None:
        """emit_persistent_batch() preserves explicitly provided correlation IDs."""
        received_corr_ids: list[str] = []

        async def capture_handler(p: EventPayload) -> None:
            received_corr_ids.append(p.correlation_id)

        bus.register_handler(capture_handler)

        mock_session, captured_rows = self._make_batch_session()

        events = [
            (EventType.PLAN_GENERATED, "t1", {}, "my-corr-id"),
        ]

        with patch("sqlalchemy.insert", return_value=MagicMock()):
            await bus.emit_persistent_batch(mock_session, events)

        # Row built for DB must carry the explicit correlation ID.
        assert captured_rows[0]["correlation_id"] == "my-corr-id"
        # In-memory handler also receives it.
        assert "my-corr-id" in received_corr_ids

    @pytest.mark.asyncio
    async def test_batch_dispatches_in_memory_handlers(self, bus: EventBus) -> None:
        """emit_persistent_batch() fires in-memory handlers for each event."""
        received: list[EventPayload] = []

        async def handler(p: EventPayload) -> None:
            received.append(p)

        bus.register_handler(handler)

        mock_session, _ = self._make_batch_session()

        events = [
            (EventType.PLAN_GENERATED, "t1", {"x": 1}, "c1"),
            (EventType.RUN_STARTED, "t2", {}, "c2"),
        ]

        with patch("sqlalchemy.insert", return_value=MagicMock()):
            await bus.emit_persistent_batch(mock_session, events)

        tenant_ids = {p.tenant_id for p in received}
        assert "t1" in tenant_ids
        assert "t2" in tenant_ids

    @pytest.mark.asyncio
    async def test_batch_logs_insert_count(
        self, bus: EventBus, caplog: pytest.LogCaptureFixture
    ) -> None:
        """emit_persistent_batch() logs the number of events inserted."""
        mock_session, _ = self._make_batch_session()

        events = [
            (EventType.PLAN_GENERATED, "t1", {}, "c1"),
            (EventType.PLAN_APPROVED, "t1", {}, "c2"),
        ]

        with patch("sqlalchemy.insert", return_value=MagicMock()), \
             caplog.at_level(logging.INFO):
            await bus.emit_persistent_batch(mock_session, events)

        assert any("2" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# EventOutboxRepository new methods (count_pending, mark_delivered_batch)
# ---------------------------------------------------------------------------


class TestEventOutboxRepositoryNewMethods:
    """Smoke tests for count_pending() and mark_delivered_batch() via mock session."""

    @pytest.mark.asyncio
    async def test_count_pending_executes_query(self) -> None:
        """count_pending() executes a SELECT COUNT query."""
        from core_engine.state.repository import EventOutboxRepository

        mock_session = AsyncMock()
        scalar_result = MagicMock()
        scalar_result.scalar_one = MagicMock(return_value=7)
        mock_session.execute = AsyncMock(return_value=scalar_result)

        repo = EventOutboxRepository(mock_session)
        count = await repo.count_pending()

        assert count == 7
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_delivered_batch_executes_update(self) -> None:
        """mark_delivered_batch() executes a single UPDATE and flush."""
        from core_engine.state.repository import EventOutboxRepository

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.flush = AsyncMock()

        repo = EventOutboxRepository(mock_session)
        await repo.mark_delivered_batch([1, 2, 3])

        mock_session.execute.assert_called_once()
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_delivered_batch_empty_list_is_noop(self) -> None:
        """mark_delivered_batch([]) makes no DB calls."""
        from core_engine.state.repository import EventOutboxRepository

        mock_session = AsyncMock()

        repo = EventOutboxRepository(mock_session)
        await repo.mark_delivered_batch([])

        mock_session.execute.assert_not_called()
        mock_session.flush.assert_not_called()
