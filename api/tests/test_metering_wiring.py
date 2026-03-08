"""Tests for metering wiring in api/dependencies.py

Covers:
- MeteringCollector initialisation and shutdown lifecycle
- Event recording for each UsageEventType
- Event metadata fields populated correctly
- Flush and pending_count behaviour
- Singleton lifecycle (init → get → dispose → RuntimeError)
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from core_engine.metering.collector import MeteringCollector
from core_engine.metering.events import UsageEvent, UsageEventType

# ---------------------------------------------------------------------------
# Collector lifecycle
# ---------------------------------------------------------------------------


class TestMeteringCollectorLifecycle:
    """Verify collector init, flush, and teardown."""

    def test_collector_starts_with_empty_buffer(self) -> None:
        """A freshly created collector has zero pending events."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)
        assert collector.pending_count == 0

    def test_record_event_increments_pending(self) -> None:
        """Recording events increases the pending count."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.API_REQUEST,
        )
        assert collector.pending_count == 1

        collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.PLAN_RUN,
        )
        assert collector.pending_count == 2

    def test_flush_drains_buffer(self) -> None:
        """Flushing sends events to the sink and resets the buffer."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.PLAN_RUN,
        )
        collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.AI_CALL,
        )
        assert collector.pending_count == 2

        flushed = collector.flush()
        assert flushed == 2
        assert collector.pending_count == 0
        sink.flush.assert_called_once()
        assert len(sink.flush.call_args[0][0]) == 2

    def test_flush_empty_buffer_returns_zero(self) -> None:
        """Flushing an empty buffer returns 0 without calling the sink."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        flushed = collector.flush()
        assert flushed == 0
        sink.flush.assert_not_called()

    def test_auto_flush_on_max_buffer_size(self) -> None:
        """Buffer auto-flushes when max_buffer_size is reached."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink, max_buffer_size=3)

        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        assert collector.pending_count == 2
        sink.flush.assert_not_called()

        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        sink.flush.assert_called_once()
        assert collector.pending_count == 0


# ---------------------------------------------------------------------------
# Event types and metadata
# ---------------------------------------------------------------------------


class TestEventRecording:
    """Verify events are recorded with correct types and metadata."""

    def test_plan_run_event(self) -> None:
        """PLAN_RUN events carry plan metadata."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.PLAN_RUN,
            metadata={"plan_id": "p1", "total_steps": 5},
        )

        assert event.tenant_id == "tenant-abc"
        assert event.event_type == UsageEventType.PLAN_RUN
        assert event.quantity == 1
        assert event.metadata["plan_id"] == "p1"
        assert event.metadata["total_steps"] == 5

    def test_plan_apply_event(self) -> None:
        """PLAN_APPLY events carry plan_id and steps_executed."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.PLAN_APPLY,
            metadata={"plan_id": "p1", "steps_executed": 3},
        )

        assert event.event_type == UsageEventType.PLAN_APPLY
        assert event.metadata["steps_executed"] == 3

    def test_ai_call_event(self) -> None:
        """AI_CALL events carry call_type metadata."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.AI_CALL,
            metadata={"call_type": "semantic_classify"},
        )

        assert event.event_type == UsageEventType.AI_CALL
        assert event.metadata["call_type"] == "semantic_classify"

    def test_model_loaded_event_with_quantity(self) -> None:
        """MODEL_LOADED events carry quantity for number of models."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.MODEL_LOADED,
            quantity=12,
        )

        assert event.event_type == UsageEventType.MODEL_LOADED
        assert event.quantity == 12

    def test_backfill_run_event(self) -> None:
        """BACKFILL_RUN events carry model name and date range."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.BACKFILL_RUN,
            metadata={
                "model_name": "staging.orders",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        assert event.event_type == UsageEventType.BACKFILL_RUN
        assert event.metadata["model_name"] == "staging.orders"

    def test_api_request_event(self) -> None:
        """API_REQUEST events carry method, path, status_code."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="tenant-abc",
            event_type=UsageEventType.API_REQUEST,
            metadata={"method": "GET", "path": "/api/v1/plans", "status_code": 200},
        )

        assert event.event_type == UsageEventType.API_REQUEST
        assert event.metadata["method"] == "GET"
        assert event.metadata["status_code"] == 200


# ---------------------------------------------------------------------------
# Event identity and timestamps
# ---------------------------------------------------------------------------


class TestEventFields:
    """Verify event_id uniqueness and timestamp population."""

    def test_event_ids_are_unique(self) -> None:
        """Each recorded event gets a unique event_id."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        e1 = collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        e2 = collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)

        assert e1.event_id != e2.event_id
        assert e1.event_id.startswith("evt-")
        assert e2.event_id.startswith("evt-")

    def test_event_has_timestamp(self) -> None:
        """Events have a UTC timestamp set automatically."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.PLAN_RUN,
        )
        assert event.timestamp is not None
        assert event.timestamp.tzinfo is not None

    def test_record_raw_event(self) -> None:
        """The record() method accepts a pre-built UsageEvent."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        event = UsageEvent(
            tenant_id="t1",
            event_type=UsageEventType.PLAN_RUN,
            quantity=5,
            metadata={"custom": True},
        )
        collector.record(event)

        assert collector.pending_count == 1
        collector.flush()
        flushed_events = sink.flush.call_args[0][0]
        assert flushed_events[0].quantity == 5
        assert flushed_events[0].metadata["custom"] is True


# ---------------------------------------------------------------------------
# Usage summary
# ---------------------------------------------------------------------------


class TestUsageSummary:
    """Verify in-memory usage summary by type."""

    def test_summary_groups_by_type(self) -> None:
        """get_usage_summary groups event counts by event_type."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        collector.record_event(tenant_id="t1", event_type=UsageEventType.PLAN_RUN)

        summary = collector.get_usage_summary()
        assert summary["api_request"] == 2
        assert summary["plan_run"] == 1

    def test_summary_filters_by_tenant(self) -> None:
        """get_usage_summary(tenant_id=...) filters to one tenant."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink)

        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        collector.record_event(tenant_id="t2", event_type=UsageEventType.API_REQUEST)
        collector.record_event(tenant_id="t1", event_type=UsageEventType.PLAN_RUN)

        summary = collector.get_usage_summary(tenant_id="t1")
        assert summary["api_request"] == 1
        assert summary["plan_run"] == 1
        assert "t2" not in str(summary)


# ---------------------------------------------------------------------------
# Sink failure resilience
# ---------------------------------------------------------------------------


class TestSinkResilience:
    """Verify collector handles sink failures gracefully."""

    def test_sink_error_does_not_propagate(self) -> None:
        """If the sink.flush raises, the error is swallowed and the buffer is cleared."""
        sink = MagicMock()
        sink.flush.side_effect = RuntimeError("database connection lost")
        collector = MeteringCollector(sink=sink)

        collector.record_event(tenant_id="t1", event_type=UsageEventType.API_REQUEST)
        flushed = collector.flush()
        # The events are "lost" when the sink fails, but the collector continues.
        assert flushed == 1
        assert collector.pending_count == 0


# ---------------------------------------------------------------------------
# Background flush thread
# ---------------------------------------------------------------------------


class TestBackgroundFlush:
    """Verify start/stop of background flush thread."""

    def test_start_and_stop_background_flush(self) -> None:
        """Background flush thread starts and stops cleanly."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink, flush_interval_seconds=0.05)

        collector.start_background_flush()
        assert collector._flush_thread is not None
        assert collector._flush_thread.is_alive()

        collector.stop_background_flush()
        assert collector._flush_thread is None

    def test_double_start_is_idempotent(self) -> None:
        """Calling start_background_flush twice doesn't create a second thread."""
        sink = MagicMock()
        collector = MeteringCollector(sink=sink, flush_interval_seconds=0.1)

        collector.start_background_flush()
        thread1 = collector._flush_thread

        collector.start_background_flush()
        thread2 = collector._flush_thread

        assert thread1 is thread2

        collector.stop_background_flush()


# ---------------------------------------------------------------------------
# Dependency lifecycle (init → get → dispose)
# ---------------------------------------------------------------------------


class TestDependencyLifecycle:
    """Verify the init_metering / get_metering_collector / dispose_metering flow."""

    def test_get_before_init_raises(self) -> None:
        """get_metering_collector raises RuntimeError before init_metering."""
        import api.dependencies as deps

        original = deps._metering_collector
        try:
            deps._metering_collector = None
            with pytest.raises(RuntimeError, match="MeteringCollector has not been initialised"):
                deps.get_metering_collector()
        finally:
            deps._metering_collector = original

    def test_dispose_clears_singleton(self) -> None:
        """dispose_metering stops the background thread and clears the singleton."""
        import api.dependencies as deps

        mock_collector = MagicMock()
        original = deps._metering_collector
        try:
            deps._metering_collector = mock_collector
            deps.dispose_metering()
            mock_collector.stop_background_flush.assert_called_once()
            assert deps._metering_collector is None
        finally:
            deps._metering_collector = original

    def test_dispose_when_none_is_noop(self) -> None:
        """dispose_metering when already None doesn't raise."""
        import api.dependencies as deps

        original = deps._metering_collector
        try:
            deps._metering_collector = None
            deps.dispose_metering()  # Should not raise
            assert deps._metering_collector is None
        finally:
            deps._metering_collector = original
