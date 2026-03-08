"""Tests for the metering pipeline."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from core_engine.metering.events import UsageEvent, UsageEventType
from core_engine.metering.collector import (
    FileSink,
    MeteringCollector,
)

# ---------------------------------------------------------------------------
# UsageEvent
# ---------------------------------------------------------------------------


class TestUsageEvent:
    """Verify usage event creation."""

    def test_creates_with_defaults(self) -> None:
        event = UsageEvent(
            tenant_id="tenant-1",
            event_type=UsageEventType.PLAN_RUN,
        )
        assert event.tenant_id == "tenant-1"
        assert event.event_type == UsageEventType.PLAN_RUN
        assert event.quantity == 1
        assert event.event_id.startswith("evt-")
        assert event.metadata == {}

    def test_custom_quantity(self) -> None:
        event = UsageEvent(
            tenant_id="tenant-1",
            event_type=UsageEventType.AI_CALL,
            quantity=150,
            metadata={"model": "gpt-4"},
        )
        assert event.quantity == 150
        assert event.metadata["model"] == "gpt-4"

    def test_all_event_types(self) -> None:
        for evt_type in UsageEventType:
            event = UsageEvent(
                tenant_id="tenant-1",
                event_type=evt_type,
            )
            assert event.event_type == evt_type

    def test_serializes_to_json(self) -> None:
        event = UsageEvent(
            tenant_id="tenant-1",
            event_type=UsageEventType.PLAN_RUN,
        )
        data = json.loads(event.model_dump_json())
        assert data["tenant_id"] == "tenant-1"
        assert data["event_type"] == "plan_run"


# ---------------------------------------------------------------------------
# FileSink
# ---------------------------------------------------------------------------


class TestFileSink:
    """Verify file-based metering sink."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        events = [
            UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN),
            UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_APPLY),
        ]
        sink.flush(events)

        lines = (tmp_path / "usage.jsonl").read_text().strip().split("\n")
        assert len(lines) == 2

        data = json.loads(lines[0])
        assert data["event_type"] == "plan_run"

    def test_appends_to_existing(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        e1 = [UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN)]
        e2 = [UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_APPLY)]

        sink.flush(e1)
        sink.flush(e2)

        lines = (tmp_path / "usage.jsonl").read_text().strip().split("\n")
        assert len(lines) == 2

    def test_empty_flush_noop(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        sink.flush([])
        assert not (tmp_path / "usage.jsonl").exists()

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "deep" / "nested" / "usage.jsonl")
        sink.flush([UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN)])
        assert (tmp_path / "deep" / "nested" / "usage.jsonl").exists()


# ---------------------------------------------------------------------------
# MeteringCollector
# ---------------------------------------------------------------------------


class TestMeteringCollector:
    """Verify the in-memory event collector."""

    def test_record_and_flush(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink)

        event = UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN)
        collector.record(event)

        assert collector.pending_count == 1
        flushed = collector.flush()
        assert flushed == 1
        assert collector.pending_count == 0

    def test_record_event_shorthand(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink)

        event = collector.record_event(
            tenant_id="t1",
            event_type=UsageEventType.AI_CALL,
            quantity=100,
            metadata={"model": "gpt-4"},
        )

        assert event.quantity == 100
        assert collector.pending_count == 1

    def test_auto_flush_on_max_buffer(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink, max_buffer_size=5)

        for _ in range(5):
            collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.MODEL_LOADED))

        # Buffer should have been auto-flushed.
        assert collector.pending_count == 0

        lines = (tmp_path / "usage.jsonl").read_text().strip().split("\n")
        assert len(lines) == 5

    def test_multiple_flushes(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink)

        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN))
        collector.flush()

        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_APPLY))
        collector.flush()

        lines = (tmp_path / "usage.jsonl").read_text().strip().split("\n")
        assert len(lines) == 2

    def test_empty_flush_returns_zero(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink)
        assert collector.flush() == 0

    def test_usage_summary(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink)

        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN))
        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN, quantity=2))
        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.AI_CALL))
        collector.record(UsageEvent(tenant_id="t2", event_type=UsageEventType.PLAN_RUN))

        # All tenants.
        summary = collector.get_usage_summary()
        assert summary["plan_run"] == 4  # 1 + 2 + 1
        assert summary["ai_call"] == 1

        # Filtered by tenant.
        t1_summary = collector.get_usage_summary(tenant_id="t1")
        assert t1_summary["plan_run"] == 3
        assert t1_summary["ai_call"] == 1

        t2_summary = collector.get_usage_summary(tenant_id="t2")
        assert t2_summary["plan_run"] == 1
        assert "ai_call" not in t2_summary

    def test_thread_safety(self, tmp_path: Path) -> None:
        """Verify collector is thread-safe under concurrent writes."""
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink, max_buffer_size=1000)

        errors: list[Exception] = []

        def _writer(tenant: str, count: int) -> None:
            try:
                for _ in range(count):
                    collector.record(UsageEvent(tenant_id=tenant, event_type=UsageEventType.API_REQUEST))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_writer, args=(f"t{i}", 100)) for i in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"

        # All 500 events should be buffered.
        flushed = collector.flush()
        assert flushed == 500

    def test_background_flush(self, tmp_path: Path) -> None:
        """Verify background flush thread operates correctly."""
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink, flush_interval_seconds=0.1)

        collector.start_background_flush()

        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN))

        # Wait for flush.
        time.sleep(0.3)

        collector.stop_background_flush()

        assert collector.pending_count == 0
        assert (tmp_path / "usage.jsonl").exists()

    def test_stop_does_final_flush(self, tmp_path: Path) -> None:
        sink = FileSink(tmp_path / "usage.jsonl")
        collector = MeteringCollector(sink, flush_interval_seconds=60)

        collector.start_background_flush()
        collector.record(UsageEvent(tenant_id="t1", event_type=UsageEventType.PLAN_RUN))

        # Immediately stop -- should do final flush even though interval hasn't elapsed.
        collector.stop_background_flush()

        assert collector.pending_count == 0
        lines = (tmp_path / "usage.jsonl").read_text().strip().split("\n")
        assert len(lines) == 1
