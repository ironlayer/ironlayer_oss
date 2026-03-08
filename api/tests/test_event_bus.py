"""Tests for the event bus lifecycle hook system.

Validates handler registration, event emission, error isolation, and
the built-in audit log and metrics handlers.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from api.services.event_bus import (
    EventBus,
    EventPayload,
    EventType,
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
