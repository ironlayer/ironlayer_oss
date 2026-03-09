"""Tests for the continuous performance profiling module.

Covers the ``@profile_operation`` decorator (sync & async), the
``ProfileCollector`` singleton (thread safety, stats, reset), and the
``ProfileResult`` dataclass.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import pytest
from core_engine.telemetry.profiling import (
    ProfileCollector,
    ProfileResult,
    profile_operation,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_collector():
    """Ensure a fresh collector singleton for each test."""
    ProfileCollector.reset()
    yield
    ProfileCollector.reset()


# ---------------------------------------------------------------------------
# ProfileResult
# ---------------------------------------------------------------------------


class TestProfileResult:
    def test_frozen_dataclass(self) -> None:
        result = ProfileResult(operation="test.op", duration_ms=1.234, peak_memory_mb=0.0)
        assert result.operation == "test.op"
        assert result.duration_ms == 1.234
        assert result.peak_memory_mb == 0.0
        assert result.metadata == {}

    def test_with_metadata(self) -> None:
        result = ProfileResult(
            operation="dag.build",
            duration_ms=42.5,
            peak_memory_mb=12.3,
            metadata={"model_count": 100},
        )
        assert result.metadata["model_count"] == 100

    def test_immutable(self) -> None:
        result = ProfileResult(operation="x", duration_ms=1.0, peak_memory_mb=0.0)
        with pytest.raises(AttributeError):
            result.operation = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ProfileCollector
# ---------------------------------------------------------------------------


class TestProfileCollector:
    def test_singleton_identity(self) -> None:
        a = ProfileCollector.get_instance()
        b = ProfileCollector.get_instance()
        assert a is b

    def test_reset_creates_new_instance(self) -> None:
        a = ProfileCollector.get_instance()
        ProfileCollector.reset()
        b = ProfileCollector.get_instance()
        assert a is not b

    def test_record_and_get_stats(self) -> None:
        collector = ProfileCollector.get_instance()
        for i in range(10):
            collector.record(ProfileResult(operation="op.a", duration_ms=float(i + 1), peak_memory_mb=0.0))
        stats = collector.get_stats("op.a")
        assert stats is not None
        assert stats["operation"] == "op.a"
        assert stats["count"] == 10
        assert stats["mean_ms"] == 5.5
        assert stats["min_ms"] == 1.0
        assert stats["max_ms"] == 10.0

    def test_get_stats_nonexistent(self) -> None:
        collector = ProfileCollector.get_instance()
        assert collector.get_stats("nonexistent") is None

    def test_percentile_p50(self) -> None:
        collector = ProfileCollector.get_instance()
        # 1-100 evenly spaced → p50 should be ~50.5
        for i in range(1, 101):
            collector.record(ProfileResult(operation="perc", duration_ms=float(i), peak_memory_mb=0.0))
        stats = collector.get_stats("perc")
        assert stats is not None
        assert 49.0 <= stats["p50_ms"] <= 51.0

    def test_percentile_p95(self) -> None:
        collector = ProfileCollector.get_instance()
        for i in range(1, 101):
            collector.record(ProfileResult(operation="perc95", duration_ms=float(i), peak_memory_mb=0.0))
        stats = collector.get_stats("perc95")
        assert stats is not None
        assert 94.0 <= stats["p95_ms"] <= 96.0

    def test_percentile_p99(self) -> None:
        collector = ProfileCollector.get_instance()
        for i in range(1, 101):
            collector.record(ProfileResult(operation="perc99", duration_ms=float(i), peak_memory_mb=0.0))
        stats = collector.get_stats("perc99")
        assert stats is not None
        assert 98.0 <= stats["p99_ms"] <= 100.0

    def test_max_results_eviction(self) -> None:
        collector = ProfileCollector(max_results=5)
        for i in range(10):
            collector.record(ProfileResult(operation="evict", duration_ms=float(i), peak_memory_mb=0.0))
        stats = collector.get_stats("evict")
        assert stats is not None
        assert stats["count"] == 5
        # Oldest (0-4) should be evicted; only 5-9 remain.
        assert stats["min_ms"] == 5.0

    def test_clear(self) -> None:
        collector = ProfileCollector.get_instance()
        collector.record(ProfileResult(operation="clear_me", duration_ms=1.0, peak_memory_mb=0.0))
        assert collector.get_stats("clear_me") is not None
        collector.clear()
        assert collector.get_stats("clear_me") is None

    def test_get_all_stats_sorted(self) -> None:
        collector = ProfileCollector.get_instance()
        for name in ["zz.op", "aa.op", "mm.op"]:
            collector.record(ProfileResult(operation=name, duration_ms=1.0, peak_memory_mb=0.0))
        all_stats = collector.get_all_stats()
        assert [s["operation"] for s in all_stats] == ["aa.op", "mm.op", "zz.op"]

    def test_thread_safety(self) -> None:
        """Concurrent writers should not corrupt the collector."""
        collector = ProfileCollector.get_instance()
        errors: list[Exception] = []

        def writer(op_name: str, count: int) -> None:
            try:
                for i in range(count):
                    collector.record(ProfileResult(operation=op_name, duration_ms=float(i), peak_memory_mb=0.0))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(f"thread_{t}", 100)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # Each thread wrote 100 entries for its own operation.
        for t_idx in range(8):
            stats = collector.get_stats(f"thread_{t_idx}")
            assert stats is not None
            assert stats["count"] == 100


# ---------------------------------------------------------------------------
# @profile_operation — sync functions
# ---------------------------------------------------------------------------


class TestProfileOperationSync:
    def test_records_timing(self) -> None:
        @profile_operation("test.sync")
        def slow_add(a: int, b: int) -> int:
            time.sleep(0.01)  # ~10ms
            return a + b

        result = slow_add(2, 3)
        assert result == 5

        stats = ProfileCollector.get_instance().get_stats("test.sync")
        assert stats is not None
        assert stats["count"] == 1
        assert stats["mean_ms"] >= 5.0  # should be ~10ms, allow margin

    def test_preserves_function_metadata(self) -> None:
        @profile_operation("test.meta")
        def documented_fn() -> str:
            """A docstring."""
            return "ok"

        assert documented_fn.__name__ == "documented_fn"
        assert documented_fn.__doc__ == "A docstring."

    def test_return_value_preserved(self) -> None:
        @profile_operation("test.ret")
        def identity(x: object) -> object:
            return x

        sentinel = object()
        assert identity(sentinel) is sentinel

    def test_exception_propagated(self) -> None:
        @profile_operation("test.exc")
        def raiser() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            raiser()

        # Timing should still be recorded even on exception.
        stats = ProfileCollector.get_instance().get_stats("test.exc")
        assert stats is not None
        assert stats["count"] == 1

    def test_multiple_calls_accumulate(self) -> None:
        @profile_operation("test.multi")
        def noop() -> None:
            pass

        for _ in range(5):
            noop()

        stats = ProfileCollector.get_instance().get_stats("test.multi")
        assert stats is not None
        assert stats["count"] == 5


# ---------------------------------------------------------------------------
# @profile_operation — async functions
# ---------------------------------------------------------------------------


class TestProfileOperationAsync:
    def test_records_async_timing(self) -> None:
        @profile_operation("test.async")
        async def slow_coro() -> str:
            await asyncio.sleep(0.01)
            return "done"

        result = asyncio.get_event_loop().run_until_complete(slow_coro())
        assert result == "done"

        stats = ProfileCollector.get_instance().get_stats("test.async")
        assert stats is not None
        assert stats["count"] == 1
        assert stats["mean_ms"] >= 5.0

    def test_preserves_async_metadata(self) -> None:
        @profile_operation("test.async_meta")
        async def async_documented() -> None:
            """Async docstring."""

        assert async_documented.__name__ == "async_documented"
        assert async_documented.__doc__ == "Async docstring."

    def test_async_exception_propagated(self) -> None:
        @profile_operation("test.async_exc")
        async def async_raiser() -> None:
            raise RuntimeError("async boom")

        with pytest.raises(RuntimeError, match="async boom"):
            asyncio.get_event_loop().run_until_complete(async_raiser())

        stats = ProfileCollector.get_instance().get_stats("test.async_exc")
        assert stats is not None
        assert stats["count"] == 1


# ---------------------------------------------------------------------------
# DEBUG logging output
# ---------------------------------------------------------------------------


class TestProfilingLogging:
    def test_debug_log_emitted(self, caplog: pytest.LogCaptureFixture) -> None:
        @profile_operation("test.logging")
        def fast_op() -> int:
            return 42

        with caplog.at_level(logging.DEBUG, logger="core_engine.telemetry.profiling"):
            fast_op()

        assert any("PROFILE test.logging" in record.message for record in caplog.records)

    def test_async_debug_log_emitted(self, caplog: pytest.LogCaptureFixture) -> None:
        @profile_operation("test.async_logging")
        async def fast_async() -> int:
            return 99

        with caplog.at_level(logging.DEBUG, logger="core_engine.telemetry.profiling"):
            asyncio.get_event_loop().run_until_complete(fast_async())

        assert any("PROFILE test.async_logging" in record.message for record in caplog.records)
