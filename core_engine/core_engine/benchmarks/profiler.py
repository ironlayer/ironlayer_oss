"""Performance profiler for benchmarking core-engine operations.

Wraps critical code paths with precise timing (``time.perf_counter_ns``) and
optional memory tracking (``tracemalloc``) to produce structured
:class:`BenchmarkResult` records.

Results are frozen dataclasses that can be serialised to JSON, stored in CI
artefacts, or used for automated regression assertions.
"""

from __future__ import annotations

import dataclasses
import logging
import time
import tracemalloc
from collections.abc import Callable
from typing import Any

from core_engine.benchmarks.graph_generator import SyntheticGraphGenerator
from core_engine.models.model_definition import ModelDefinition

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class BenchmarkResult:
    """Immutable record of a single benchmark run."""

    operation: str
    model_count: int
    duration_ms: float
    peak_memory_mb: float
    throughput_ops_per_sec: float
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)


def _time_call(func: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[Any, float, float]:
    """Execute *func* and return ``(result, duration_ms, peak_memory_mb)``."""
    tracemalloc.start()
    tracemalloc.reset_peak()

    start = time.perf_counter_ns()
    result = func(*args, **kwargs)
    elapsed_ns = time.perf_counter_ns() - start

    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    duration_ms = elapsed_ns / 1_000_000
    peak_mb = peak_bytes / (1024 * 1024)
    return result, duration_ms, peak_mb


class BenchmarkProfiler:
    """Profile core-engine operations against synthetic graphs.

    All methods accept a model count and return a :class:`BenchmarkResult`
    capturing timing, memory, and throughput.
    """

    # ------------------------------------------------------------------
    # DAG build
    # ------------------------------------------------------------------

    @staticmethod
    def profile_dag_build(
        models: list[ModelDefinition],
        *,
        topology: str = "unknown",
    ) -> BenchmarkResult:
        """Profile :func:`build_dag` on the given models.

        Parameters
        ----------
        models:
            Pre-generated ``ModelDefinition`` list.
        topology:
            Name of the graph topology (for metadata).
        """
        from core_engine.graph.dag_builder import build_dag

        _, duration_ms, peak_mb = _time_call(build_dag, models)

        throughput = (len(models) / (duration_ms / 1000)) if duration_ms > 0 else 0.0

        result = BenchmarkResult(
            operation="dag.build",
            model_count=len(models),
            duration_ms=round(duration_ms, 3),
            peak_memory_mb=round(peak_mb, 3),
            throughput_ops_per_sec=round(throughput, 1),
            metadata={"topology": topology},
        )
        logger.debug(
            "DAG build: %d models, %.1fms, %.2fMB peak, %.0f ops/s",
            result.model_count,
            result.duration_ms,
            result.peak_memory_mb,
            result.throughput_ops_per_sec,
        )
        return result

    # ------------------------------------------------------------------
    # Topological sort
    # ------------------------------------------------------------------

    @staticmethod
    def profile_topological_sort(
        models: list[ModelDefinition],
        *,
        topology: str = "unknown",
    ) -> BenchmarkResult:
        """Profile topological sorting of the DAG.

        Builds the DAG first (not timed), then times only the sort.
        """
        from core_engine.graph.dag_builder import build_dag

        dag = build_dag(models)

        _, duration_ms, peak_mb = _time_call(dag.topological_sort)

        throughput = (len(models) / (duration_ms / 1000)) if duration_ms > 0 else 0.0

        result = BenchmarkResult(
            operation="dag.topological_sort",
            model_count=len(models),
            duration_ms=round(duration_ms, 3),
            peak_memory_mb=round(peak_mb, 3),
            throughput_ops_per_sec=round(throughput, 1),
            metadata={"topology": topology},
        )
        logger.debug(
            "Topo sort: %d models, %.1fms, %.2fMB peak",
            result.model_count,
            result.duration_ms,
            result.peak_memory_mb,
        )
        return result

    # ------------------------------------------------------------------
    # Plan generation
    # ------------------------------------------------------------------

    @staticmethod
    def profile_plan_generation(
        models: list[ModelDefinition],
        *,
        changed_fraction: float = 0.1,
        topology: str = "unknown",
    ) -> BenchmarkResult:
        """Profile plan generation for a subset of changed models.

        Parameters
        ----------
        models:
            Full model list.
        changed_fraction:
            Fraction of models to mark as changed (0.0-1.0).
        topology:
            Name of the graph topology (for metadata).
        """
        from core_engine.graph.dag_builder import build_dag

        dag = build_dag(models)

        # Mark a fraction of models as changed.
        num_changed = max(1, int(len(models) * changed_fraction))
        changed_models = [m.name for m in models[:num_changed]]

        def _generate_plan() -> list[str]:
            """Simulate plan generation by computing affected downstream."""
            affected: set[str] = set()
            for model_name in changed_models:
                affected.add(model_name)
                try:
                    downstream = dag.get_downstream(model_name)
                    affected.update(downstream)
                except (KeyError, AttributeError):
                    pass
            return sorted(affected)

        plan, duration_ms, peak_mb = _time_call(_generate_plan)

        throughput = (len(models) / (duration_ms / 1000)) if duration_ms > 0 else 0.0

        result = BenchmarkResult(
            operation="plan.generate",
            model_count=len(models),
            duration_ms=round(duration_ms, 3),
            peak_memory_mb=round(peak_mb, 3),
            throughput_ops_per_sec=round(throughput, 1),
            metadata={
                "topology": topology,
                "changed_count": num_changed,
                "affected_count": len(plan),
            },
        )
        logger.debug(
            "Plan gen: %d models (%d changed → %d affected), %.1fms",
            result.model_count,
            num_changed,
            len(plan),
            result.duration_ms,
        )
        return result

    # ------------------------------------------------------------------
    # SQL normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def profile_sql_normalization(
        models: list[ModelDefinition],
    ) -> BenchmarkResult:
        """Profile SQL normalisation across all models.

        Times per-model normalisation and reports aggregate throughput.
        """
        from core_engine.parser.normalizer import normalize_sql

        def _normalize_all() -> int:
            count = 0
            for model in models:
                if model.raw_sql:
                    normalize_sql(model.raw_sql)
                    count += 1
            return count

        count, duration_ms, peak_mb = _time_call(_normalize_all)

        per_model_ms = duration_ms / count if count > 0 else 0.0
        throughput = (count / (duration_ms / 1000)) if duration_ms > 0 else 0.0

        result = BenchmarkResult(
            operation="sql.normalize",
            model_count=len(models),
            duration_ms=round(duration_ms, 3),
            peak_memory_mb=round(peak_mb, 3),
            throughput_ops_per_sec=round(throughput, 1),
            metadata={
                "models_normalized": count,
                "per_model_ms": round(per_model_ms, 3),
            },
        )
        logger.debug(
            "SQL norm: %d models, %.1fms total, %.3fms/model",
            count,
            result.duration_ms,
            per_model_ms,
        )
        return result

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    @staticmethod
    def profile_full_pipeline(
        n: int,
        *,
        topology: str = "realistic",
        seed: int = 42,
    ) -> list[BenchmarkResult]:
        """Run all profiling operations on a synthetic graph.

        Parameters
        ----------
        n:
            Number of models to generate.
        topology:
            Graph topology: ``"linear"``, ``"fanout"``, ``"diamond"``,
            or ``"realistic"`` (default).
        seed:
            Random seed for the realistic generator.

        Returns
        -------
        list[BenchmarkResult]
            One result per profiled operation.
        """
        gen = SyntheticGraphGenerator

        if topology == "linear":
            models = gen.generate_linear_chain(n)
        elif topology == "fanout":
            models = gen.generate_wide_fanout(n)
        elif topology == "diamond":
            models = gen.generate_diamond(n)
        else:
            models = gen.generate_realistic(n, seed=seed)

        results = [
            BenchmarkProfiler.profile_dag_build(models, topology=topology),
            BenchmarkProfiler.profile_topological_sort(models, topology=topology),
            BenchmarkProfiler.profile_plan_generation(models, topology=topology),
            BenchmarkProfiler.profile_sql_normalization(models),
        ]

        logger.info(
            "Full pipeline profile: %d models (%s), %d operations",
            n,
            topology,
            len(results),
        )
        return results
