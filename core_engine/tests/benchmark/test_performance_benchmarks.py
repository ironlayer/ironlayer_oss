"""Performance benchmark tests for core-engine operations.

These tests generate synthetic DAGs of varying sizes and topologies,
then assert that critical operations complete within defined time
budgets.  They serve as regression gates: if a code change causes a
> 2× slowdown, the CI pipeline will catch it.

Marked with ``@pytest.mark.benchmark`` so they can be run selectively::

    pytest -m benchmark -v
"""

from __future__ import annotations

import pytest
from core_engine.benchmarks.graph_generator import SyntheticGraphGenerator
from core_engine.benchmarks.profiler import BenchmarkProfiler, BenchmarkResult

# ---------------------------------------------------------------------------
# Graph generator validation
# ---------------------------------------------------------------------------


class TestSyntheticGraphGenerator:
    """Validate that graph generators produce well-formed models."""

    @pytest.mark.parametrize("n", [1, 10, 100, 500])
    def test_linear_chain_correct_count(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_linear_chain(n)
        assert len(models) == n

    @pytest.mark.parametrize("n", [1, 10, 100, 500])
    def test_linear_chain_dependencies(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_linear_chain(n)
        for i, model in enumerate(models):
            if i == 0:
                assert model.dependencies == []
            else:
                assert len(model.dependencies) == 1

    def test_linear_chain_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="n must be >= 1"):
            SyntheticGraphGenerator.generate_linear_chain(0)

    @pytest.mark.parametrize("n", [1, 11, 50, 111])
    def test_wide_fanout_correct_count(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_wide_fanout(n)
        assert len(models) == n

    def test_wide_fanout_root_has_no_deps(self) -> None:
        models = SyntheticGraphGenerator.generate_wide_fanout(20)
        assert models[0].dependencies == []

    def test_wide_fanout_children_depend_on_root(self) -> None:
        models = SyntheticGraphGenerator.generate_wide_fanout(20, fanout=5)
        root_name = models[0].name
        for model in models[1:6]:
            assert root_name in model.dependencies

    @pytest.mark.parametrize("n", [1, 5, 20, 100])
    def test_diamond_correct_count(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_diamond(n)
        assert len(models) == n

    def test_diamond_root_has_no_deps(self) -> None:
        models = SyntheticGraphGenerator.generate_diamond(20)
        assert models[0].dependencies == []

    @pytest.mark.parametrize("n", [1, 10, 100, 500])
    def test_realistic_correct_count(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_realistic(n)
        assert len(models) == n

    def test_realistic_deterministic(self) -> None:
        """Same seed must produce identical models."""
        a = SyntheticGraphGenerator.generate_realistic(50, seed=42)
        b = SyntheticGraphGenerator.generate_realistic(50, seed=42)
        assert [m.name for m in a] == [m.name for m in b]
        assert [m.raw_sql for m in a] == [m.raw_sql for m in b]

    def test_realistic_different_seed_differs(self) -> None:
        """Different seeds must produce different dependency structures."""
        a = SyntheticGraphGenerator.generate_realistic(50, seed=42)
        b = SyntheticGraphGenerator.generate_realistic(50, seed=99)
        deps_a = [tuple(m.dependencies) for m in a]
        deps_b = [tuple(m.dependencies) for m in b]
        assert deps_a != deps_b

    def test_all_models_have_valid_sql(self) -> None:
        """Every generated model must carry non-empty SQL."""
        for gen_fn in [
            lambda: SyntheticGraphGenerator.generate_linear_chain(20),
            lambda: SyntheticGraphGenerator.generate_wide_fanout(20),
            lambda: SyntheticGraphGenerator.generate_diamond(20),
            lambda: SyntheticGraphGenerator.generate_realistic(20),
        ]:
            models = gen_fn()
            for model in models:
                assert model.raw_sql, f"{model.name} has empty SQL"
                assert model.clean_sql, f"{model.name} has empty clean_sql"

    def test_all_models_have_file_path(self) -> None:
        """Every model should have a plausible file_path."""
        models = SyntheticGraphGenerator.generate_realistic(50)
        for model in models:
            assert model.file_path.startswith("models/")
            assert model.file_path.endswith(".sql")

    def test_model_names_zero_padded(self) -> None:
        models = SyntheticGraphGenerator.generate_linear_chain(5)
        names = [m.name for m in models]
        assert names == ["m_0000", "m_0001", "m_0002", "m_0003", "m_0004"]


# ---------------------------------------------------------------------------
# Performance assertions
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
class TestDAGBuildPerformance:
    """DAG construction must complete within time budgets."""

    @pytest.mark.parametrize(
        "n,max_ms",
        [
            (100, 500),
            (500, 2000),
            (1000, 5000),
        ],
    )
    def test_dag_build_within_budget(self, n: int, max_ms: float) -> None:
        models = SyntheticGraphGenerator.generate_realistic(n)
        result = BenchmarkProfiler.profile_dag_build(models, topology="realistic")

        assert result.duration_ms < max_ms, (
            f"DAG build for {n} models took {result.duration_ms:.1f}ms (budget: {max_ms}ms)"
        )
        assert result.model_count == n

    def test_dag_build_at_5000_under_10_seconds(self) -> None:
        """5000-model DAG build should complete in under 10 seconds."""
        models = SyntheticGraphGenerator.generate_realistic(5000)
        result = BenchmarkProfiler.profile_dag_build(models, topology="realistic")
        assert result.duration_ms < 10_000, f"5000-model DAG build took {result.duration_ms:.1f}ms (> 10s)"


@pytest.mark.benchmark
class TestTopologicalSortPerformance:
    """Topological sort must complete within time budgets."""

    @pytest.mark.parametrize(
        "n,max_ms",
        [
            (100, 100),
            (500, 500),
            (1000, 1500),
        ],
    )
    def test_topo_sort_within_budget(self, n: int, max_ms: float) -> None:
        models = SyntheticGraphGenerator.generate_realistic(n)
        result = BenchmarkProfiler.profile_topological_sort(models, topology="realistic")

        assert result.duration_ms < max_ms, (
            f"Topo sort for {n} models took {result.duration_ms:.1f}ms (budget: {max_ms}ms)"
        )

    def test_topo_sort_at_5000_under_5_seconds(self) -> None:
        models = SyntheticGraphGenerator.generate_realistic(5000)
        result = BenchmarkProfiler.profile_topological_sort(models, topology="realistic")
        assert result.duration_ms < 5_000, f"5000-model topo sort took {result.duration_ms:.1f}ms (> 5s)"


@pytest.mark.benchmark
class TestPlanGenerationPerformance:
    """Plan generation must complete within time budgets."""

    @pytest.mark.parametrize(
        "n,max_ms",
        [
            (100, 1000),
            (500, 3000),
            (1000, 5000),
        ],
    )
    def test_plan_gen_within_budget(self, n: int, max_ms: float) -> None:
        models = SyntheticGraphGenerator.generate_realistic(n)
        result = BenchmarkProfiler.profile_plan_generation(models, topology="realistic")

        assert result.duration_ms < max_ms, (
            f"Plan gen for {n} models took {result.duration_ms:.1f}ms (budget: {max_ms}ms)"
        )


@pytest.mark.benchmark
class TestSQLNormalizationPerformance:
    """SQL normalisation must be fast on a per-model basis."""

    @pytest.mark.parametrize("n", [100, 500, 1000])
    def test_sql_norm_per_model_under_threshold(self, n: int) -> None:
        models = SyntheticGraphGenerator.generate_realistic(n)
        result = BenchmarkProfiler.profile_sql_normalization(models)

        per_model_ms = result.metadata.get("per_model_ms", float("inf"))
        assert per_model_ms < 50.0, f"SQL normalisation averaged {per_model_ms:.3f}ms/model (> 50ms)"


@pytest.mark.benchmark
class TestFullPipelinePerformance:
    """Full pipeline profiling across topologies."""

    @pytest.mark.parametrize("topology", ["linear", "fanout", "diamond", "realistic"])
    def test_full_pipeline_completes(self, topology: str) -> None:
        results = BenchmarkProfiler.profile_full_pipeline(100, topology=topology)
        assert len(results) == 4
        for result in results:
            assert isinstance(result, BenchmarkResult)
            assert result.duration_ms >= 0
            assert result.peak_memory_mb >= 0
            assert result.model_count == 100


@pytest.mark.benchmark
class TestBenchmarkResultStructure:
    """BenchmarkResult instances must be well-formed."""

    def test_result_is_frozen(self) -> None:
        models = SyntheticGraphGenerator.generate_linear_chain(10)
        result = BenchmarkProfiler.profile_dag_build(models)
        with pytest.raises(AttributeError):
            result.duration_ms = 999.0  # type: ignore[misc]

    def test_result_has_all_fields(self) -> None:
        models = SyntheticGraphGenerator.generate_linear_chain(10)
        result = BenchmarkProfiler.profile_dag_build(models)
        assert result.operation == "dag.build"
        assert result.model_count == 10
        assert isinstance(result.duration_ms, float)
        assert isinstance(result.peak_memory_mb, float)
        assert isinstance(result.throughput_ops_per_sec, float)
        assert isinstance(result.metadata, dict)
