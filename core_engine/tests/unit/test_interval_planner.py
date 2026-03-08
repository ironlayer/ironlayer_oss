"""Unit tests for core_engine.planner.interval_planner."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from core_engine.graph.dag_builder import build_dag
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import (
    ModelDefinition,
    ModelKind,
)
from core_engine.models.plan import Plan, RunType
from core_engine.planner.interval_planner import (
    PlannerConfig,
    generate_plan,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AS_OF = date(2025, 6, 1)


def _model(
    name: str,
    kind: ModelKind = ModelKind.FULL_REFRESH,
    referenced_tables: list[str] | None = None,
    dependencies: list[str] | None = None,
    time_column: str | None = None,
    unique_key: str | None = None,
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=kind,
        file_path=f"models/{name}.sql",
        raw_sql=f"SELECT 1",
        referenced_tables=referenced_tables or [],
        dependencies=dependencies or [],
        time_column=time_column,
        unique_key=unique_key,
    )


def _models_dict(*defs: ModelDefinition) -> dict[str, ModelDefinition]:
    return {m.name: m for m in defs}


def _empty_diff() -> DiffResult:
    return DiffResult()


def _diff(
    added: list[str] | None = None,
    removed: list[str] | None = None,
    modified: list[str] | None = None,
) -> DiffResult:
    return DiffResult(
        added_models=added or [],
        removed_models=removed or [],
        modified_models=modified or [],
    )


# ---------------------------------------------------------------------------
# generate_plan - basic scenarios
# ---------------------------------------------------------------------------


class TestGeneratePlanBasic:
    def test_single_full_refresh_model_changed(self):
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )

        assert isinstance(plan, Plan)
        assert len(plan.steps) == 1
        assert plan.steps[0].model == "A"
        assert plan.steps[0].run_type == RunType.FULL_REFRESH

    def test_no_changes_produces_empty_plan(self):
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _empty_diff()

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )
        assert len(plan.steps) == 0
        assert plan.summary.total_steps == 0

    def test_added_model_gets_full_refresh(self):
        m = _model("new_model")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(added=["new_model"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        assert len(plan.steps) == 1
        assert plan.steps[0].run_type == RunType.FULL_REFRESH
        assert plan.steps[0].reason == "new model added"


# ---------------------------------------------------------------------------
# Downstream propagation
# ---------------------------------------------------------------------------


class TestDownstreamPropagation:
    def test_downstream_models_included(self):
        """When A changes, B (downstream of A) should also be in the plan."""
        a = _model("A")
        b = _model("B", referenced_tables=["A"])
        models = _models_dict(a, b)
        dag = build_dag([a, b])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        step_models = {s.model for s in plan.steps}
        assert "A" in step_models
        assert "B" in step_models

    def test_transitive_downstream(self):
        """A -> B -> C.  Change A => A, B, C all in plan."""
        a = _model("A")
        b = _model("B", referenced_tables=["A"])
        c = _model("C", referenced_tables=["B"])
        models = _models_dict(a, b, c)
        dag = build_dag([a, b, c])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        step_models = {s.model for s in plan.steps}
        assert step_models == {"A", "B", "C"}


# ---------------------------------------------------------------------------
# Model kinds
# ---------------------------------------------------------------------------


class TestModelKinds:
    def test_incremental_by_time_range_with_watermarks(self):
        m = _model("A", kind=ModelKind.INCREMENTAL_BY_TIME_RANGE, time_column="ts")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])
        watermarks = {"A": (date(2025, 5, 1), date(2025, 5, 15))}

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats={},
            as_of_date=AS_OF,
        )
        assert len(plan.steps) == 1
        step = plan.steps[0]
        assert step.run_type == RunType.INCREMENTAL
        assert step.input_range is not None
        # Should start from watermark end.
        assert step.input_range.start == date(2025, 5, 15)
        assert step.input_range.end == AS_OF

    def test_incremental_without_watermark_uses_lookback(self):
        config = PlannerConfig(default_lookback_days=7)
        m = _model("A", kind=ModelKind.INCREMENTAL_BY_TIME_RANGE, time_column="ts")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            config=config,
            as_of_date=AS_OF,
        )
        step = plan.steps[0]
        assert step.run_type == RunType.INCREMENTAL
        assert step.input_range is not None
        expected_start = AS_OF - timedelta(days=7)
        assert step.input_range.start == expected_start

    def test_merge_by_key_gets_full_refresh(self):
        m = _model("A", kind=ModelKind.MERGE_BY_KEY, unique_key="id")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        assert plan.steps[0].run_type == RunType.FULL_REFRESH
        assert plan.steps[0].input_range is None

    def test_append_only_gets_incremental(self):
        m = _model("A", kind=ModelKind.APPEND_ONLY)
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        assert plan.steps[0].run_type == RunType.INCREMENTAL

    def test_newly_added_incremental_model_gets_full_refresh(self):
        m = _model("A", kind=ModelKind.INCREMENTAL_BY_TIME_RANGE, time_column="ts")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(added=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        # Even though model is incremental, being newly added means full refresh.
        assert plan.steps[0].run_type == RunType.FULL_REFRESH


# ---------------------------------------------------------------------------
# Multiple changed models
# ---------------------------------------------------------------------------


class TestMultipleChangedModels:
    def test_multiple_changed(self):
        a = _model("A")
        b = _model("B")
        models = _models_dict(a, b)
        dag = build_dag([a, b])
        diff = _diff(modified=["A", "B"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        step_models = {s.model for s in plan.steps}
        assert step_models == {"A", "B"}

    def test_models_sorted_in_plan(self):
        z = _model("Z")
        a = _model("A")
        models = _models_dict(z, a)
        dag = build_dag([z, a])
        diff = _diff(modified=["Z", "A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        assert plan.steps[0].model == "A"
        assert plan.steps[1].model == "Z"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_inputs_produce_identical_plan(self):
        a = _model("A")
        b = _model("B", referenced_tables=["A"])
        models = _models_dict(a, b)
        dag = build_dag([a, b])
        diff = _diff(modified=["A"])

        plan1 = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )
        plan2 = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )

        assert plan1.plan_id == plan2.plan_id
        assert len(plan1.steps) == len(plan2.steps)
        for s1, s2 in zip(plan1.steps, plan2.steps):
            assert s1.step_id == s2.step_id
            assert s1.model == s2.model
            assert s1.run_type == s2.run_type

    def test_plan_id_changes_with_different_base(self):
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan1 = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )
        plan2 = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap_DIFFERENT",
            target="snap2",
            as_of_date=AS_OF,
        )
        assert plan1.plan_id != plan2.plan_id


# ---------------------------------------------------------------------------
# PlannerConfig
# ---------------------------------------------------------------------------


class TestPlannerConfig:
    def test_default_values(self):
        config = PlannerConfig()
        assert config.default_lookback_days == 30
        assert config.cost_per_compute_second == 0.0007

    def test_custom_values(self):
        config = PlannerConfig(default_lookback_days=7, cost_per_compute_second=0.005)
        assert config.default_lookback_days == 7
        assert config.cost_per_compute_second == 0.005


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------


class TestCostEstimation:
    def test_uses_run_stats_when_available(self):
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])
        run_stats = {"A": {"avg_runtime_seconds": 120.0}}
        config = PlannerConfig(cost_per_compute_second=0.001)

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats=run_stats,
            config=config,
            as_of_date=AS_OF,
        )
        step = plan.steps[0]
        assert step.estimated_compute_seconds == 120.0
        assert step.estimated_cost_usd == pytest.approx(0.12, abs=1e-6)

    def test_default_estimate_when_no_stats(self):
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        step = plan.steps[0]
        # Default is 300s (5 min) at 0.0007/s.
        assert step.estimated_compute_seconds == 300.0
        assert step.estimated_cost_usd == pytest.approx(300.0 * 0.0007, abs=1e-6)

    def test_plan_summary_cost(self):
        a = _model("A")
        b = _model("B")
        models = _models_dict(a, b)
        dag = build_dag([a, b])
        diff = _diff(modified=["A", "B"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )
        total_step_cost = sum(s.estimated_cost_usd for s in plan.steps)
        assert plan.summary.estimated_cost_usd == pytest.approx(total_step_cost, abs=1e-6)
