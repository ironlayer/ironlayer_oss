"""Plan reproducibility tests.

These tests verify the cardinal invariant of the IronLayer planner:
**identical inputs always produce byte-identical plan JSON**.

These run in CI on every push and weekly as a regression gate.
"""

from __future__ import annotations

import json
from datetime import date

import networkx as nx
import pytest

from core_engine.diff.structural_diff import compute_structural_diff
from core_engine.graph.dag_builder import build_dag
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)
from core_engine.planner.interval_planner import PlannerConfig, generate_plan
from core_engine.planner.plan_serializer import serialize_plan

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_model(
    name: str,
    kind: ModelKind = ModelKind.FULL_REFRESH,
    deps: list[str] | None = None,
    time_column: str | None = None,
    unique_key: str | None = None,
) -> ModelDefinition:
    """Create a minimal ModelDefinition for testing."""
    return ModelDefinition(
        name=name,
        kind=kind,
        materialization=Materialization.TABLE,
        time_column=time_column,
        unique_key=unique_key,
        dependencies=deps or [],
        file_path=f"models/{name}.sql",
        raw_sql=f"SELECT * FROM source_{name}",
        clean_sql=f"SELECT * FROM source_{name}",
        content_hash=f"hash_{name}",
        referenced_tables=[],
        output_columns=["id", "value"],
    )


def _build_test_scenario(
    model_specs: list[dict],
    changed_models: list[str],
    added_models: list[str] | None = None,
) -> tuple[dict[str, ModelDefinition], DiffResult, nx.DiGraph, dict, dict]:
    """Build a complete test scenario from specs."""
    models: list[ModelDefinition] = []
    for spec in model_specs:
        models.append(
            _make_model(
                name=spec["name"],
                kind=spec.get("kind", ModelKind.FULL_REFRESH),
                deps=spec.get("deps", []),
                time_column=spec.get("time_column"),
                unique_key=spec.get("unique_key"),
            )
        )

    models_dict = {m.name: m for m in models}
    dag = build_dag(models)

    base_versions = {}
    target_versions = {}
    for m in models:
        target_versions[m.name] = m.content_hash
        if m.name not in (added_models or []):
            if m.name in changed_models:
                base_versions[m.name] = f"old_hash_{m.name}"
            else:
                base_versions[m.name] = m.content_hash

    diff_result = compute_structural_diff(base_versions, target_versions)

    watermarks: dict[str, tuple[date, date]] = {}
    run_stats: dict[str, dict] = {}

    return models_dict, diff_result, dag, watermarks, run_stats


# ---------------------------------------------------------------------------
# Core determinism tests
# ---------------------------------------------------------------------------


class TestPlannerDeterminism:
    """Verify that the planner produces identical output for identical input."""

    def test_identical_inputs_produce_identical_json(self) -> None:
        """The same inputs run twice must produce byte-identical JSON."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "raw.events", "kind": ModelKind.FULL_REFRESH},
                {"name": "staging.events_clean", "deps": ["raw.events"]},
                {"name": "analytics.daily_summary", "deps": ["staging.events_clean"]},
            ],
            changed_models=["raw.events"],
        )

        config = PlannerConfig(default_lookback_days=30)
        fixed_date = date(2025, 1, 15)

        plan_a = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            config=config,
            base="abc123",
            target="def456",
            as_of_date=fixed_date,
        )
        json_a = serialize_plan(plan_a)

        plan_b = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            config=config,
            base="abc123",
            target="def456",
            as_of_date=fixed_date,
        )
        json_b = serialize_plan(plan_b)

        assert json_a == json_b, "Identical inputs must produce identical JSON"

    def test_plan_ids_are_deterministic(self) -> None:
        """Plan IDs must be derived from content, not random."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "model_a"},
                {"name": "model_b", "deps": ["model_a"]},
            ],
            changed_models=["model_a"],
        )

        config = PlannerConfig()
        fixed_date = date(2025, 6, 1)

        ids = set()
        for _ in range(10):
            plan = generate_plan(
                models=models_dict,
                diff_result=diff,
                dag=dag,
                watermarks=watermarks,
                run_stats=stats,
                config=config,
                base="sha_base",
                target="sha_target",
                as_of_date=fixed_date,
            )
            ids.add(plan.plan_id)

        assert len(ids) == 1, f"Plan ID should be deterministic, got {len(ids)} distinct IDs"

    def test_step_ids_are_deterministic(self) -> None:
        """Step IDs must be SHA-256 derived from model+base+target."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "source"},
                {"name": "derived", "deps": ["source"]},
            ],
            changed_models=["source"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b1",
            target="t1",
            as_of_date=date(2025, 3, 1),
        )

        step_ids_a = [s.step_id for s in plan.steps]

        plan2 = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b1",
            target="t1",
            as_of_date=date(2025, 3, 1),
        )
        step_ids_b = [s.step_id for s in plan2.steps]

        assert step_ids_a == step_ids_b

    def test_no_timestamps_in_plan_json(self) -> None:
        """Plan JSON must not contain any timestamp fields."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[{"name": "model_x"}],
            changed_models=["model_x"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="base",
            target="target",
            as_of_date=date(2025, 1, 1),
        )

        json_str = serialize_plan(plan)
        data = json.loads(json_str)

        # Walk the entire structure looking for timestamp-like keys
        timestamp_keys = {"created_at", "generated_at", "timestamp", "updated_at"}
        found = _find_keys_recursive(data, timestamp_keys)
        assert not found, f"Plan JSON contains timestamp keys: {found}"

    def test_sorted_keys_in_serialized_json(self) -> None:
        """Serialized JSON must have sorted keys."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "a_model"},
                {"name": "b_model", "deps": ["a_model"]},
                {"name": "c_model", "deps": ["b_model"]},
            ],
            changed_models=["a_model"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        json_str = serialize_plan(plan)
        data = json.loads(json_str)

        # Verify top-level keys are sorted
        keys = list(data.keys())
        assert keys == sorted(keys), f"Top-level keys not sorted: {keys}"

    def test_models_changed_list_is_sorted(self) -> None:
        """The models_changed list must be alphabetically sorted."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "zebra"},
                {"name": "alpha"},
                {"name": "middle"},
            ],
            changed_models=["zebra", "alpha", "middle"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        assert plan.summary.models_changed == sorted(plan.summary.models_changed)


class TestPlannerScenarios:
    """Test various planning scenarios for correctness."""

    def test_single_model_change(self) -> None:
        """Changing a single leaf model produces a 1-step plan."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "raw.events"},
                {"name": "analytics.summary", "deps": ["raw.events"]},
            ],
            changed_models=["analytics.summary"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        assert plan.summary.total_steps == 1
        assert plan.steps[0].model == "analytics.summary"

    def test_upstream_change_propagates_downstream(self) -> None:
        """Changing an upstream model includes all downstream models."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "source"},
                {"name": "mid", "deps": ["source"]},
                {"name": "leaf", "deps": ["mid"]},
            ],
            changed_models=["source"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        affected = {s.model for s in plan.steps}
        assert affected == {"source", "mid", "leaf"}

    def test_incremental_model_gets_date_range(self) -> None:
        """Incremental models produce a step with an input_range."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {
                    "name": "inc_model",
                    "kind": ModelKind.INCREMENTAL_BY_TIME_RANGE,
                    "time_column": "event_date",
                },
            ],
            changed_models=["inc_model"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 6, 15),
        )

        assert plan.steps[0].input_range is not None
        assert plan.steps[0].input_range.end == date(2025, 6, 15)

    def test_new_model_gets_full_refresh(self) -> None:
        """Newly added models always get FULL_REFRESH regardless of kind."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {
                    "name": "new_inc",
                    "kind": ModelKind.INCREMENTAL_BY_TIME_RANGE,
                    "time_column": "ts",
                },
            ],
            changed_models=[],
            added_models=["new_inc"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        assert plan.steps[0].run_type.value == "FULL_REFRESH"

    def test_empty_diff_produces_empty_plan(self) -> None:
        """No changes = no steps."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[{"name": "stable"}],
            changed_models=[],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        assert plan.summary.total_steps == 0
        assert plan.steps == []

    def test_diamond_dependency(self) -> None:
        """Diamond pattern: A->B, A->C, B->D, C->D. Change A -> all rebuilt."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "A"},
                {"name": "B", "deps": ["A"]},
                {"name": "C", "deps": ["A"]},
                {"name": "D", "deps": ["B", "C"]},
            ],
            changed_models=["A"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        affected = {s.model for s in plan.steps}
        assert affected == {"A", "B", "C", "D"}

    def test_parallel_groups_respect_dependencies(self) -> None:
        """Steps in the same parallel group must not depend on each other."""
        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "root"},
                {"name": "left", "deps": ["root"]},
                {"name": "right", "deps": ["root"]},
                {"name": "merge", "deps": ["left", "right"]},
            ],
            changed_models=["root"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        groups: dict[int, list[str]] = {}
        for step in plan.steps:
            groups.setdefault(step.parallel_group, []).append(step.model)

        # left and right should be in the same group (no mutual dependency)
        for group_models in groups.values():
            for i, m1 in enumerate(group_models):
                for m2 in group_models[i + 1 :]:
                    assert not dag.has_edge(m1, m2) and not dag.has_edge(m2, m1), (
                        f"Models {m1} and {m2} in same parallel group but have dependency"
                    )


class TestSerializerRoundTrip:
    """Verify plan serialization round-trips correctly."""

    def test_serialize_deserialize_roundtrip(self) -> None:
        """Plan survives JSON round-trip without data loss."""
        from core_engine.planner.plan_serializer import deserialize_plan

        models_dict, diff, dag, watermarks, stats = _build_test_scenario(
            model_specs=[
                {"name": "a"},
                {"name": "b", "deps": ["a"]},
            ],
            changed_models=["a"],
        )

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats=stats,
            base="base_sha",
            target="target_sha",
            as_of_date=date(2025, 1, 1),
        )

        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)

        assert plan.plan_id == restored.plan_id
        assert plan.base == restored.base
        assert plan.target == restored.target
        assert plan.summary.total_steps == restored.summary.total_steps
        assert len(plan.steps) == len(restored.steps)
        for orig, rest in zip(plan.steps, restored.steps):
            assert orig.step_id == rest.step_id
            assert orig.model == rest.model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_keys_recursive(obj: object, target_keys: set[str]) -> list[str]:
    """Recursively find target keys in a nested dict/list structure."""
    found: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in target_keys:
                found.append(key)
            found.extend(_find_keys_recursive(value, target_keys))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(_find_keys_recursive(item, target_keys))
    return found
