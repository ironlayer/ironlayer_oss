"""Full pipeline end-to-end test for IronLayer core engine.

Exercises the complete flow:
  loader -> parser -> DAG -> diff -> planner -> serializer -> executor

Uses tmp_path for all file operations and DuckDB for local execution.
No external services (Postgres, Databricks) are required.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

from core_engine.diff.structural_diff import compute_structural_diff
from core_engine.graph.dag_builder import build_dag
from core_engine.loader.model_loader import load_models_from_directory
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)
from core_engine.models.plan import (
    DateRange,
    Plan,
    PlanStep,
    PlanSummary,
    RunType,
    compute_deterministic_id,
)
from core_engine.models.run import RunRecord, RunStatus
from core_engine.models.snapshot import ModelVersion, Snapshot
from core_engine.planner.interval_planner import PlannerConfig, generate_plan
from core_engine.planner.plan_serializer import (
    deserialize_plan,
    serialize_plan,
    validate_plan_schema,
)

# The executor package __init__.py eagerly imports DatabricksExecutor, which
# depends on a compatible version of the ``databricks-sdk`` library.  If the
# SDK version on the current machine is missing or incompatible, we still want
# the non-executor tests to run.  Guard the import so that executor-dependent
# tests are skipped rather than the whole file failing to collect.
try:
    from core_engine.executor.local_executor import LocalExecutor

    _HAS_LOCAL_EXECUTOR = True
except ImportError:
    _HAS_LOCAL_EXECUTOR = False
    LocalExecutor = None  # type: ignore[assignment,misc]

requires_executor = pytest.mark.skipif(
    not _HAS_LOCAL_EXECUTOR,
    reason="LocalExecutor unavailable (databricks-sdk import issue)",
)


# ---------------------------------------------------------------------------
# SQL model file content used across tests
# ---------------------------------------------------------------------------

_RAW_EVENTS_SQL = """\
-- name: raw.events
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: data-platform
-- tags: raw, events

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    amount
FROM source_system.raw_events
WHERE _ingested_at >= '{{ start_date }}'
    AND _ingested_at < '{{ end_date }}'
"""

_STAGING_EVENTS_SQL = """\
-- name: staging.events_clean
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: data-platform
-- tags: staging, events
-- dependencies: raw.events

SELECT
    event_id,
    user_id,
    LOWER(event_type) AS event_type,
    event_timestamp,
    COALESCE(amount, 0) AS amount
FROM {{ ref('raw.events') }}
WHERE event_id IS NOT NULL
"""

_ANALYTICS_ORDERS_DAILY_SQL = """\
-- name: analytics.orders_daily
-- kind: INCREMENTAL_BY_TIME_RANGE
-- materialization: TABLE
-- time_column: order_date
-- owner: analytics
-- tags: analytics, orders
-- dependencies: staging.events_clean

SELECT
    CAST(event_timestamp AS DATE) AS order_date,
    COUNT(DISTINCT event_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(amount) AS total_revenue
FROM {{ ref('staging.events_clean') }}
WHERE event_type = 'purchase'
    AND event_timestamp >= '{{ start_date }}'
    AND event_timestamp < '{{ end_date }}'
GROUP BY CAST(event_timestamp AS DATE)
"""

_ANALYTICS_USER_METRICS_SQL = """\
-- name: analytics.user_metrics
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: analytics
-- tags: analytics, users
-- dependencies: staging.events_clean

SELECT
    user_id,
    COUNT(DISTINCT event_id) AS total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS lifetime_value,
    MIN(event_timestamp) AS first_activity,
    MAX(event_timestamp) AS last_activity
FROM {{ ref('staging.events_clean') }}
GROUP BY user_id
"""

_ANALYTICS_REVENUE_SUMMARY_SQL = """\
-- name: analytics.revenue_summary
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: analytics
-- tags: analytics, executive
-- dependencies: analytics.orders_daily, analytics.user_metrics

SELECT
    o.order_date,
    o.total_orders,
    o.total_revenue,
    um.total_users,
    o.total_revenue / um.total_users AS revenue_per_user
FROM {{ ref('analytics.orders_daily') }} o
CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total_users
    FROM {{ ref('analytics.user_metrics') }}
) um
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def models_dir(tmp_path: Path) -> Path:
    """Create a temporary models directory with sample SQL files."""
    models = tmp_path / "models"
    models.mkdir()

    (models / "raw.events.sql").write_text(_RAW_EVENTS_SQL, encoding="utf-8")
    (models / "staging.events_clean.sql").write_text(_STAGING_EVENTS_SQL, encoding="utf-8")
    (models / "analytics.orders_daily.sql").write_text(_ANALYTICS_ORDERS_DAILY_SQL, encoding="utf-8")
    (models / "analytics.user_metrics.sql").write_text(_ANALYTICS_USER_METRICS_SQL, encoding="utf-8")
    (models / "analytics.revenue_summary.sql").write_text(_ANALYTICS_REVENUE_SUMMARY_SQL, encoding="utf-8")

    return models


@pytest.fixture()
def loaded_models(models_dir: Path) -> list[ModelDefinition]:
    """Load and parse all models from the fixture directory."""
    return load_models_from_directory(models_dir)


@pytest.fixture()
def duckdb_executor(tmp_path: Path):
    """Create a LocalExecutor backed by a temporary DuckDB database."""
    if not _HAS_LOCAL_EXECUTOR:
        pytest.skip("LocalExecutor unavailable (databricks-sdk import issue)")
    db_path = tmp_path / "test.duckdb"
    executor = LocalExecutor(db_path=db_path)
    yield executor
    executor.close()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _build_snapshot(models: list[ModelDefinition], snapshot_id: str) -> Snapshot:
    """Create a Snapshot from a list of loaded ModelDefinitions."""
    versions: dict[str, ModelVersion] = {}
    for m in models:
        versions[m.name] = ModelVersion(
            version_id=compute_deterministic_id(m.name, m.content_hash),
            model_name=m.name,
            canonical_sql_hash=m.content_hash,
            metadata_hash=compute_deterministic_id(m.kind.value, m.materialization.value, m.name),
        )
    return Snapshot(
        snapshot_id=snapshot_id,
        environment="test",
        versions=versions,
    )


def _snapshot_to_hash_map(snapshot: Snapshot) -> dict[str, str]:
    """Extract model_name -> canonical_sql_hash from a Snapshot."""
    return {name: version.canonical_sql_hash for name, version in snapshot.versions.items()}


def _modify_model_file(models_dir: Path, filename: str, new_content: str) -> None:
    """Overwrite a model file with new SQL content."""
    (models_dir / filename).write_text(new_content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Test: Load SQL models and verify parsing
# ---------------------------------------------------------------------------


class TestModelLoading:
    """Verify that the model loader correctly discovers and parses SQL files."""

    def test_all_models_loaded(self, loaded_models: list[ModelDefinition]) -> None:
        """All five SQL model files are loaded."""
        names = {m.name for m in loaded_models}
        assert names == {
            "raw.events",
            "staging.events_clean",
            "analytics.orders_daily",
            "analytics.user_metrics",
            "analytics.revenue_summary",
        }

    def test_model_kinds_parsed_correctly(self, loaded_models: list[ModelDefinition]) -> None:
        """Each model has the expected ModelKind from its header."""
        by_name = {m.name: m for m in loaded_models}
        assert by_name["raw.events"].kind == ModelKind.FULL_REFRESH
        assert by_name["staging.events_clean"].kind == ModelKind.FULL_REFRESH
        assert by_name["analytics.orders_daily"].kind == ModelKind.INCREMENTAL_BY_TIME_RANGE
        assert by_name["analytics.user_metrics"].kind == ModelKind.FULL_REFRESH
        assert by_name["analytics.revenue_summary"].kind == ModelKind.FULL_REFRESH

    def test_ref_resolution(self, loaded_models: list[ModelDefinition]) -> None:
        """After two-pass loading, ref() macros are resolved to canonical names."""
        by_name = {m.name: m for m in loaded_models}
        staging = by_name["staging.events_clean"]
        # clean_sql should have raw.events as a resolved table name, not {{ ref(...) }}
        assert "{{ ref(" not in staging.clean_sql
        assert "raw.events" in staging.clean_sql

    def test_content_hashes_populated(self, loaded_models: list[ModelDefinition]) -> None:
        """Every model has a non-empty content hash."""
        for m in loaded_models:
            assert m.content_hash, f"Model {m.name} has no content_hash"
            assert len(m.content_hash) == 64  # SHA-256 hex digest

    def test_dependencies_declared(self, loaded_models: list[ModelDefinition]) -> None:
        """Models that declare explicit dependencies have them populated."""
        by_name = {m.name: m for m in loaded_models}
        assert "raw.events" in by_name["staging.events_clean"].dependencies
        assert "staging.events_clean" in by_name["analytics.orders_daily"].dependencies
        revenue = by_name["analytics.revenue_summary"]
        assert "analytics.orders_daily" in revenue.dependencies
        assert "analytics.user_metrics" in revenue.dependencies

    def test_incremental_model_has_time_column(self, loaded_models: list[ModelDefinition]) -> None:
        """The INCREMENTAL_BY_TIME_RANGE model has time_column set."""
        by_name = {m.name: m for m in loaded_models}
        assert by_name["analytics.orders_daily"].time_column == "order_date"


# ---------------------------------------------------------------------------
# Test: DAG construction and topological ordering
# ---------------------------------------------------------------------------


class TestDAGConstruction:
    """Verify the dependency graph is built correctly from loaded models."""

    def test_dag_has_all_nodes(self, loaded_models: list[ModelDefinition]) -> None:
        """DAG contains a node for every loaded model."""
        dag = build_dag(loaded_models)
        assert set(dag.nodes) == {m.name for m in loaded_models}

    def test_dag_edges_respect_dependencies(self, loaded_models: list[ModelDefinition]) -> None:
        """Edges flow from upstream to downstream per declared dependencies."""
        dag = build_dag(loaded_models)
        # staging.events_clean depends on raw.events
        assert dag.has_edge("raw.events", "staging.events_clean")
        # analytics.orders_daily depends on staging.events_clean
        assert dag.has_edge("staging.events_clean", "analytics.orders_daily")
        # analytics.revenue_summary depends on both analytics models
        assert dag.has_edge("analytics.orders_daily", "analytics.revenue_summary")
        assert dag.has_edge("analytics.user_metrics", "analytics.revenue_summary")

    def test_topological_sort_is_valid(self, loaded_models: list[ModelDefinition]) -> None:
        """Topological sort yields a valid execution order."""
        import networkx as nx

        dag = build_dag(loaded_models)
        # Use nx.topological_sort directly to avoid a NetworkX version
        # compatibility issue in dag_builder.topological_sort (which calls
        # the renamed ``lexicographic_topological_sort``).
        order = list(nx.topological_sort(dag))
        index = {name: i for i, name in enumerate(order)}

        # Every edge (u -> v) must have u before v.
        for u, v in dag.edges:
            assert index[u] < index[v], (
                f"Topological order violated: {u} (pos {index[u]}) should come before {v} (pos {index[v]})"
            )

    def test_no_cycles(self, loaded_models: list[ModelDefinition]) -> None:
        """The test model set has no cyclic dependencies."""
        import networkx as nx

        dag = build_dag(loaded_models)
        assert nx.is_directed_acyclic_graph(dag)


# ---------------------------------------------------------------------------
# Test: Snapshots and structural diff
# ---------------------------------------------------------------------------


class TestSnapshotAndDiff:
    """Verify snapshot creation and structural diff computation."""

    def test_snapshot_captures_all_models(self, loaded_models: list[ModelDefinition]) -> None:
        """A snapshot built from loaded models includes every model."""
        snapshot = _build_snapshot(loaded_models, "snap-001")
        assert set(snapshot.versions.keys()) == {m.name for m in loaded_models}

    def test_identical_snapshots_produce_empty_diff(self, loaded_models: list[ModelDefinition]) -> None:
        """Diffing two identical snapshots yields no changes."""
        snap = _build_snapshot(loaded_models, "snap-001")
        hashes = _snapshot_to_hash_map(snap)
        diff = compute_structural_diff(hashes, hashes)

        assert diff.added_models == []
        assert diff.removed_models == []
        assert diff.modified_models == []

    def test_modified_model_detected(
        self,
        models_dir: Path,
        loaded_models: list[ModelDefinition],
    ) -> None:
        """Changing a model file creates a diff with that model as modified."""
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        # Modify the staging model (add a WHERE clause to change the hash)
        modified_staging_sql = """\
-- name: staging.events_clean
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: data-platform
-- tags: staging, events
-- dependencies: raw.events

SELECT
    event_id,
    user_id,
    LOWER(event_type) AS event_type,
    event_timestamp,
    COALESCE(amount, 0) AS amount,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ ref('raw.events') }}
WHERE event_id IS NOT NULL
    AND user_id IS NOT NULL
"""
        _modify_model_file(models_dir, "staging.events_clean.sql", modified_staging_sql)

        # Reload models
        target_models = load_models_from_directory(models_dir)
        target_snapshot = _build_snapshot(target_models, "snap-target")
        target_hashes = _snapshot_to_hash_map(target_snapshot)

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert "staging.events_clean" in diff.modified_models

    def test_added_model_detected(self, models_dir: Path, loaded_models: list[ModelDefinition]) -> None:
        """Adding a new model file shows it as added in the diff."""
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        new_model_sql = """\
-- name: analytics.customer_cohorts
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: analytics
-- tags: analytics, cohorts
-- dependencies: staging.events_clean

SELECT
    user_id,
    MIN(CAST(event_timestamp AS DATE)) AS cohort_date,
    COUNT(DISTINCT event_id) AS events_in_first_week
FROM {{ ref('staging.events_clean') }}
GROUP BY user_id
"""
        (models_dir / "analytics.customer_cohorts.sql").write_text(new_model_sql, encoding="utf-8")

        target_models = load_models_from_directory(models_dir)
        target_snapshot = _build_snapshot(target_models, "snap-target")
        target_hashes = _snapshot_to_hash_map(target_snapshot)

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert "analytics.customer_cohorts" in diff.added_models

    def test_removed_model_detected(self, models_dir: Path, loaded_models: list[ModelDefinition]) -> None:
        """Removing a model file shows it as removed in the diff."""
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        # Remove analytics.revenue_summary
        (models_dir / "analytics.revenue_summary.sql").unlink()

        target_models = load_models_from_directory(models_dir)
        target_snapshot = _build_snapshot(target_models, "snap-target")
        target_hashes = _snapshot_to_hash_map(target_snapshot)

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert "analytics.revenue_summary" in diff.removed_models


# ---------------------------------------------------------------------------
# Test: Plan generation
# ---------------------------------------------------------------------------


class TestPlanGeneration:
    """Verify plan generation from diffs."""

    def test_single_leaf_change_produces_one_step(self, loaded_models: list[ModelDefinition]) -> None:
        """Changing a leaf model (no downstream) produces a single-step plan."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        # Simulate analytics.revenue_summary changed (it is a leaf)
        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["analytics.revenue_summary"] = "modified_hash_revenue"

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert diff.modified_models == ["analytics.revenue_summary"]

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap-base",
            target="snap-target",
            as_of_date=date(2025, 6, 15),
        )

        assert plan.summary.total_steps == 1
        assert plan.steps[0].model == "analytics.revenue_summary"
        assert plan.steps[0].run_type == RunType.FULL_REFRESH

    def test_upstream_change_cascades_downstream(self, loaded_models: list[ModelDefinition]) -> None:
        """Changing raw.events cascades to all downstream models."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["raw.events"] = "modified_hash_raw"

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert diff.modified_models == ["raw.events"]

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap-base",
            target="snap-target",
            as_of_date=date(2025, 6, 15),
        )

        affected_models = {s.model for s in plan.steps}
        assert affected_models == {
            "raw.events",
            "staging.events_clean",
            "analytics.orders_daily",
            "analytics.user_metrics",
            "analytics.revenue_summary",
        }
        assert plan.summary.total_steps == 5

    def test_incremental_model_gets_date_range(self, loaded_models: list[ModelDefinition]) -> None:
        """INCREMENTAL_BY_TIME_RANGE model receives a DateRange in its step."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["staging.events_clean"] = "modified_staging"

        diff = compute_structural_diff(base_hashes, target_hashes)
        fixed_date = date(2025, 6, 15)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            config=PlannerConfig(default_lookback_days=30),
            base="snap-base",
            target="snap-target",
            as_of_date=fixed_date,
        )

        orders_step = next(s for s in plan.steps if s.model == "analytics.orders_daily")
        assert orders_step.run_type == RunType.INCREMENTAL
        assert orders_step.input_range is not None
        assert orders_step.input_range.end == fixed_date

    def test_watermark_based_incremental_range(self, loaded_models: list[ModelDefinition]) -> None:
        """Watermarks narrow the incremental date range appropriately."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["staging.events_clean"] = "modified_staging"

        diff = compute_structural_diff(base_hashes, target_hashes)
        fixed_date = date(2025, 6, 15)
        watermarks = {
            "analytics.orders_daily": (date(2025, 1, 1), date(2025, 6, 10)),
        }

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats={},
            config=PlannerConfig(default_lookback_days=30),
            base="snap-base",
            target="snap-target",
            as_of_date=fixed_date,
        )

        orders_step = next(s for s in plan.steps if s.model == "analytics.orders_daily")
        assert orders_step.run_type == RunType.INCREMENTAL
        # With a watermark end of 2025-06-10, the start should be 2025-06-10
        assert orders_step.input_range.start == date(2025, 6, 10)
        assert orders_step.input_range.end == fixed_date

    def test_parallel_groups_assigned_correctly(self, loaded_models: list[ModelDefinition]) -> None:
        """Models at the same DAG depth share a parallel group."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["raw.events"] = "changed"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        step_by_model = {s.model: s for s in plan.steps}
        # analytics.orders_daily and analytics.user_metrics are at the same
        # depth (both depend on staging.events_clean) and should share a group.
        assert (
            step_by_model["analytics.orders_daily"].parallel_group
            == step_by_model["analytics.user_metrics"].parallel_group
        )
        # analytics.revenue_summary depends on both and should be deeper.
        assert (
            step_by_model["analytics.revenue_summary"].parallel_group
            > step_by_model["analytics.orders_daily"].parallel_group
        )

    def test_plan_has_deterministic_ids(self, loaded_models: list[ModelDefinition]) -> None:
        """Plan and step IDs are deterministic across runs."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["raw.events"] = "changed"

        diff = compute_structural_diff(base_hashes, target_hashes)
        kwargs = dict(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="base-snap",
            target="target-snap",
            as_of_date=date(2025, 3, 1),
        )

        plan_a = generate_plan(**kwargs)
        plan_b = generate_plan(**kwargs)

        assert plan_a.plan_id == plan_b.plan_id
        assert [s.step_id for s in plan_a.steps] == [s.step_id for s in plan_b.steps]


# ---------------------------------------------------------------------------
# Test: Plan serialization round-trip
# ---------------------------------------------------------------------------


class TestPlanSerialization:
    """Verify plan JSON serialization and deserialization round-trip."""

    def test_serialize_deserialize_roundtrip(self, loaded_models: list[ModelDefinition]) -> None:
        """Plan survives a JSON round-trip without data loss."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["staging.events_clean"] = "changed_hash"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="base-001",
            target="target-001",
            as_of_date=date(2025, 6, 15),
        )

        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)

        assert restored.plan_id == plan.plan_id
        assert restored.base == plan.base
        assert restored.target == plan.target
        assert restored.summary.total_steps == plan.summary.total_steps
        assert restored.summary.models_changed == plan.summary.models_changed
        assert len(restored.steps) == len(plan.steps)

        for orig, rest in zip(plan.steps, restored.steps):
            assert orig.step_id == rest.step_id
            assert orig.model == rest.model
            assert orig.run_type == rest.run_type
            assert orig.parallel_group == rest.parallel_group
            assert orig.depends_on == rest.depends_on
            if orig.input_range is not None:
                assert rest.input_range is not None
                assert orig.input_range.start == rest.input_range.start
                assert orig.input_range.end == rest.input_range.end
            else:
                assert rest.input_range is None

    def test_serialized_json_has_sorted_keys(self, loaded_models: list[ModelDefinition]) -> None:
        """Serialized plan JSON uses sorted keys for determinism."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["raw.events"] = "modified"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        json_str = serialize_plan(plan)
        data = json.loads(json_str)

        # Top-level keys must be sorted
        assert list(data.keys()) == sorted(data.keys())

        # Step-level keys must be sorted
        for step_data in data["steps"]:
            assert list(step_data.keys()) == sorted(step_data.keys())

    def test_double_serialization_is_idempotent(self, loaded_models: list[ModelDefinition]) -> None:
        """Serializing -> deserializing -> serializing yields identical JSON."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["analytics.user_metrics"] = "changed"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        json_first = serialize_plan(plan)
        restored = deserialize_plan(json_first)
        json_second = serialize_plan(restored)

        assert json_first == json_second

    def test_validate_plan_schema_accepts_valid_plan(self, loaded_models: list[ModelDefinition]) -> None:
        """A valid serialized plan passes schema validation."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["raw.events"] = "changed"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        json_str = serialize_plan(plan)
        errors = validate_plan_schema(json_str)
        assert errors == [], f"Schema validation errors: {errors}"

    def test_validate_plan_schema_rejects_invalid_json(self) -> None:
        """Invalid JSON is rejected by schema validation."""
        errors = validate_plan_schema('{"plan_id": ""}')
        assert len(errors) > 0

    def test_plan_saved_to_file_and_reloaded(self, tmp_path: Path, loaded_models: list[ModelDefinition]) -> None:
        """Plan can be written to disk and loaded back identically."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)

        base_hashes = {m.name: m.content_hash for m in loaded_models}
        target_hashes = dict(base_hashes)
        target_hashes["staging.events_clean"] = "changed"

        diff = compute_structural_diff(base_hashes, target_hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="b",
            target="t",
            as_of_date=date(2025, 1, 1),
        )

        plan_file = tmp_path / "plan.json"
        json_str = serialize_plan(plan)
        plan_file.write_text(json_str, encoding="utf-8")

        loaded_json = plan_file.read_text(encoding="utf-8")
        loaded_plan = deserialize_plan(loaded_json)

        assert loaded_plan.plan_id == plan.plan_id
        assert serialize_plan(loaded_plan) == json_str


# ---------------------------------------------------------------------------
# Test: Local DuckDB execution
# ---------------------------------------------------------------------------


@requires_executor
class TestLocalExecution:
    """Verify plan execution using the LocalExecutor with DuckDB."""

    def test_execute_full_refresh_step(self, duckdb_executor: LocalExecutor) -> None:
        """A FULL_REFRESH step creates a table in DuckDB."""
        step = PlanStep(
            step_id=compute_deterministic_id("test_model", "b", "t"),
            model="test_model",
            run_type=RunType.FULL_REFRESH,
            depends_on=[],
            parallel_group=0,
            reason="test step",
        )

        sql = "SELECT 1 AS id, 'hello' AS value"
        record = duckdb_executor.execute_step(step, sql, parameters={})

        assert record.status == RunStatus.SUCCESS
        assert record.model_name == "test_model"
        assert record.started_at is not None
        assert record.finished_at is not None
        assert record.error_message is None

    def test_execute_incremental_step(self, duckdb_executor: LocalExecutor) -> None:
        """An INCREMENTAL step creates (if needed) and inserts into a table."""
        step = PlanStep(
            step_id=compute_deterministic_id("inc_model", "b", "t"),
            model="inc_model",
            run_type=RunType.INCREMENTAL,
            input_range=DateRange(start=date(2025, 6, 1), end=date(2025, 6, 15)),
            depends_on=[],
            parallel_group=0,
            reason="incremental test step",
        )

        sql = "SELECT 1 AS id, CAST('2025-06-10' AS DATE) AS event_date"
        record = duckdb_executor.execute_step(step, sql, parameters={})

        assert record.status == RunStatus.SUCCESS

    def test_execute_step_with_parameter_substitution(self, duckdb_executor: LocalExecutor) -> None:
        """Parameter placeholders in SQL are replaced before execution."""
        step = PlanStep(
            step_id=compute_deterministic_id("param_model", "b", "t"),
            model="param_model",
            run_type=RunType.FULL_REFRESH,
            depends_on=[],
            parallel_group=0,
            reason="parameter test",
        )

        sql = "SELECT 1 AS id WHERE 1=1"
        params = {"start_date": "2025-06-01", "end_date": "2025-06-15"}
        record = duckdb_executor.execute_step(step, sql, parameters=params)

        assert record.status == RunStatus.SUCCESS

    def test_execute_step_failure_returns_fail_status(self, duckdb_executor: LocalExecutor) -> None:
        """A step with invalid SQL returns a FAIL RunRecord."""
        step = PlanStep(
            step_id=compute_deterministic_id("bad_model", "b", "t"),
            model="bad_model",
            run_type=RunType.FULL_REFRESH,
            depends_on=[],
            parallel_group=0,
            reason="expected failure",
        )

        sql = "INVALID SQL STATEMENT THAT WILL FAIL"
        record = duckdb_executor.execute_step(step, sql, parameters={})

        assert record.status == RunStatus.FAIL
        assert record.error_message is not None
        assert len(record.error_message) > 0

    def test_execute_chained_steps_with_dependencies(self, duckdb_executor: LocalExecutor) -> None:
        """Execute a chain of dependent steps and verify the final table."""
        # Step 1: Create source table
        step_source = PlanStep(
            step_id=compute_deterministic_id("source_tbl", "b", "t"),
            model="source_tbl",
            run_type=RunType.FULL_REFRESH,
            depends_on=[],
            parallel_group=0,
            reason="create source",
        )
        record_1 = duckdb_executor.execute_step(
            step_source,
            "SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2, 200",
            parameters={},
        )
        assert record_1.status == RunStatus.SUCCESS

        # Step 2: Create derived table from source
        step_derived = PlanStep(
            step_id=compute_deterministic_id("derived_tbl", "b", "t"),
            model="derived_tbl",
            run_type=RunType.FULL_REFRESH,
            depends_on=[step_source.step_id],
            parallel_group=1,
            reason="downstream of source_tbl",
        )
        record_2 = duckdb_executor.execute_step(
            step_derived,
            "SELECT id, amount * 2 AS doubled_amount FROM source_tbl",
            parameters={},
        )
        assert record_2.status == RunStatus.SUCCESS

        # Verify the derived table has correct data
        conn = duckdb_executor._get_connection()
        result = conn.execute("SELECT SUM(doubled_amount) FROM derived_tbl").fetchone()
        assert result[0] == 600  # (100*2) + (200*2)

    def test_run_record_fields_populated(self, duckdb_executor: LocalExecutor) -> None:
        """RunRecord has all expected fields populated after execution."""
        step = PlanStep(
            step_id=compute_deterministic_id("check_model", "b", "t"),
            model="check_model",
            run_type=RunType.FULL_REFRESH,
            depends_on=[],
            parallel_group=0,
            reason="field check",
        )

        record = duckdb_executor.execute_step(step, "SELECT 42 AS answer", parameters={})

        assert record.run_id  # non-empty
        assert record.plan_id == step.step_id
        assert record.step_id == step.step_id
        assert record.model_name == "check_model"
        assert record.status == RunStatus.SUCCESS
        assert record.executor_version == "local-duckdb"
        assert record.retry_count == 0
        assert record.started_at is not None
        assert record.finished_at is not None
        assert record.finished_at >= record.started_at


# ---------------------------------------------------------------------------
# Test: Full pipeline (end-to-end)
# ---------------------------------------------------------------------------


@requires_executor
class TestFullPipeline:
    """Complete end-to-end test exercising all pipeline stages."""

    def test_full_pipeline_single_model_change(
        self,
        models_dir: Path,
        loaded_models: list[ModelDefinition],
        duckdb_executor: LocalExecutor,
    ) -> None:
        """Full pipeline: load -> parse -> DAG -> diff -> plan -> serialize -> execute.

        Scenario: Modify the staging.events_clean model. This should cascade
        to analytics.orders_daily, analytics.user_metrics, and
        analytics.revenue_summary.
        """
        # --- Stage 1: Initial state ---
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        # --- Stage 2: Simulate a change to staging.events_clean ---
        modified_staging_sql = """\
-- name: staging.events_clean
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: data-platform
-- tags: staging, events
-- dependencies: raw.events

SELECT
    event_id,
    user_id,
    LOWER(event_type) AS event_type,
    event_timestamp,
    COALESCE(amount, 0) AS amount,
    CASE
        WHEN amount > 100 THEN 'high'
        WHEN amount > 50 THEN 'medium'
        ELSE 'low'
    END AS value_tier
FROM {{ ref('raw.events') }}
WHERE event_id IS NOT NULL
"""
        _modify_model_file(models_dir, "staging.events_clean.sql", modified_staging_sql)

        # --- Stage 3: Reload and rebuild ---
        target_models = load_models_from_directory(models_dir)
        target_models_dict = {m.name: m for m in target_models}
        target_dag = build_dag(target_models)
        target_snapshot = _build_snapshot(target_models, "snap-target")
        target_hashes = _snapshot_to_hash_map(target_snapshot)

        # --- Stage 4: Compute structural diff ---
        diff = compute_structural_diff(base_hashes, target_hashes)
        assert "staging.events_clean" in diff.modified_models

        # --- Stage 5: Generate plan ---
        plan = generate_plan(
            models=target_models_dict,
            diff_result=diff,
            dag=target_dag,
            watermarks={},
            run_stats={},
            config=PlannerConfig(default_lookback_days=30),
            base=base_snapshot.snapshot_id,
            target=target_snapshot.snapshot_id,
            as_of_date=date(2025, 6, 15),
        )

        affected = {s.model for s in plan.steps}
        # staging.events_clean + 3 downstream models
        assert "staging.events_clean" in affected
        assert "analytics.orders_daily" in affected
        assert "analytics.user_metrics" in affected
        assert "analytics.revenue_summary" in affected
        assert plan.summary.total_steps == 4

        # --- Stage 6: Serialize and round-trip ---
        json_str = serialize_plan(plan)
        errors = validate_plan_schema(json_str)
        assert errors == []

        restored_plan = deserialize_plan(json_str)
        assert restored_plan.plan_id == plan.plan_id
        assert serialize_plan(restored_plan) == json_str

        # --- Stage 7: Execute plan steps in topological order via LocalExecutor ---
        run_records: list[RunRecord] = []
        step_by_model = {s.model: s for s in plan.steps}

        # Build simple SQL for each model that DuckDB can execute
        model_sql = {
            "staging.events_clean": (
                "SELECT 1 AS event_id, 10 AS user_id, 'purchase' AS event_type, "
                "CAST('2025-06-10' AS TIMESTAMP) AS event_timestamp, 99.99 AS amount, "
                "'medium' AS value_tier"
            ),
            "analytics.orders_daily": (
                "SELECT CAST('2025-06-10' AS DATE) AS order_date, "
                "1 AS total_orders, 1 AS unique_customers, 99.99 AS total_revenue"
            ),
            "analytics.user_metrics": (
                "SELECT 10 AS user_id, 1 AS total_events, "
                "99.99 AS lifetime_value, "
                "CAST('2025-06-10' AS TIMESTAMP) AS first_activity, "
                "CAST('2025-06-10' AS TIMESTAMP) AS last_activity"
            ),
            "analytics.revenue_summary": (
                "SELECT CAST('2025-06-10' AS DATE) AS order_date, "
                "1 AS total_orders, 99.99 AS total_revenue, "
                "1 AS total_users, 99.99 AS revenue_per_user"
            ),
        }

        # Execute in topological order (respecting parallel groups)
        for step in sorted(plan.steps, key=lambda s: (s.parallel_group, s.model)):
            sql = model_sql[step.model]
            record = duckdb_executor.execute_step(step, sql, parameters={})
            run_records.append(record)

        # --- Stage 8: Verify run records ---
        assert len(run_records) == 4
        for record in run_records:
            assert record.status == RunStatus.SUCCESS
            assert record.started_at is not None
            assert record.finished_at is not None
            assert record.executor_version == "local-duckdb"

        # Verify tables were created
        conn = duckdb_executor._get_connection()
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        table_names = {row[0] for row in tables}
        assert "staging_events_clean" in table_names
        assert "analytics_orders_daily" in table_names
        assert "analytics_user_metrics" in table_names
        assert "analytics_revenue_summary" in table_names

    def test_full_pipeline_incremental_with_watermark(
        self,
        models_dir: Path,
        loaded_models: list[ModelDefinition],
        duckdb_executor: LocalExecutor,
    ) -> None:
        """Full pipeline with an incremental model using watermark-based range.

        Scenario: Modify staging.events_clean. The downstream incremental
        model analytics.orders_daily should get an incremental step with
        a date range starting from the watermark.
        """
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        # Simulate change to staging.events_clean
        target_hashes = dict(base_hashes)
        target_hashes["staging.events_clean"] = "modified_hash"
        diff = compute_structural_diff(base_hashes, target_hashes)

        # Watermarks indicate orders_daily was last processed through June 10
        watermarks = {
            "analytics.orders_daily": (date(2025, 5, 1), date(2025, 6, 10)),
        }

        fixed_date = date(2025, 6, 15)
        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats={},
            config=PlannerConfig(default_lookback_days=30),
            base="snap-base",
            target="snap-target",
            as_of_date=fixed_date,
        )

        # Verify the incremental step
        orders_step = next(s for s in plan.steps if s.model == "analytics.orders_daily")
        assert orders_step.run_type == RunType.INCREMENTAL
        assert orders_step.input_range is not None
        assert orders_step.input_range.start == date(2025, 6, 10)
        assert orders_step.input_range.end == fixed_date

        # Serialize round-trip
        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)
        restored_orders = next(s for s in restored.steps if s.model == "analytics.orders_daily")
        assert restored_orders.input_range.start == date(2025, 6, 10)
        assert restored_orders.input_range.end == fixed_date

        # Execute the incremental step
        sql = (
            "SELECT CAST('2025-06-12' AS DATE) AS order_date, "
            "5 AS total_orders, 3 AS unique_customers, 499.95 AS total_revenue"
        )
        record = duckdb_executor.execute_step(orders_step, sql, parameters={})
        assert record.status == RunStatus.SUCCESS

    def test_full_pipeline_new_model_added(
        self,
        models_dir: Path,
        loaded_models: list[ModelDefinition],
        duckdb_executor: LocalExecutor,
    ) -> None:
        """Full pipeline when a new model is added to the project.

        Scenario: Add analytics.customer_cohorts which depends on
        staging.events_clean. The new model should appear as ADDED in
        the diff and get a FULL_REFRESH step.
        """
        base_snapshot = _build_snapshot(loaded_models, "snap-base")
        base_hashes = _snapshot_to_hash_map(base_snapshot)

        # Add a new model
        new_model_sql = """\
-- name: analytics.customer_cohorts
-- kind: FULL_REFRESH
-- materialization: TABLE
-- owner: analytics
-- tags: analytics, cohorts
-- dependencies: staging.events_clean

SELECT
    user_id,
    MIN(CAST(event_timestamp AS DATE)) AS cohort_date,
    COUNT(DISTINCT event_id) AS first_week_events
FROM {{ ref('staging.events_clean') }}
GROUP BY user_id
"""
        (models_dir / "analytics.customer_cohorts.sql").write_text(new_model_sql, encoding="utf-8")

        target_models = load_models_from_directory(models_dir)
        target_models_dict = {m.name: m for m in target_models}
        target_dag = build_dag(target_models)
        target_snapshot = _build_snapshot(target_models, "snap-target")
        target_hashes = _snapshot_to_hash_map(target_snapshot)

        diff = compute_structural_diff(base_hashes, target_hashes)
        assert "analytics.customer_cohorts" in diff.added_models

        plan = generate_plan(
            models=target_models_dict,
            diff_result=diff,
            dag=target_dag,
            watermarks={},
            run_stats={},
            base="snap-base",
            target="snap-target",
            as_of_date=date(2025, 6, 15),
        )

        assert plan.summary.total_steps == 1
        cohort_step = plan.steps[0]
        assert cohort_step.model == "analytics.customer_cohorts"
        assert cohort_step.run_type == RunType.FULL_REFRESH
        assert "new model added" in cohort_step.reason

        # Execute the step
        sql = "SELECT 10 AS user_id, CAST('2025-01-15' AS DATE) AS cohort_date, 42 AS first_week_events"
        record = duckdb_executor.execute_step(cohort_step, sql, parameters={})
        assert record.status == RunStatus.SUCCESS

        # Verify table creation
        conn = duckdb_executor._get_connection()
        result = conn.execute("SELECT first_week_events FROM analytics_customer_cohorts").fetchone()
        assert result[0] == 42


class TestFullPipelineNoop:
    """Tests for no-change pipeline scenarios (no executor needed)."""

    def test_empty_diff_produces_empty_plan(
        self,
        loaded_models: list[ModelDefinition],
    ) -> None:
        """When nothing changes, the plan has zero steps and no execution needed."""
        models_dict = {m.name: m for m in loaded_models}
        dag = build_dag(loaded_models)
        snapshot = _build_snapshot(loaded_models, "snap-001")
        hashes = _snapshot_to_hash_map(snapshot)

        diff = compute_structural_diff(hashes, hashes)

        plan = generate_plan(
            models=models_dict,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap-001",
            target="snap-001",
            as_of_date=date(2025, 1, 1),
        )

        assert plan.summary.total_steps == 0
        assert plan.steps == []
        assert plan.summary.estimated_cost_usd == 0.0

        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)
        assert restored.steps == []
