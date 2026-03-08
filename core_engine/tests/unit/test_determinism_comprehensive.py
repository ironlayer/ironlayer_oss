"""Determinism and hash stability tests.

Validates that:
- The planner requires explicit as_of_date for deterministic planning.
- Same inputs always produce identical plans (byte-identical JSON).
- Different as_of_date produces different plans for incremental models.
- The normalizer raises NormalizationError instead of returning raw SQL.
- Snapshot hash computation includes tenant_id for multi-tenant isolation.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import networkx as nx
import pytest

from core_engine.graph.dag_builder import build_dag
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import (
    ModelDefinition,
    ModelKind,
)
from core_engine.models.plan import Plan, RunType, compute_deterministic_id
from core_engine.parser.normalizer import (
    NormalizationError,
    compute_canonical_hash,
    normalize_sql,
)
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
        raw_sql="SELECT 1",
        referenced_tables=referenced_tables or [],
        dependencies=dependencies or [],
        time_column=time_column,
        unique_key=unique_key,
    )


def _models_dict(*defs: ModelDefinition) -> dict[str, ModelDefinition]:
    return {m.name: m for m in defs}


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
# Planner determinism
# ---------------------------------------------------------------------------


class TestPlannerDeterminism:
    """Verify planner requires explicit as_of_date and produces deterministic output."""

    def test_missing_as_of_date_raises(self) -> None:
        """Calling generate_plan without as_of_date raises ValueError."""
        m = _model("A")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        with pytest.raises(ValueError, match="as_of_date is required"):
            generate_plan(
                models=models,
                diff_result=diff,
                dag=dag,
                watermarks={},
                run_stats={},
                base="snap1",
                target="snap2",
                as_of_date=None,
            )

    def test_same_inputs_produce_same_plan_twice(self) -> None:
        """Identical inputs must produce byte-identical plan JSON."""
        a = _model("A")
        b = _model("B", referenced_tables=["A"])
        c = _model("C", referenced_tables=["B"])
        models = _models_dict(a, b, c)
        dag = build_dag([a, b, c])
        diff = _diff(modified=["A"])

        kwargs = dict(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap-base",
            target="snap-target",
            as_of_date=AS_OF,
        )

        plan1 = generate_plan(**kwargs)
        plan2 = generate_plan(**kwargs)

        # Plan IDs are content-derived -- must match exactly
        assert plan1.plan_id == plan2.plan_id

        # Every step must match
        assert len(plan1.steps) == len(plan2.steps)
        for s1, s2 in zip(plan1.steps, plan2.steps):
            assert s1.step_id == s2.step_id
            assert s1.model == s2.model
            assert s1.run_type == s2.run_type
            assert s1.depends_on == s2.depends_on
            assert s1.parallel_group == s2.parallel_group
            assert s1.reason == s2.reason

        # JSON serialisation must be byte-identical
        json1 = plan1.model_dump_json(indent=None)
        json2 = plan2.model_dump_json(indent=None)
        assert json1 == json2

    def test_different_as_of_date_produces_different_plan_for_incremental(self) -> None:
        """Different as_of_date changes incremental ranges, producing a different plan."""
        m = _model("A", kind=ModelKind.INCREMENTAL_BY_TIME_RANGE, time_column="ts")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])

        plan_june = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=date(2025, 6, 1),
        )
        plan_july = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=date(2025, 7, 1),
        )

        # Both should have incremental steps with different date ranges
        assert plan_june.steps[0].input_range is not None
        assert plan_july.steps[0].input_range is not None
        assert plan_june.steps[0].input_range.end != plan_july.steps[0].input_range.end

    def test_same_as_of_date_same_incremental_range(self) -> None:
        """Same as_of_date produces identical incremental ranges."""
        m = _model("A", kind=ModelKind.INCREMENTAL_BY_TIME_RANGE, time_column="ts")
        models = _models_dict(m)
        dag = build_dag([m])
        diff = _diff(modified=["A"])
        watermarks = {"A": (date(2025, 5, 1), date(2025, 5, 15))}

        kwargs = dict(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks=watermarks,
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
        )

        plan1 = generate_plan(**kwargs)
        plan2 = generate_plan(**kwargs)

        assert plan1.steps[0].input_range == plan2.steps[0].input_range
        assert plan1.steps[0].input_range.start == date(2025, 5, 15)
        assert plan1.steps[0].input_range.end == AS_OF

    def test_step_ordering_is_alphabetical(self) -> None:
        """Steps are always sorted alphabetically by model name regardless of insertion order."""
        z = _model("Z_model")
        a = _model("A_model")
        m_mid = _model("M_model")
        models = _models_dict(z, a, m_mid)
        dag = build_dag([z, a, m_mid])
        diff = _diff(modified=["Z_model", "A_model", "M_model"])

        plan = generate_plan(
            models=models,
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )

        model_names = [s.model for s in plan.steps]
        assert model_names == sorted(model_names)
        assert model_names == ["A_model", "M_model", "Z_model"]

    def test_plan_id_is_sha256_hex(self) -> None:
        """Plan ID must be a 64-character hex SHA-256 digest."""
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

        assert len(plan.plan_id) == 64
        assert all(c in "0123456789abcdef" for c in plan.plan_id)


# ---------------------------------------------------------------------------
# compute_deterministic_id
# ---------------------------------------------------------------------------


class TestComputeDeterministicId:
    """Verify the deterministic ID helper used for plan/step/test IDs."""

    def test_same_parts_same_id(self) -> None:
        """Identical input parts produce identical IDs."""
        id1 = compute_deterministic_id("model_a", "FULL_REFRESH", "col1")
        id2 = compute_deterministic_id("model_a", "FULL_REFRESH", "col1")
        assert id1 == id2

    def test_different_parts_different_id(self) -> None:
        """Different input parts produce different IDs."""
        id1 = compute_deterministic_id("model_a", "FULL_REFRESH")
        id2 = compute_deterministic_id("model_b", "FULL_REFRESH")
        assert id1 != id2

    def test_order_matters(self) -> None:
        """Part ordering affects the hash (domain separator prevents collisions)."""
        id1 = compute_deterministic_id("a", "b")
        id2 = compute_deterministic_id("b", "a")
        assert id1 != id2

    def test_result_is_hex_sha256(self) -> None:
        """Output is a 64-character hex string."""
        result = compute_deterministic_id("test")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_null_byte_separator_prevents_collisions(self) -> None:
        """Concatenation of 'ab' + '' differs from 'a' + 'b' due to null byte separator."""
        id1 = compute_deterministic_id("ab", "")
        id2 = compute_deterministic_id("a", "b")
        assert id1 != id2


# ---------------------------------------------------------------------------
# Normalizer hard fail
# ---------------------------------------------------------------------------


class TestNormalizerHardFail:
    """Verify normalizer raises NormalizationError instead of returning raw SQL."""

    def test_valid_sql_normalizes_correctly(self) -> None:
        """Valid SQL is normalized without errors."""
        result = normalize_sql("SELECT id, name FROM users WHERE active = 1")
        assert result  # Non-empty
        assert "SELECT" in result.upper()

    def test_completely_invalid_sql_raises_normalization_error(self) -> None:
        """Completely invalid SQL raises NormalizationError, not a generic exception."""
        with pytest.raises(NormalizationError, match="Failed to canonicalize"):
            normalize_sql("THIS IS NOT SQL AT ALL @@@ $$$ %%% !!!")

    def test_partial_sql_raises_normalization_error(self) -> None:
        """Partial/truncated SQL that cannot be parsed raises NormalizationError."""
        with pytest.raises(NormalizationError, match="Failed to canonicalize"):
            normalize_sql("SELECT FROM WHERE GROUP")

    def test_empty_string_returns_empty(self) -> None:
        """Empty input returns empty string (not an error)."""
        assert normalize_sql("") == ""

    def test_comment_only_returns_empty(self) -> None:
        """Comment-only SQL returns empty string."""
        assert normalize_sql("-- just a comment\n/* block comment */") == ""

    def test_normalization_is_idempotent(self) -> None:
        """Normalizing an already-normalized query produces the same result."""
        sql = "SELECT id, name FROM users WHERE id > 10"
        first = normalize_sql(sql)
        second = normalize_sql(first)
        assert first == second

    def test_whitespace_differences_normalize_to_same(self) -> None:
        """Different whitespace formatting produces identical canonical SQL."""
        sql_a = "SELECT   id,\n   name\n  FROM   users   WHERE  id=1"
        sql_b = "SELECT id,name FROM users WHERE id=1"
        assert normalize_sql(sql_a) == normalize_sql(sql_b)

    def test_canonical_hash_changes_with_sql(self) -> None:
        """Different SQL produces different canonical hashes."""
        h1 = compute_canonical_hash("SELECT id FROM users")
        h2 = compute_canonical_hash("SELECT name FROM users")
        assert h1 != h2

    def test_canonical_hash_stable(self) -> None:
        """Same SQL always produces the same hash."""
        sql = "SELECT count(*) FROM orders WHERE status = 'active'"
        h1 = compute_canonical_hash(sql)
        h2 = compute_canonical_hash(sql)
        assert h1 == h2


# ---------------------------------------------------------------------------
# Snapshot hash includes tenant_id
# ---------------------------------------------------------------------------


class TestSnapshotHash:
    """Verify snapshot hash computation includes tenant_id for multi-tenant isolation.

    The hash is computed in SnapshotRepository.create_snapshot by combining
    tenant_id, environment, and sorted model versions. We replicate that
    logic here to verify the tenant isolation property.
    """

    @staticmethod
    def _compute_snapshot_hash(tenant_id: str, environment: str, model_versions: dict[str, str]) -> str:
        """Replicate the snapshot ID computation from SnapshotRepository.create_snapshot."""
        hasher = hashlib.sha256()
        hasher.update(tenant_id.encode("utf-8"))
        hasher.update(environment.encode("utf-8"))
        for name in sorted(model_versions):
            hasher.update(name.encode("utf-8"))
            hasher.update(model_versions[name].encode("utf-8"))
        return hasher.hexdigest()

    def test_same_data_different_tenant_produces_different_hash(self) -> None:
        """Identical model versions with different tenants produce different snapshot IDs."""
        model_versions = {"orders": "hash_abc", "users": "hash_def"}
        env = "production"

        hash_tenant_a = self._compute_snapshot_hash("tenant_a", env, model_versions)
        hash_tenant_b = self._compute_snapshot_hash("tenant_b", env, model_versions)

        assert hash_tenant_a != hash_tenant_b

    def test_same_data_same_tenant_produces_same_hash(self) -> None:
        """Identical inputs produce the same snapshot ID deterministically."""
        model_versions = {"orders": "hash_abc", "users": "hash_def"}
        env = "production"
        tenant = "tenant_x"

        hash1 = self._compute_snapshot_hash(tenant, env, model_versions)
        hash2 = self._compute_snapshot_hash(tenant, env, model_versions)

        assert hash1 == hash2

    def test_different_environment_produces_different_hash(self) -> None:
        """Same tenant and models in different environments produce different hashes."""
        model_versions = {"orders": "hash_abc"}
        tenant = "tenant_x"

        hash_prod = self._compute_snapshot_hash(tenant, "production", model_versions)
        hash_staging = self._compute_snapshot_hash(tenant, "staging", model_versions)

        assert hash_prod != hash_staging

    def test_model_version_order_does_not_matter(self) -> None:
        """Model versions are sorted before hashing, so dict order is irrelevant."""
        tenant = "tenant_x"
        env = "production"

        # Insert in different orders
        versions_a = {"b_model": "hash_b", "a_model": "hash_a"}
        versions_b = {"a_model": "hash_a", "b_model": "hash_b"}

        hash_a = self._compute_snapshot_hash(tenant, env, versions_a)
        hash_b = self._compute_snapshot_hash(tenant, env, versions_b)

        assert hash_a == hash_b

    def test_hash_is_sha256(self) -> None:
        """Snapshot hash is a 64-character hex SHA-256 digest."""
        h = self._compute_snapshot_hash("t", "e", {"m": "v"})
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_empty_models_still_includes_tenant(self) -> None:
        """Even with no model versions, tenant_id influences the hash."""
        hash_a = self._compute_snapshot_hash("tenant_a", "prod", {})
        hash_b = self._compute_snapshot_hash("tenant_b", "prod", {})
        assert hash_a != hash_b

    def test_additional_model_changes_hash(self) -> None:
        """Adding a model to the versions dict changes the snapshot hash."""
        tenant = "tenant_x"
        env = "production"

        hash_one = self._compute_snapshot_hash(tenant, env, {"orders": "v1"})
        hash_two = self._compute_snapshot_hash(tenant, env, {"orders": "v1", "users": "v2"})

        assert hash_one != hash_two
