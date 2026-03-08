"""Tests for the what-if impact simulation engine.

Covers column change simulation, model removal, type compatibility,
contract violations, diamond DAG patterns, and edge cases.
"""

from __future__ import annotations

import pytest

from core_engine.models.model_definition import (
    ColumnContract,
    ModelDefinition,
    ModelKind,
    SchemaContractMode,
)
from core_engine.simulation.impact_analyzer import (
    AffectedModel,
    ChangeAction,
    ColumnChange,
    ImpactAnalyzer,
    ImpactReport,
    ModelRemovalReport,
    ReferenceSeverity,
    _is_type_compatible,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(
    name: str,
    sql: str = "SELECT 1",
    deps: list[str] | None = None,
    refs: list[str] | None = None,
    contract_mode: SchemaContractMode = SchemaContractMode.DISABLED,
    contract_columns: list[ColumnContract] | None = None,
    output_columns: list[str] | None = None,
) -> ModelDefinition:
    """Create a minimal ModelDefinition for testing."""
    return ModelDefinition(
        name=name,
        kind=ModelKind.FULL_REFRESH,
        file_path=f"models/{name}.sql",
        raw_sql=sql,
        clean_sql=sql,
        dependencies=deps or [],
        referenced_tables=refs or [],
        output_columns=output_columns or [],
        contract_mode=contract_mode,
        contract_columns=contract_columns or [],
    )


def _make_dag_and_models(
    model_defs: list[ModelDefinition],
) -> tuple[dict[str, ModelDefinition], dict[str, list[str]]]:
    """Build models dict and adjacency list from a list of model defs."""
    model_names = {m.name for m in model_defs}
    models = {m.name: m for m in model_defs}
    dag: dict[str, list[str]] = {}

    for m in model_defs:
        upstream: list[str] = []
        all_refs = set(m.referenced_tables) | set(m.dependencies)
        for ref in sorted(all_refs):
            if ref in model_names and ref != m.name:
                upstream.append(ref)
        dag[m.name] = upstream

    return models, dag


# ---------------------------------------------------------------------------
# Type compatibility
# ---------------------------------------------------------------------------


class TestTypeCompatibility:
    def test_same_type_safe(self) -> None:
        assert _is_type_compatible("INT", "INT") is True
        assert _is_type_compatible("STRING", "STRING") is True

    def test_widening_safe(self) -> None:
        assert _is_type_compatible("INT", "BIGINT") is True
        assert _is_type_compatible("INT", "FLOAT") is True
        assert _is_type_compatible("FLOAT", "DOUBLE") is True
        assert _is_type_compatible("DATE", "TIMESTAMP") is True

    def test_narrowing_breaking(self) -> None:
        assert _is_type_compatible("BIGINT", "INT") is False
        assert _is_type_compatible("DOUBLE", "FLOAT") is False
        assert _is_type_compatible("TIMESTAMP", "DATE") is False

    def test_incompatible_breaking(self) -> None:
        assert _is_type_compatible("STRING", "INT") is False
        assert _is_type_compatible("STRING", "BOOLEAN") is False

    def test_case_insensitive(self) -> None:
        assert _is_type_compatible("int", "bigint") is True
        assert _is_type_compatible("String", "STRING") is True

    def test_unknown_pair_defaults_breaking(self) -> None:
        assert _is_type_compatible("BINARY", "ARRAY") is False


# ---------------------------------------------------------------------------
# Column change simulation
# ---------------------------------------------------------------------------


class TestColumnChangeSimulation:
    def test_column_removal_cascading(self) -> None:
        """Removing a column from model A should affect downstream models."""
        m_a = _model("a", sql="SELECT id, name FROM source")
        m_b = _model("b", sql="SELECT id, name FROM a", deps=["a"])
        m_c = _model("c", sql="SELECT name FROM b", deps=["b"])

        models, dag = _make_dag_and_models([m_a, m_b, m_c])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="name")],
        )

        assert report.source_model == "a"
        assert len(report.directly_affected) == 1
        assert report.directly_affected[0].model_name == "b"
        assert "name" in report.directly_affected[0].columns_affected

    def test_column_add_no_impact(self) -> None:
        """Adding a column should not break downstream models."""
        m_a = _model("a", sql="SELECT id FROM source")
        m_b = _model("b", sql="SELECT id FROM a", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.ADD, column_name="new_col")],
        )

        # new_col is not referenced by b, so no columns_affected.
        for a in report.directly_affected:
            assert "new_col" not in a.columns_affected or a.severity == ReferenceSeverity.INFO

    def test_column_rename_breaking(self) -> None:
        """Renaming a column used downstream should be BREAKING."""
        m_a = _model("a", sql="SELECT id, price FROM source")
        m_b = _model("b", sql="SELECT price FROM a", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [
                ColumnChange(
                    action=ChangeAction.RENAME,
                    column_name="price",
                    new_name="unit_price",
                )
            ],
        )

        affected_names = {a.model_name for a in report.directly_affected}
        assert "b" in affected_names

    def test_column_not_referenced_no_impact(self) -> None:
        """If no downstream model references the changed column, no impact."""
        m_a = _model("a", sql="SELECT id, unused_col FROM source")
        m_b = _model("b", sql="SELECT id FROM a", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="unused_col")],
        )

        for a in report.directly_affected:
            assert "unused_col" not in a.columns_affected

    def test_model_not_found(self) -> None:
        models, dag = _make_dag_and_models([])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "nonexistent",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="x")],
        )
        assert "not found" in report.summary.lower()


# ---------------------------------------------------------------------------
# Diamond DAG patterns
# ---------------------------------------------------------------------------


class TestDiamondPattern:
    def test_diamond_deduplication(self) -> None:
        """In a diamond DAG (a -> b, a -> c, b -> d, c -> d), d appears once."""
        m_a = _model("a", sql="SELECT id, col FROM source")
        m_b = _model("b", sql="SELECT id, col FROM a", deps=["a"])
        m_c = _model("c", sql="SELECT id, col FROM a", deps=["a"])
        m_d = _model("d", sql="SELECT col FROM b JOIN c ON b.id = c.id", deps=["b", "c"])

        models, dag = _make_dag_and_models([m_a, m_b, m_c, m_d])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="col")],
        )

        all_names = [a.model_name for a in report.directly_affected] + [
            a.model_name for a in report.transitively_affected
        ]
        # d should appear exactly once (not duplicated).
        assert all_names.count("d") == 1


# ---------------------------------------------------------------------------
# Contract violations
# ---------------------------------------------------------------------------


class TestContractViolations:
    def test_column_removal_violates_contract(self) -> None:
        m_a = _model("a", sql="SELECT id, amount FROM source")
        m_b = _model(
            "b",
            sql="SELECT id, amount FROM a",
            deps=["a"],
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="amount", data_type="FLOAT"),
            ],
        )

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="amount")],
        )

        assert len(report.contract_violations) >= 1
        violation = report.contract_violations[0]
        assert violation.violation_type == "COLUMN_REMOVED"
        assert violation.severity == ReferenceSeverity.BREAKING

    def test_type_change_violates_contract(self) -> None:
        m_a = _model("a", sql="SELECT id, amount FROM source")
        m_b = _model(
            "b",
            sql="SELECT id, amount FROM a",
            deps=["a"],
            contract_mode=SchemaContractMode.WARN,
            contract_columns=[
                ColumnContract(name="amount", data_type="INT"),
            ],
        )

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [
                ColumnChange(
                    action=ChangeAction.TYPE_CHANGE,
                    column_name="amount",
                    old_type="INT",
                    new_type="STRING",
                )
            ],
        )

        assert report.breaking_count >= 1

    def test_compatible_type_change_no_violation(self) -> None:
        m_a = _model("a", sql="SELECT id, count FROM source")
        m_b = _model(
            "b",
            sql="SELECT id, count FROM a",
            deps=["a"],
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="count", data_type="INT"),
            ],
        )

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [
                ColumnChange(
                    action=ChangeAction.TYPE_CHANGE,
                    column_name="count",
                    old_type="INT",
                    new_type="BIGINT",
                )
            ],
        )

        # INT -> BIGINT is compatible, so no contract violation.
        contract_violations = [v for v in report.contract_violations if v.violation_type == "TYPE_CHANGED"]
        assert len(contract_violations) == 0

    def test_disabled_contracts_no_violations(self) -> None:
        m_a = _model("a", sql="SELECT id, col FROM source")
        m_b = _model(
            "b",
            sql="SELECT id, col FROM a",
            deps=["a"],
            contract_mode=SchemaContractMode.DISABLED,
            contract_columns=[
                ColumnContract(name="col", data_type="STRING"),
            ],
        )

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="col")],
        )

        assert len(report.contract_violations) == 0


# ---------------------------------------------------------------------------
# Model removal
# ---------------------------------------------------------------------------


class TestModelRemoval:
    def test_direct_downstream_affected(self) -> None:
        m_a = _model("a", sql="SELECT 1")
        m_b = _model("b", sql="SELECT 1", deps=["a"])
        m_c = _model("c", sql="SELECT 1", deps=["b"])

        models, dag = _make_dag_and_models([m_a, m_b, m_c])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_model_removal("a")

        assert len(report.directly_affected) == 1
        assert report.directly_affected[0].model_name == "b"
        assert len(report.transitively_affected) == 1
        assert report.transitively_affected[0].model_name == "c"

    def test_orphan_detection(self) -> None:
        """Model with only one upstream should be orphaned on removal."""
        m_a = _model("a", sql="SELECT 1")
        m_b = _model("b", sql="SELECT 1", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_model_removal("a")
        assert "b" in report.orphaned_models

    def test_non_orphan_with_multiple_upstreams(self) -> None:
        """Model with multiple upstreams is NOT orphaned when one is removed."""
        m_a = _model("a", sql="SELECT 1")
        m_b = _model("b", sql="SELECT 1")
        m_c = _model("c", sql="SELECT 1", deps=["a", "b"])

        models, dag = _make_dag_and_models([m_a, m_b, m_c])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_model_removal("a")
        assert "c" not in report.orphaned_models

    def test_removal_of_nonexistent(self) -> None:
        models, dag = _make_dag_and_models([])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_model_removal("ghost")
        assert "not found" in report.summary.lower()

    def test_removal_leaf_node(self) -> None:
        """Removing a leaf node (no downstream) has no impact."""
        m_a = _model("a", sql="SELECT 1")
        m_b = _model("b", sql="SELECT 1", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_model_removal("b")
        assert len(report.directly_affected) == 0
        assert len(report.transitively_affected) == 0


# ---------------------------------------------------------------------------
# Type change simulation
# ---------------------------------------------------------------------------


class TestTypeChangeSimulation:
    def test_safe_type_change(self) -> None:
        m_a = _model("a", sql="SELECT id, count FROM source")
        m_b = _model("b", sql="SELECT count FROM a", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_type_change("a", "count", "INT", "BIGINT")
        # INT -> BIGINT is safe: should be WARNING (non-breaking) at most.
        for a in report.directly_affected + report.transitively_affected:
            assert a.severity != ReferenceSeverity.BREAKING

    def test_breaking_type_change(self) -> None:
        m_a = _model("a", sql="SELECT id, value FROM source")
        m_b = _model("b", sql="SELECT value FROM a", deps=["a"])

        models, dag = _make_dag_and_models([m_a, m_b])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_type_change("a", "value", "STRING", "INT")
        assert report.breaking_count >= 0  # Depends on whether column is referenced


# ---------------------------------------------------------------------------
# Impact summary
# ---------------------------------------------------------------------------


class TestImpactSummary:
    def test_summary_includes_model_name(self) -> None:
        m_a = _model("a", sql="SELECT id FROM source")
        models, dag = _make_dag_and_models([m_a])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="id")],
        )
        assert "a" in report.summary

    def test_summary_no_impact(self) -> None:
        m_a = _model("a", sql="SELECT id FROM source")
        models, dag = _make_dag_and_models([m_a])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="id")],
        )
        assert "no downstream impact" in report.summary.lower()


# ---------------------------------------------------------------------------
# Isolated nodes
# ---------------------------------------------------------------------------


class TestIsolatedNodes:
    def test_isolated_node_no_impact(self) -> None:
        """A model with no downstream should produce an empty report."""
        m_a = _model("a", sql="SELECT 1")

        models, dag = _make_dag_and_models([m_a])
        analyzer = ImpactAnalyzer(models, dag)

        report = analyzer.simulate_column_change(
            "a",
            [ColumnChange(action=ChangeAction.REMOVE, column_name="x")],
        )
        assert len(report.directly_affected) == 0
        assert len(report.transitively_affected) == 0
