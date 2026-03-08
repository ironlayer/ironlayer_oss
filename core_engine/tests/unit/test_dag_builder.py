"""Unit tests for core_engine.graph.dag_builder."""

from __future__ import annotations

import pytest

from core_engine.graph.dag_builder import (
    CyclicDependencyError,
    assign_parallel_groups,
    build_dag,
    detect_cycles,
    get_downstream,
    get_upstream,
    topological_sort,
    validate_dag,
)
from core_engine.models.model_definition import (
    ModelDefinition,
    ModelKind,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(
    name: str,
    referenced_tables: list[str] | None = None,
    dependencies: list[str] | None = None,
) -> ModelDefinition:
    """Create a minimal ModelDefinition for DAG tests."""
    return ModelDefinition(
        name=name,
        kind=ModelKind.FULL_REFRESH,
        file_path=f"models/{name}.sql",
        raw_sql=f"SELECT 1",
        referenced_tables=referenced_tables or [],
        dependencies=dependencies or [],
    )


# ---------------------------------------------------------------------------
# build_dag
# ---------------------------------------------------------------------------


class TestBuildDag:
    def test_linear_chain(self):
        """A -> B -> C."""
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["B"]),
        ]
        dag = build_dag(models)
        assert dag.has_node("A")
        assert dag.has_node("B")
        assert dag.has_node("C")
        assert dag.has_edge("A", "B")
        assert dag.has_edge("B", "C")
        assert not dag.has_edge("A", "C")

    def test_diamond_dependency(self):
        """
        A -> B -> D
        A -> C -> D
        """
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
            _model("D", referenced_tables=["B", "C"]),
        ]
        dag = build_dag(models)
        assert dag.has_edge("A", "B")
        assert dag.has_edge("A", "C")
        assert dag.has_edge("B", "D")
        assert dag.has_edge("C", "D")

    def test_fan_out(self):
        """A -> B, A -> C, A -> D."""
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
            _model("D", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        assert list(dag.successors("A")) == sorted(["B", "C", "D"])

    def test_isolated_nodes(self):
        models = [
            _model("A"),
            _model("B"),
            _model("C"),
        ]
        dag = build_dag(models)
        assert dag.number_of_nodes() == 3
        assert dag.number_of_edges() == 0

    def test_external_table_not_added_as_edge(self):
        models = [
            _model("A", referenced_tables=["external_source"]),
        ]
        dag = build_dag(models)
        assert not dag.has_node("external_source")
        assert dag.number_of_edges() == 0

    def test_self_reference_ignored(self):
        models = [
            _model("A", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        assert not dag.has_edge("A", "A")

    def test_explicit_dependencies_create_edges(self):
        models = [
            _model("A"),
            _model("B", dependencies=["A"]),
        ]
        dag = build_dag(models)
        assert dag.has_edge("A", "B")

    def test_node_data_contains_model(self):
        models = [_model("A")]
        dag = build_dag(models)
        assert dag.nodes["A"]["model"].name == "A"


# ---------------------------------------------------------------------------
# topological_sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_linear_chain(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["B"]),
        ]
        dag = build_dag(models)
        order = topological_sort(dag)
        assert order.index("A") < order.index("B") < order.index("C")

    def test_deterministic_lexicographic(self):
        models = [
            _model("C"),
            _model("B"),
            _model("A"),
        ]
        dag = build_dag(models)
        order = topological_sort(dag)
        # No ordering constraints, so should be lexicographic.
        assert order == ["A", "B", "C"]

    def test_diamond(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
            _model("D", referenced_tables=["B", "C"]),
        ]
        dag = build_dag(models)
        order = topological_sort(dag)
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("D")
        assert order.index("C") < order.index("D")

    def test_cycle_raises_cyclic_dependency_error(self):
        import networkx as nx

        dag = nx.DiGraph()
        dag.add_edge("A", "B")
        dag.add_edge("B", "A")
        with pytest.raises(CyclicDependencyError):
            topological_sort(dag)


# ---------------------------------------------------------------------------
# get_downstream
# ---------------------------------------------------------------------------


class TestGetDownstream:
    def test_direct_children(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        downstream = get_downstream(dag, "A")
        assert downstream == {"B", "C"}

    def test_transitive_closure(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["B"]),
        ]
        dag = build_dag(models)
        downstream = get_downstream(dag, "A")
        assert downstream == {"B", "C"}

    def test_leaf_node(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        downstream = get_downstream(dag, "B")
        assert downstream == set()

    def test_nonexistent_node(self):
        models = [_model("A")]
        dag = build_dag(models)
        assert get_downstream(dag, "Z") == set()


# ---------------------------------------------------------------------------
# get_upstream
# ---------------------------------------------------------------------------


class TestGetUpstream:
    def test_direct_parents(self):
        models = [
            _model("A"),
            _model("B"),
            _model("C", referenced_tables=["A", "B"]),
        ]
        dag = build_dag(models)
        upstream = get_upstream(dag, "C")
        assert upstream == {"A", "B"}

    def test_transitive_ancestors(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["B"]),
        ]
        dag = build_dag(models)
        upstream = get_upstream(dag, "C")
        assert upstream == {"A", "B"}

    def test_root_node(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        upstream = get_upstream(dag, "A")
        assert upstream == set()

    def test_nonexistent_node(self):
        models = [_model("A")]
        dag = build_dag(models)
        assert get_upstream(dag, "Z") == set()


# ---------------------------------------------------------------------------
# assign_parallel_groups
# ---------------------------------------------------------------------------


class TestAssignParallelGroups:
    def test_linear_each_in_own_group(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["B"]),
        ]
        dag = build_dag(models)
        groups = assign_parallel_groups(dag, ["A", "B", "C"])
        # Each should be in a different group.
        assert groups["A"] != groups["B"]
        assert groups["B"] != groups["C"]
        # A is first (group 1), B is second (group 2), C is third (group 3).
        assert groups["A"] == 1
        assert groups["B"] == 2
        assert groups["C"] == 3

    def test_diamond_proper_grouping(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
            _model("D", referenced_tables=["B", "C"]),
        ]
        dag = build_dag(models)
        groups = assign_parallel_groups(dag, ["A", "B", "C", "D"])
        assert groups["A"] == 1
        assert groups["B"] == groups["C"]  # Same group - can run in parallel.
        assert groups["B"] == 2
        assert groups["D"] == 3

    def test_subset_of_models(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
            _model("C", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        # Only run B and C (not A).
        groups = assign_parallel_groups(dag, ["B", "C"])
        # B and C have no mutual dependency in the subgraph.
        assert groups["B"] == groups["C"]

    def test_empty_input(self):
        models = [_model("A")]
        dag = build_dag(models)
        groups = assign_parallel_groups(dag, [])
        assert groups == {}

    def test_isolated_nodes_same_group(self):
        models = [
            _model("A"),
            _model("B"),
            _model("C"),
        ]
        dag = build_dag(models)
        groups = assign_parallel_groups(dag, ["A", "B", "C"])
        assert groups["A"] == groups["B"] == groups["C"] == 1


# ---------------------------------------------------------------------------
# detect_cycles
# ---------------------------------------------------------------------------


class TestDetectCycles:
    def test_no_cycles_returns_empty(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        # detect_cycles raises if there are cycles, so no raise = no cycles.
        # Since the dag is acyclic, it returns empty.
        result = detect_cycles(dag)
        assert result == []

    def test_simple_cycle_raises(self):
        import networkx as nx

        dag = nx.DiGraph()
        dag.add_edge("A", "B")
        dag.add_edge("B", "A")
        with pytest.raises(CyclicDependencyError) as exc_info:
            detect_cycles(dag)
        assert len(exc_info.value.cycles) > 0

    def test_complex_cycle_raises(self):
        import networkx as nx

        dag = nx.DiGraph()
        dag.add_edge("A", "B")
        dag.add_edge("B", "C")
        dag.add_edge("C", "A")
        with pytest.raises(CyclicDependencyError) as exc_info:
            detect_cycles(dag)
        assert len(exc_info.value.cycles) > 0


# ---------------------------------------------------------------------------
# validate_dag
# ---------------------------------------------------------------------------


class TestValidateDag:
    def test_all_refs_known_no_warnings(self):
        models = [
            _model("A"),
            _model("B", referenced_tables=["A"]),
        ]
        dag = build_dag(models)
        warnings = validate_dag(dag, known_models={"A", "B"})
        assert warnings == []

    def test_unknown_external_ref_produces_warning(self):
        models = [
            _model("A", referenced_tables=["external_source"]),
        ]
        dag = build_dag(models)
        warnings = validate_dag(dag, known_models={"A"})
        assert len(warnings) == 1
        assert "external_source" in warnings[0]

    def test_external_ref_not_in_dag_or_known_models(self):
        models = [
            _model("A", referenced_tables=["external_table"]),
            _model("B", dependencies=["unknown_dep"]),
        ]
        dag = build_dag(models)
        warnings = validate_dag(dag, known_models={"A", "B"})
        assert len(warnings) == 2

    def test_empty_dag_no_warnings(self):
        import networkx as nx

        dag = nx.DiGraph()
        warnings = validate_dag(dag, known_models=set())
        assert warnings == []


# ---------------------------------------------------------------------------
# CyclicDependencyError
# ---------------------------------------------------------------------------


class TestCyclicDependencyError:
    def test_error_message_format(self):
        exc = CyclicDependencyError([["A", "B", "C"]])
        assert "A -> B -> C -> A" in str(exc)

    def test_cycles_attribute(self):
        cycles = [["A", "B"], ["X", "Y", "Z"]]
        exc = CyclicDependencyError(cycles)
        assert exc.cycles == cycles
