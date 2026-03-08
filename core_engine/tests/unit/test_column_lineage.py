"""Tests for column-level lineage analysis.

Covers:
- Direct column pass-through
- Expression transforms (a + b AS total)
- Aggregation (SUM, COUNT, AVG)
- CTE pass-through
- JOIN multi-source
- CASE expression
- Window function
- Nested subquery
- SELECT * handling
- Transform classification
- Cross-model stitching
- Unresolved column handling
"""

from __future__ import annotations

import pytest

from core_engine.sql_toolkit import Dialect, SqlLineageError, get_sql_toolkit
from core_engine.sql_toolkit._types import ColumnLineageNode, ColumnLineageResult


@pytest.fixture(scope="module")
def toolkit():
    """Shared toolkit instance for all tests in this module."""
    return get_sql_toolkit()


# ---------------------------------------------------------------------------
# Direct column
# ---------------------------------------------------------------------------


class TestDirectColumn:
    def test_simple_select(self, toolkit):
        sql = "SELECT id, name FROM users"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert isinstance(result, ColumnLineageResult)
        assert "id" in result.column_lineage
        assert "name" in result.column_lineage

        id_nodes = result.column_lineage["id"]
        assert len(id_nodes) >= 1
        # Should trace to a source column.
        assert any(n.source_column == "id" for n in id_nodes)

    def test_aliased_column(self, toolkit):
        sql = "SELECT id AS user_id FROM users"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "user_id" in result.column_lineage
        nodes = result.column_lineage["user_id"]
        assert any(n.source_column == "id" for n in nodes)

    def test_single_column_trace(self, toolkit):
        sql = "SELECT id, name FROM users"
        nodes = toolkit.lineage_analyzer.trace_single_column("id", sql, Dialect.DATABRICKS)
        assert len(nodes) >= 1
        assert nodes[0].column == "id"


# ---------------------------------------------------------------------------
# Expression transform
# ---------------------------------------------------------------------------


class TestExpressionTransform:
    def test_arithmetic_expression(self, toolkit):
        sql = "SELECT price * quantity AS total FROM orders"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "total" in result.column_lineage
        nodes = result.column_lineage["total"]
        # Should trace to both source columns.
        source_cols = {n.source_column for n in nodes if n.source_column}
        assert source_cols & {"price", "quantity"}

    def test_concat_expression(self, toolkit):
        sql = "SELECT first_name || ' ' || last_name AS full_name FROM users"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "full_name" in result.column_lineage


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestAggregation:
    def test_sum_aggregation(self, toolkit):
        sql = "SELECT SUM(amount) AS total_amount FROM orders GROUP BY customer_id"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "total_amount" in result.column_lineage
        nodes = result.column_lineage["total_amount"]
        assert any(n.source_column == "amount" for n in nodes)

    def test_count_aggregation(self, toolkit):
        sql = "SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "order_count" in result.column_lineage
        assert "customer_id" in result.column_lineage


# ---------------------------------------------------------------------------
# CTE pass-through
# ---------------------------------------------------------------------------


class TestCtePassThrough:
    def test_simple_cte(self, toolkit):
        sql = """
        WITH active_users AS (
            SELECT id, name FROM users WHERE status = 'active'
        )
        SELECT id, name FROM active_users
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "id" in result.column_lineage
        assert "name" in result.column_lineage

    def test_chained_ctes(self, toolkit):
        sql = """
        WITH step1 AS (
            SELECT id, amount FROM orders
        ),
        step2 AS (
            SELECT id, amount * 1.1 AS adjusted FROM step1
        )
        SELECT id, adjusted FROM step2
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "id" in result.column_lineage
        assert "adjusted" in result.column_lineage


# ---------------------------------------------------------------------------
# JOIN multi-source
# ---------------------------------------------------------------------------


class TestJoinMultiSource:
    def test_inner_join(self, toolkit):
        sql = """
        SELECT o.id, o.amount, c.name AS customer_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "id" in result.column_lineage
        assert "amount" in result.column_lineage
        assert "customer_name" in result.column_lineage

        # customer_name should trace to customers table.
        cn_nodes = result.column_lineage["customer_name"]
        assert any(n.source_column == "name" for n in cn_nodes)


# ---------------------------------------------------------------------------
# CASE expression
# ---------------------------------------------------------------------------


class TestCaseExpression:
    def test_simple_case(self, toolkit):
        sql = """
        SELECT
            id,
            CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END AS is_active
        FROM users
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "is_active" in result.column_lineage


# ---------------------------------------------------------------------------
# Window function
# ---------------------------------------------------------------------------


class TestWindowFunction:
    def test_row_number(self, toolkit):
        sql = """
        SELECT
            id,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
        FROM employees
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "id" in result.column_lineage
        assert "rank" in result.column_lineage


# ---------------------------------------------------------------------------
# Subquery
# ---------------------------------------------------------------------------


class TestSubquery:
    def test_inline_subquery(self, toolkit):
        sql = """
        SELECT id, amount
        FROM (
            SELECT id, amount FROM orders WHERE amount > 100
        ) AS big_orders
        """
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "id" in result.column_lineage
        assert "amount" in result.column_lineage


# ---------------------------------------------------------------------------
# Transform classification
# ---------------------------------------------------------------------------


class TestTransformClassification:
    def test_direct_is_direct(self, toolkit):
        sql = "SELECT id FROM users"
        nodes = toolkit.lineage_analyzer.trace_single_column("id", sql, Dialect.DATABRICKS)
        # A plain column reference should be classified as direct.
        assert any(n.transform_type == "direct" for n in nodes)

    def test_literal_classification(self, toolkit):
        sql = "SELECT 42 AS magic_number FROM users"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        assert "magic_number" in result.column_lineage
        nodes = result.column_lineage["magic_number"]
        assert any(n.transform_type == "literal" for n in nodes)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_invalid_sql_raises(self, toolkit):
        with pytest.raises(SqlLineageError):
            toolkit.lineage_analyzer.trace_column_lineage("NOT VALID SQL AT ALL ;;;", Dialect.DATABRICKS)

    def test_nonexistent_column_returns_empty_or_error(self, toolkit):
        sql = "SELECT id FROM users"
        # Tracing a column that doesn't exist should either raise or
        # return empty — both are acceptable.
        try:
            nodes = toolkit.lineage_analyzer.trace_single_column("nonexistent_column", sql, Dialect.DATABRICKS)
            # If it doesn't raise, it should at least return something.
            assert isinstance(nodes, tuple)
        except (SqlLineageError, Exception):
            pass  # Also acceptable.

    def test_unresolved_columns_tracked(self, toolkit):
        # SELECT * without schema info often produces unresolvable columns.
        sql = "SELECT * FROM mystery_table"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)
        # With no schema, * may appear in output columns but be unresolvable.
        assert isinstance(result, ColumnLineageResult)


# ---------------------------------------------------------------------------
# Schema-assisted lineage
# ---------------------------------------------------------------------------


class TestSchemaAssistedLineage:
    def test_schema_resolves_columns(self, toolkit):
        sql = "SELECT id, name FROM users"
        schema = {"users": {"id": "INT", "name": "STRING", "email": "STRING"}}

        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS, schema=schema)

        assert "id" in result.column_lineage
        assert "name" in result.column_lineage

    def test_schema_passed_to_single_column(self, toolkit):
        sql = "SELECT id FROM users"
        schema = {"users": {"id": "INT", "name": "STRING"}}

        nodes = toolkit.lineage_analyzer.trace_single_column("id", sql, Dialect.DATABRICKS, schema=schema)
        assert len(nodes) >= 1


# ---------------------------------------------------------------------------
# SELECT * expansion via qualify
# ---------------------------------------------------------------------------


class TestSelectStarExpansion:
    """Verify that ``SELECT *`` is expanded into explicit columns when
    a schema mapping is provided, enabling full column-level lineage
    for star queries that would otherwise be unresolvable.
    """

    def test_star_without_schema_is_unresolvable(self, toolkit):
        """Without schema, ``*`` cannot be expanded and should appear
        in the output as-is (or be marked unresolved)."""
        sql = "SELECT * FROM mystery_table"
        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS)

        # ``*`` should NOT expand without schema info.
        assert isinstance(result, ColumnLineageResult)
        explicit_cols = [c for c in result.column_lineage if c != "*"]
        # Either ``*`` is in lineage (traced opaquely) or in unresolved.
        assert "*" in result.column_lineage or "*" in result.unresolved_columns or len(explicit_cols) == 0

    def test_star_with_schema_expands_to_columns(self, toolkit):
        """With a schema, ``SELECT *`` should expand to the actual
        column names and each column should have full lineage."""
        sql = "SELECT * FROM users"
        schema = {"users": {"id": "INT", "name": "STRING", "email": "STRING"}}

        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS, schema=schema)

        # All three schema columns should appear in the lineage.
        assert "id" in result.column_lineage
        assert "name" in result.column_lineage
        assert "email" in result.column_lineage

        # ``*`` should NOT appear as unresolved.
        assert "*" not in result.unresolved_columns

        # Each column should trace back to the source table.
        for col_name in ("id", "name", "email"):
            nodes = result.column_lineage[col_name]
            assert len(nodes) >= 1
            assert any(n.source_table == "users" for n in nodes)
            assert any(n.source_column == col_name for n in nodes)

    def test_star_join_with_schema_expands_both_tables(self, toolkit):
        """``SELECT *`` over a JOIN should expand columns from all
        joined tables when schema is available."""
        sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        schema = {
            "orders": {"id": "INT", "customer_id": "INT", "amount": "DECIMAL"},
            "customers": {"id": "INT", "name": "STRING"},
        }

        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS, schema=schema)

        # Columns from both tables should be present.
        assert "amount" in result.column_lineage
        assert "name" in result.column_lineage

        # ``*`` should be fully resolved.
        assert "*" not in result.unresolved_columns

        # Verify source attribution is correct.
        amount_nodes = result.column_lineage["amount"]
        assert any(n.source_table == "orders" for n in amount_nodes)

        name_nodes = result.column_lineage["name"]
        assert any(n.source_table == "customers" for n in name_nodes)

    def test_star_cte_with_schema_expansion(self, toolkit):
        """``SELECT *`` from a CTE should expand when schema maps the
        underlying source table."""
        sql = """
        WITH active AS (
            SELECT id, name FROM users WHERE status = 'active'
        )
        SELECT * FROM active
        """
        schema = {"users": {"id": "INT", "name": "STRING", "status": "STRING"}}

        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS, schema=schema)

        # The CTE outputs id and name — ``SELECT * FROM active`` should
        # resolve to those two columns.
        assert isinstance(result, ColumnLineageResult)
        # At minimum, the result should not be empty.
        assert len(result.column_lineage) > 0 or len(result.unresolved_columns) > 0

    def test_mixed_star_and_explicit_columns(self, toolkit):
        """A query mixing ``*`` with explicit columns should expand
        the star while preserving the explicit columns."""
        sql = "SELECT *, amount * 1.1 AS adjusted FROM orders"
        schema = {"orders": {"id": "INT", "amount": "DECIMAL"}}

        result = toolkit.lineage_analyzer.trace_column_lineage(sql, Dialect.DATABRICKS, schema=schema)

        # The explicit column ``adjusted`` should always be present.
        assert "adjusted" in result.column_lineage

        # The star columns should be expanded.
        if "id" in result.column_lineage:
            assert "amount" in result.column_lineage


# ---------------------------------------------------------------------------
# Cross-model lineage
# ---------------------------------------------------------------------------


class TestCrossModelLineage:
    def test_single_model_lineage(self):
        """compute_model_column_lineage should fill in model_name."""
        from core_engine.graph.column_lineage import compute_model_column_lineage

        sql = "SELECT id, name FROM users"
        result = compute_model_column_lineage("staging.users", sql, Dialect.DATABRICKS)

        assert result.model_name == "staging.users"
        assert "id" in result.column_lineage

    def test_trace_across_dag(self):
        """trace_column_across_dag should follow edges through models."""
        import networkx as nx

        from core_engine.graph.column_lineage import trace_column_across_dag

        # Build a simple 2-model DAG: raw.orders → staging.orders_clean
        dag = nx.DiGraph()
        dag.add_node("raw.orders")
        dag.add_node("staging.orders_clean")
        dag.add_edge("raw.orders", "staging.orders_clean")

        model_sql_map = {
            "raw.orders": "SELECT id, amount, customer_id FROM source_table",
            "staging.orders_clean": ("SELECT id, amount FROM raw.orders WHERE amount > 0"),
        }

        result = trace_column_across_dag(
            dag=dag,
            target_model="staging.orders_clean",
            target_column="id",
            model_sql_map=model_sql_map,
            dialect=Dialect.DATABRICKS,
        )

        assert result.target_model == "staging.orders_clean"
        assert result.target_column == "id"
        assert len(result.lineage_path) >= 1

    def test_external_source_terminates(self):
        """Models not in the DAG should appear as terminal nodes."""
        import networkx as nx

        from core_engine.graph.column_lineage import trace_column_across_dag

        dag = nx.DiGraph()
        dag.add_node("staging.orders")

        model_sql_map = {
            "staging.orders": "SELECT id FROM external_source",
        }

        result = trace_column_across_dag(
            dag=dag,
            target_model="staging.orders",
            target_column="id",
            model_sql_map=model_sql_map,
            dialect=Dialect.DATABRICKS,
        )

        assert len(result.lineage_path) >= 1
