"""Unit tests for core_engine.executor.sql_rewriter.

Validates SQL rewriting via SQLGlot AST manipulation for environment isolation.
Covers single tables, JOINs, CTEs, subqueries, qualified names, no-op paths,
parse error fallback, and case-insensitive matching.
"""

from __future__ import annotations

import pytest

from core_engine.executor.sql_rewriter import SQLRewriter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise(sql: str) -> str:
    """Normalise whitespace for comparison."""
    return " ".join(sql.split())


# ---------------------------------------------------------------------------
# Single table rewrite
# ---------------------------------------------------------------------------


class TestSingleTableRewrite:
    """Verify rewriting of single-table SELECT statements."""

    def test_unqualified_table(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM orders")
        assert "dev" in result.lower()
        assert "dev_raw" in result.lower()
        assert "orders" in result.lower()

    def test_schema_qualified_table(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM raw.orders")
        assert "dev" in result.lower()
        assert "dev_raw" in result.lower()

    def test_fully_qualified_table(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM main.raw.orders")
        assert "dev" in result.lower()
        assert "dev_raw" in result.lower()

    def test_non_matching_catalog_not_rewritten(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM other_catalog.other_schema.orders")
        normalised = _normalise(result).lower()
        assert "other_catalog" in normalised
        assert "other_schema" in normalised

    def test_non_matching_schema_not_rewritten(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM staging.orders")
        normalised = _normalise(result).lower()
        # staging != raw so the schema should not change to dev_raw
        assert "staging" in normalised


# ---------------------------------------------------------------------------
# Multi-table JOINs
# ---------------------------------------------------------------------------


class TestJoinRewrite:
    """Verify that all tables in JOIN clauses are rewritten."""

    def test_inner_join(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        SELECT o.id, c.name
        FROM raw.orders o
        INNER JOIN raw.customers c ON o.customer_id = c.id
        """
        result = rewriter.rewrite(sql)
        lower = result.lower()
        # Both tables should be rewritten
        assert lower.count("dev_raw") >= 2

    def test_left_join_with_qualified_names(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        SELECT *
        FROM main.raw.orders o
        LEFT JOIN main.raw.products p ON o.product_id = p.id
        """
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert "main" not in lower.split("dev")[0] or lower.count("dev.dev_raw") >= 2


# ---------------------------------------------------------------------------
# CTEs (WITH clauses)
# ---------------------------------------------------------------------------


class TestCTERewrite:
    """Verify that table references inside CTEs are rewritten."""

    def test_cte_table_references(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        WITH recent_orders AS (
            SELECT * FROM raw.orders WHERE order_date > '2024-01-01'
        )
        SELECT * FROM recent_orders
        JOIN raw.customers ON recent_orders.customer_id = customers.id
        """
        result = rewriter.rewrite(sql)
        lower = result.lower()
        # raw.orders and raw.customers should be rewritten
        assert "dev_raw" in lower

    def test_nested_ctes(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        WITH
            step1 AS (SELECT id FROM raw.orders),
            step2 AS (SELECT * FROM step1 JOIN raw.items ON step1.id = items.order_id)
        SELECT * FROM step2
        """
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert "dev_raw" in lower


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------


class TestSubqueryRewrite:
    """Verify that table references in subqueries are rewritten."""

    def test_subquery_in_from(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        SELECT * FROM (
            SELECT id, amount FROM raw.orders WHERE amount > 100
        ) sub
        """
        result = rewriter.rewrite(sql)
        assert "dev_raw" in result.lower()

    def test_subquery_in_where(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = """
        SELECT * FROM raw.orders
        WHERE customer_id IN (
            SELECT id FROM raw.customers WHERE region = 'US'
        )
        """
        result = rewriter.rewrite(sql)
        lower = result.lower()
        # Both orders and customers should be rewritten
        assert lower.count("dev_raw") >= 2


# ---------------------------------------------------------------------------
# Qualified names (catalog.schema.table)
# ---------------------------------------------------------------------------


class TestQualifiedNames:
    """Verify handling of fully-qualified three-part names."""

    def test_three_part_name_matching(self) -> None:
        rewriter = SQLRewriter("prod_catalog", "analytics", "staging_catalog", "stg_analytics")
        sql = "SELECT * FROM prod_catalog.analytics.revenue"
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert "staging_catalog" in lower
        assert "stg_analytics" in lower

    def test_three_part_name_not_matching(self) -> None:
        rewriter = SQLRewriter("prod_catalog", "analytics", "staging_catalog", "stg_analytics")
        sql = "SELECT * FROM other_catalog.analytics.revenue"
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert "other_catalog" in lower
        assert "staging_catalog" not in lower


# ---------------------------------------------------------------------------
# No-op when source == target
# ---------------------------------------------------------------------------


class TestNoop:
    """Verify no rewriting when source and target are identical."""

    def test_noop_returns_unchanged(self) -> None:
        rewriter = SQLRewriter("main", "raw", "main", "raw")
        sql = "SELECT * FROM main.raw.orders"
        result = rewriter.rewrite(sql)
        assert result == sql

    def test_noop_case_insensitive(self) -> None:
        rewriter = SQLRewriter("Main", "RAW", "main", "raw")
        sql = "SELECT * FROM main.raw.orders"
        result = rewriter.rewrite(sql)
        assert result == sql

    def test_is_noop_property(self) -> None:
        assert SQLRewriter("main", "raw", "main", "raw").is_noop is True
        assert SQLRewriter("main", "raw", "dev", "raw").is_noop is False


# ---------------------------------------------------------------------------
# Parse error fallback
# ---------------------------------------------------------------------------


class TestParseErrorFallback:
    """Verify that unparseable SQL is returned unchanged."""

    def test_invalid_sql_returns_original(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        bad_sql = "THIS IS NOT VALID SQL AT ALL !!! {{{}}"
        result = rewriter.rewrite(bad_sql)
        assert result == bad_sql

    def test_empty_string(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        result = rewriter.rewrite("")
        assert result == ""


# ---------------------------------------------------------------------------
# Case-insensitive matching
# ---------------------------------------------------------------------------


class TestCaseInsensitive:
    """Verify that catalog/schema matching is case-insensitive."""

    def test_uppercase_source_matches(self) -> None:
        rewriter = SQLRewriter("MAIN", "RAW", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM main.raw.orders")
        lower = result.lower()
        assert "dev" in lower
        assert "dev_raw" in lower

    def test_mixed_case_source_matches(self) -> None:
        rewriter = SQLRewriter("Main", "Raw", "dev", "dev_raw")
        result = rewriter.rewrite("SELECT * FROM MAIN.RAW.orders")
        lower = result.lower()
        assert "dev" in lower
        assert "dev_raw" in lower


# ---------------------------------------------------------------------------
# Multiple statements
# ---------------------------------------------------------------------------


class TestMultipleStatements:
    """Verify that multiple SQL statements separated by semicolons are handled."""

    def test_multiple_statements(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = "SELECT * FROM raw.orders; SELECT * FROM raw.customers"
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert lower.count("dev_raw") >= 2


# ---------------------------------------------------------------------------
# CREATE / INSERT statements
# ---------------------------------------------------------------------------


class TestDMLStatements:
    """Verify rewriting works on non-SELECT statements."""

    def test_insert_statement(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = "INSERT INTO raw.orders SELECT * FROM raw.staging_orders"
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert lower.count("dev_raw") >= 2

    def test_create_table_as_select(self) -> None:
        rewriter = SQLRewriter("main", "raw", "dev", "dev_raw")
        sql = "CREATE TABLE raw.summary AS SELECT count(*) FROM raw.orders"
        result = rewriter.rewrite(sql)
        lower = result.lower()
        assert "dev_raw" in lower
