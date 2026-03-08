"""Unit tests for core_engine.parser.ast_parser."""

from __future__ import annotations

import pytest

from core_engine.parser.ast_parser import (
    ModelASTMetadata,
    SQLParseError,
    extract_ctes,
    extract_output_columns,
    extract_referenced_tables,
    parse_sql,
)

# ---------------------------------------------------------------------------
# parse_sql
# ---------------------------------------------------------------------------


class TestParseSql:
    def test_simple_select(self):
        meta = parse_sql("SELECT id, name FROM users")
        assert isinstance(meta, ModelASTMetadata)
        assert "users" in meta.referenced_tables
        assert meta.has_aggregation is False
        assert meta.has_window_functions is False

    def test_join(self):
        sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
        meta = parse_sql(sql)
        assert "orders" in meta.referenced_tables
        assert "customers" in meta.referenced_tables

    def test_subquery(self):
        sql = "SELECT * FROM (  SELECT id FROM orders WHERE amount > 100) sub"
        meta = parse_sql(sql)
        assert "orders" in meta.referenced_tables

    def test_cte(self):
        sql = "WITH recent AS (SELECT id FROM orders WHERE order_date > '2024-01-01') SELECT * FROM recent"
        meta = parse_sql(sql)
        assert "orders" in meta.referenced_tables
        # CTE name should NOT appear as a referenced table.
        assert "recent" not in meta.referenced_tables
        assert "recent" in meta.ctes

    def test_aggregation(self):
        sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
        meta = parse_sql(sql)
        assert meta.has_aggregation is True

    def test_window_functions(self):
        sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn FROM orders"
        meta = parse_sql(sql)
        assert meta.has_window_functions is True

    def test_multiple_ctes(self):
        sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a JOIN b ON 1=1"
        meta = parse_sql(sql)
        assert sorted(meta.ctes) == ["a", "b"]

    def test_schema_qualified_table(self):
        sql = "SELECT * FROM staging.raw_orders"
        meta = parse_sql(sql)
        assert "staging.raw_orders" in meta.referenced_tables


# ---------------------------------------------------------------------------
# extract_referenced_tables
# ---------------------------------------------------------------------------


class TestExtractReferencedTables:
    def test_basic_table(self):
        tables = extract_referenced_tables("SELECT * FROM orders")
        assert "orders" in tables

    def test_multiple_tables(self):
        sql = "SELECT * FROM orders o JOIN customers c ON o.cid = c.id JOIN products p ON o.pid = p.id"
        tables = extract_referenced_tables(sql)
        assert "orders" in tables
        assert "customers" in tables
        assert "products" in tables

    def test_cte_exclusion(self):
        sql = "WITH cte AS (SELECT id FROM orders) SELECT * FROM cte"
        tables = extract_referenced_tables(sql)
        assert "orders" in tables
        assert "cte" not in tables

    def test_schema_qualified_tables(self):
        sql = "SELECT * FROM staging.orders JOIN analytics.customers ON 1=1"
        tables = extract_referenced_tables(sql)
        assert "staging.orders" in tables
        assert "analytics.customers" in tables

    def test_sorted_output(self):
        sql = "SELECT * FROM z_table JOIN a_table ON 1=1"
        tables = extract_referenced_tables(sql)
        assert tables == sorted(tables)


# ---------------------------------------------------------------------------
# extract_output_columns
# ---------------------------------------------------------------------------


class TestExtractOutputColumns:
    def test_named_columns(self):
        cols = extract_output_columns("SELECT id, name FROM users")
        assert "id" in cols
        assert "name" in cols

    def test_aliases(self):
        cols = extract_output_columns("SELECT id AS user_id, name AS user_name FROM users")
        assert "user_id" in cols
        assert "user_name" in cols

    def test_star_expansion(self):
        cols = extract_output_columns("SELECT * FROM users")
        assert "*" in cols

    def test_sorted_output(self):
        cols = extract_output_columns("SELECT z_col, a_col FROM tbl")
        assert cols == sorted(cols)


# ---------------------------------------------------------------------------
# extract_ctes
# ---------------------------------------------------------------------------


class TestExtractCtes:
    def test_single_cte(self):
        sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte"
        ctes = extract_ctes(sql)
        assert ctes == ["my_cte"]

    def test_multiple_ctes(self):
        sql = "WITH cte_b AS (SELECT 1), cte_a AS (SELECT 2) SELECT * FROM cte_a JOIN cte_b ON 1=1"
        ctes = extract_ctes(sql)
        assert ctes == ["cte_a", "cte_b"]  # sorted

    def test_no_ctes(self):
        ctes = extract_ctes("SELECT 1 FROM orders")
        assert ctes == []


# ---------------------------------------------------------------------------
# SQLParseError
# ---------------------------------------------------------------------------


class TestSQLParseError:
    def test_invalid_sql_raises(self):
        with pytest.raises(SQLParseError):
            parse_sql("THIS IS NOT VALID SQL AT ALL ;;; DROP")

    def test_error_attributes(self):
        try:
            parse_sql("COMPLETELY INVALID SQL STATEMENT GARBAGE")
        except SQLParseError as exc:
            assert exc.sql_fragment != ""
            assert exc.reason != ""
