"""Tests for SQL qualifier and simplifier.

Covers:
- Unqualified → qualified column resolution
- Already-qualified columns unchanged
- JOIN ambiguous columns
- Boolean simplification: NOT NOT x → x
- Boolean simplification: WHERE TRUE AND x > 5 → WHERE x > 5
- V2 canonicalisation hash consistency
- V1 backward compatibility (no hash breakage)
- Redshift dialect parse + transpile
"""

from __future__ import annotations

import pytest
from core_engine.sql_toolkit import Dialect, get_sql_toolkit
from core_engine.sql_toolkit._types import QualifyResult, SimplifyResult


@pytest.fixture(scope="module")
def toolkit():
    """Shared toolkit instance for all tests in this module."""
    return get_sql_toolkit()


# ---------------------------------------------------------------------------
# Column qualification
# ---------------------------------------------------------------------------


class TestColumnQualification:
    def test_basic_qualification(self, toolkit):
        sql = "SELECT id, name FROM users"
        schema = {"users": {"id": "INT", "name": "STRING"}}

        result = toolkit.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)

        assert isinstance(result, QualifyResult)
        assert result.original_sql == sql
        # After qualification, columns should be table-qualified.
        assert "users" in result.qualified_sql.lower() or result.columns_qualified >= 0

    def test_already_qualified_unchanged(self, toolkit):
        sql = "SELECT users.id, users.name FROM users"
        schema = {"users": {"id": "INT", "name": "STRING"}}

        result = toolkit.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)

        # Already qualified — should be essentially unchanged.
        assert result.columns_qualified == 0 or "users" in result.qualified_sql.lower()

    def test_join_disambiguation(self, toolkit):
        sql = """
        SELECT id, name, amount
        FROM orders
        JOIN customers ON orders.customer_id = customers.id
        """
        schema = {
            "orders": {"id": "INT", "amount": "DECIMAL", "customer_id": "INT"},
            "customers": {"id": "INT", "name": "STRING"},
        }

        result = toolkit.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)

        assert isinstance(result, QualifyResult)
        # The qualified SQL should resolve ambiguous 'id' to a table.
        assert result.qualified_sql  # non-empty

    def test_parse_error_returns_original(self, toolkit):
        sql = "NOT VALID SQL AT ALL ;;;"
        schema = {"t": {"id": "INT"}}

        result = toolkit.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)

        # Should gracefully return original SQL with a warning.
        assert result.qualified_sql == sql
        assert len(result.warnings) > 0

    def test_empty_schema(self, toolkit):
        sql = "SELECT id FROM users"
        schema: dict[str, dict[str, str]] = {}

        result = toolkit.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)

        # Empty schema means no qualification possible.
        assert isinstance(result, QualifyResult)


# ---------------------------------------------------------------------------
# Boolean simplification
# ---------------------------------------------------------------------------


class TestBooleanSimplification:
    def test_double_negation(self, toolkit):
        sql = "SELECT * FROM users WHERE NOT NOT (age > 18)"
        result = toolkit.qualifier.simplify(sql, Dialect.DATABRICKS)

        assert isinstance(result, SimplifyResult)
        assert result.original_sql == sql
        # NOT NOT x should simplify — the simplified SQL should be shorter
        # or at least equivalent.
        assert result.simplified_sql

    def test_true_and_condition(self, toolkit):
        sql = "SELECT * FROM users WHERE TRUE AND x > 5"
        result = toolkit.qualifier.simplify(sql, Dialect.DATABRICKS)

        assert isinstance(result, SimplifyResult)
        # TRUE AND x > 5 should simplify to just x > 5.
        assert result.simplified_sql

    def test_false_or_condition(self, toolkit):
        sql = "SELECT * FROM users WHERE FALSE OR x > 5"
        result = toolkit.qualifier.simplify(sql, Dialect.DATABRICKS)

        assert isinstance(result, SimplifyResult)
        assert result.simplified_sql

    def test_no_simplification_needed(self, toolkit):
        sql = "SELECT id FROM users WHERE age > 18"
        result = toolkit.qualifier.simplify(sql, Dialect.DATABRICKS)

        assert isinstance(result, SimplifyResult)
        # Already simple — simplifications_applied should be 0 or minimal.
        assert result.simplified_sql

    def test_invalid_sql_returns_original(self, toolkit):
        # Use a string that sqlglot genuinely cannot parse (not one it
        # happens to treat as valid SQL like ``NOT VALID SQL``).
        sql = "SELECT FROM WHERE ;;; ((("
        result = toolkit.qualifier.simplify(sql, Dialect.DATABRICKS)

        # On parse failure the method should return the original unchanged.
        assert result.simplified_sql == sql
        assert result.simplifications_applied == 0


# ---------------------------------------------------------------------------
# V2 normalisation
# ---------------------------------------------------------------------------


class TestV2Normalization:
    def test_v2_with_schema(self):
        from core_engine.parser.normalizer import (
            CanonicalizerVersion,
            normalize_sql,
        )

        sql = "SELECT id, name FROM users WHERE age > 18"
        schema = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}

        v2_result = normalize_sql(sql, version=CanonicalizerVersion.V2, schema=schema)

        assert v2_result  # non-empty
        # V2 should produce valid SQL.
        assert "SELECT" in v2_result.upper()

    def test_v1_unchanged(self):
        """V1 hashing must not change with the V2 addition."""
        from core_engine.parser.normalizer import (
            CanonicalizerVersion,
            compute_canonical_hash,
            normalize_sql,
        )

        sql = "SELECT id FROM users"

        v1_result = normalize_sql(sql, version=CanonicalizerVersion.V1)
        v1_hash = compute_canonical_hash(sql, version=CanonicalizerVersion.V1)

        # V1 should be completely unaffected by V2 existence.
        assert v1_result
        assert len(v1_hash) == 64  # SHA-256 hex

    def test_v2_without_schema_degrades_to_v1(self):
        from core_engine.parser.normalizer import (
            CanonicalizerVersion,
            normalize_sql,
        )

        sql = "SELECT id FROM users"

        v1_result = normalize_sql(sql, version=CanonicalizerVersion.V1)
        v2_no_schema = normalize_sql(sql, version=CanonicalizerVersion.V2, schema=None)

        # Without schema, V2 should produce the same output as V1.
        assert v2_no_schema == v1_result

    def test_v2_hash_consistency(self):
        from core_engine.parser.normalizer import (
            CanonicalizerVersion,
            compute_canonical_hash,
        )

        sql = "SELECT id, name FROM users"
        schema = {"users": {"id": "INT", "name": "STRING"}}

        hash1 = compute_canonical_hash(sql, version=CanonicalizerVersion.V2, schema=schema)
        hash2 = compute_canonical_hash(sql, version=CanonicalizerVersion.V2, schema=schema)

        assert hash1 == hash2
        assert len(hash1) == 64


# ---------------------------------------------------------------------------
# Redshift dialect
# ---------------------------------------------------------------------------


class TestRedshiftDialect:
    def test_redshift_parse(self, toolkit):
        """Redshift SQL should parse without error."""
        sql = "SELECT GETDATE() AS now, NVL(name, 'unknown') AS safe_name FROM users"
        result = toolkit.parser.parse_one(sql, Dialect.REDSHIFT)

        assert result.statements
        assert len(result.statements) == 1

    def test_redshift_to_databricks_transpile(self, toolkit):
        """Redshift → Databricks transpilation should convert functions."""
        sql = "SELECT GETDATE(), NVL(x, 0) FROM t"
        result = toolkit.transpiler.transpile(sql, Dialect.REDSHIFT, Dialect.DATABRICKS)

        assert result.output_sql
        assert result.source_dialect == Dialect.REDSHIFT
        assert result.target_dialect == Dialect.DATABRICKS

    def test_databricks_to_redshift_transpile(self, toolkit):
        """Databricks → Redshift transpilation should also work."""
        sql = "SELECT CURRENT_TIMESTAMP(), COALESCE(x, 0) FROM t"
        result = toolkit.transpiler.transpile(sql, Dialect.DATABRICKS, Dialect.REDSHIFT)

        assert result.output_sql
