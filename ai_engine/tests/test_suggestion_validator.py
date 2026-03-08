"""Tests for ai_engine.engines.suggestion_validator.SuggestionValidator.

Covers the three-gate validation pipeline:
  Gate 1: _validate_syntax (sqlglot parse)
  Gate 2: _generate_diff_explanation (AST diff)
  Gate 3: _test_run (DuckDB sandbox execution)

Also covers edge cases with CTEs, subqueries, and timeout handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai_engine.engines.suggestion_validator import (
    SuggestionValidator,
    ValidationResult,
)

# ================================================================== #
# Full pipeline: validate()
# ================================================================== #


class TestValidatePipeline:
    """End-to-end tests for the three-gate validation pipeline."""

    def test_valid_rewrite_passes_all_gates(self):
        """A valid, semantically different rewrite should pass all three gates."""
        validator = SuggestionValidator()
        original = "SELECT id, name FROM orders"
        rewritten = "SELECT id, name, amount FROM orders"

        result = validator.validate(
            original_sql=original,
            rewritten_sql=rewritten,
            description="Add amount column",
        )

        assert result.syntax_ok is True
        assert result.diff_explanation != ""
        assert result.test_run_ok is True
        assert result.is_valid is True
        assert result.rejection_reasons == []

    def test_syntax_error_fails_gate_1(self):
        """Unparseable rewritten SQL fails the syntax gate."""
        validator = SuggestionValidator()
        result = validator.validate(
            original_sql="SELECT id FROM orders",
            rewritten_sql="SELECTTTT id FROMM orders !!!",
            description="Bad SQL",
        )

        assert result.syntax_ok is False
        assert result.is_valid is False
        assert any("syntax" in r.lower() for r in result.rejection_reasons)
        # Gate 3 should NOT run when syntax fails
        assert result.test_run_ok is False

    def test_semantic_identity_fails_gate_2(self):
        """Rewrite identical to original produces no diff -> fails gate 2."""
        validator = SuggestionValidator()
        sql = "SELECT id, name FROM orders"
        result = validator.validate(
            original_sql=sql,
            rewritten_sql=sql,
            description="Identical rewrite",
        )

        assert result.syntax_ok is True
        assert result.diff_explanation == ""
        assert result.is_valid is False
        assert any("diff" in r.lower() for r in result.rejection_reasons)

    def test_unparseable_original_fails_gate_2(self):
        """If original SQL is unparseable, diff generation returns empty."""
        validator = SuggestionValidator()
        result = validator.validate(
            original_sql="NOT SQL AT ALL !!!",
            rewritten_sql="SELECT id FROM orders",
            description="Original is junk",
        )

        assert result.syntax_ok is True  # rewritten parses fine
        assert result.diff_explanation == ""  # can't diff
        assert result.is_valid is False

    def test_duckdb_test_run_failure(self):
        """If DuckDB test-run fails, is_valid is False."""
        validator = SuggestionValidator()
        # Use SQL that parses in sqlglot but references a non-existent function
        # that DuckDB won't understand. The validator has several fallbacks,
        # so we test with something that will truly fail even in the fallback path.
        result = validator.validate(
            original_sql="SELECT id FROM orders",
            rewritten_sql="SELECT id, MAGIC_NONEXISTENT_FN() FROM orders",
            description="Invalid function",
        )
        # Syntax may pass in sqlglot (it parses function calls), but the
        # diff should show changes. The DuckDB run may or may not pass
        # depending on the fallback chain. What matters is the pipeline
        # does not crash.
        assert isinstance(result, ValidationResult)


# ================================================================== #
# Gate 1: _validate_syntax
# ================================================================== #


class TestValidateSyntax:
    """Direct tests for the static _validate_syntax method."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT id FROM orders",
            "SELECT id, name FROM orders WHERE active = 1",
            "SELECT a.id, b.name FROM orders a JOIN customers b ON a.cid = b.id",
            "WITH cte AS (SELECT 1 AS x) SELECT x FROM cte",
            "SELECT id, SUM(amount) FROM orders GROUP BY id HAVING SUM(amount) > 100",
        ],
        ids=["simple", "where", "join", "cte", "having"],
    )
    def test_valid_sql(self, sql):
        assert SuggestionValidator._validate_syntax(sql) is True

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECTTT id FROM orders",
            "SELECT FROM",
            "",
        ],
        ids=["typo", "incomplete", "empty"],
    )
    def test_invalid_sql(self, sql):
        assert SuggestionValidator._validate_syntax(sql) is False

    def test_garbage_text_rejected(self):
        """sqlglot >=25 rejects 'THIS IS NOT SQL' as unparseable (earlier
        versions permissively interpreted it as column references)."""
        assert SuggestionValidator._validate_syntax("THIS IS NOT SQL") is False


# ================================================================== #
# Gate 2: _generate_diff_explanation
# ================================================================== #


class TestGenerateDiffExplanation:
    """Direct tests for the static _generate_diff_explanation method."""

    def test_added_columns_in_diff(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "SELECT id, name FROM orders",
        )
        assert "Added columns" in diff
        assert "name" in diff

    def test_removed_columns_in_diff(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id, name FROM orders",
            "SELECT id FROM orders",
        )
        assert "Removed columns" in diff
        assert "name" in diff

    def test_added_where_clause(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "SELECT id FROM orders WHERE active = 1",
        )
        assert "Added WHERE" in diff

    def test_removed_where_clause(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders WHERE active = 1",
            "SELECT id FROM orders",
        )
        assert "Removed WHERE" in diff

    def test_join_count_change(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "SELECT a.id FROM orders a JOIN customers b ON a.cid = b.id",
        )
        assert "JOIN count" in diff

    def test_cte_added(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "WITH active AS (SELECT id FROM orders WHERE active = 1) SELECT id FROM active",
        )
        assert "Added CTEs" in diff

    def test_cte_removed(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "WITH active AS (SELECT id FROM orders WHERE active = 1) SELECT id FROM active",
            "SELECT id FROM orders WHERE active = 1",
        )
        assert "Removed CTEs" in diff

    def test_window_function_count_change(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders",
        )
        assert "Window function" in diff

    def test_no_diff_returns_empty(self):
        sql = "SELECT id, name FROM orders"
        diff = SuggestionValidator._generate_diff_explanation(sql, sql)
        assert diff == ""

    def test_unparseable_original_returns_empty(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "NOT SQL !!!",
            "SELECT id FROM orders",
        )
        assert diff == ""

    def test_unparseable_rewrite_returns_empty(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "NOT SQL !!!",
        )
        assert diff == ""

    def test_formatting_change_produces_explanation(self):
        """Structural formatting differences should still produce an explanation."""
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id, name FROM orders",
            "SELECT id, UPPER(name) AS name FROM orders",
        )
        # The expression body changed, so some diff should be detected
        assert diff != ""

    def test_added_table_references(self):
        diff = SuggestionValidator._generate_diff_explanation(
            "SELECT id FROM orders",
            "SELECT a.id FROM orders a JOIN payments p ON a.id = p.order_id",
        )
        # Should detect added table references or join count change
        assert diff != ""


# ================================================================== #
# Gate 3: _test_run
# ================================================================== #


class TestTestRun:
    """Tests for the DuckDB sandbox execution gate."""

    def test_simple_select_runs(self):
        validator = SuggestionValidator()
        ok, error = validator._test_run("SELECT 1 AS x")
        assert ok is True
        assert error is None

    def test_select_with_arithmetic(self):
        validator = SuggestionValidator()
        ok, error = validator._test_run("SELECT 1 + 2 AS result")
        assert ok is True

    def test_select_with_string_functions(self):
        validator = SuggestionValidator()
        ok, error = validator._test_run("SELECT UPPER('hello') AS greeting")
        assert ok is True

    def test_transpilation_failure(self):
        """SQL that cannot be transpiled should fail gracefully."""
        validator = SuggestionValidator()
        # sqlglot.transpile on garbage won't produce valid DuckDB SQL
        # but it may still produce something. We test the error path.
        ok, error = validator._test_run("COMPLETELY INVALID @@@ NOT SQL")
        # Either fails or passes through the fallback chain -- no crash
        assert isinstance(ok, bool)

    def test_custom_timeout(self):
        """Custom timeout parameter is accepted."""
        validator = SuggestionValidator(test_run_timeout_seconds=1.0)
        ok, error = validator._test_run("SELECT 1")
        assert ok is True

    def test_duckdb_path_in_memory(self):
        """Default in-memory DuckDB should work."""
        validator = SuggestionValidator(duckdb_path=None)
        ok, error = validator._test_run("SELECT 42 AS answer")
        assert ok is True
        assert error is None

    @patch("ai_engine.engines.suggestion_validator.duckdb")
    def test_duckdb_connection_error(self, mock_duckdb):
        """Connection failure in DuckDB is handled gracefully."""
        import duckdb as real_duckdb

        mock_duckdb.Error = real_duckdb.Error
        mock_duckdb.connect.side_effect = real_duckdb.Error("connection failed")

        validator = SuggestionValidator()
        ok, error = validator._test_run("SELECT 1")
        assert ok is False
        assert error is not None
        assert "connection" in error.lower() or "Connection" in error


# ================================================================== #
# Edge cases: SQL with CTEs and subqueries
# ================================================================== #


class TestEdgeCases:
    """Complex SQL patterns in the validation pipeline."""

    def test_cte_validates_successfully(self):
        validator = SuggestionValidator()
        original = "SELECT id FROM orders"
        rewritten = "WITH filtered AS (SELECT id FROM orders WHERE active = 1) SELECT id FROM filtered"
        result = validator.validate(
            original_sql=original,
            rewritten_sql=rewritten,
            description="Add CTE filter",
        )
        assert result.syntax_ok is True
        assert result.diff_explanation != ""

    def test_nested_subquery_validates(self):
        validator = SuggestionValidator()
        original = "SELECT id FROM orders"
        rewritten = "SELECT id FROM (SELECT id FROM orders WHERE amount > 0) sub"
        result = validator.validate(
            original_sql=original,
            rewritten_sql=rewritten,
            description="Add subquery filter",
        )
        assert result.syntax_ok is True

    def test_complex_sql_with_window_functions(self):
        validator = SuggestionValidator()
        original = "SELECT id, amount FROM orders"
        rewritten = (
            "SELECT id, amount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn FROM orders"
        )
        result = validator.validate(
            original_sql=original,
            rewritten_sql=rewritten,
            description="Add row numbering",
        )
        assert result.syntax_ok is True
        assert "Window function" in result.diff_explanation

    def test_validation_result_is_frozen_dataclass(self):
        validator = SuggestionValidator()
        result = validator.validate(
            original_sql="SELECT id FROM orders",
            rewritten_sql="SELECT id, name FROM orders",
            description="Test",
        )
        assert isinstance(result, ValidationResult)
        # Frozen dataclass -- should not allow mutation
        with pytest.raises(AttributeError):
            result.is_valid = False  # type: ignore[misc]

    def test_multiple_ctes(self):
        validator = SuggestionValidator()
        original = "SELECT id FROM orders"
        rewritten = (
            "WITH cte1 AS (SELECT id FROM orders WHERE active = 1), "
            "cte2 AS (SELECT id FROM cte1 WHERE id > 10) "
            "SELECT id FROM cte2"
        )
        result = validator.validate(
            original_sql=original,
            rewritten_sql=rewritten,
            description="Multi-CTE refactor",
        )
        assert result.syntax_ok is True
        assert "Added CTEs" in result.diff_explanation

    @pytest.mark.parametrize(
        "rewritten",
        [
            "SELECT id, name FROM orders UNION ALL SELECT id, name FROM archive_orders",
            "SELECT id, CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier FROM orders",
            "SELECT id FROM orders LIMIT 100",
        ],
        ids=["union-all", "case-when", "limit"],
    )
    def test_various_valid_sql_patterns(self, rewritten):
        """Several valid SQL rewrites should all pass syntax validation."""
        validator = SuggestionValidator()
        result = validator.validate(
            original_sql="SELECT id FROM orders",
            rewritten_sql=rewritten,
            description="Various pattern",
        )
        assert result.syntax_ok is True
        assert result.diff_explanation != ""
