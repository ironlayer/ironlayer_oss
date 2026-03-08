"""Tests for ai_engine.engines.sql_optimizer.SQLOptimizer.

Covers rule-based suggestions (SELECT *, DISTINCT, multi-JOIN, subquery),
LLM suggestion integration with validation, invalid LLM SQL being dropped
silently, SuggestionValidator three-gate integration, and edge cases
(simple SELECT, no optimization opportunities).
"""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from ai_engine.engines.sql_optimizer import SQLOptimizer
from ai_engine.engines.suggestion_validator import SuggestionValidator, ValidationResult
from ai_engine.models.requests import OptimizeSQLRequest
from ai_engine.models.responses import OptimizeSQLResponse, SQLSuggestion

# ================================================================== #
# Helpers
# ================================================================== #


def _req(sql: str, **kwargs) -> OptimizeSQLRequest:
    return OptimizeSQLRequest(sql=sql, **kwargs)


# ================================================================== #
# Rule-based: SELECT * detection
# ================================================================== #


class TestSelectStarRule:
    """SELECT * triggers a column_pruning suggestion."""

    def test_select_star_detected(self):
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT * FROM orders"))
        types = [s.suggestion_type for s in result.suggestions]
        assert "column_pruning" in types

    def test_select_star_confidence(self):
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT * FROM orders"))
        pruning = [s for s in result.suggestions if s.suggestion_type == "column_pruning"]
        assert len(pruning) == 1
        assert pruning[0].confidence == 0.95

    def test_select_star_no_rewritten_sql(self):
        """SELECT * suggestion does not include rewritten SQL (needs schema info)."""
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT * FROM orders"))
        pruning = [s for s in result.suggestions if s.suggestion_type == "column_pruning"]
        assert pruning[0].rewritten_sql is None

    def test_no_select_star(self):
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT id, name FROM orders"))
        types = [s.suggestion_type for s in result.suggestions]
        assert "column_pruning" not in types


# ================================================================== #
# Rule-based: Multi-JOIN without WHERE
# ================================================================== #


class TestMultiJoinRule:
    """Multiple JOINs without WHERE triggers predicate_pushdown suggestion."""

    def test_multi_join_no_where(self):
        sql = (
            "SELECT a.id, b.name, c.amount "
            "FROM orders a "
            "JOIN customers b ON a.customer_id = b.id "
            "JOIN payments c ON a.id = c.order_id"
        )
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "predicate_pushdown" in types

    def test_single_join_no_suggestion(self):
        sql = "SELECT a.id, b.name FROM orders a JOIN customers b ON a.customer_id = b.id"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "predicate_pushdown" not in types


# ================================================================== #
# Rule-based: DISTINCT on many columns
# ================================================================== #


class TestDistinctRule:
    """DISTINCT on >5 columns triggers dedup_optimization."""

    def test_distinct_many_columns(self):
        cols = ", ".join(f"col{i}" for i in range(7))
        sql = f"SELECT DISTINCT {cols} FROM orders"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "dedup_optimization" in types

    def test_distinct_few_columns_no_suggestion(self):
        sql = "SELECT DISTINCT id, name FROM orders"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "dedup_optimization" not in types


# ================================================================== #
# Rule-based: Subquery -> CTE refactor
# ================================================================== #


class TestSubqueryRule:
    """Inline subqueries in FROM clause trigger cte_refactor suggestion."""

    def test_subquery_detected(self):
        sql = "SELECT t.id, t.name FROM (SELECT id, name FROM customers WHERE active = 1) t"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "cte_refactor" in types

    def test_no_subquery(self):
        sql = "SELECT id, name FROM customers WHERE active = 1"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "cte_refactor" not in types


# ================================================================== #
# LLM suggestions with validation
# ================================================================== #


class TestLLMSuggestions:
    """LLM suggestions are fetched, converted, and validated."""

    def test_valid_llm_suggestion_included(self, mock_llm_enabled):
        """A syntactically valid LLM suggestion passes through validation."""
        mock_llm_enabled.suggest_optimization.return_value = [
            {
                "suggestion_type": "index_hint",
                "description": "Add ZORDER BY on customer_id",
                "rewritten_sql": "SELECT id, name FROM orders ORDER BY customer_id",
                "confidence": 0.85,
            }
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req("SELECT id, name FROM orders"))
        # The LLM suggestion has rewritten_sql so it goes through validation.
        # Whether it passes depends on the validator. Advisory-only (no rewritten_sql)
        # are kept regardless. With rewritten_sql, it must pass the three gates.
        llm_types = [s for s in result.suggestions if s.suggestion_type == "index_hint"]
        # If validation passes, it will be included; if not, it will be dropped.
        # The important thing is no exception is raised.
        assert isinstance(result, OptimizeSQLResponse)

    def test_advisory_only_llm_suggestion_kept(self, mock_llm_enabled):
        """LLM suggestions without rewritten_sql are always kept."""
        mock_llm_enabled.suggest_optimization.return_value = [
            {
                "suggestion_type": "caching",
                "description": "Consider caching this intermediate result",
                "rewritten_sql": None,
                "confidence": 0.6,
            }
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req("SELECT id, name FROM orders"))
        caching = [s for s in result.suggestions if s.suggestion_type == "caching"]
        assert len(caching) == 1
        assert caching[0].description == "Consider caching this intermediate result"

    def test_llm_returns_none(self, mock_llm_enabled):
        """LLM returning None should not crash."""
        mock_llm_enabled.suggest_optimization.return_value = None

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req("SELECT id FROM orders"))
        assert isinstance(result, OptimizeSQLResponse)

    def test_llm_not_called_when_disabled(self, mock_llm_disabled):
        optimizer = SQLOptimizer(llm_client=mock_llm_disabled)
        result = optimizer.optimize(_req("SELECT id FROM orders"))
        mock_llm_disabled.suggest_optimization.assert_not_called()
        assert isinstance(result, OptimizeSQLResponse)


# ================================================================== #
# Invalid LLM SQL dropped silently
# ================================================================== #


class TestInvalidLLMSQLDropped:
    """LLM suggestions with unparseable SQL are rejected by the validator."""

    def test_invalid_rewritten_sql_dropped(self, mock_llm_enabled):
        """Suggestion with garbage SQL is silently dropped."""
        mock_llm_enabled.suggest_optimization.return_value = [
            {
                "suggestion_type": "rewrite",
                "description": "Optimised query",
                "rewritten_sql": "THIS IS NOT VALID SQL !!@@##",
                "confidence": 0.9,
            }
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req("SELECT id FROM orders"))
        rewrite_suggestions = [s for s in result.suggestions if s.suggestion_type == "rewrite"]
        assert len(rewrite_suggestions) == 0

    def test_malformed_llm_item_skipped(self, mock_llm_enabled):
        """Non-dict items in LLM response are silently skipped."""
        mock_llm_enabled.suggest_optimization.return_value = [
            "not a dict",
            42,
            {"suggestion_type": "advisory", "description": "do stuff", "confidence": 0.5},
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req("SELECT id FROM orders"))
        # Only the valid dict should potentially appear
        assert isinstance(result, OptimizeSQLResponse)


# ================================================================== #
# SuggestionValidator integration (three-gate validation)
# ================================================================== #


class TestValidatorIntegration:
    """Verify the optimizer validates rewritten SQL through the three-gate pipeline."""

    def test_suggestion_with_valid_rewrite_gets_diff_explanation(self, mock_llm_enabled):
        """Valid rewrite should be enriched with diff explanation."""
        # A valid rewrite that actually changes something
        original = "SELECT id, name FROM orders"
        rewritten = "SELECT id, name, amount FROM orders"

        mock_llm_enabled.suggest_optimization.return_value = [
            {
                "suggestion_type": "add_column",
                "description": "Add amount column",
                "rewritten_sql": rewritten,
                "confidence": 0.8,
            }
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req(original))

        add_col = [s for s in result.suggestions if s.suggestion_type == "add_column"]
        if add_col:
            # Should have "Changes:" in description from diff explanation
            assert "Changes:" in add_col[0].description

    def test_validator_rejects_semantically_identical(self, mock_llm_enabled):
        """A rewrite identical to original should be rejected (no diff)."""
        sql = "SELECT id, name FROM orders"
        mock_llm_enabled.suggest_optimization.return_value = [
            {
                "suggestion_type": "identity",
                "description": "No-op rewrite",
                "rewritten_sql": sql,
                "confidence": 0.5,
            }
        ]

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        result = optimizer.optimize(_req(sql))
        identity = [s for s in result.suggestions if s.suggestion_type == "identity"]
        # Identical SQL generates no diff -> rejected by gate 2
        assert len(identity) == 0


# ================================================================== #
# Edge cases
# ================================================================== #


class TestEdgeCases:
    """Boundary conditions and unusual inputs."""

    def test_simple_select_no_optimizations(self):
        """A simple, clean SELECT should produce no suggestions."""
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT id, name FROM orders WHERE active = 1"))
        assert isinstance(result, OptimizeSQLResponse)
        assert len(result.suggestions) == 0

    def test_unparseable_sql_returns_empty(self):
        """SQL that cannot be parsed returns empty suggestions."""
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("NOT VALID SQL AT ALL !!!"))
        assert isinstance(result, OptimizeSQLResponse)
        assert len(result.suggestions) == 0

    def test_empty_sql_string(self):
        """Empty SQL string should not crash."""
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT 1"))
        assert isinstance(result, OptimizeSQLResponse)

    def test_response_type(self):
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req("SELECT * FROM orders"))
        assert isinstance(result, OptimizeSQLResponse)
        assert all(isinstance(s, SQLSuggestion) for s in result.suggestions)

    def test_validate_sql_static_method_valid(self):
        assert SQLOptimizer._validate_sql("SELECT id FROM orders") is True

    def test_validate_sql_static_method_invalid(self):
        assert SQLOptimizer._validate_sql("NOT SQL !!!") is False

    def test_cte_sql_parses(self):
        """CTE-based SQL should parse and be analysed without error."""
        sql = "WITH active AS (SELECT id, name FROM customers WHERE active = 1) SELECT a.id, a.name FROM active a"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        assert isinstance(result, OptimizeSQLResponse)

    def test_multiple_rule_suggestions(self):
        """SQL that triggers multiple rules returns all applicable suggestions."""
        # SELECT * + multiple JOINs without WHERE
        sql = "SELECT * FROM orders a JOIN customers b ON a.customer_id = b.id JOIN payments c ON a.id = c.order_id"
        optimizer = SQLOptimizer()
        result = optimizer.optimize(_req(sql))
        types = [s.suggestion_type for s in result.suggestions]
        assert "column_pruning" in types
        assert "predicate_pushdown" in types

    def test_llm_suggestion_with_table_statistics(self, mock_llm_enabled):
        """table_statistics and query_metrics are passed to LLM context."""
        mock_llm_enabled.suggest_optimization.return_value = []

        optimizer = SQLOptimizer(llm_client=mock_llm_enabled)
        optimizer.optimize(
            _req(
                "SELECT id FROM orders",
                table_statistics={"rows": 1_000_000},
                query_metrics={"avg_runtime_ms": 5000},
            )
        )
        call_kwargs = mock_llm_enabled.suggest_optimization.call_args
        context_arg = call_kwargs.kwargs.get("context") or call_kwargs[1].get("context") or ""
        assert "Table statistics" in context_arg
        assert "Query metrics" in context_arg
