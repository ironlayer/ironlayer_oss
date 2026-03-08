"""SQL optimisation engine -- rule-based with optional LLM suggestions.

All suggestions returned are **advisory only** and must be validated
before use.  The engine validates every suggestion (rule-based and LLM)
through ``sqlglot.parse_one()`` and silently drops those that fail.

INVARIANT: This engine **never** mutates execution plans.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp as sqlglot_exp

from ai_engine.engines.suggestion_validator import SuggestionValidator
from ai_engine.models.requests import OptimizeSQLRequest
from ai_engine.models.responses import OptimizeSQLResponse, SQLSuggestion

if TYPE_CHECKING:
    from ai_engine.engines.llm_client import LLMClient

logger = logging.getLogger(__name__)

# Pattern matchers for rule-based heuristics
_SELECT_STAR_RE = re.compile(r"\bSELECT\s+\*", re.IGNORECASE)
_DISTINCT_RE = re.compile(r"\bSELECT\s+DISTINCT\b", re.IGNORECASE)


class SQLOptimizer:
    """Generate SQL optimisation suggestions."""

    def __init__(self, llm_client: LLMClient | None = None) -> None:
        self._llm = llm_client
        self._validator = SuggestionValidator()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def optimize(self, request: OptimizeSQLRequest) -> OptimizeSQLResponse:
        """Analyse SQL and return validated optimisation suggestions."""

        suggestions: list[SQLSuggestion] = []

        # 1. Rule-based analysis (always)
        rule_suggestions = self._rule_based_suggestions(
            sql=request.sql,
            stats=request.table_statistics,
        )
        suggestions.extend(rule_suggestions)

        # 2. LLM suggestions (optional; respect per-tenant opt-out)
        llm_enabled = getattr(request, "llm_enabled", True)
        if self._llm is not None and self._llm.enabled and llm_enabled:
            llm_suggestions = self._llm_suggestions(request)
            suggestions.extend(llm_suggestions)

        # 3. Validate all suggestions through the three-gate pipeline
        validated: list[SQLSuggestion] = []
        for s in suggestions:
            if s.rewritten_sql is not None:
                result = self._validator.validate(
                    original_sql=request.sql,
                    rewritten_sql=s.rewritten_sql,
                    description=s.description,
                )
                if result.is_valid:
                    # Enrich the suggestion with the diff explanation
                    validated.append(
                        SQLSuggestion(
                            suggestion_type=s.suggestion_type,
                            description=f"{s.description} | Changes: {result.diff_explanation}",
                            rewritten_sql=s.rewritten_sql,
                            confidence=s.confidence,
                        )
                    )
                else:
                    logger.info(
                        "Rejected suggestion type=%s: %s",
                        s.suggestion_type,
                        "; ".join(result.rejection_reasons),
                    )
            else:
                # Advisory-only suggestions without rewritten SQL are always kept
                validated.append(s)

        return OptimizeSQLResponse(suggestions=validated)

    # ------------------------------------------------------------------
    # Rule-based suggestions
    # ------------------------------------------------------------------

    def _rule_based_suggestions(
        self,
        sql: str,
        stats: dict | None,
    ) -> list[SQLSuggestion]:
        """Apply deterministic pattern-matching rules."""

        suggestions: list[SQLSuggestion] = []

        # Parse AST once for structural checks
        try:
            ast = sqlglot.parse_one(sql, read="databricks")
        except sqlglot.errors.ParseError:
            logger.warning("Could not parse SQL for optimisation analysis")
            return suggestions

        # Rule 1: SELECT *
        if _SELECT_STAR_RE.search(sql):
            suggestions.append(
                SQLSuggestion(
                    suggestion_type="column_pruning",
                    description=(
                        "SELECT * reads all columns.  Listing only the "
                        "required columns reduces I/O and improves performance."
                    ),
                    rewritten_sql=None,  # cannot auto-rewrite without schema
                    confidence=0.95,
                )
            )

        # Rule 2: MERGE without partition filter
        merge_nodes = list(ast.find_all(sqlglot_exp.Merge))
        for merge in merge_nodes:
            partition_refs = list(merge.find_all(sqlglot_exp.PartitionedByProperty))
            if not partition_refs:
                suggestions.append(
                    SQLSuggestion(
                        suggestion_type="partition_strategy",
                        description=(
                            "MERGE statement without partition pruning detected.  "
                            "Adding a partition filter can significantly reduce "
                            "the data scanned."
                        ),
                        rewritten_sql=None,
                        confidence=0.75,
                    )
                )

        # Rule 3: Multiple JOINs without early filters
        joins = list(ast.find_all(sqlglot_exp.Join))
        if len(joins) >= 2:
            where = ast.find(sqlglot_exp.Where)
            # Check if there's a filter that could be pushed down
            if where is None:
                suggestions.append(
                    SQLSuggestion(
                        suggestion_type="predicate_pushdown",
                        description=(
                            "Multiple JOINs detected without a WHERE clause.  "
                            "Adding filters before joins (predicate pushdown) "
                            "can reduce intermediate result sizes."
                        ),
                        rewritten_sql=None,
                        confidence=0.7,
                    )
                )
            else:
                # WHERE exists but check if predicates reference only one table
                conditions = list(where.find_all(sqlglot_exp.Column))
                tables_in_conditions = {c.table for c in conditions if c.table}
                tables_in_joins = set()
                for j in joins:
                    on_clause = j.find(sqlglot_exp.Column)
                    if on_clause and on_clause.table:
                        tables_in_joins.add(on_clause.table)
                # If filter applies to only one table in a multi-join,
                # it might benefit from pushdown
                if tables_in_conditions and tables_in_joins:
                    pushdown_candidates = tables_in_conditions - tables_in_joins
                    if pushdown_candidates:
                        suggestions.append(
                            SQLSuggestion(
                                suggestion_type="predicate_pushdown",
                                description=(
                                    f"Filters on table(s) {', '.join(sorted(pushdown_candidates))} "
                                    "could be pushed into subqueries or CTEs "
                                    "for earlier filtering."
                                ),
                                rewritten_sql=None,
                                confidence=0.65,
                            )
                        )

        # Rule 4: DISTINCT on many columns
        if _DISTINCT_RE.search(sql):
            select = ast.find(sqlglot_exp.Select)
            if select is not None and len(select.expressions) > 5:
                suggestions.append(
                    SQLSuggestion(
                        suggestion_type="dedup_optimization",
                        description=(
                            f"DISTINCT on {len(select.expressions)} columns detected.  "
                            "Consider using ROW_NUMBER() with a window function "
                            "or GROUP BY for more efficient deduplication."
                        ),
                        rewritten_sql=None,
                        confidence=0.7,
                    )
                )

        # Rule 5: Subquery that could be a CTE
        subqueries = list(ast.find_all(sqlglot_exp.Subquery))
        # Filter to only subqueries used as derived tables (in FROM / JOIN)
        derived_subqueries = [
            sq
            for sq in subqueries
            if isinstance(sq.parent, (sqlglot_exp.From, sqlglot_exp.Join, sqlglot_exp.Table))
            or (sq.parent and isinstance(sq.parent, sqlglot_exp.Subquery))
        ]
        if derived_subqueries:
            suggestions.append(
                SQLSuggestion(
                    suggestion_type="cte_refactor",
                    description=(
                        f"{len(derived_subqueries)} inline subquery(ies) found.  "
                        "Refactoring to CTEs (WITH clause) improves "
                        "readability and may enable query plan reuse."
                    ),
                    rewritten_sql=None,
                    confidence=0.6,
                )
            )

        return suggestions

    # ------------------------------------------------------------------
    # LLM suggestions
    # ------------------------------------------------------------------

    def _llm_suggestions(
        self,
        request: OptimizeSQLRequest,
    ) -> list[SQLSuggestion]:
        """Get suggestions from the LLM and convert to SQLSuggestion models."""
        assert self._llm is not None

        context_parts: list[str] = []
        if request.table_statistics:
            context_parts.append(f"Table statistics: {request.table_statistics}")
        if request.query_metrics:
            context_parts.append(f"Query metrics: {request.query_metrics}")

        # Extract per-tenant API key if provided.
        tenant_api_key: str | None = None
        if hasattr(request, "api_key") and request.api_key is not None:
            tenant_api_key = request.api_key.get_secret_value()

        raw = self._llm.suggest_optimization(
            sql=request.sql,
            context="\n".join(context_parts),
            api_key=tenant_api_key,
        )

        if raw is None:
            return []

        suggestions: list[SQLSuggestion] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                suggestions.append(
                    SQLSuggestion(
                        suggestion_type=str(item.get("suggestion_type", "llm_suggestion")),
                        description=str(item.get("description", "")),
                        rewritten_sql=item.get("rewritten_sql"),
                        confidence=float(item.get("confidence", 0.5)),
                    )
                )
            except (ValueError, TypeError):
                logger.debug("Skipping malformed LLM suggestion: %s", item)
                continue

        return suggestions

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_sql(sql: str) -> bool:
        """Return True if the SQL parses successfully via sqlglot."""
        try:
            sqlglot.parse_one(sql, read="databricks")
            return True
        except sqlglot.errors.ParseError:
            return False
