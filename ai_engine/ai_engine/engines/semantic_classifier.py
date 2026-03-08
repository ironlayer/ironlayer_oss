"""Semantic change classifier -- dual-path: rule-based primary, optional LLM enrichment.

The classifier analyses the *old* and *new* SQL for a dbt/Spark model and
returns a structured classification that downstream systems use to decide
rebuild scope, alerting, and approval gates.

INVARIANT: This engine **never** mutates execution plans.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp as sqlglot_exp

from ai_engine.models.requests import SemanticClassifyRequest
from ai_engine.models.responses import SemanticClassifyResponse

if TYPE_CHECKING:
    from ai_engine.engines.llm_client import LLMClient

logger = logging.getLogger(__name__)

# Whitespace / comment normalisation pattern
_WS_RE = re.compile(r"\s+")
_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)

# Date truncation / extraction functions that indicate partition changes.
# These are matched as function call patterns (e.g. "FUNC(") not substrings.
_DATE_FUNC_PATTERNS = re.compile(
    r"\b(?:DATE_TRUNC|TIMESTAMP_TRUNC|DATE|YEAR|MONTH|DAY|HOUR|WEEK|QUARTER)\s*\(",
    re.IGNORECASE,
)


def _normalise_sql(sql: str) -> str:
    """Collapse whitespace and strip comments for comparison."""
    text = _COMMENT_RE.sub(" ", sql)
    text = _WS_RE.sub(" ", text).strip().lower()
    return text


def _extract_select_expressions(
    parsed: sqlglot_exp.Expression,
) -> list[sqlglot_exp.Expression]:
    """Return the top-level SELECT column expressions."""
    select = parsed.find(sqlglot_exp.Select)
    if select is None:
        return []
    return list(select.expressions)


def _expression_name(expr: sqlglot_exp.Expression) -> str:
    """Best-effort column name / alias for a SELECT expression."""
    if isinstance(expr, sqlglot_exp.Alias):
        return expr.alias
    if hasattr(expr, "name"):
        return expr.name
    return expr.sql()


def _expression_body(expr: sqlglot_exp.Expression) -> str:
    """The SQL text of the expression *without* any alias."""
    if isinstance(expr, sqlglot_exp.Alias):
        return expr.this.sql()
    return expr.sql()


def _strip_table_qualifiers(sql_text: str) -> str:
    """Remove table alias qualifiers (e.g. 'a.id' -> 'id') for comparison."""
    return re.sub(r"\b\w+\.", "", sql_text)


def _extract_where_sql(ast: sqlglot_exp.Expression) -> str:
    """Extract the WHERE clause SQL, or empty string if none."""
    where = ast.find(sqlglot_exp.Where)
    return where.sql() if where else ""


def _extract_from_sql(ast: sqlglot_exp.Expression) -> str:
    """Extract FROM clause source tables/subqueries as normalised text."""
    froms = ast.find(sqlglot_exp.From)
    return froms.sql() if froms else ""


def _extract_joins(ast: sqlglot_exp.Expression) -> list[str]:
    """Extract JOIN clauses as sorted SQL strings."""
    return sorted(j.sql() for j in ast.find_all(sqlglot_exp.Join))


def _extract_group_by(ast: sqlglot_exp.Expression) -> str:
    """Extract GROUP BY clause SQL."""
    group = ast.find(sqlglot_exp.Group)
    return group.sql() if group else ""


def _extract_having(ast: sqlglot_exp.Expression) -> str:
    """Extract HAVING clause SQL."""
    having = ast.find(sqlglot_exp.Having)
    return having.sql() if having else ""


def _extract_order_by(ast: sqlglot_exp.Expression) -> str:
    """Extract ORDER BY clause SQL."""
    # Exclude window-internal ORDER BY by only checking direct children
    for node in ast.walk():
        if isinstance(node, sqlglot_exp.Order) and not isinstance(node.parent, sqlglot_exp.Window):
            return node.sql()
    return ""


def _extract_distinct(ast: sqlglot_exp.Expression) -> bool:
    """Check if the SELECT uses DISTINCT."""
    select = ast.find(sqlglot_exp.Select)
    if select is None:
        return False
    return bool(select.args.get("distinct"))


def _extract_cte_bodies(ast: sqlglot_exp.Expression) -> dict[str, str]:
    """Extract CTE names and their body SQL for comparison."""
    result: dict[str, str] = {}
    with_clause = ast.find(sqlglot_exp.With)
    if with_clause is None:
        return result
    for cte in with_clause.find_all(sqlglot_exp.CTE):
        name = cte.alias
        body = cte.this.sql() if cte.this else ""
        result[name] = body
    return result


def _has_date_transformation(exprs: list[sqlglot_exp.Expression]) -> bool:
    """Check if any SELECT expression uses a date truncation/extraction function."""
    for expr in exprs:
        body = _expression_body(expr)
        if _DATE_FUNC_PATTERNS.search(body):
            return True
    return False


def _get_select_star(ast: sqlglot_exp.Expression) -> bool:
    """Check if the SELECT uses SELECT *."""
    select = ast.find(sqlglot_exp.Select)
    if select is None:
        return False
    return any(isinstance(expr, sqlglot_exp.Star) for expr in select.expressions)


class SemanticClassifier:
    """Classify SQL model changes into semantic categories."""

    def __init__(
        self,
        llm_client: LLMClient | None = None,
        confidence_threshold: float = 0.7,
    ) -> None:
        self._llm = llm_client
        self._confidence_threshold = confidence_threshold

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def classify(
        self,
        request: SemanticClassifyRequest,
    ) -> SemanticClassifyResponse:
        """Classify a change -- rule-based first, optional LLM refinement."""
        result = self._rule_based_classify(
            old_sql=request.old_sql,
            new_sql=request.new_sql,
            schema_diff=request.schema_diff,
        )

        # If low confidence and LLM available, attempt enrichment.
        # Respect per-request llm_enabled flag (per-tenant opt-out).
        llm_enabled = getattr(request, "llm_enabled", True)
        if (
            result.confidence < self._confidence_threshold
            and self._llm is not None
            and self._llm.enabled
            and llm_enabled
        ):
            result = self._llm_enrich(request, result)

        return result

    # ------------------------------------------------------------------
    # Rule-based classifier
    # ------------------------------------------------------------------

    def _rule_based_classify(
        self,
        old_sql: str,
        new_sql: str,
        schema_diff: dict | None,
    ) -> SemanticClassifyResponse:
        """Pure rule-based classification -- always deterministic."""

        # 1. Brand-new model
        if not old_sql or not old_sql.strip():
            return SemanticClassifyResponse(
                change_type="non_breaking",
                confidence=1.0,
                requires_full_rebuild=False,
                impact_scope="New model -- no downstream impact from change.",
            )

        # 2. Cosmetic (normalised SQL identical)
        old_norm = _normalise_sql(old_sql)
        new_norm = _normalise_sql(new_sql)
        if old_norm == new_norm:
            return SemanticClassifyResponse(
                change_type="cosmetic",
                confidence=1.0,
                requires_full_rebuild=False,
                impact_scope="Whitespace or comment-only change.",
            )

        # 3. AST-level comparison via sqlglot
        try:
            old_ast = sqlglot.parse_one(old_sql, read="databricks")
            new_ast = sqlglot.parse_one(new_sql, read="databricks")
        except sqlglot.errors.ParseError:
            logger.warning("sqlglot parse error -- defaulting to breaking")
            return SemanticClassifyResponse(
                change_type="breaking",
                confidence=0.3,
                requires_full_rebuild=True,
                impact_scope="Unable to parse SQL -- treating as potentially breaking.",
            )

        # --- Extract SELECT expressions ---
        old_exprs = _extract_select_expressions(old_ast)
        new_exprs = _extract_select_expressions(new_ast)

        old_names = {_expression_name(e) for e in old_exprs}
        new_names = {_expression_name(e) for e in new_exprs}

        old_bodies = {_expression_name(e): _expression_body(e) for e in old_exprs}
        new_bodies = {_expression_name(e): _expression_body(e) for e in new_exprs}

        removed_names = old_names - new_names
        added_names = new_names - old_names
        common_names = old_names & new_names

        # --- Extract structural components ---
        old_where = _extract_where_sql(old_ast)
        new_where = _extract_where_sql(new_ast)
        old_joins = _extract_joins(old_ast)
        new_joins = _extract_joins(new_ast)
        old_from = _extract_from_sql(old_ast)
        new_from = _extract_from_sql(new_ast)
        old_group = _extract_group_by(old_ast)
        new_group = _extract_group_by(new_ast)
        old_having = _extract_having(old_ast)
        new_having = _extract_having(new_ast)
        old_order = _extract_order_by(old_ast)
        new_order = _extract_order_by(new_ast)
        old_distinct = _extract_distinct(old_ast)
        new_distinct = _extract_distinct(new_ast)
        old_ctes = _extract_cte_bodies(old_ast)
        new_ctes = _extract_cte_bodies(new_ast)
        old_aggs = {e.sql() for e in old_ast.find_all(sqlglot_exp.AggFunc)}
        new_aggs = {e.sql() for e in new_ast.find_all(sqlglot_exp.AggFunc)}
        old_select_star = _get_select_star(old_ast)
        new_select_star = _get_select_star(new_ast)

        # 3a. Check for DDL PARTITION BY changes
        old_partitions = {w.sql() for w in old_ast.find_all(sqlglot_exp.PartitionedByProperty)}
        new_partitions = {w.sql() for w in new_ast.find_all(sqlglot_exp.PartitionedByProperty)}
        if old_partitions != new_partitions and (old_partitions or new_partitions):
            return SemanticClassifyResponse(
                change_type="partition_shift",
                confidence=0.85,
                requires_full_rebuild=True,
                impact_scope="Partition scheme changed -- full rebuild required.",
            )

        # 3a-ii. Check for date transformation changes in SELECT that indicate
        # partition scheme shifts (e.g. order_date -> date_trunc('month', order_date))
        if removed_names and added_names:
            new_has_date_func = _has_date_transformation(new_exprs)
            old_has_date_func = _has_date_transformation(old_exprs)
            if new_has_date_func and not old_has_date_func:
                return SemanticClassifyResponse(
                    change_type="partition_shift",
                    confidence=0.8,
                    requires_full_rebuild=True,
                    impact_scope="Date transformation added to SELECT -- likely partition scheme change.",
                )

        # 3a-iii. Check window function PARTITION BY changes
        old_windows = sorted(w.sql() for w in old_ast.find_all(sqlglot_exp.Window))
        new_windows = sorted(w.sql() for w in new_ast.find_all(sqlglot_exp.Window))
        if old_windows and new_windows and old_windows != new_windows:
            old_win_parts = sorted(
                ", ".join(p.sql() for p in (w.args.get("partition_by") or []))
                for w in old_ast.find_all(sqlglot_exp.Window)
            )
            new_win_parts = sorted(
                ", ".join(p.sql() for p in (w.args.get("partition_by") or []))
                for w in new_ast.find_all(sqlglot_exp.Window)
            )
            if old_win_parts != new_win_parts:
                return SemanticClassifyResponse(
                    change_type="partition_shift",
                    confidence=0.85,
                    requires_full_rebuild=True,
                    impact_scope="Window partition clause changed -- full rebuild required.",
                )

        # 3b. DISTINCT changes -> metric_semantic
        if old_distinct != new_distinct:
            if old_distinct and not new_distinct:
                return SemanticClassifyResponse(
                    change_type="metric_semantic",
                    confidence=0.8,
                    requires_full_rebuild=True,
                    impact_scope="DISTINCT removed -- row semantics changed.",
                )
            else:
                return SemanticClassifyResponse(
                    change_type="non_breaking",
                    confidence=0.8,
                    requires_full_rebuild=False,
                    impact_scope="DISTINCT added -- deduplication applied.",
                )

        # 3c. SELECT * -> explicit columns is non-breaking
        if old_select_star and not new_select_star:
            return SemanticClassifyResponse(
                change_type="non_breaking",
                confidence=0.7,
                requires_full_rebuild=False,
                impact_scope="SELECT * replaced with explicit columns.",
            )

        # 3d. Columns added, none removed -> non_breaking (even if new
        # columns contain aggregations — the additive change is safe).
        # But only if GROUP BY and FROM haven't changed (those indicate
        # deeper structural shifts, not simple column addition).
        if added_names and not removed_names:
            modified = [n for n in common_names if old_bodies.get(n) != new_bodies.get(n)]
            if not modified and old_group == new_group and old_from == new_from:
                return SemanticClassifyResponse(
                    change_type="non_breaking",
                    confidence=0.85,
                    requires_full_rebuild=False,
                    impact_scope=f"Columns added: {', '.join(sorted(added_names))}.",
                )

        # 3e. Aggregation expression changes in EXISTING columns -> metric_semantic
        if old_aggs != new_aggs:
            return SemanticClassifyResponse(
                change_type="metric_semantic",
                confidence=0.8,
                requires_full_rebuild=True,
                impact_scope="Aggregation logic changed -- metrics may be affected.",
            )

        # 3f. HAVING changes -> metric_semantic
        if old_having != new_having:
            return SemanticClassifyResponse(
                change_type="metric_semantic",
                confidence=0.8,
                requires_full_rebuild=True,
                impact_scope="HAVING clause changed -- aggregate filtering modified.",
            )

        # 3g-i. GROUP BY changes -> metric_semantic
        if old_group != new_group and old_group:
            return SemanticClassifyResponse(
                change_type="metric_semantic",
                confidence=0.8,
                requires_full_rebuild=True,
                impact_scope="GROUP BY clause changed -- aggregation granularity modified.",
            )

        # 3g. Check for pure alias/table alias renames
        if not removed_names and not added_names:
            bodies_changed = any(old_bodies.get(n) != new_bodies.get(n) for n in common_names)
            if not bodies_changed:
                # SELECT expressions identical -- check structural changes
                structural = self._classify_structural_change(
                    old_where=old_where,
                    new_where=new_where,
                    old_joins=old_joins,
                    new_joins=new_joins,
                    old_from=old_from,
                    new_from=new_from,
                    old_order=old_order,
                    new_order=new_order,
                    old_ctes=old_ctes,
                    new_ctes=new_ctes,
                )
                if structural is not None:
                    return structural
                return SemanticClassifyResponse(
                    change_type="cosmetic",
                    confidence=1.0,
                    requires_full_rebuild=False,
                    impact_scope="No semantic change detected at expression level.",
                )

        # Check for pure alias renames (same body text, different names)
        old_body_set = set(old_bodies.values())
        new_body_set = set(new_bodies.values())
        if old_body_set == new_body_set and old_names != new_names:
            return SemanticClassifyResponse(
                change_type="rename_only",
                confidence=0.9,
                requires_full_rebuild=False,
                impact_scope="Column aliases renamed -- downstream references may need updating.",
            )

        # Check for table alias renames: same columns after stripping qualifiers
        old_stripped = {n: _strip_table_qualifiers(b) for n, b in old_bodies.items()}
        new_stripped = {n: _strip_table_qualifiers(b) for n, b in new_bodies.items()}
        old_stripped_set = set(old_stripped.values())
        new_stripped_set = set(new_stripped.values())
        if old_stripped_set == new_stripped_set and old_body_set != new_body_set:
            return SemanticClassifyResponse(
                change_type="rename_only",
                confidence=0.85,
                requires_full_rebuild=False,
                impact_scope="Table aliases renamed -- same underlying columns.",
            )

        # 3h. Columns removed -> breaking
        if removed_names:
            return SemanticClassifyResponse(
                change_type="breaking",
                confidence=0.9,
                requires_full_rebuild=True,
                impact_scope=f"Columns removed: {', '.join(sorted(removed_names))}.",
            )

        # 3i. Window function changes (same partition, different frame/function)
        if old_windows != new_windows:
            return SemanticClassifyResponse(
                change_type="metric_semantic",
                confidence=0.75,
                requires_full_rebuild=True,
                impact_scope="Window function changed -- metrics may be affected.",
            )

        # 3k. Structural changes when SELECT has modifications
        structural = self._classify_structural_change(
            old_where=old_where,
            new_where=new_where,
            old_joins=old_joins,
            new_joins=new_joins,
            old_from=old_from,
            new_from=new_from,
            old_order=old_order,
            new_order=new_order,
            old_ctes=old_ctes,
            new_ctes=new_ctes,
        )
        if structural is not None:
            return structural

        # 3l. Default conservative
        return SemanticClassifyResponse(
            change_type="breaking",
            confidence=0.5,
            requires_full_rebuild=True,
            impact_scope="Unrecognised change pattern -- treating as breaking.",
        )

    # ------------------------------------------------------------------
    # Structural comparison helpers
    # ------------------------------------------------------------------

    def _classify_structural_change(
        self,
        *,
        old_where: str,
        new_where: str,
        old_joins: list[str],
        new_joins: list[str],
        old_from: str,
        new_from: str,
        old_order: str,
        new_order: str,
        old_ctes: dict[str, str],
        new_ctes: dict[str, str],
    ) -> SemanticClassifyResponse | None:
        """Classify changes in non-SELECT structural clauses.

        Returns None if no structural differences are detected.
        """
        # JOIN type or condition changes -> breaking
        if old_joins != new_joins:
            if len(old_joins) != len(new_joins):
                return SemanticClassifyResponse(
                    change_type="breaking",
                    confidence=0.85,
                    requires_full_rebuild=True,
                    impact_scope="JOIN structure changed -- number of joins differs.",
                )
            return SemanticClassifyResponse(
                change_type="breaking",
                confidence=0.85,
                requires_full_rebuild=True,
                impact_scope="JOIN clause changed -- join type or condition modified.",
            )

        # WHERE clause changes
        if old_where != new_where:
            if old_where and not new_where:
                return SemanticClassifyResponse(
                    change_type="breaking",
                    confidence=0.85,
                    requires_full_rebuild=True,
                    impact_scope="WHERE clause removed -- filter dropped.",
                )
            if not old_where and new_where:
                return SemanticClassifyResponse(
                    change_type="non_breaking",
                    confidence=0.8,
                    requires_full_rebuild=False,
                    impact_scope="WHERE clause added -- filter narrows result set.",
                )
            # WHERE modified: check if old WHERE is a subset of new WHERE
            # (i.e. new WHERE is a narrowing/tightening of the filter)
            old_where_norm = _normalise_sql(old_where)
            new_where_norm = _normalise_sql(new_where)
            if old_where_norm in new_where_norm:
                return SemanticClassifyResponse(
                    change_type="non_breaking",
                    confidence=0.75,
                    requires_full_rebuild=False,
                    impact_scope="WHERE clause narrowed -- additional filter conditions added.",
                )
            return SemanticClassifyResponse(
                change_type="breaking",
                confidence=0.7,
                requires_full_rebuild=True,
                impact_scope="WHERE clause modified -- filter semantics changed.",
            )

        # FROM source changes -> breaking (unless it's a CTE rename)
        if old_from != new_from:
            # Check if this is just a CTE rename: different CTE names but
            # same CTE bodies, and FROM references the renamed CTE.
            if old_ctes and new_ctes and len(old_ctes) == len(new_ctes):
                old_cte_bodies_sorted = sorted(old_ctes.values())
                new_cte_bodies_sorted = sorted(new_ctes.values())
                if old_cte_bodies_sorted == new_cte_bodies_sorted:
                    return SemanticClassifyResponse(
                        change_type="rename_only",
                        confidence=0.85,
                        requires_full_rebuild=False,
                        impact_scope="CTE renamed -- same underlying query logic.",
                    )
            return SemanticClassifyResponse(
                change_type="breaking",
                confidence=0.8,
                requires_full_rebuild=True,
                impact_scope="FROM source changed -- data source modified.",
            )

        # CTE body changes -> breaking
        if old_ctes and new_ctes:
            common_cte_names = set(old_ctes.keys()) & set(new_ctes.keys())
            for name in common_cte_names:
                if old_ctes[name] != new_ctes[name]:
                    return SemanticClassifyResponse(
                        change_type="breaking",
                        confidence=0.75,
                        requires_full_rebuild=True,
                        impact_scope=f"CTE '{name}' body changed -- upstream logic modified.",
                    )

        # ORDER BY changes -> non_breaking (doesn't affect data)
        if old_order != new_order:
            return SemanticClassifyResponse(
                change_type="non_breaking",
                confidence=0.85,
                requires_full_rebuild=False,
                impact_scope="ORDER BY changed -- sort order modified.",
            )

        return None

    # ------------------------------------------------------------------
    # LLM enrichment
    # ------------------------------------------------------------------

    def _llm_enrich(
        self,
        request: SemanticClassifyRequest,
        rule_result: SemanticClassifyResponse,
    ) -> SemanticClassifyResponse:
        """Attempt LLM enrichment.  LLM can only *refine* confidence, not
        override the rule-based change_type."""
        assert self._llm is not None

        context_parts: list[str] = []
        if request.schema_diff:
            context_parts.append(f"Schema diff: {request.schema_diff}")
        if request.column_lineage:
            context_parts.append(f"Column lineage: {request.column_lineage}")
        context_parts.append(f"Rule-based result: type={rule_result.change_type}, confidence={rule_result.confidence}")

        # Extract per-tenant API key if provided.
        tenant_api_key: str | None = None
        if hasattr(request, "api_key") and request.api_key is not None:
            tenant_api_key = request.api_key.get_secret_value()

        llm_result = self._llm.classify_change(
            old_sql=request.old_sql,
            new_sql=request.new_sql,
            context="\n".join(context_parts),
            api_key=tenant_api_key,
        )

        if llm_result is None:
            logger.debug("LLM returned None -- keeping rule-based result")
            return rule_result

        # LLM CANNOT override change_type -- only refine confidence
        llm_confidence = llm_result.get("confidence")
        if isinstance(llm_confidence, (int, float)) and 0.0 <= llm_confidence <= 1.0:
            # Weighted blend: rule-based 70%, LLM 30%
            blended = rule_result.confidence * 0.7 + float(llm_confidence) * 0.3
            reasoning = llm_result.get("reasoning", "")
            impact_scope = rule_result.impact_scope
            if reasoning:
                impact_scope = f"{impact_scope} LLM note: {reasoning}"

            return SemanticClassifyResponse(
                change_type=rule_result.change_type,
                confidence=round(min(max(blended, 0.0), 1.0), 4),
                requires_full_rebuild=rule_result.requires_full_rebuild,
                impact_scope=impact_scope,
            )

        return rule_result
