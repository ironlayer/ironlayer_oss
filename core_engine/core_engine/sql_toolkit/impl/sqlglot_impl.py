"""SQLGlot-backed implementation of the SQL toolkit protocols.

This is the ONLY file in the entire codebase that imports ``sqlglot`` directly.
All consumer code goes through the protocol interfaces defined in
:mod:`core_engine.sql_toolkit._protocols`.

Every piece of generic SQL logic that was previously scattered across 12+
consumer files has been consolidated here.  Consumer files retain only their
business logic and delegate all SQL parsing, analysis, transpilation,
normalisation, diffing, safety checking, and rewriting to this implementation.

Supports SQLGlot v25.x (pinned at v25.34.1).
"""

from __future__ import annotations

import logging
import re
from typing import Any

import sqlglot
from sqlglot import diff as sqlglot_diff_module
from sqlglot import exp
from sqlglot.diff import Keep
from sqlglot.errors import ErrorLevel, ParseError, SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify import qualify as sqlglot_qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer.simplify import simplify as sqlglot_simplify
from sqlglot.schema import MappingSchema

from .._types import (
    AstDiffResult,
    ColumnExtractionResult,
    ColumnLineageNode,
    ColumnLineageResult,
    ColumnRef,
    Dialect,
    DiffEdit,
    DiffEditKind,
    NormalizationResult,
    ParseResult,
    QualifyResult,
    RewriteResult,
    RewriteRule,
    SafetyCheckResult,
    SafetyViolation,
    ScopeResult,
    SimplifyResult,
    SqlLineageError,
    SqlNode,
    SqlNodeKind,
    SqlNormalizationError,
    SqlParseError,
    SqlTranspileError,
    TableRef,
    TranspileResult,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal: SQLGlot expression → SqlNodeKind mapping
# ---------------------------------------------------------------------------

# Maps sqlglot expression class names to our SqlNodeKind enum.
# This is the single point where sqlglot types are translated into our
# implementation-agnostic types.  Kept as a dict for O(1) lookup.
_EXP_KIND_MAP: dict[str, SqlNodeKind] = {
    "Select": SqlNodeKind.SELECT,
    "Create": SqlNodeKind.CREATE,
    "Insert": SqlNodeKind.INSERT,
    "Delete": SqlNodeKind.DELETE,
    "Drop": SqlNodeKind.DROP,
    "Alter": SqlNodeKind.ALTER,
    "TruncateTable": SqlNodeKind.TRUNCATE,
    "Merge": SqlNodeKind.MERGE,
    "Grant": SqlNodeKind.GRANT,
    "Command": SqlNodeKind.COMMAND,
    "With": SqlNodeKind.WITH,
    "CTE": SqlNodeKind.CTE,
    "From": SqlNodeKind.FROM,
    "Join": SqlNodeKind.JOIN,
    "Where": SqlNodeKind.WHERE,
    "Group": SqlNodeKind.GROUP,
    "Having": SqlNodeKind.HAVING,
    "Order": SqlNodeKind.ORDER,
    "Table": SqlNodeKind.TABLE,
    "Column": SqlNodeKind.COLUMN,
    "Star": SqlNodeKind.STAR,
    "Alias": SqlNodeKind.ALIAS,
    "TableAlias": SqlNodeKind.TABLE_ALIAS,
    "Window": SqlNodeKind.WINDOW,
    "Subquery": SqlNodeKind.SUBQUERY,
    "PartitionedByProperty": SqlNodeKind.PARTITIONED_BY,
}

# Add aggregate function types that sqlglot uses.
_AGG_FUNC_NAMES: frozenset[str] = frozenset(
    {
        "Sum",
        "Count",
        "Avg",
        "Min",
        "Max",
        "ArrayAgg",
        "GroupConcat",
        "CountIf",
        "ApproxDistinct",
        "Variance",
        "Stddev",
        "StddevPop",
        "StddevSamp",
        "VariancePop",
        "VarianceSamp",
        "Percentile",
        "AnyValue",
        "First",
        "Last",
        "Collect",
        "CollectSet",
        "CollectList",
    }
)

# Revoke may not exist in all sqlglot versions.
if hasattr(exp, "Revoke"):
    _EXP_KIND_MAP["Revoke"] = SqlNodeKind.REVOKE

# Comment-stripping regexes (used in normalisation).
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)

# Pattern for CREATE USER detection in Command expressions.
_RE_CREATE_USER = re.compile(r"\bUSER\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Internal: AST conversion helpers
# ---------------------------------------------------------------------------


def _dialect_value(dialect: Dialect) -> str:
    """Return the sqlglot dialect string for a :class:`Dialect` enum member."""
    return dialect.value


def _classify_node(node: exp.Expression) -> SqlNodeKind:
    """Map a sqlglot expression to a :class:`SqlNodeKind`.

    Checks the class hierarchy: if the node is a subclass of ``AggFunc``
    (which covers SUM, COUNT, AVG, etc.), return ``AGG_FUNC``.
    """
    cls_name = type(node).__name__

    # Direct lookup first (fast path).
    kind = _EXP_KIND_MAP.get(cls_name)
    if kind is not None:
        return kind

    # Aggregate function subclass check.
    if isinstance(node, exp.AggFunc):
        return SqlNodeKind.AGG_FUNC

    return SqlNodeKind.UNKNOWN


def _node_name(node: exp.Expression) -> str:
    """Extract a meaningful name from a sqlglot expression node."""
    # Tables have a .name property.
    if isinstance(node, exp.Table):
        return node.name or ""
    # Columns have a .name property.
    if isinstance(node, exp.Column):
        return node.name or ""
    # CTEs have an alias.
    if isinstance(node, exp.CTE):
        alias = node.args.get("alias")
        if alias is not None:
            return alias.name or ""
        return ""
    # Stars have no name.
    if isinstance(node, exp.Star):
        return "*"
    # For aliases, use the alias name.
    if isinstance(node, exp.Alias):
        return node.alias or ""
    # Generic fallback.
    if hasattr(node, "name"):
        return str(node.name) if node.name else ""
    return ""


def _node_alias(node: exp.Expression) -> str:
    """Extract the alias from a sqlglot expression, if present."""
    if isinstance(node, exp.Alias):
        return node.alias or ""
    alias_node = node.args.get("alias")
    if alias_node is not None and hasattr(alias_node, "name"):
        return alias_node.name or ""
    return ""


def _to_sql_node(node: exp.Expression, dialect: Dialect, *, depth: int = 0) -> SqlNode:
    """Recursively convert a sqlglot AST into a :class:`SqlNode` tree.

    Limits recursion depth to 100 to avoid stack overflow on deeply nested SQL.
    """
    if depth > 100:
        return SqlNode(
            kind=SqlNodeKind.UNKNOWN,
            name="",
            alias="",
            children=(),
            sql_text="...",
            raw=node,
        )

    kind = _classify_node(node)
    name = _node_name(node)
    alias = _node_alias(node)

    # Convert children.  sqlglot's .walk() is breadth-first and includes
    # self; we want direct children only.  Use .iter_expressions() for that.
    children: list[SqlNode] = []
    for child_node in node.iter_expressions():
        children.append(_to_sql_node(child_node, dialect, depth=depth + 1))

    # Render the SQL text for this node.
    try:
        sql_text = node.sql(dialect=_dialect_value(dialect))
    except Exception:
        sql_text = ""

    return SqlNode(
        kind=kind,
        name=name,
        alias=alias,
        children=tuple(children),
        sql_text=sql_text,
        raw=node,
    )


def _normalise_table_name(table: exp.Table) -> TableRef:
    """Convert a sqlglot ``Table`` node to a :class:`TableRef`."""
    catalog = None
    schema = None
    name = table.name or ""

    cat_node = table.args.get("catalog")
    if cat_node is not None and hasattr(cat_node, "name") and cat_node.name:
        catalog = cat_node.name

    db_node = table.args.get("db")
    if db_node is not None and hasattr(db_node, "name") and db_node.name:
        schema = db_node.name

    return TableRef(catalog=catalog, schema=schema, name=name)


def _collect_cte_names(ast: exp.Expression) -> set[str]:
    """Collect all CTE alias names defined anywhere in *ast*."""
    cte_names: set[str] = set()
    for cte_node in ast.find_all(exp.CTE):
        alias = cte_node.args.get("alias")
        if alias is not None and hasattr(alias, "name") and alias.name:
            cte_names.add(alias.name)
    return cte_names


# ---------------------------------------------------------------------------
# SqlGlotParser
# ---------------------------------------------------------------------------


class SqlGlotParser:
    """SQLGlot-backed :class:`SqlParser` implementation."""

    def parse_one(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        raise_on_error: bool = True,
    ) -> ParseResult:
        """Parse a single SQL statement."""
        try:
            if raise_on_error:
                ast = sqlglot.parse_one(
                    sql,
                    read=_dialect_value(dialect),
                    error_level=ErrorLevel.RAISE,
                )
            else:
                ast = sqlglot.parse_one(
                    sql,
                    read=_dialect_value(dialect),
                    error_level=ErrorLevel.WARN,
                )
        except ParseError as exc:
            if raise_on_error:
                raise SqlParseError(f"Failed to parse SQL: {exc}") from exc
            # Return empty result with warning.
            return ParseResult(
                statements=(),
                dialect=dialect,
                warnings=[str(exc)],
            )

        node = _to_sql_node(ast, dialect)
        return ParseResult(
            statements=(node,),
            dialect=dialect,
            warnings=[],
        )

    def parse_multi(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ParseResult:
        """Parse potentially multi-statement SQL."""
        try:
            asts = sqlglot.parse(sql, read=_dialect_value(dialect))
        except Exception as exc:
            raise SqlParseError(f"Failed to parse multi-statement SQL: {exc}") from exc

        nodes: list[SqlNode] = []
        warnings: list[str] = []
        for ast in asts:
            if ast is None:
                warnings.append("Empty statement encountered")
                continue
            nodes.append(_to_sql_node(ast, dialect))

        return ParseResult(
            statements=tuple(nodes),
            dialect=dialect,
            warnings=warnings,
        )


# ---------------------------------------------------------------------------
# SqlGlotRenderer
# ---------------------------------------------------------------------------


class SqlGlotRenderer:
    """SQLGlot-backed :class:`SqlRenderer` implementation."""

    def render(
        self,
        node: SqlNode,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        pretty: bool = False,
        normalize_keywords: bool = True,
    ) -> str:
        """Render an AST node to a SQL string."""
        raw = node.raw
        if raw is None:
            raise ValueError("SqlNode has no raw expression attached")

        if not isinstance(raw, exp.Expression):
            raise TypeError(f"Expected sqlglot Expression, got {type(raw).__name__}")

        return raw.sql(
            dialect=_dialect_value(dialect),
            pretty=pretty,
            normalize=normalize_keywords,
        )

    def render_expression(
        self,
        node: SqlNode,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> str:
        """Render a single expression fragment to SQL."""
        raw = node.raw
        if raw is None:
            return node.sql_text or ""

        if not isinstance(raw, exp.Expression):
            return node.sql_text or ""

        try:
            return raw.sql(dialect=_dialect_value(dialect))
        except Exception:
            return node.sql_text or ""


# ---------------------------------------------------------------------------
# SqlGlotScopeAnalyzer
# ---------------------------------------------------------------------------


class SqlGlotScopeAnalyzer:
    """SQLGlot-backed :class:`SqlScopeAnalyzer` implementation.

    Consolidates scope-analysis logic from ``ast_parser.py`` (lines 153-175)
    and column extraction from ``ast_parser.py`` (lines 188-218).
    """

    def extract_tables(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ScopeResult:
        """Extract table references with CTE-aware scope resolution."""
        try:
            ast = sqlglot.parse_one(
                sql,
                read=_dialect_value(dialect),
                error_level=ErrorLevel.RAISE,
            )
        except ParseError as exc:
            raise SqlParseError(f"Failed to parse SQL for table extraction: {exc}") from exc

        cte_names = _collect_cte_names(ast)
        tables: set[TableRef] = set()

        # Attempt scope-based analysis (preferred — correctly handles CTEs
        # and subqueries).
        root_scope = build_scope(ast)
        if root_scope is None:
            # Fallback: walk the AST directly if scope building is not
            # supported for this statement type (e.g. DDL).
            for table in ast.find_all(exp.Table):
                ref = _normalise_table_name(table)
                if ref.name and ref.name not in cte_names:
                    tables.add(ref)
        else:
            for scope in root_scope.traverse():
                for _source_name, source in scope.sources.items():
                    if isinstance(source, exp.Table):
                        ref = _normalise_table_name(source)
                        if ref.name and ref.name not in cte_names:
                            tables.add(ref)

        # Sort for determinism.
        sorted_tables = tuple(sorted(tables, key=lambda t: t.fully_qualified))
        sorted_ctes = tuple(sorted(cte_names))

        return ScopeResult(
            referenced_tables=sorted_tables,
            cte_names=sorted_ctes,
        )

    def extract_columns(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ColumnExtractionResult:
        """Extract column references and output columns from SQL."""
        try:
            ast = sqlglot.parse_one(
                sql,
                read=_dialect_value(dialect),
                error_level=ErrorLevel.RAISE,
            )
        except ParseError as exc:
            raise SqlParseError(f"Failed to parse SQL for column extraction: {exc}") from exc

        # --- Output columns (from top-level SELECT) ---
        output_columns = self._extract_output_columns(ast, dialect)

        # --- Referenced columns (all column refs in the query) ---
        referenced: list[ColumnRef] = []
        for col_node in ast.find_all(exp.Column):
            col_name = col_node.name
            if col_name:
                table_name = None
                table_node = col_node.args.get("table")
                if table_node is not None and hasattr(table_node, "name"):
                    table_name = table_node.name or None
                referenced.append(ColumnRef(table=table_name, name=col_name))

        # Deduplicate and sort referenced columns.
        seen: set[tuple[str | None, str]] = set()
        unique_refs: list[ColumnRef] = []
        for ref in referenced:
            key = (ref.table, ref.name)
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)
        unique_refs.sort(key=lambda r: (r.table or "", r.name))

        # --- Boolean flags ---
        has_star = any(True for _ in ast.find_all(exp.Star))
        has_aggregation = any(True for _ in ast.find_all(exp.AggFunc))
        has_window_functions = any(True for _ in ast.find_all(exp.Window))

        return ColumnExtractionResult(
            output_columns=tuple(output_columns),
            referenced_columns=tuple(unique_refs),
            has_star=has_star,
            has_aggregation=has_aggregation,
            has_window_functions=has_window_functions,
        )

    @staticmethod
    def _extract_output_columns(
        ast: exp.Expression,
        dialect: Dialect,
    ) -> list[str]:
        """Extract output column names from the top-level SELECT.

        Logic consolidated from ``ast_parser.py`` lines 188-218.
        """
        columns: list[str] = []

        # Find the outermost SELECT.
        select: exp.Select | None = None
        if isinstance(ast, exp.Select):
            select = ast
        else:
            found = ast.find(exp.Select)
            if isinstance(found, exp.Select):
                select = found

        if select is None:
            return columns

        for expression in select.expressions:
            alias_node = expression.args.get("alias")
            if alias_node is not None and hasattr(alias_node, "name") and alias_node.name:
                columns.append(alias_node.name)
            elif isinstance(expression, exp.Column):
                columns.append(expression.name)
            elif isinstance(expression, exp.Star):
                columns.append("*")
            else:
                # Fallback: use the SQL representation.
                columns.append(expression.sql(dialect=_dialect_value(dialect)))

        return sorted(columns)


# ---------------------------------------------------------------------------
# SqlGlotTranspiler
# ---------------------------------------------------------------------------


class SqlGlotTranspiler:
    """SQLGlot-backed :class:`SqlTranspiler` implementation."""

    def transpile(
        self,
        sql: str,
        source_dialect: Dialect,
        target_dialect: Dialect,
        *,
        pretty: bool = False,
    ) -> TranspileResult:
        """Transpile SQL from one dialect to another.

        Falls back to returning the original SQL with ``fallback_used=True``
        if transpilation fails, matching the existing behavior in
        ``local_executor.py`` and ``execution_service.py``.
        """
        try:
            results = sqlglot.transpile(
                sql,
                read=_dialect_value(source_dialect),
                write=_dialect_value(target_dialect),
                pretty=pretty,
            )
            if results:
                return TranspileResult(
                    output_sql=results[0],
                    source_dialect=source_dialect,
                    target_dialect=target_dialect,
                    warnings=[],
                    fallback_used=False,
                )
        except SqlglotError as exc:
            logger.warning(
                "sqlglot transpilation failed (%s → %s): %s",
                source_dialect.value,
                target_dialect.value,
                exc,
            )
            return TranspileResult(
                output_sql=sql,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
                warnings=[f"Transpilation failed: {exc}"],
                fallback_used=True,
            )
        except Exception as exc:
            logger.warning(
                "Unexpected transpilation error (%s → %s): %s",
                source_dialect.value,
                target_dialect.value,
                exc,
            )
            return TranspileResult(
                output_sql=sql,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
                warnings=[f"Unexpected error: {exc}"],
                fallback_used=True,
            )

        # Empty result from transpile — return original with fallback.
        return TranspileResult(
            output_sql=sql,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            warnings=["Transpilation produced no output"],
            fallback_used=True,
        )


# ---------------------------------------------------------------------------
# SqlGlotNormalizer
# ---------------------------------------------------------------------------


class SqlGlotNormalizer:
    """SQLGlot-backed :class:`SqlNormalizer` implementation.

    Consolidates normalisation logic from ``normalizer.py`` including CTE
    reordering with forward-reference detection.
    """

    def normalize(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        canonicalization_version: str = "v1",
    ) -> NormalizationResult:
        """Normalise SQL to a canonical form.

        Normalisation rules (v1):
        1. Strip comments (line and block).
        2. Parse via sqlglot.
        3. Reorder CTEs alphabetically (when no forward references exist).
        4. Regenerate with ``normalize=True`` and ``pretty=False``.
        """
        original = sql
        applied_rules: list[str] = []

        if canonicalization_version != "v1":
            logger.warning(
                "Unknown canonicalization version '%s'; falling back to v1",
                canonicalization_version,
            )

        # Rule 1: Strip comments.
        cleaned = _LINE_COMMENT_RE.sub("", sql)
        cleaned = _BLOCK_COMMENT_RE.sub("", cleaned)
        cleaned = cleaned.strip()
        applied_rules.append("strip_comments")

        if not cleaned:
            return NormalizationResult(
                normalized_sql="",
                original_sql=original,
                applied_rules=tuple(applied_rules),
            )

        # Rule 2: Parse.
        try:
            parsed = sqlglot.parse_one(cleaned, read=_dialect_value(dialect))
        except ParseError as exc:
            raise SqlNormalizationError(f"Failed to canonicalize SQL: {cleaned[:200]}") from exc
        applied_rules.append("parse_and_regenerate")

        # Rule 3: Reorder CTEs alphabetically when safe.
        if self._reorder_ctes_if_safe(parsed):
            applied_rules.append("reorder_ctes_alphabetically")

        # Rule 4: Regenerate with consistent formatting.
        try:
            result = parsed.sql(
                dialect=_dialect_value(dialect),
                pretty=False,
                normalize=True,
            )
            if not result:
                raise SqlNormalizationError(f"Failed to canonicalize SQL: {cleaned[:200]}")
        except SqlNormalizationError:
            raise
        except Exception as exc:
            raise SqlNormalizationError(f"Failed to canonicalize SQL: {cleaned[:200]}") from exc

        applied_rules.append("normalize_keywords")

        return NormalizationResult(
            normalized_sql=result,
            original_sql=original,
            applied_rules=tuple(applied_rules),
        )

    @staticmethod
    def _reorder_ctes_if_safe(tree: exp.Expression) -> bool:
        """Reorder CTE definitions alphabetically if no forward references.

        Returns ``True`` if reordering was applied, ``False`` otherwise.

        Logic consolidated from ``normalizer.py`` lines 123-165.
        """
        with_clause = tree.find(exp.With)
        if with_clause is None:
            return False

        ctes = list(with_clause.expressions)
        if len(ctes) <= 1:
            return False

        # Build name → position map for current order.
        cte_names: list[str] = []
        for cte in ctes:
            alias = cte.find(exp.TableAlias)
            name = alias.alias_or_name if alias else ""
            cte_names.append(name.lower())

        name_to_pos = {name: i for i, name in enumerate(cte_names)}

        # Check for forward references: if any CTE references a CTE that
        # appears later in the current order, reordering is unsafe.
        for i, cte in enumerate(ctes):
            for table_ref in cte.find_all(exp.Table):
                ref_name = table_ref.name.lower() if table_ref.name else ""
                if ref_name in name_to_pos and name_to_pos[ref_name] > i:
                    # Forward reference detected — preserve original order.
                    return False

        # Safe to reorder alphabetically.
        sorted_ctes = sorted(
            ctes,
            key=lambda c: c.find(exp.TableAlias).alias_or_name.lower() if c.find(exp.TableAlias) else "",
        )

        # Check if already in order.
        if sorted_ctes == ctes:
            return False

        with_clause.set("expressions", sorted_ctes)
        return True


# ---------------------------------------------------------------------------
# SqlGlotDiffer
# ---------------------------------------------------------------------------


class SqlGlotDiffer:
    """SQLGlot-backed :class:`SqlDiffer` implementation.

    Consolidates diff logic from ``ast_diff.py`` including the two-phase
    cosmetic check and full AST diff.
    """

    def diff(
        self,
        old_sql: str,
        new_sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> AstDiffResult:
        """Compute a semantic diff between two SQL statements.

        Two-phase approach:
        1. Fast cosmetic check via transpile normalisation.
        2. Full AST diff via ``sqlglot.diff()``.
        """
        # Phase 1: Cosmetic check — if normalised forms are identical,
        # the change is purely cosmetic.
        is_cosmetic = self._is_cosmetic_only(old_sql, new_sql, dialect)
        if is_cosmetic:
            return AstDiffResult(
                edits=(),
                is_identical=False,
                is_cosmetic_only=True,
            )

        # Phase 2: Full AST diff.
        dialect_str = _dialect_value(dialect)
        try:
            base_ast = sqlglot.parse_one(old_sql, read=dialect_str)
            target_ast = sqlglot.parse_one(new_sql, read=dialect_str)
        except (SqlglotError, Exception):
            logger.warning("sqlglot parse failed during diff; defaulting to non-identical.")
            return AstDiffResult(
                edits=(),
                is_identical=False,
                is_cosmetic_only=False,
            )

        try:
            raw_edits = sqlglot_diff_module(base_ast, target_ast)
        except Exception:
            logger.warning("sqlglot diff failed; defaulting to non-identical.")
            return AstDiffResult(
                edits=(),
                is_identical=False,
                is_cosmetic_only=False,
            )

        # Filter out Keep operations to find semantic changes.
        semantic_changes = [e for e in raw_edits if not isinstance(e, Keep)]

        if not semantic_changes:
            return AstDiffResult(
                edits=(),
                is_identical=True,
                is_cosmetic_only=False,
            )

        # Convert sqlglot edit objects to our DiffEdit type.
        edits = self._convert_edits(semantic_changes, dialect_str)

        return AstDiffResult(
            edits=tuple(edits),
            is_identical=False,
            is_cosmetic_only=False,
        )

    def extract_column_changes(
        self,
        old_sql: str,
        new_sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> dict[str, str]:
        """Extract column-level changes between two SELECT statements.

        Returns ``{column_name: "added" | "removed" | "modified"}``.
        """
        dialect_str = _dialect_value(dialect)
        try:
            old_ast = sqlglot.parse_one(old_sql, read=dialect_str)
            new_ast = sqlglot.parse_one(new_sql, read=dialect_str)
        except Exception:
            return {}

        old_cols = self._extract_select_columns(old_ast, dialect_str)
        new_cols = self._extract_select_columns(new_ast, dialect_str)

        old_keys = set(old_cols)
        new_keys = set(new_cols)

        changes: dict[str, str] = {}

        for name in sorted(new_keys - old_keys):
            changes[name] = "added"

        for name in sorted(old_keys - new_keys):
            changes[name] = "removed"

        for name in sorted(old_keys & new_keys):
            if old_cols[name] != new_cols[name]:
                changes[name] = "modified"

        return changes

    @staticmethod
    def _is_cosmetic_only(
        old_sql: str,
        new_sql: str,
        dialect: Dialect,
    ) -> bool:
        """Check if two SQL strings differ only cosmetically.

        Both are normalised through sqlglot's transpiler with formatting
        stripped.  Logic from ``ast_diff.py`` lines 105-121.
        """
        dialect_str = _dialect_value(dialect)
        try:
            norm_old = sqlglot.transpile(old_sql, read=dialect_str, write=dialect_str, pretty=False)[0]
            norm_new = sqlglot.transpile(new_sql, read=dialect_str, write=dialect_str, pretty=False)[0]
        except Exception:
            return False

        return norm_old == norm_new

    @staticmethod
    def _extract_select_columns(
        ast: exp.Expression,
        dialect_str: str,
    ) -> dict[str, str]:
        """Extract ``{column_alias_or_name: sql_text}`` from a SELECT.

        Logic from ``ast_diff.py`` lines 171-184.
        """
        columns: dict[str, str] = {}
        for expr in ast.expressions:
            name = expr.alias_or_name
            sql_text = expr.sql(dialect=dialect_str)
            columns[name] = sql_text
        return columns

    @staticmethod
    def _convert_edits(
        semantic_changes: list[Any],
        dialect_str: str,
        max_entries: int = 50,
    ) -> list[DiffEdit]:
        """Convert sqlglot diff edit objects to :class:`DiffEdit` instances.

        Logic from ``ast_diff.py`` lines 214-242.
        """
        edits: list[DiffEdit] = []
        for change in semantic_changes:
            if len(edits) >= max_entries:
                break

            cls_name = type(change).__name__

            # Map sqlglot diff types to our DiffEditKind.
            if cls_name == "Insert":
                kind = DiffEditKind.INSERT
            elif cls_name == "Remove":
                kind = DiffEditKind.REMOVE
            elif cls_name == "Update":
                kind = DiffEditKind.UPDATE
            elif cls_name == "Move":
                kind = DiffEditKind.MOVE
            else:
                kind = DiffEditKind.UPDATE

            # Extract SQL fragments from the edit.
            source_sql = ""
            target_sql = ""

            if hasattr(change, "source") and change.source is not None:
                try:
                    source_sql = change.source.sql(dialect=dialect_str)
                except Exception:
                    source_sql = repr(change.source)

            if hasattr(change, "target") and change.target is not None:
                try:
                    target_sql = change.target.sql(dialect=dialect_str)
                except Exception:
                    target_sql = repr(change.target)

            # For Insert/Remove, the node is on .expression attribute.
            if not source_sql and not target_sql:
                node = getattr(change, "expression", None)
                if node is not None:
                    try:
                        source_sql = node.sql(dialect=dialect_str)
                    except Exception:
                        source_sql = repr(node)

            edits.append(
                DiffEdit(
                    kind=kind,
                    source_sql=source_sql,
                    target_sql=target_sql,
                )
            )

        # Sort for determinism.
        edits.sort(key=lambda e: (e.kind.value, e.source_sql, e.target_sql))
        return edits


# ---------------------------------------------------------------------------
# SqlGlotSafetyGuard
# ---------------------------------------------------------------------------


# Mapping from DROP kind to violation type string.
_DROP_KIND_MAP: dict[str, str] = {
    "TABLE": "DROP_TABLE",
    "VIEW": "DROP_VIEW",
    "SCHEMA": "DROP_SCHEMA",
    "DATABASE": "DROP_SCHEMA",
}


class SqlGlotSafetyGuard:
    """SQLGlot-backed :class:`SqlSafetyGuard` implementation.

    Consolidates ALL detection logic from ``sql_guard.py`` lines 190-442.
    Uses AST-based detection (never regex on raw SQL) to prevent obfuscation.
    """

    def check(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        allow_create: bool = True,
        allow_insert: bool = False,
    ) -> SafetyCheckResult:
        """Check SQL for dangerous operations."""
        dialect_str = _dialect_value(dialect)
        violations: list[SafetyViolation] = []

        try:
            statements = sqlglot.parse(sql, read=dialect_str)
        except Exception as exc:
            logger.warning("SQL safety guard could not parse input: %s", exc)
            violations.append(
                SafetyViolation(
                    violation_type="UNPARSEABLE",
                    target="",
                    detail=f"SQL could not be parsed for safety analysis: {exc}",
                    severity="error",
                )
            )
            return SafetyCheckResult(
                is_safe=False,
                violations=tuple(violations),
                checked_statements=0,
            )

        checked = 0
        for statement in statements:
            if statement is None:
                continue
            checked += 1
            stmt_violations = self._check_statement(
                statement,
                dialect_str,
                allow_create=allow_create,
                allow_insert=allow_insert,
            )
            violations.extend(stmt_violations)

        return SafetyCheckResult(
            is_safe=len(violations) == 0,
            violations=tuple(violations),
            checked_statements=checked,
        )

    @staticmethod
    def _check_statement(
        node: exp.Expression,
        dialect_str: str,
        *,
        allow_create: bool,
        allow_insert: bool,
    ) -> list[SafetyViolation]:
        """Inspect a single AST statement for dangerous operations.

        Comprehensive detection logic consolidated from ``sql_guard.py``
        ``_check_statement()`` (lines 190-377).
        """
        violations: list[SafetyViolation] = []

        for descendant in node.walk():
            # walk() yields (node, parent, key) tuples.
            if isinstance(descendant, tuple):
                descendant = descendant[0]

            # --- DROP TABLE / VIEW / SCHEMA ---
            if isinstance(descendant, exp.Drop):
                kind = (descendant.args.get("kind") or "").upper()
                violation_type = _DROP_KIND_MAP.get(kind)
                if violation_type is not None:
                    target = descendant.this.sql(dialect=dialect_str) if descendant.this else "unknown"
                    violations.append(
                        SafetyViolation(
                            violation_type=violation_type,
                            target=target,
                            detail=f"DROP {kind} detected on `{target}`",
                            severity="error",
                        )
                    )

            # --- TRUNCATE TABLE ---
            if isinstance(descendant, exp.TruncateTable):
                tables = descendant.args.get("expressions") or []
                target = ", ".join(t.sql(dialect=dialect_str) for t in tables) or "unknown"
                violations.append(
                    SafetyViolation(
                        violation_type="TRUNCATE",
                        target=target,
                        detail=f"TRUNCATE TABLE detected on `{target}`",
                        severity="error",
                    )
                )

            # --- DELETE without WHERE ---
            if isinstance(descendant, exp.Delete):
                where = descendant.args.get("where")
                if where is None:
                    target = descendant.this.sql(dialect=dialect_str) if descendant.this else "unknown"
                    violations.append(
                        SafetyViolation(
                            violation_type="DELETE_WITHOUT_WHERE",
                            target=target,
                            detail=(f"DELETE without WHERE clause on `{target}` would remove all rows"),
                            severity="error",
                        )
                    )

            # --- ALTER TABLE ... DROP COLUMN ---
            if isinstance(descendant, exp.Alter):
                for action in descendant.args.get("actions") or []:
                    if isinstance(action, exp.Drop):
                        action_kind = (action.args.get("kind") or "").upper()
                        if action_kind in {"COLUMN", "COLUMNS"}:
                            violations.append(
                                SafetyViolation(
                                    violation_type="ALTER_DROP_COLUMN",
                                    target="",
                                    detail="ALTER TABLE ... DROP COLUMN detected",
                                    severity="error",
                                )
                            )

            # --- GRANT ---
            if isinstance(descendant, exp.Grant):
                violations.append(
                    SafetyViolation(
                        violation_type="GRANT",
                        target="",
                        detail="GRANT statement detected",
                        severity="error",
                    )
                )

            # --- REVOKE ---
            if hasattr(exp, "Revoke") and isinstance(descendant, exp.Revoke):
                violations.append(
                    SafetyViolation(
                        violation_type="REVOKE",
                        target="",
                        detail="REVOKE statement detected",
                        severity="error",
                    )
                )

            # --- INSERT OVERWRITE without partition ---
            if isinstance(descendant, exp.Insert):
                overwrite = descendant.args.get("overwrite")
                partition = descendant.args.get("partition")
                if overwrite and not partition:
                    target = descendant.this.sql(dialect=dialect_str) if descendant.this else "unknown"
                    violations.append(
                        SafetyViolation(
                            violation_type="INSERT_OVERWRITE_ALL",
                            target=target,
                            detail=(
                                f"INSERT OVERWRITE without PARTITION clause on `{target}` replaces the entire table"
                            ),
                            severity="warning",
                        )
                    )
                elif not allow_insert and not overwrite:
                    # Regular INSERT blocked when allow_insert=False.
                    target = descendant.this.sql(dialect=dialect_str) if descendant.this else "unknown"
                    violations.append(
                        SafetyViolation(
                            violation_type="INSERT",
                            target=target,
                            detail=f"INSERT INTO detected on `{target}`",
                            severity="warning",
                        )
                    )

            # --- Command fallback: TRUNCATE, ALTER DROP COLUMN, EXEC ---
            if isinstance(descendant, exp.Command):
                cmd_name = (str(descendant.this) or "").upper().strip()
                cmd_expr = str(descendant.expression or "").upper().strip()
                full_cmd = f"{cmd_name} {cmd_expr}".strip()

                if cmd_name == "TRUNCATE":
                    violations.append(
                        SafetyViolation(
                            violation_type="TRUNCATE",
                            target="",
                            detail=f"TRUNCATE detected via command: `{full_cmd}`",
                            severity="error",
                        )
                    )

                if cmd_name == "ALTER" and "DROP" in cmd_expr and "COLUMN" in cmd_expr:
                    violations.append(
                        SafetyViolation(
                            violation_type="ALTER_DROP_COLUMN",
                            target="",
                            detail="ALTER TABLE ... DROP COLUMN detected via command fallback",
                            severity="error",
                        )
                    )

                if cmd_name in {"EXEC", "EXECUTE"}:
                    violations.append(
                        SafetyViolation(
                            violation_type="RAW_EXEC",
                            target="",
                            detail=f"Raw EXEC/EXECUTE detected: `{full_cmd[:80]}`",
                            severity="error",
                        )
                    )

                if cmd_name == "CREATE" and _RE_CREATE_USER.search(cmd_expr):
                    violations.append(
                        SafetyViolation(
                            violation_type="CREATE_USER",
                            target="",
                            detail="CREATE USER detected",
                            severity="error",
                        )
                    )

            # --- CREATE USER via exp.Create ---
            if isinstance(descendant, exp.Create):
                kind = (descendant.args.get("kind") or "").upper()
                if kind == "USER":
                    violations.append(
                        SafetyViolation(
                            violation_type="CREATE_USER",
                            target="",
                            detail="CREATE USER detected",
                            severity="error",
                        )
                    )
                elif not allow_create and kind in {"TABLE", "VIEW", "SCHEMA", "DATABASE"}:
                    target = descendant.this.sql(dialect=dialect_str) if descendant.this else "unknown"
                    violations.append(
                        SafetyViolation(
                            violation_type=f"CREATE_{kind}",
                            target=target,
                            detail=f"CREATE {kind} detected on `{target}`",
                            severity="warning",
                        )
                    )

        return violations


# ---------------------------------------------------------------------------
# SqlGlotRewriter
# ---------------------------------------------------------------------------


class SqlGlotRewriter:
    """SQLGlot-backed :class:`SqlRewriter` implementation.

    Consolidates rewriting logic from ``sql_rewriter.py``.
    """

    def rewrite_tables(
        self,
        sql: str,
        rules: list[RewriteRule],
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> RewriteResult:
        """Rewrite table references according to the given rules.

        Uses AST-based in-place mutation for correctness with CTEs,
        subqueries, and quoted identifiers.
        """
        dialect_str = _dialect_value(dialect)

        if not rules:
            return RewriteResult(
                rewritten_sql=sql,
                tables_rewritten=(),
                tables_unchanged=(),
            )

        try:
            parsed = sqlglot.parse(sql, read=dialect_str)
        except ParseError as exc:
            logger.warning(
                "SQLGlot parse error during rewrite; returning original SQL: %s",
                exc,
            )
            return RewriteResult(
                rewritten_sql=sql,
                tables_rewritten=(),
                tables_unchanged=(),
            )

        rewritten: list[TableRef] = []
        unchanged: list[TableRef] = []

        rewritten_parts: list[str] = []
        for statement in parsed:
            if statement is None:
                continue
            for table in statement.find_all(exp.Table):
                ref = _normalise_table_name(table)
                matched = self._apply_rules(table, rules)
                if matched:
                    rewritten.append(ref)
                else:
                    unchanged.append(ref)
            rewritten_parts.append(statement.sql(dialect=dialect_str))

        if not rewritten_parts:
            return RewriteResult(
                rewritten_sql=sql,
                tables_rewritten=(),
                tables_unchanged=(),
            )

        output_sql = "; ".join(rewritten_parts)

        return RewriteResult(
            rewritten_sql=output_sql,
            tables_rewritten=tuple(sorted(set(rewritten), key=lambda t: t.fully_qualified)),
            tables_unchanged=tuple(sorted(set(unchanged), key=lambda t: t.fully_qualified)),
        )

    def quote_identifier(
        self,
        name: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> str:
        """Safely quote an identifier for the given dialect."""
        return exp.to_identifier(name, quoted=True).sql(dialect=_dialect_value(dialect))

    @staticmethod
    def _apply_rules(table: exp.Table, rules: list[RewriteRule]) -> bool:
        """Apply rewrite rules to a single table reference.

        Returns ``True`` if a rule matched and the table was rewritten.

        Logic consolidated from ``sql_rewriter.py`` ``_rewrite_table()``
        (lines 106-149).  Handles fully-qualified, schema-qualified,
        catalog-qualified, and unqualified table references.
        """
        current_catalog = table.catalog or ""
        current_schema = table.db or ""

        for rule in rules:
            matched = False

            # Fully qualified: catalog.schema.table
            if current_catalog and current_schema:
                if (
                    rule.source_catalog
                    and rule.source_schema
                    and current_catalog.lower() == rule.source_catalog.lower()
                    and current_schema.lower() == rule.source_schema.lower()
                ):
                    matched = True

            # Schema-qualified only: schema.table
            elif current_schema and not current_catalog:
                if rule.source_schema and current_schema.lower() == rule.source_schema.lower():
                    matched = True

            # Catalog-qualified only (rare): catalog..table
            elif current_catalog and not current_schema:
                if rule.source_catalog and current_catalog.lower() == rule.source_catalog.lower():
                    matched = True

            # Unqualified table: apply target unconditionally.
            else:
                if rule.target_catalog or rule.target_schema:
                    matched = True

            if matched:
                if rule.target_catalog:
                    table.set("catalog", exp.to_identifier(rule.target_catalog))
                if rule.target_schema:
                    table.set("db", exp.to_identifier(rule.target_schema))
                return True

        return False


# ---------------------------------------------------------------------------
# SqlGlotToolkit — Composite Implementation
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Lineage Analyzer
# ---------------------------------------------------------------------------

_AGG_EXPRESSION_NAMES: frozenset[str] = frozenset(
    {
        "Sum",
        "Count",
        "Avg",
        "Min",
        "Max",
        "ArrayAgg",
        "GroupConcat",
        "Stddev",
        "StddevPop",
        "StddevSamp",
        "Variance",
        "VariancePop",
        "ApproxDistinct",
        "CountIf",
        "Percentile",
    }
)


class SqlGlotLineageAnalyzer:
    """SQLGlot-backed column lineage analyzer.

    Uses ``sqlglot.lineage.lineage()`` to trace each output column back
    through CTEs, joins, subqueries, and expressions to source table
    columns.  Falls back to conservative behaviour (marking columns as
    unresolved) when sqlglot cannot determine lineage — this matches the
    codebase's principle of "when in doubt, assume changed/unknown".
    """

    def trace_column_lineage(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        schema: dict[str, dict[str, str]] | None = None,
    ) -> ColumnLineageResult:
        """Trace lineage for all output columns in a SQL statement.

        When a schema is provided and the query contains ``SELECT *``,
        this method uses ``sqlglot.optimizer.qualify`` to expand the
        star into explicit column references before tracing.  This
        converts an otherwise-unresolvable ``*`` into full per-column
        lineage — a critical improvement for migration validation where
        users need to verify every column survived the transition.

        Without a schema, ``*`` is still recorded as unresolved (safe
        default — we never fabricate lineage data we can't prove).
        """
        dialect_str = _dialect_value(dialect)

        # Parse to extract output column names.
        try:
            ast = sqlglot.parse_one(sql, read=dialect_str, error_level=ErrorLevel.RAISE)
        except (ParseError, SqlglotError) as exc:
            raise SqlLineageError(f"Cannot parse SQL for lineage: {exc}") from exc

        output_columns = self._extract_output_column_names(ast)

        # ── SELECT * expansion ──────────────────────────────────────
        # When the query uses ``SELECT *`` and we have schema info,
        # use sqlglot's qualify optimizer to expand ``*`` into the
        # actual column list.  This produces a rewritten SQL string
        # that the lineage engine can trace column-by-column.
        effective_sql = sql
        if "*" in output_columns and schema:
            try:
                mapping_schema = MappingSchema(schema, dialect=dialect_str)
                qualified_ast = sqlglot_qualify(
                    ast,
                    schema=mapping_schema,
                    dialect=dialect_str,
                    validate_qualify_columns=False,
                )
                expanded_columns = self._extract_output_column_names(qualified_ast)
                if expanded_columns and "*" not in expanded_columns:
                    # Expansion succeeded — use the qualified SQL and
                    # the expanded column list for lineage tracing.
                    output_columns = expanded_columns
                    effective_sql = qualified_ast.sql(dialect=dialect_str)
                    logger.debug(
                        "SELECT * expanded to %d columns via qualify",
                        len(expanded_columns),
                    )
            except Exception:
                # Expansion failed — fall back to original SQL with
                # ``*`` marked as unresolved (safe default).
                logger.debug(
                    "SELECT * expansion via qualify failed; falling back",
                    exc_info=True,
                )

        column_lineage: dict[str, tuple[ColumnLineageNode, ...]] = {}
        unresolved: list[str] = []

        for col_name in output_columns:
            try:
                nodes = self.trace_single_column(col_name, effective_sql, dialect, schema=schema)
                column_lineage[col_name] = nodes
            except (SqlLineageError, Exception):
                # Conservative: mark as unresolved rather than crash.
                unresolved.append(col_name)
                logger.debug(
                    "Column lineage unresolved for '%s': falling back to unresolved",
                    col_name,
                    exc_info=True,
                )

        return ColumnLineageResult(
            model_name="",  # Caller fills this in.
            column_lineage=column_lineage,
            unresolved_columns=tuple(sorted(unresolved)),
            dialect=dialect,
        )

    def trace_single_column(
        self,
        column: str,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        schema: dict[str, dict[str, str]] | None = None,
    ) -> tuple[ColumnLineageNode, ...]:
        """Trace a single output column to its source columns."""
        dialect_str = _dialect_value(dialect)

        # Build MappingSchema if schema provided.
        mapping_schema: MappingSchema | None = None
        if schema:
            try:
                mapping_schema = MappingSchema(schema, dialect=dialect_str)
            except Exception:
                logger.debug("Failed to build MappingSchema, proceeding without", exc_info=True)
                mapping_schema = None

        try:
            lineage_node = sqlglot_lineage(
                column,
                sql,
                schema=mapping_schema or {},
                dialect=dialect_str,
            )
        except Exception as exc:
            raise SqlLineageError(f"sqlglot.lineage failed for column '{column}': {exc}") from exc

        # Walk the lineage tree and collect all leaf (source) nodes.
        nodes: list[ColumnLineageNode] = []
        self._walk_lineage_tree(lineage_node, column, nodes)

        if not nodes:
            # Lineage returned but no source columns found — likely a literal.
            nodes.append(
                ColumnLineageNode(
                    column=column,
                    source_table=None,
                    source_column=None,
                    transform_type="literal",
                    transform_sql=lineage_node.expression.sql(dialect=dialect_str) if lineage_node.expression else "",
                )
            )

        return tuple(nodes)

    def _walk_lineage_tree(
        self,
        node: Any,
        output_column: str,
        acc: list[ColumnLineageNode],
    ) -> None:
        """Recursively walk a sqlglot LineageNode tree.

        Leaf nodes (no downstream) are the actual source columns.
        Interior nodes represent intermediate transforms.

        sqlglot's lineage API returns leaf nodes where:
        - ``node.name`` is a dot-separated string like ``"table.column"``
        - ``node.expression`` is typically ``exp.Column`` for direct
          references or ``exp.Table`` for table-level leaf nodes.
        We must parse ``node.name`` as a fallback when the expression
        is a Table (which carries table info but not the column name).
        """
        downstream = getattr(node, "downstream", [])

        if not downstream:
            # Leaf node — this is a source column.
            source_table: str | None = None
            source_column: str | None = None
            expression = getattr(node, "expression", None)
            name_str: str = getattr(node, "name", "") or ""

            if expression is not None:
                if isinstance(expression, exp.Column):
                    source_column = expression.name
                    if expression.table:
                        source_table = expression.table
                elif isinstance(expression, exp.Table):
                    # Table leaf: the expression holds the table, but the
                    # column name is encoded in ``node.name`` as
                    # ``"table.column"`` or just ``"column"``.
                    source_table = expression.name
                    if name_str:
                        # Parse "table.column" → extract column part.
                        parts = name_str.split(".")
                        if len(parts) >= 2:
                            source_column = parts[-1]
                            # If source_table is empty, try to get it
                            # from the dot-separated name.
                            if not source_table:
                                source_table = parts[-2]
                        else:
                            source_column = parts[0]
                else:
                    # Expression is something else (literal, function, etc.)
                    # Try to extract column from name_str.
                    if name_str:
                        parts = name_str.split(".")
                        source_column = parts[-1]
                        if len(parts) >= 2:
                            source_table = parts[-2]
                    else:
                        try:
                            source_column = expression.sql()
                        except Exception:
                            source_column = None
            elif name_str:
                # No expression at all, but name is available.
                parts = name_str.split(".")
                source_column = parts[-1]
                if len(parts) >= 2:
                    source_table = parts[-2]

            transform_type = self._classify_transform(node)
            transform_sql = ""
            if expression is not None:
                try:
                    transform_sql = expression.sql()
                except (SqlglotError, AttributeError):
                    pass

            acc.append(
                ColumnLineageNode(
                    column=output_column,
                    source_table=source_table,
                    source_column=source_column,
                    transform_type=transform_type,
                    transform_sql=transform_sql,
                )
            )
        else:
            # Interior node — recurse into downstream.
            for child in downstream:
                self._walk_lineage_tree(child, output_column, acc)

    @staticmethod
    def _classify_transform(node: Any) -> str:
        """Classify the transformation type of a lineage node.

        Inspects the expression attached to the node to determine the
        category of transformation applied.  Handles the fact that
        sqlglot's lineage API returns ``exp.Table`` expressions at
        leaf nodes for direct column references.
        """
        expression = getattr(node, "expression", None)
        if expression is None:
            return "direct"

        # Unwrap Alias to inspect the underlying expression.
        # sqlglot's lineage API often returns ``exp.Alias`` at leaf nodes
        # (e.g. ``42 AS magic_number``).  We need to classify based on
        # the inner expression, not the Alias wrapper itself.
        inner = expression
        if isinstance(inner, exp.Alias):
            inner = inner.this

        # Direct column reference.
        if isinstance(inner, exp.Column):
            return "direct"

        # Table leaf node in lineage tree — this represents a direct
        # source column reference (the column info is in node.name).
        if isinstance(inner, exp.Table):
            return "direct"

        inner_class_name = type(inner).__name__

        # Aggregate functions.
        if inner_class_name in _AGG_EXPRESSION_NAMES or isinstance(inner, exp.AggFunc):
            return "aggregation"

        # Window functions.
        if isinstance(inner, exp.Window):
            return "window"

        # CASE expressions.
        if isinstance(inner, (exp.Case, exp.If)):
            return "case"

        # Literal values.
        if isinstance(inner, (exp.Literal, exp.Null, exp.Boolean)):
            return "literal"

        # Check if inner expression contains aggregate functions (nested).
        for child_expr in inner.walk():
            if isinstance(child_expr, exp.AggFunc) or type(child_expr).__name__ in _AGG_EXPRESSION_NAMES:
                return "aggregation"
            if isinstance(child_expr, exp.Window):
                return "window"

        # Any other expression (arithmetic, string ops, casts, etc.).
        return "expression"

    @staticmethod
    def _extract_output_column_names(ast: exp.Expression) -> list[str]:
        """Extract output column names from a SELECT statement.

        Handles aliases, bare column references, and star expansions.
        Returns names in SELECT order.
        """
        columns: list[str] = []

        # Find the outermost SELECT.
        select = ast.find(exp.Select)
        if select is None:
            return columns

        for expr in select.expressions:
            if isinstance(expr, exp.Alias):
                columns.append(expr.alias)
            elif isinstance(expr, exp.Column):
                columns.append(expr.name)
            elif isinstance(expr, exp.Star):
                columns.append("*")
            else:
                # Unnamed expression — use SQL text as identifier.
                sql_text = expr.sql()
                columns.append(sql_text[:64] if len(sql_text) > 64 else sql_text)

        return columns


# ---------------------------------------------------------------------------
# Qualifier / Simplifier
# ---------------------------------------------------------------------------


class SqlGlotQualifier:
    """SQLGlot-backed column qualifier and boolean simplifier.

    Uses ``sqlglot.optimizer.qualify`` to resolve ambiguous column
    references when schema information is available, and
    ``sqlglot.optimizer.simplify`` to reduce boolean expressions.
    """

    def qualify_columns(
        self,
        sql: str,
        schema: dict[str, dict[str, str]],
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> QualifyResult:
        """Qualify unqualified column references using schema information."""
        dialect_str = _dialect_value(dialect)
        warnings: list[str] = []

        try:
            ast = sqlglot.parse_one(sql, read=dialect_str, error_level=ErrorLevel.RAISE)
        except (ParseError, SqlglotError) as exc:
            return QualifyResult(
                qualified_sql=sql,
                original_sql=sql,
                warnings=[f"Parse error: {exc}"],
            )

        # Count unqualified columns before.
        pre_unqualified = sum(1 for col in ast.find_all(exp.Column) if not col.table)

        try:
            mapping = MappingSchema(schema, dialect=dialect_str)
            qualified_ast = sqlglot_qualify(
                ast,
                schema=mapping,
                dialect=dialect_str,
                qualify_columns=True,
                qualify_tables=True,
                validate_qualify_columns=False,
            )
        except Exception as exc:
            logger.debug("Qualification failed: %s", exc, exc_info=True)
            warnings.append(f"Qualification partially failed: {exc}")
            qualified_ast = ast

        # Count unqualified columns after.
        post_unqualified = sum(1 for col in qualified_ast.find_all(exp.Column) if not col.table)
        columns_qualified = max(0, pre_unqualified - post_unqualified)

        qualified_sql = qualified_ast.sql(dialect=dialect_str)

        return QualifyResult(
            qualified_sql=qualified_sql,
            original_sql=sql,
            columns_qualified=columns_qualified,
            tables_qualified=0,
            warnings=warnings,
        )

    def simplify(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> SimplifyResult:
        """Simplify boolean expressions in SQL."""
        dialect_str = _dialect_value(dialect)

        try:
            ast = sqlglot.parse_one(sql, read=dialect_str, error_level=ErrorLevel.RAISE)
        except (ParseError, SqlglotError):
            return SimplifyResult(
                simplified_sql=sql,
                original_sql=sql,
                simplifications_applied=0,
            )

        original_node_count = sum(1 for _ in ast.walk())

        try:
            simplified_ast = sqlglot_simplify(ast, dialect=dialect_str)
        except Exception:
            logger.debug("Simplification failed, returning original", exc_info=True)
            return SimplifyResult(
                simplified_sql=sql,
                original_sql=sql,
                simplifications_applied=0,
            )

        simplified_node_count = sum(1 for _ in simplified_ast.walk())
        simplifications = max(0, original_node_count - simplified_node_count)
        simplified_sql = simplified_ast.sql(dialect=dialect_str)

        return SimplifyResult(
            simplified_sql=simplified_sql,
            original_sql=sql,
            simplifications_applied=simplifications,
        )


# ---------------------------------------------------------------------------
# Composite Toolkit
# ---------------------------------------------------------------------------


class SqlGlotToolkit:
    """Composite :class:`SqlToolkit` backed by SQLGlot.

    Instantiates all individual protocol implementations and exposes them
    as properties.  This is the default implementation returned by
    :func:`get_sql_toolkit`.
    """

    def __init__(self) -> None:
        self._parser = SqlGlotParser()
        self._renderer = SqlGlotRenderer()
        self._scope_analyzer = SqlGlotScopeAnalyzer()
        self._transpiler = SqlGlotTranspiler()
        self._normalizer = SqlGlotNormalizer()
        self._differ = SqlGlotDiffer()
        self._safety_guard = SqlGlotSafetyGuard()
        self._rewriter = SqlGlotRewriter()
        self._lineage_analyzer = SqlGlotLineageAnalyzer()
        self._qualifier = SqlGlotQualifier()

    @property
    def parser(self) -> SqlGlotParser:
        return self._parser

    @property
    def renderer(self) -> SqlGlotRenderer:
        return self._renderer

    @property
    def scope_analyzer(self) -> SqlGlotScopeAnalyzer:
        return self._scope_analyzer

    @property
    def transpiler(self) -> SqlGlotTranspiler:
        return self._transpiler

    @property
    def normalizer(self) -> SqlGlotNormalizer:
        return self._normalizer

    @property
    def differ(self) -> SqlGlotDiffer:
        return self._differ

    @property
    def safety_guard(self) -> SqlGlotSafetyGuard:
        return self._safety_guard

    @property
    def rewriter(self) -> SqlGlotRewriter:
        return self._rewriter

    @property
    def lineage_analyzer(self) -> SqlGlotLineageAnalyzer:
        return self._lineage_analyzer

    @property
    def qualifier(self) -> SqlGlotQualifier:
        return self._qualifier
