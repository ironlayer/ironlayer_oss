"""SQL toolkit shared types.

Every type here is implementation-agnostic. Consumer code operates on these
types exclusively. The backing implementation (SQLGlot, sqloxide, custom)
converts to/from its native types internally.

ZERO dependency on any SQL parsing library.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Dialect
# ---------------------------------------------------------------------------


class Dialect(str, enum.Enum):
    """Supported SQL dialects."""

    DATABRICKS = "databricks"
    DUCKDB = "duckdb"
    REDSHIFT = "redshift"


# ---------------------------------------------------------------------------
# AST Node Types
# ---------------------------------------------------------------------------


class SqlNodeKind(str, enum.Enum):
    """Enumeration of SQL node types that consumer code needs to inspect.

    This is NOT a 1:1 mapping to any parser's internal types — it is the
    subset that IronLayer actually uses.  Kept minimal to reduce coupling.
    """

    # Statement types
    SELECT = "select"
    CREATE = "create"
    INSERT = "insert"
    DELETE = "delete"
    DROP = "drop"
    ALTER = "alter"
    TRUNCATE = "truncate"
    MERGE = "merge"
    GRANT = "grant"
    REVOKE = "revoke"
    COMMAND = "command"

    # Clause types
    WITH = "with"
    CTE = "cte"
    FROM = "from"
    JOIN = "join"
    WHERE = "where"
    GROUP = "group"
    HAVING = "having"
    ORDER = "order"

    # Expression types
    TABLE = "table"
    COLUMN = "column"
    STAR = "star"
    ALIAS = "alias"
    TABLE_ALIAS = "table_alias"
    WINDOW = "window"
    AGG_FUNC = "agg_func"
    SUBQUERY = "subquery"

    # Databricks-specific
    PARTITIONED_BY = "partitioned_by"

    # Catch-all
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Reference Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TableRef:
    """A fully-resolved table reference.

    Immutable.  Deterministic ``__hash__`` and ``__eq__`` via *frozen=True*.
    """

    catalog: str | None = None
    schema: str | None = None
    name: str = ""

    @property
    def fully_qualified(self) -> str:
        """Return ``catalog.schema.name``, omitting None parts."""
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)

    def with_catalog(self, catalog: str) -> TableRef:
        """Return a copy with the catalog replaced."""
        return TableRef(catalog=catalog, schema=self.schema, name=self.name)

    def with_schema(self, schema: str) -> TableRef:
        """Return a copy with the schema replaced."""
        return TableRef(catalog=self.catalog, schema=schema, name=self.name)

    def __str__(self) -> str:  # pragma: no cover — convenience
        return self.fully_qualified


@dataclass(frozen=True, slots=True)
class ColumnRef:
    """A column reference, optionally qualified by table."""

    table: str | None = None
    name: str = ""

    def __str__(self) -> str:  # pragma: no cover — convenience
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name


# ---------------------------------------------------------------------------
# AST Wrapper
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SqlNode:
    """Opaque wrapper around an AST node.

    Consumer code can inspect ``kind``, ``name``, ``alias``, ``children``,
    and ``sql_text``.  The ``raw`` field holds the implementation-specific
    object (e.g. ``sqlglot.exp.Expression``) for escape-hatch operations.
    ``raw`` is excluded from ``__eq__`` / ``__hash__`` so that two nodes
    wrapping different implementation objects compare equal when their
    logical content matches.
    """

    kind: SqlNodeKind
    name: str = ""
    alias: str = ""
    children: tuple[SqlNode, ...] = ()
    sql_text: str = ""
    raw: Any = field(default=None, repr=False, compare=False, hash=False)

    # -- traversal helpers ---------------------------------------------------

    def find_all(self, kind: SqlNodeKind) -> list[SqlNode]:
        """Recursively find all descendant nodes of the given kind."""
        result: list[SqlNode] = []
        self._collect(kind, result)
        return result

    def _collect(self, kind: SqlNodeKind, acc: list[SqlNode]) -> None:
        for child in self.children:
            if child.kind == kind:
                acc.append(child)
            child._collect(kind, acc)

    def find(self, kind: SqlNodeKind) -> SqlNode | None:
        """Find the first descendant of *kind* (depth-first), or ``None``."""
        for child in self.children:
            if child.kind == kind:
                return child
            found = child.find(kind)
            if found is not None:
                return found
        return None

    def walk(self) -> list[SqlNode]:
        """Return a flat list of all descendant nodes (pre-order DFS)."""
        result: list[SqlNode] = []
        self._walk(result)
        return result

    def _walk(self, acc: list[SqlNode]) -> None:
        acc.append(self)
        for child in self.children:
            child._walk(acc)

    @property
    def descendant_count(self) -> int:
        """Total number of nodes in the subtree (including self)."""
        return 1 + sum(c.descendant_count for c in self.children)


# ---------------------------------------------------------------------------
# Result Containers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParseResult:
    """Result of parsing a SQL string.

    ``statements`` handles multi-statement SQL (separated by ``;``).
    """

    statements: tuple[SqlNode, ...]
    dialect: Dialect
    warnings: list[str] = field(default_factory=list)

    @property
    def single(self) -> SqlNode:
        """Return the single statement, or raise if zero / multiple."""
        if len(self.statements) != 1:
            raise ValueError(f"Expected exactly 1 statement, got {len(self.statements)}")
        return self.statements[0]


@dataclass(frozen=True, slots=True)
class ScopeResult:
    """Scope-aware table extraction result.

    ``referenced_tables`` has CTE names excluded — the key feature that
    prevents CTEs from appearing as external table dependencies.
    """

    referenced_tables: tuple[TableRef, ...]
    cte_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ColumnExtractionResult:
    """Columns extracted from a SQL statement."""

    output_columns: tuple[str, ...]
    referenced_columns: tuple[ColumnRef, ...]
    has_star: bool
    has_aggregation: bool
    has_window_functions: bool


@dataclass(frozen=True, slots=True)
class NormalizationResult:
    """Result of SQL normalisation."""

    normalized_sql: str
    original_sql: str
    applied_rules: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TranspileResult:
    """Result of transpiling SQL between dialects."""

    output_sql: str
    source_dialect: Dialect
    target_dialect: Dialect
    warnings: list[str] = field(default_factory=list)
    fallback_used: bool = False


class DiffEditKind(str, enum.Enum):
    """Types of AST edit operations."""

    KEEP = "keep"
    INSERT = "insert"
    REMOVE = "remove"
    UPDATE = "update"
    MOVE = "move"


@dataclass(frozen=True, slots=True)
class DiffEdit:
    """A single edit operation in an AST diff."""

    kind: DiffEditKind
    source_sql: str = ""
    target_sql: str = ""


@dataclass(frozen=True, slots=True)
class AstDiffResult:
    """Result of diffing two SQL ASTs."""

    edits: tuple[DiffEdit, ...]
    is_identical: bool
    is_cosmetic_only: bool


@dataclass(frozen=True, slots=True)
class SafetyViolation:
    """A dangerous SQL operation detected by the safety guard."""

    violation_type: str
    target: str
    detail: str
    severity: str  # "error" | "warning"


@dataclass(frozen=True, slots=True)
class SafetyCheckResult:
    """Result of a SQL safety check."""

    is_safe: bool
    violations: tuple[SafetyViolation, ...]
    checked_statements: int


@dataclass(frozen=True, slots=True)
class RewriteRule:
    """A table reference rewrite rule."""

    source_catalog: str | None = None
    source_schema: str | None = None
    target_catalog: str | None = None
    target_schema: str | None = None


@dataclass(frozen=True, slots=True)
class RewriteResult:
    """Result of rewriting table references in SQL."""

    rewritten_sql: str
    tables_rewritten: tuple[TableRef, ...]
    tables_unchanged: tuple[TableRef, ...]


# ---------------------------------------------------------------------------
# Column-Level Lineage Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ColumnLineageNode:
    """A single node in a column lineage trace.

    Represents one hop: an output column was derived from a source column,
    potentially through a transformation (expression).
    """

    column: str
    source_table: str | None = None
    source_column: str | None = None
    transform_type: str = "direct"  # direct|expression|aggregation|window|case|literal
    transform_sql: str = ""


@dataclass(frozen=True, slots=True)
class ColumnLineageResult:
    """Result of column lineage analysis for a single SQL statement.

    Maps each output column to the list of source columns it derives from.
    A single output column may derive from multiple source columns (e.g.,
    ``col_a + col_b AS total``).
    """

    model_name: str
    column_lineage: dict[str, tuple[ColumnLineageNode, ...]]
    unresolved_columns: tuple[str, ...] = ()
    dialect: Dialect = Dialect.DATABRICKS


@dataclass(frozen=True, slots=True)
class CrossModelColumnLineage:
    """End-to-end column lineage across the model DAG.

    Traces a single column from a target model back through all upstream
    models to the ultimate source tables/columns.
    """

    target_model: str
    target_column: str
    lineage_path: tuple[ColumnLineageNode, ...] = ()


# ---------------------------------------------------------------------------
# Qualifier / Simplifier Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualifyResult:
    """Result of column/table qualification."""

    qualified_sql: str
    original_sql: str
    columns_qualified: int = 0
    tables_qualified: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SimplifyResult:
    """Result of boolean expression simplification."""

    simplified_sql: str
    original_sql: str
    simplifications_applied: int = 0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SqlToolkitError(Exception):
    """Base exception for all sql_toolkit errors."""


class SqlParseError(SqlToolkitError):
    """SQL could not be parsed."""


class SqlTranspileError(SqlToolkitError):
    """SQL could not be transpiled between dialects."""


class SqlNormalizationError(SqlToolkitError):
    """SQL could not be normalised."""


class SqlLineageError(SqlToolkitError):
    """Column lineage analysis failed."""
