"""SQL toolkit protocol definitions.

These define the interface contract that ANY implementation must satisfy.
Consumer code depends on these protocols, never on concrete implementations.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ._types import (
    AstDiffResult,
    ColumnExtractionResult,
    ColumnLineageNode,
    ColumnLineageResult,
    Dialect,
    NormalizationResult,
    ParseResult,
    QualifyResult,
    RewriteResult,
    RewriteRule,
    SafetyCheckResult,
    ScopeResult,
    SimplifyResult,
    SqlNode,
    TranspileResult,
)

# ---------------------------------------------------------------------------
# Individual Capability Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class SqlParser(Protocol):
    """Parse SQL strings into AST representations."""

    def parse_one(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        raise_on_error: bool = True,
    ) -> ParseResult:
        """Parse a single SQL statement.

        Args:
            sql: The SQL string to parse.
            dialect: Source dialect.
            raise_on_error: If ``True``, raise :class:`SqlParseError` on
                invalid SQL.  If ``False``, return a ``ParseResult`` with
                warnings instead.

        Returns:
            ``ParseResult`` with a single statement.

        Raises:
            SqlParseError: If *raise_on_error* is ``True`` and SQL is invalid.
        """
        ...

    def parse_multi(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ParseResult:
        """Parse potentially multi-statement SQL (separated by ``;``).

        Returns:
            ``ParseResult`` with one or more statements.
        """
        ...


@runtime_checkable
class SqlRenderer(Protocol):
    """Render AST nodes back to SQL strings."""

    def render(
        self,
        node: SqlNode,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        pretty: bool = False,
        normalize_keywords: bool = True,
    ) -> str:
        """Render an AST node to a SQL string.

        Args:
            node: The AST node to render (must have ``raw`` set).
            dialect: Target dialect for rendering.
            pretty: If ``True``, format with indentation and newlines.
            normalize_keywords: If ``True``, uppercase SQL keywords.

        Returns:
            The rendered SQL string.
        """
        ...

    def render_expression(
        self,
        node: SqlNode,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> str:
        """Render a single expression fragment (column, function, …) to SQL."""
        ...


@runtime_checkable
class SqlScopeAnalyzer(Protocol):
    """Scope-aware analysis: resolve tables and columns through CTEs / subqueries."""

    def extract_tables(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ScopeResult:
        """Extract table references with CTE-aware scope resolution.

        CTE names are excluded from the returned table references.
        Subquery tables are included.  This is the correct behaviour for
        dependency extraction.
        """
        ...

    def extract_columns(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> ColumnExtractionResult:
        """Extract column references and output columns from SQL.

        Returns output columns (SELECT list), all referenced columns in the
        query, and boolean flags for star / aggregation / window function
        usage.
        """
        ...


@runtime_checkable
class SqlTranspiler(Protocol):
    """Transpile SQL between dialects."""

    def transpile(
        self,
        sql: str,
        source_dialect: Dialect,
        target_dialect: Dialect,
        *,
        pretty: bool = False,
    ) -> TranspileResult:
        """Transpile SQL from one dialect to another.

        If transpilation fails and the implementation supports graceful
        fallback, the original SQL is returned with
        ``TranspileResult.fallback_used = True``.

        Raises:
            SqlTranspileError: If transpilation fails without fallback.
        """
        ...


@runtime_checkable
class SqlNormalizer(Protocol):
    """Normalise SQL for deterministic hashing."""

    def normalize(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        canonicalization_version: str = "v1",
    ) -> NormalizationResult:
        """Normalise SQL to a canonical form suitable for content hashing.

        Normalisation rules (v1):
        1. Parse and regenerate via the SQL toolkit.
        2. Uppercase all SQL keywords.
        3. Normalise whitespace (collapse multiple spaces, trim).
        4. Strip comments.
        5. Fully qualify table references where possible.
        6. Reorder CTEs alphabetically (when no forward references exist).

        Raises:
            SqlNormalizationError: If SQL cannot be parsed for normalisation.
        """
        ...


@runtime_checkable
class SqlDiffer(Protocol):
    """Diff two SQL statements at the AST level."""

    def diff(
        self,
        old_sql: str,
        new_sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> AstDiffResult:
        """Compute a semantic diff between two SQL statements.

        Two-phase approach:
        1. Fast cosmetic check: normalise both and compare strings.
        2. Full AST diff: compute edit operations between the two ASTs.
        """
        ...

    def extract_column_changes(
        self,
        old_sql: str,
        new_sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> dict[str, str]:
        """Extract column-level changes between two SELECT statements.

        Returns ``{column_name: "added" | "removed" | "modified"}``.
        Columns present in both with identical expressions are omitted.
        """
        ...


@runtime_checkable
class SqlSafetyGuard(Protocol):
    """Detect dangerous SQL operations."""

    def check(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        allow_create: bool = True,
        allow_insert: bool = False,
    ) -> SafetyCheckResult:
        """Check SQL for dangerous operations.

        Detects: DROP, TRUNCATE, DELETE without WHERE, ALTER DROP COLUMN,
        GRANT, REVOKE, INSERT OVERWRITE without partition, raw EXEC /
        SYSTEM commands.

        Uses AST-based detection (not regex) to prevent obfuscation.
        """
        ...


@runtime_checkable
class SqlRewriter(Protocol):
    """Rewrite table references in SQL statements."""

    def rewrite_tables(
        self,
        sql: str,
        rules: list[RewriteRule],
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> RewriteResult:
        """Rewrite table references according to the given rules.

        Uses AST-based rewriting (not string replacement) for correctness
        with CTEs, subqueries, and quoted identifiers.
        """
        ...

    def quote_identifier(
        self,
        name: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> str:
        """Safely quote an identifier for the given dialect."""
        ...


@runtime_checkable
class SqlLineageAnalyzer(Protocol):
    """Trace column-level lineage through SQL statements."""

    def trace_column_lineage(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        schema: dict[str, dict[str, str]] | None = None,
    ) -> ColumnLineageResult:
        """Trace the lineage of all output columns in a SQL statement.

        Args:
            sql: A single SQL statement (SELECT with optional CTEs).
            dialect: SQL dialect for parsing.
            schema: Optional schema mapping ``{table_name: {column_name: type}}``.
                    Enables resolution of ``SELECT *`` and unqualified columns.

        Returns:
            ColumnLineageResult mapping each output column to its sources.

        Raises:
            SqlLineageError: If lineage analysis fails.
        """
        ...

    def trace_single_column(
        self,
        column: str,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
        *,
        schema: dict[str, dict[str, str]] | None = None,
    ) -> tuple[ColumnLineageNode, ...]:
        """Trace a single output column back to its source columns.

        Args:
            column: The output column name to trace.
            sql: The SQL statement containing the column.
            dialect: SQL dialect.
            schema: Optional schema mapping.

        Returns:
            Tuple of ColumnLineageNode entries for this column.
        """
        ...


@runtime_checkable
class SqlQualifier(Protocol):
    """Qualify column and table references using schema information."""

    def qualify_columns(
        self,
        sql: str,
        schema: dict[str, dict[str, str]],
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> QualifyResult:
        """Qualify unqualified column references using schema information.

        Resolves ambiguous column references (e.g., ``SELECT id`` becomes
        ``SELECT orders.id``) using the provided schema mapping.
        """
        ...

    def simplify(
        self,
        sql: str,
        dialect: Dialect = Dialect.DATABRICKS,
    ) -> SimplifyResult:
        """Simplify boolean expressions in SQL.

        Applies: double-negation removal, constant folding,
        AND/OR identity, De Morgan's simplification.
        """
        ...


# ---------------------------------------------------------------------------
# Composite Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class SqlToolkit(Protocol):
    """Composite protocol: a complete SQL toolkit implementation.

    This is what consumer code receives from the factory.  It combines all
    individual protocols into a single interface.
    """

    @property
    def parser(self) -> SqlParser: ...

    @property
    def renderer(self) -> SqlRenderer: ...

    @property
    def scope_analyzer(self) -> SqlScopeAnalyzer: ...

    @property
    def transpiler(self) -> SqlTranspiler: ...

    @property
    def normalizer(self) -> SqlNormalizer: ...

    @property
    def differ(self) -> SqlDiffer: ...

    @property
    def safety_guard(self) -> SqlSafetyGuard: ...

    @property
    def rewriter(self) -> SqlRewriter: ...

    @property
    def lineage_analyzer(self) -> SqlLineageAnalyzer: ...

    @property
    def qualifier(self) -> SqlQualifier: ...
