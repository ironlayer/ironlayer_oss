"""SQL parsing and AST analysis.

This module provides functions to parse SQL statements, extract table
references (handling CTEs and subqueries correctly via scope analysis),
identify output columns, detect aggregation/window function usage, and
bundle all metadata into a structured :class:`ModelASTMetadata` result.

All SQL parsing is delegated to :mod:`core_engine.sql_toolkit`.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from core_engine.sql_toolkit import Dialect, get_sql_toolkit
from core_engine.sql_toolkit import SqlParseError as ToolkitParseError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SQLParseError(Exception):
    """Raised when a SQL statement cannot be parsed."""

    def __init__(self, sql_fragment: str, reason: str) -> None:
        self.sql_fragment = sql_fragment
        self.reason = reason
        super().__init__(f"Failed to parse SQL: {reason}")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class ModelASTMetadata(BaseModel):
    """Structured metadata extracted from a parsed SQL statement."""

    referenced_tables: list[str] = Field(
        default_factory=list,
        description="Deduplicated, sorted table references (schema.table or table).",
    )
    output_columns: list[str] = Field(
        default_factory=list,
        description="Column names/aliases produced by the top-level SELECT.",
    )
    ctes: list[str] = Field(
        default_factory=list,
        description="Names of Common Table Expressions defined in the query.",
    )
    has_aggregation: bool = Field(
        default=False,
        description="True if the query contains aggregate functions (SUM, COUNT, ...).",
    )
    has_window_functions: bool = Field(
        default=False,
        description="True if the query contains window function expressions.",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_sql(sql: str) -> ModelASTMetadata:
    """Parse a SQL statement and return rich AST metadata.

    Parameters
    ----------
    sql:
        A single SQL statement (typically a SELECT with optional CTEs).

    Returns
    -------
    ModelASTMetadata
        Structured metadata including referenced tables, output columns,
        CTE names, and boolean flags for aggregation/window usage.

    Raises
    ------
    SQLParseError
        If *sql* cannot be parsed.
    """
    tk = get_sql_toolkit()

    try:
        scope = tk.scope_analyzer.extract_tables(sql, Dialect.DATABRICKS)
        cols = tk.scope_analyzer.extract_columns(sql, Dialect.DATABRICKS)
    except ToolkitParseError as exc:
        raise SQLParseError(
            sql_fragment=sql[:200],
            reason=str(exc),
        ) from exc

    return ModelASTMetadata(
        referenced_tables=[t.fully_qualified for t in scope.referenced_tables],
        output_columns=list(cols.output_columns),
        ctes=list(scope.cte_names),
        has_aggregation=cols.has_aggregation,
        has_window_functions=cols.has_window_functions,
    )


def extract_referenced_tables(sql: str) -> list[str]:
    """Return a deduplicated, sorted list of table references in *sql*.

    Uses scope-based analysis to correctly exclude CTE names and
    inline subqueries from the result set.
    """
    tk = get_sql_toolkit()
    try:
        scope = tk.scope_analyzer.extract_tables(sql, Dialect.DATABRICKS)
    except ToolkitParseError as exc:
        raise SQLParseError(
            sql_fragment=sql[:200],
            reason=str(exc),
        ) from exc
    return [t.fully_qualified for t in scope.referenced_tables]


def extract_output_columns(sql: str) -> list[str]:
    """Return the column names/aliases from the top-level SELECT.

    Star expressions (``*``) are returned as ``"*"``.  Unnamed expressions
    are returned as the SQL text of the expression.
    """
    tk = get_sql_toolkit()
    try:
        cols = tk.scope_analyzer.extract_columns(sql, Dialect.DATABRICKS)
    except ToolkitParseError as exc:
        raise SQLParseError(
            sql_fragment=sql[:200],
            reason=str(exc),
        ) from exc
    return list(cols.output_columns)


def extract_ctes(sql: str) -> list[str]:
    """Return CTE names defined in *sql*, sorted alphabetically."""
    tk = get_sql_toolkit()
    try:
        scope = tk.scope_analyzer.extract_tables(sql, Dialect.DATABRICKS)
    except ToolkitParseError as exc:
        raise SQLParseError(
            sql_fragment=sql[:200],
            reason=str(exc),
        ) from exc
    return list(scope.cte_names)
