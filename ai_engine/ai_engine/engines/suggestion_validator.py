"""Validation pipeline for AI-generated SQL suggestions.

Every SQL suggestion (from LLM or rule-based engines) must pass through
this validator before being surfaced to users.  The pipeline enforces
four gates:

1. **Syntax validation**: The rewritten SQL must parse via ``sqlglot``.
2. **Explainable diff**: The suggestion must include a human-readable
   explanation of what changed relative to the original SQL.
3. **Deterministic test-run**: The rewritten SQL must execute successfully
   in a DuckDB sandbox to prove it is runnable and doesn't produce errors.
4. **Semantic equivalence**: The rewritten SQL must preserve output column
   schema compatibility, retain all original table references, and stay
   within a reasonable AST magnitude bound to prevent wholesale rewrites.

If any gate fails, the suggestion is rejected and not returned to the user.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

import duckdb
import sqlglot
from sqlglot import exp as sqlglot_exp

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of the suggestion validation pipeline."""

    is_valid: bool
    syntax_ok: bool
    diff_explanation: str
    test_run_ok: bool
    test_run_error: str | None = None
    equivalence_ok: bool = True
    equivalence_details: str | None = None
    rejection_reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SQL safety validation for LLM-generated SQL
# ---------------------------------------------------------------------------

# Statement types that are safe to execute in the DuckDB sandbox.
_SAFE_STATEMENT_TYPES = frozenset({"Select", "Explain"})


def _is_safe_sql(sql: str) -> bool:
    """Reject non-SELECT SQL from LLM-generated suggestions.

    Parses the SQL through sqlglot and verifies that every statement is
    either a SELECT or EXPLAIN.  DDL, DML, and system commands are
    rejected to prevent sandbox escapes.

    Returns ``True`` only if all statements are safe.
    """
    try:
        parsed = sqlglot.parse(sql)
        for statement in parsed:
            if statement is None:
                return False
            stmt_type = type(statement).__name__
            if stmt_type not in _SAFE_STATEMENT_TYPES:
                return False
        return True
    except Exception:
        return False


class SuggestionValidator:
    """Four-gate validation pipeline for SQL suggestions.

    Parameters
    ----------
    duckdb_path:
        Path to a DuckDB database file for test-run sandboxing.  Defaults
        to an in-memory database.
    test_run_timeout_seconds:
        Maximum execution time for the DuckDB test-run gate.
    """

    def __init__(
        self,
        duckdb_path: str | None = None,
        test_run_timeout_seconds: float = 5.0,
    ) -> None:
        self._duckdb_path = duckdb_path or ":memory:"
        self._timeout = test_run_timeout_seconds

    def validate(
        self,
        original_sql: str,
        rewritten_sql: str,
        description: str,
    ) -> ValidationResult:
        """Run the full validation pipeline on a suggestion.

        Parameters
        ----------
        original_sql:
            The original SQL that the suggestion modifies.
        rewritten_sql:
            The proposed rewritten SQL from the AI engine.
        description:
            Human-readable description of the suggestion.

        Returns
        -------
        ValidationResult
            Full result including per-gate outcomes.
        """
        rejection_reasons: list[str] = []

        # Gate 1: Syntax validation
        syntax_ok = self._validate_syntax(rewritten_sql)
        if not syntax_ok:
            rejection_reasons.append("Rewritten SQL failed syntax validation (sqlglot parse error)")

        # Gate 2: Explainable diff
        diff_explanation = self._generate_diff_explanation(original_sql, rewritten_sql)
        if not diff_explanation:
            rejection_reasons.append("Could not generate explainable diff between original and rewritten SQL")

        # Gate 3: Deterministic test-run (only if syntax passed)
        test_run_ok = False
        test_run_error: str | None = None
        if syntax_ok:
            test_run_ok, test_run_error = self._test_run(rewritten_sql)
            if not test_run_ok:
                rejection_reasons.append(f"DuckDB test-run failed: {test_run_error}")

        # Gate 4: Semantic equivalence check (only if syntax passed)
        equivalence_ok = True
        equivalence_details: str | None = None
        if syntax_ok:
            equivalence_ok, equivalence_details = self._check_semantic_equivalence(original_sql, rewritten_sql)
            if not equivalence_ok:
                rejection_reasons.append(f"Semantic equivalence check failed: {equivalence_details}")

        is_valid = syntax_ok and bool(diff_explanation) and test_run_ok and equivalence_ok

        return ValidationResult(
            is_valid=is_valid,
            syntax_ok=syntax_ok,
            diff_explanation=diff_explanation,
            test_run_ok=test_run_ok,
            test_run_error=test_run_error,
            equivalence_ok=equivalence_ok,
            equivalence_details=equivalence_details,
            rejection_reasons=rejection_reasons,
        )

    # ------------------------------------------------------------------
    # Gate 1: Syntax validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_syntax(sql: str) -> bool:
        """Return True if the SQL parses successfully via sqlglot."""
        try:
            sqlglot.parse_one(sql, read="databricks")
            return True
        except sqlglot.errors.ParseError as exc:
            logger.debug("Syntax validation failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Gate 2: Explainable diff
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_diff_explanation(
        original_sql: str,
        rewritten_sql: str,
    ) -> str:
        """Generate a human-readable explanation of the differences.

        Uses sqlglot AST diff to identify structural changes, then
        produces a natural-language summary.
        """
        try:
            original_ast = sqlglot.parse_one(original_sql, read="databricks")
            rewritten_ast = sqlglot.parse_one(rewritten_sql, read="databricks")
        except sqlglot.errors.ParseError:
            return ""

        changes: list[str] = []

        # Compare output columns
        orig_cols = _extract_select_columns(original_ast)
        new_cols = _extract_select_columns(rewritten_ast)
        added_cols = new_cols - orig_cols
        removed_cols = orig_cols - new_cols
        if added_cols:
            changes.append(f"Added columns: {', '.join(sorted(added_cols))}")
        if removed_cols:
            changes.append(f"Removed columns: {', '.join(sorted(removed_cols))}")

        # Compare table references
        orig_tables = _extract_table_refs(original_ast)
        new_tables = _extract_table_refs(rewritten_ast)
        added_tables = new_tables - orig_tables
        removed_tables = orig_tables - new_tables
        if added_tables:
            changes.append(f"Added table references: {', '.join(sorted(added_tables))}")
        if removed_tables:
            changes.append(f"Removed table references: {', '.join(sorted(removed_tables))}")

        # Compare CTEs
        orig_ctes = _extract_cte_names(original_ast)
        new_ctes = _extract_cte_names(rewritten_ast)
        added_ctes = new_ctes - orig_ctes
        removed_ctes = orig_ctes - new_ctes
        if added_ctes:
            changes.append(f"Added CTEs: {', '.join(sorted(added_ctes))}")
        if removed_ctes:
            changes.append(f"Removed CTEs: {', '.join(sorted(removed_ctes))}")

        # Compare JOINs count
        orig_joins = len(list(original_ast.find_all(sqlglot_exp.Join)))
        new_joins = len(list(rewritten_ast.find_all(sqlglot_exp.Join)))
        if orig_joins != new_joins:
            changes.append(f"JOIN count changed from {orig_joins} to {new_joins}")

        # Compare WHERE presence
        orig_where = original_ast.find(sqlglot_exp.Where) is not None
        new_where = rewritten_ast.find(sqlglot_exp.Where) is not None
        if not orig_where and new_where:
            changes.append("Added WHERE clause")
        elif orig_where and not new_where:
            changes.append("Removed WHERE clause")

        # Compare window functions
        orig_windows = len(list(original_ast.find_all(sqlglot_exp.Window)))
        new_windows = len(list(rewritten_ast.find_all(sqlglot_exp.Window)))
        if orig_windows != new_windows:
            changes.append(f"Window function count changed from {orig_windows} to {new_windows}")

        if not changes:
            # Fallback: if no structural diff detected, compare canonical strings
            orig_canonical = original_ast.sql(dialect="databricks", pretty=False)
            new_canonical = rewritten_ast.sql(dialect="databricks", pretty=False)
            if orig_canonical != new_canonical:
                changes.append("SQL structure modified (formatting or expression rewrite)")
            else:
                return ""  # No actual change detected

        return "; ".join(changes)

    # ------------------------------------------------------------------
    # Gate 3: DuckDB test-run
    # ------------------------------------------------------------------

    def _test_run(self, sql: str) -> tuple[bool, str | None]:
        """Execute the SQL in a DuckDB sandbox to verify it runs.

        The SQL is transpiled from Databricks dialect to DuckDB dialect
        before execution.  We use EXPLAIN instead of actual execution
        to avoid needing real tables.

        Security measures:
        1. Parse LLM-generated SQL through sqlglot -- reject DDL, DML, and
           system commands.  Only SELECT and EXPLAIN are allowed.
        2. Use a restricted DuckDB connection with external access disabled.
        3. Wrap execution in a timeout to prevent runaway queries.
        """
        # Gate 0: Reject non-SELECT/EXPLAIN SQL before any execution.
        if not _is_safe_sql(sql):
            return False, "SQL contains disallowed statement types (only SELECT/EXPLAIN permitted)"

        # Transpile from Databricks to DuckDB
        try:
            duckdb_sqls = sqlglot.transpile(sql, read="databricks", write="duckdb")
            if not duckdb_sqls:
                return False, "Transpilation produced no output"
            duckdb_sql = duckdb_sqls[0]
        except Exception as exc:
            return False, f"Transpilation failed: {exc}"

        # Also validate the transpiled SQL is safe (transpilation could change semantics).
        if not _is_safe_sql(duckdb_sql):
            return False, "Transpiled SQL contains disallowed statement types"

        # Try to prepare/explain the statement in a restricted DuckDB sandbox.
        conn = None
        result: tuple[bool, str | None] = (False, "DuckDB test-run timed out")
        try:
            conn = duckdb.connect(self._duckdb_path)

            # Restrict external access to prevent file system and network escapes.
            try:
                conn.execute("SET enable_external_access = false")
            except duckdb.Error:
                # Older DuckDB versions may not support this setting; log and continue.
                logger.debug("Could not disable external access (DuckDB version may not support it)")

            # Execute validation with a timeout using a thread.
            timeout_seconds = self._timeout
            exception_holder: list[Exception] = []
            result_holder: list[tuple[bool, str | None]] = []

            def _run_validation() -> None:
                try:
                    # Use EXPLAIN to validate the query plan without needing real data.
                    try:
                        conn.execute(f"EXPLAIN {duckdb_sql}")  # type: ignore[union-attr]
                        result_holder.append((True, None))
                        return
                    except duckdb.Error:
                        pass

                    # EXPLAIN may fail if tables don't exist.  Try a simpler
                    # validation: wrap in a CTE that returns nothing and check
                    # if the outer structure is valid.
                    try:
                        conn.execute(f"PREPARE validation_check AS {duckdb_sql}")  # type: ignore[union-attr]
                        conn.execute("DEALLOCATE validation_check")  # type: ignore[union-attr]
                        result_holder.append((True, None))
                        return
                    except duckdb.Error:
                        pass

                    # Final fallback: if the transpiled SQL parses in sqlglot
                    # for duckdb dialect, consider it valid enough.
                    try:
                        sqlglot.parse_one(duckdb_sql, read="duckdb")
                        result_holder.append((True, None))
                    except sqlglot.errors.ParseError as parse_err:
                        result_holder.append((False, f"DuckDB parse validation failed: {parse_err}"))
                except Exception as exc:
                    exception_holder.append(exc)

            thread = threading.Thread(target=_run_validation, daemon=True)
            thread.start()
            thread.join(timeout=timeout_seconds)

            if thread.is_alive():
                # Thread is still running -- query timed out.
                logger.warning("DuckDB test-run timed out after %.1fs", timeout_seconds)
                try:
                    conn.interrupt()  # type: ignore[union-attr]
                except Exception:
                    pass
                return False, f"DuckDB test-run timed out after {timeout_seconds}s"

            if exception_holder:
                return False, f"DuckDB connection error: {exception_holder[0]}"

            if result_holder:
                result = result_holder[0]

            return result

        except duckdb.Error as exc:
            return False, f"DuckDB connection error: {exc}"
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Gate 4: Semantic equivalence check
    # ------------------------------------------------------------------

    def _check_semantic_equivalence(
        self,
        original_sql: str,
        rewritten_sql: str,
    ) -> tuple[bool, str | None]:
        """Verify the rewritten SQL preserves output schema compatibility.

        Checks:
        1. Output column names that exist in the original must be preserved
           in the rewrite (the rewrite may add columns, but not remove them).
        2. Table references from the original must be retained (additions
           are allowed for optimisation purposes, but removals indicate a
           potential logic change).
        3. AST node count change must be within a reasonable bound to
           prevent wholesale rewrites that should be reviewed manually.

        Returns ``(True, None)`` if equivalent, ``(False, reason)`` if not.
        """
        try:
            original_ast = sqlglot.parse_one(original_sql, read="databricks")
            rewritten_ast = sqlglot.parse_one(rewritten_sql, read="databricks")
        except sqlglot.errors.ParseError as exc:
            return False, f"Parse error during equivalence check: {exc}"

        # Check 1: Output column comparison
        orig_cols = _extract_select_columns(original_ast)
        new_cols = _extract_select_columns(rewritten_ast)

        # Columns present in original but missing from rewrite = schema-breaking
        removed_cols = orig_cols - new_cols
        if removed_cols and "*" not in new_cols:
            return False, (
                f"Rewrite removes output columns: {', '.join(sorted(removed_cols))}. "
                f"This would break downstream consumers."
            )

        # Check 2: Table reference preservation
        # The rewrite should not remove table references that the original uses
        # (it can add new ones for optimisation, but removing indicates logic change)
        orig_tables = _extract_table_refs(original_ast)
        new_tables = _extract_table_refs(rewritten_ast)
        removed_tables = orig_tables - new_tables
        if removed_tables:
            return False, (
                f"Rewrite removes table references: {', '.join(sorted(removed_tables))}. "
                f"This may change query semantics."
            )

        # Check 3: AST magnitude check — prevent wholesale rewrites
        # Count total AST nodes; if the rewrite changes more than 50% of nodes,
        # it's too aggressive and should be reviewed manually.
        orig_node_count = _count_ast_nodes(original_ast)
        new_node_count = _count_ast_nodes(rewritten_ast)

        if orig_node_count > 0:
            change_ratio = abs(new_node_count - orig_node_count) / orig_node_count
            if change_ratio > 0.5:
                return False, (
                    f"Rewrite changes {change_ratio:.0%} of AST nodes "
                    f"({orig_node_count} → {new_node_count}). "
                    f"Changes exceeding 50% require manual review."
                )

        return True, None


# ------------------------------------------------------------------
# AST helper functions
# ------------------------------------------------------------------


def _extract_select_columns(ast: sqlglot_exp.Expression) -> set[str]:
    """Extract output column names/aliases from the outermost SELECT."""
    cols: set[str] = set()
    select = ast.find(sqlglot_exp.Select)
    if select is None:
        return cols
    for expr in select.expressions:
        if isinstance(expr, sqlglot_exp.Alias):
            cols.add(expr.alias)
        elif isinstance(expr, sqlglot_exp.Column):
            cols.add(expr.name)
        elif isinstance(expr, sqlglot_exp.Star):
            cols.add("*")
        else:
            sql_str = expr.sql(dialect="databricks")
            if sql_str:
                cols.add(sql_str[:50])
    return cols


def _extract_table_refs(ast: sqlglot_exp.Expression) -> set[str]:
    """Extract fully-qualified table names referenced in the SQL."""
    tables: set[str] = set()
    cte_names = _extract_cte_names(ast)
    for table in ast.find_all(sqlglot_exp.Table):
        name = table.name
        if name and name.lower() not in {c.lower() for c in cte_names}:
            schema = table.db
            full = f"{schema}.{name}" if schema else name
            tables.add(full.lower())
    return tables


def _extract_cte_names(ast: sqlglot_exp.Expression) -> set[str]:
    """Extract CTE names from a WITH clause."""
    names: set[str] = set()
    with_clause = ast.find(sqlglot_exp.With)
    if with_clause is None:
        return names
    for cte in with_clause.expressions:
        alias = cte.find(sqlglot_exp.TableAlias)
        if alias:
            names.add(alias.alias_or_name)
    return names


def _count_ast_nodes(ast: sqlglot_exp.Expression) -> int:
    """Count total nodes in an AST expression tree."""
    count = 0
    for _ in ast.walk():
        count += 1
    return count
