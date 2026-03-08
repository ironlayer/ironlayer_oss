"""Local DuckDB executor for development and testing.

Provides a lightweight, zero-infrastructure execution path that translates
Databricks-dialect SQL into DuckDB using :mod:`sqlglot`, then runs the query
against an embedded DuckDB database.  All runs are synchronous and complete
before ``execute_step`` returns.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from core_engine.models.plan import PlanStep, RunType
from core_engine.sql_toolkit import Dialect, get_sql_toolkit
from core_engine.models.run import RunRecord, RunStatus
from core_engine.parser.sql_guard import SQLGuardConfig, assert_sql_safe

logger = logging.getLogger(__name__)

# Pattern for {{ parameter_name }} placeholders (with or without spaces).
_PARAM_PATTERN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class LocalExecutor:
    """Execute SQL model steps locally using DuckDB.

    Implements the :class:`ExecutorInterface` protocol for local development.
    SQL written in the Databricks dialect is transparently transpiled to DuckDB
    via :mod:`sqlglot` before execution.

    Parameters
    ----------
    db_path:
        Path to the DuckDB database file.  Parent directories are created
        automatically.  Defaults to ``.ironlayer/local.duckdb``.
    """

    def __init__(
        self,
        db_path: Path = Path(".ironlayer/local.duckdb"),
        sql_guard_config: SQLGuardConfig | None = None,
    ) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._run_logs: dict[str, str] = {}
        self._sql_guard_config = sql_guard_config

    # -- Connection management -----------------------------------------------

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return (and lazily create) the DuckDB connection."""
        if self._connection is None:
            logger.info("Opening DuckDB database at %s", self._db_path)
            self._connection = duckdb.connect(str(self._db_path))
        return self._connection

    def close(self) -> None:
        """Close the DuckDB connection if it is open."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                logger.debug("Ignoring error while closing DuckDB connection")
            finally:
                self._connection = None

    # -- Context manager support -----------------------------------------------

    def __enter__(self) -> LocalExecutor:
        """Enter a context block -- return self for ``with`` usage."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit a context block -- always close the DuckDB connection."""
        self.close()

    # -- ExecutorInterface implementation ------------------------------------

    def execute_step(
        self,
        step: PlanStep,
        sql: str,
        parameters: dict[str, str],
        plan_id: str = "",
    ) -> RunRecord:
        """Execute a single SQL step against the local DuckDB instance.

        The SQL is parameter-substituted, dialect-translated, wrapped with
        the appropriate DDL, and executed synchronously.
        """
        run_id = str(uuid4())
        started_at = datetime.now(UTC)
        conn = self._get_connection()

        logger.info(
            "Executing step %s for model %s locally (run %s)",
            step.step_id[:12],
            step.model,
            run_id[:12],
        )

        try:
            # 0. Substitute parameters first so safety checks see the final SQL.
            rendered_sql = self._substitute_parameters(sql, parameters)

            # 1. SQL safety check -- block dangerous operations before execution.
            assert_sql_safe(rendered_sql, self._sql_guard_config)

            # 2. Translate Databricks dialect to DuckDB.
            translated_sql = self._translate_dialect(rendered_sql)

            # 3. Wrap with DDL based on run type.
            run_type = "FULL_REFRESH" if step.run_type == RunType.FULL_REFRESH else "INCREMENTAL"
            final_sql = self._wrap_with_ddl(translated_sql, step.model, run_type)

            # 4. Execute.
            start_time = time.monotonic()
            conn.execute(final_sql)
            elapsed = time.monotonic() - start_time

            logger.info(
                "Step %s completed in %.2fs (model %s)",
                step.step_id[:12],
                elapsed,
                step.model,
            )

            self._run_logs[run_id] = f"Executed successfully in {elapsed:.2f}s"

            return RunRecord(
                run_id=run_id,
                plan_id=plan_id or step.step_id,
                step_id=step.step_id,
                model_name=step.model,
                status=RunStatus.SUCCESS,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                executor_version="local-duckdb",
            )

        except Exception as exc:
            finished_at = datetime.now(UTC)
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.error(
                "Step %s failed for model %s: %s",
                step.step_id[:12],
                step.model,
                error_msg,
            )

            self._run_logs[run_id] = error_msg

            return RunRecord(
                run_id=run_id,
                plan_id=plan_id or step.step_id,
                step_id=step.step_id,
                model_name=step.model,
                status=RunStatus.FAIL,
                started_at=started_at,
                finished_at=finished_at,
                error_message=error_msg,
                executor_version="local-duckdb",
            )

    def poll_status(self, run_id: str) -> RunStatus:
        """Return SUCCESS; local execution is always synchronous."""
        return RunStatus.SUCCESS

    def cancel(self, run_id: str) -> None:
        """No-op -- local execution is synchronous and not cancellable."""

    def get_logs(self, run_id: str) -> str:
        """Return stored logs for a completed local run."""
        return self._run_logs.get(run_id, "")

    def verify_run(self, run_id: str) -> RunStatus:
        """Verify the final status of a local run.

        Checks the stored log entry to determine whether the run succeeded.
        Returns ``PENDING`` if no record of the run is found (unknown state).
        """
        log_entry = self._run_logs.get(run_id)
        if log_entry is None:
            return RunStatus.PENDING
        if "successfully" in log_entry.lower():
            return RunStatus.SUCCESS
        return RunStatus.FAIL

    # -- SQL processing helpers ----------------------------------------------

    @staticmethod
    def _sanitize_param_value(key: str, value: str, *, quoted: bool) -> str:
        """Sanitize a parameter value before SQL substitution.

        For quoted context, escape single quotes by doubling them.
        For unquoted context, reject values containing SQL injection vectors.
        """
        if quoted:
            # Escape single quotes inside the value for safe SQL string literals.
            return value.replace("'", "''")

        # Unquoted parameters should only be identifiers, numbers, or dates.
        # Reject values that contain SQL statement terminators or comment markers.
        _dangerous_patterns = re.compile(
            r"[;]|--|\b(DROP|DELETE|INSERT|UPDATE|ALTER|GRANT|REVOKE|EXEC|TRUNCATE)\b",
            re.IGNORECASE,
        )
        if _dangerous_patterns.search(value):
            raise ValueError(f"Parameter '{key}' contains potentially dangerous SQL content: '{value[:80]}'")
        return value

    def _substitute_parameters(
        self,
        sql: str,
        parameters: dict[str, str],
    ) -> str:
        """Replace ``{{ key }}`` placeholders with their parameter values.

        Handles both ``{{ key }}`` and ``'{{ key }}'`` (quoted) forms.  The
        quoted form is replaced with a single-quoted literal to preserve SQL
        string semantics.  Values are sanitized to prevent SQL injection.
        """
        result = sql
        for key, value in parameters.items():
            sanitized_quoted = self._sanitize_param_value(key, value, quoted=True)
            sanitized_unquoted = self._sanitize_param_value(key, value, quoted=False)

            # Quoted form: '{{ key }}' -> 'escaped_value'
            result = re.sub(
                rf"'\{{\{{\s*{re.escape(key)}\s*\}}\}}'",
                f"'{sanitized_quoted}'",
                result,
            )
            # Unquoted form: {{ key }} -> value
            result = re.sub(
                rf"\{{\{{\s*{re.escape(key)}\s*\}}\}}",
                sanitized_unquoted,
                result,
            )
        return result

    def _translate_dialect(self, sql: str) -> str:
        """Transpile SQL from Databricks dialect to DuckDB dialect.

        Falls back to the original SQL if the toolkit cannot parse the
        statement, which allows DuckDB-native SQL to pass through unchanged.
        """
        tk = get_sql_toolkit()
        try:
            result = tk.transpiler.transpile(
                sql,
                Dialect.DATABRICKS,
                Dialect.DUCKDB,
            )
            if result.output_sql:
                return result.output_sql
        except Exception:
            logger.warning("SQL transpilation failed, using original SQL")
        return sql

    def _wrap_with_ddl(
        self,
        sql: str,
        model_name: str,
        run_type: str,
    ) -> str:
        """Wrap a SELECT query with DDL appropriate for the run type.

        Parameters
        ----------
        sql:
            A SELECT statement (the model body).
        model_name:
            Canonical model name used as the target table.
        run_type:
            ``"FULL_REFRESH"`` or ``"INCREMENTAL"``.

        Returns
        -------
        str
            A complete DDL + DML statement ready for execution.
        """
        # Sanitise model name for use as a table identifier: replace dots
        # with underscores to produce a flat namespace in DuckDB, then
        # properly quote the identifier to prevent SQL injection.
        flat_name = model_name.replace(".", "_")
        tk = get_sql_toolkit()
        quoted_name = tk.rewriter.quote_identifier(flat_name, Dialect.DUCKDB)

        if run_type == "FULL_REFRESH":
            return f"CREATE OR REPLACE TABLE {quoted_name} AS {sql}"

        # Incremental: ensure the table exists first, then insert.
        create_stub = f"CREATE TABLE IF NOT EXISTS {quoted_name} AS SELECT * FROM ({sql}) WHERE 1=0"
        insert_stmt = f"INSERT INTO {quoted_name} {sql}"
        return f"{create_stub};\n{insert_stmt}"
