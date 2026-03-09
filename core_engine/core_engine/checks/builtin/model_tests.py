"""Built-in check that wraps the existing ModelTestRunner.

Executes declarative model tests (NOT_NULL, UNIQUE, ROW_COUNT_MIN/MAX,
ACCEPTED_VALUES, CUSTOM_SQL) through the check engine interface.
"""

from __future__ import annotations

import logging

from core_engine.checks.base import BaseCheck
from core_engine.checks.models import (
    CheckContext,
    CheckResult,
    CheckSeverity,
    CheckStatus,
    CheckType,
    Timer,
)
from core_engine.models.model_definition import TestSeverity
from core_engine.testing.test_runner import ModelTestRunner

logger = logging.getLogger(__name__)


class ModelTestCheck(BaseCheck):
    """Run declarative model tests via the existing ModelTestRunner.

    Wraps the ``ModelTestRunner`` to produce :class:`CheckResult` objects
    that integrate with the unified check engine.

    Parameters
    ----------
    execution_mode:
        Backend for test execution.  Defaults to ``"local_duckdb"``.
    duckdb_conn:
        Optional pre-existing DuckDB connection.
    """

    def __init__(
        self,
        *,
        execution_mode: str = "local_duckdb",
        duckdb_conn: object | None = None,
    ) -> None:
        self._runner = ModelTestRunner(execution_mode=execution_mode)
        self._duckdb_conn = duckdb_conn

    @property
    def check_type(self) -> CheckType:
        return CheckType.MODEL_TEST

    async def execute(self, context: CheckContext) -> list[CheckResult]:
        """Run model tests for all models in the context.

        Only models that have declared tests are executed.  Results are
        sorted deterministically by (model_name, test_type).
        """
        results: list[CheckResult] = []

        models = context.models
        if context.model_names is not None:
            model_name_set = set(context.model_names)
            models = [m for m in models if m.name in model_name_set]

        for model in sorted(models, key=lambda m: m.name):
            if not model.tests:
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.SKIP,
                        severity=CheckSeverity.LOW,
                        message="No tests defined for this model.",
                    )
                )
                continue

            timer = Timer()
            timer.start()

            try:
                test_results = await self._runner.run_all_tests(
                    model.name,
                    model.tests,
                    duckdb_conn=self._duckdb_conn,
                )
            except Exception as exc:
                elapsed = timer.elapsed_ms()
                logger.warning(
                    "Model test execution failed for %s: %s",
                    model.name,
                    exc,
                )
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.ERROR,
                        severity=CheckSeverity.HIGH,
                        message=f"Test execution error: {exc}",
                        duration_ms=elapsed,
                    )
                )
                continue

            for tr in test_results:
                # Map TestSeverity to CheckSeverity.
                test_def = next(
                    (
                        t
                        for t in model.tests
                        if t.test_type.value == tr.test_type
                        and (t.column or "") == (tr.model_name if hasattr(tr, "column") else "")
                    ),
                    None,
                )
                severity = CheckSeverity.HIGH
                if test_def and test_def.severity == TestSeverity.WARN:
                    severity = CheckSeverity.MEDIUM

                if tr.passed:
                    status = CheckStatus.PASS
                    message = f"Test {tr.test_type} passed."
                else:
                    status = CheckStatus.FAIL
                    message = tr.failure_message or f"Test {tr.test_type} failed."

                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=status,
                        severity=severity,
                        message=message,
                        detail=tr.test_type,
                        duration_ms=tr.duration_ms,
                    )
                )

        # Sort deterministically.
        results.sort(key=lambda r: (r.model_name, r.detail, r.status.value))
        return results
