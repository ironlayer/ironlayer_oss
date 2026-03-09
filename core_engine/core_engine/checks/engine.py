"""Check Engine -- unified orchestrator for all quality checks.

The :class:`CheckEngine` discovers registered checks, filters by the
requested check types and model names, executes each check, and
aggregates results into a :class:`CheckSummary`.
"""

from __future__ import annotations

import logging

from core_engine.checks.base import BaseCheck
from core_engine.checks.models import (
    CheckContext,
    CheckResult,
    CheckStatus,
    CheckSeverity,
    CheckSummary,
    CheckType,
    Timer,
)
from core_engine.checks.registry import CheckRegistry

logger = logging.getLogger(__name__)


class CheckEngine:
    """Unified orchestrator for running quality checks.

    The engine uses a :class:`CheckRegistry` to discover available
    checks and executes them against a :class:`CheckContext`.

    Parameters
    ----------
    registry:
        Optional pre-configured registry.  When ``None``, a new
        empty registry is created.
    """

    def __init__(self, registry: CheckRegistry | None = None) -> None:
        self._registry = registry or CheckRegistry()

    @property
    def registry(self) -> CheckRegistry:
        """The check registry backing this engine."""
        return self._registry

    def register(self, check: BaseCheck) -> None:
        """Register a check implementation with the engine."""
        self._registry.register(check)

    def get_available_types(self) -> list[CheckType]:
        """Return all registered check types, sorted."""
        return self._registry.get_types()

    async def run(self, context: CheckContext) -> CheckSummary:
        """Execute checks and return an aggregated summary.

        Parameters
        ----------
        context:
            Execution context containing models to check and optional
            filters for check types and model names.

        Returns
        -------
        CheckSummary
            Aggregated results with per-check breakdowns.
        """
        timer = Timer()
        timer.start()

        all_results: list[CheckResult] = []

        # Determine which checks to run.
        checks_to_run = self._registry.get_all()
        if context.check_types is not None:
            requested_types = set(context.check_types)
            checks_to_run = [c for c in checks_to_run if c.check_type in requested_types]

        if not checks_to_run:
            logger.info("No checks to run (none registered or all filtered out).")
            return CheckSummary.from_results([], duration_ms=timer.elapsed_ms())

        # Execute each check type.
        for check in checks_to_run:
            logger.debug("Running check: %s", check.check_type.value)
            try:
                results = await check.execute(context)
                all_results.extend(results)
            except Exception as exc:
                logger.error(
                    "Check %s raised an unhandled exception: %s",
                    check.check_type.value,
                    exc,
                )
                # Create an ERROR result for each model in context.
                models = context.models
                if context.model_names is not None:
                    model_name_set = set(context.model_names)
                    models = [m for m in models if m.name in model_name_set]

                for model in sorted(models, key=lambda m: m.name):
                    all_results.append(
                        CheckResult(
                            check_type=check.check_type,
                            model_name=model.name,
                            status=CheckStatus.ERROR,
                            severity=CheckSeverity.HIGH,
                            message=f"Unhandled error in {check.check_type.value}: {exc}",
                        )
                    )

        elapsed = timer.elapsed_ms()
        return CheckSummary.from_results(all_results, duration_ms=elapsed)


def create_default_engine(
    *,
    execution_mode: str = "local_duckdb",
    duckdb_conn: object | None = None,
) -> CheckEngine:
    """Create a :class:`CheckEngine` with all built-in checks registered.

    Parameters
    ----------
    execution_mode:
        Backend for model test execution.
    duckdb_conn:
        Optional DuckDB connection for model tests.

    Returns
    -------
    CheckEngine
        Engine with MODEL_TEST and SCHEMA_CONTRACT checks registered.
    """
    from core_engine.checks.builtin.model_tests import ModelTestCheck
    from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

    engine = CheckEngine()
    engine.register(ModelTestCheck(execution_mode=execution_mode, duckdb_conn=duckdb_conn))
    engine.register(SchemaContractCheck())
    return engine
