"""Service layer for model testing operations.

Orchestrates test execution, result persistence, and test definition
synchronisation from model headers into the database.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from core_engine.models.model_definition import ModelDefinition, ModelTestDefinition
from core_engine.models.plan import compute_deterministic_id
from core_engine.state.repository import (
    ModelRepository,
    ModelTestRepository,
    PlanRepository,
    TestResultRepository,
)
from core_engine.testing.test_runner import ModelTestRunner
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class TestService:
    """High-level service for running and managing model tests.

    Parameters
    ----------
    session:
        Active database session for persistence.
    tenant_id:
        Tenant scope for all operations.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        tenant_id: str = "default",
    ) -> None:
        self._session = session
        self._tenant_id = tenant_id
        self._test_repo = ModelTestRepository(session, tenant_id=tenant_id)
        self._result_repo = TestResultRepository(session, tenant_id=tenant_id)
        self._model_repo = ModelRepository(session, tenant_id=tenant_id)

    async def run_tests_for_model(
        self,
        model_name: str,
        *,
        plan_id: str | None = None,
        duckdb_conn: object | None = None,
    ) -> dict[str, Any]:
        """Run all registered tests for a model.

        Loads test definitions from the database, executes them via the
        test runner, persists results, and returns a summary.

        Parameters
        ----------
        model_name:
            Canonical model name whose tests should be run.
        plan_id:
            Optional plan ID to associate results with.
        duckdb_conn:
            Optional DuckDB connection for local execution.

        Returns
        -------
        dict
            Summary with ``model_name``, ``total``, ``passed``, ``failed``,
            ``blocked``, and ``results`` list.
        """
        test_rows = await self._test_repo.get_for_model(model_name)
        if not test_rows:
            return {
                "model_name": model_name,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "blocked": 0,
                "results": [],
            }

        # Convert stored test definitions back to ModelTestDefinition for the runner.
        test_defs: list[ModelTestDefinition] = []
        severity_map: dict[str, str] = {}
        for row in test_rows:
            config = row.test_config_json or {}
            test_def = ModelTestDefinition(
                test_type=row.test_type,  # type: ignore[arg-type]
                column=config.get("column"),
                threshold=config.get("threshold"),
                values=config.get("values"),
                sql=config.get("sql"),
                severity=row.severity,  # type: ignore[arg-type]
            )
            test_defs.append(test_def)
            test_id = compute_deterministic_id(
                model_name,
                test_def.test_type.value,
                test_def.column or "",
                test_def.sql or "",
            )
            severity_map[test_id] = row.severity

        runner = ModelTestRunner(execution_mode="local_duckdb")
        results = await runner.run_all_tests(model_name, test_defs, duckdb_conn=duckdb_conn)

        # Persist results and compute summary.
        output: list[dict[str, Any]] = []
        passed = 0
        failed = 0
        blocked = 0

        for r in results:
            await self._result_repo.record_result(
                test_id=r.test_id,
                plan_id=plan_id,
                model_name=r.model_name,
                test_type=r.test_type,
                passed=r.passed,
                failure_message=r.failure_message,
                execution_mode="local_duckdb",
                duration_ms=r.duration_ms,
            )

            if r.passed:
                passed += 1
            else:
                failed += 1
                severity = severity_map.get(r.test_id, "BLOCK")
                if severity == "BLOCK":
                    blocked += 1

            output.append(
                {
                    "test_id": r.test_id,
                    "model_name": r.model_name,
                    "test_type": r.test_type,
                    "passed": r.passed,
                    "failure_message": r.failure_message,
                    "duration_ms": r.duration_ms,
                }
            )

        return {
            "model_name": model_name,
            "total": len(results),
            "passed": passed,
            "failed": failed,
            "blocked": blocked,
            "results": output,
        }

    async def run_tests_for_plan(
        self,
        plan_id: str,
        *,
        duckdb_conn: object | None = None,
    ) -> dict[str, Any]:
        """Run tests for all models referenced in a plan.

        Looks up the plan to extract the list of models, then runs
        tests for each model.

        Parameters
        ----------
        plan_id:
            Plan whose models should be tested.
        duckdb_conn:
            Optional DuckDB connection for local execution.

        Returns
        -------
        dict
            Aggregate summary with per-model breakdowns.
        """
        plan_repo = PlanRepository(self._session, tenant_id=self._tenant_id)
        plan_row = await plan_repo.get_plan(plan_id)
        if plan_row is None:
            raise ValueError(f"Plan {plan_id} not found")

        plan_data = json.loads(plan_row.plan_json) if isinstance(plan_row.plan_json, str) else plan_row.plan_json

        model_names: list[str] = sorted(
            {step["model"] for step in (plan_data or {}).get("steps", []) if "model" in step}
        )

        total = 0
        passed = 0
        failed = 0
        blocked = 0
        model_results: list[dict[str, Any]] = []

        for model_name in model_names:
            result = await self.run_tests_for_model(model_name, plan_id=plan_id, duckdb_conn=duckdb_conn)
            total += result["total"]
            passed += result["passed"]
            failed += result["failed"]
            blocked += result["blocked"]
            model_results.append(result)

        return {
            "plan_id": plan_id,
            "total": total,
            "passed": passed,
            "failed": failed,
            "blocked": blocked,
            "models": model_results,
        }

    async def get_test_history(
        self,
        model_name: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return recent test results for a model.

        Parameters
        ----------
        model_name:
            Canonical model name.
        limit:
            Maximum number of results to return.

        Returns
        -------
        list[dict]
            Serialised test result records.
        """
        rows = await self._result_repo.get_for_model(model_name, limit=limit)
        return [
            {
                "test_id": r.test_id,
                "plan_id": r.plan_id,
                "model_name": r.model_name,
                "test_type": r.test_type,
                "passed": r.passed,
                "failure_message": r.failure_message,
                "execution_mode": r.execution_mode,
                "duration_ms": r.duration_ms,
                "executed_at": r.executed_at.isoformat() if r.executed_at else None,
            }
            for r in rows
        ]

    async def get_plan_test_results(self, plan_id: str) -> dict[str, Any]:
        """Return test results and summary for a plan.

        Parameters
        ----------
        plan_id:
            Plan identifier.

        Returns
        -------
        dict
            Summary with ``total``, ``passed``, ``failed``, ``blocked``,
            and a ``results`` list.
        """
        summary = await self._result_repo.get_summary(plan_id)
        rows = await self._result_repo.get_for_plan(plan_id)

        results = [
            {
                "test_id": r.test_id,
                "plan_id": r.plan_id,
                "model_name": r.model_name,
                "test_type": r.test_type,
                "passed": r.passed,
                "failure_message": r.failure_message,
                "execution_mode": r.execution_mode,
                "duration_ms": r.duration_ms,
                "executed_at": r.executed_at.isoformat() if r.executed_at else None,
            }
            for r in rows
        ]

        return {
            **summary,
            "results": results,
        }

    async def sync_tests_from_definitions(
        self,
        models: list[ModelDefinition],
    ) -> dict[str, Any]:
        """Sync test definitions from model headers into the database.

        For each model that declares tests in its header, this method:
        1. Deletes any existing test definitions for the model.
        2. Inserts the new definitions from the header.

        Parameters
        ----------
        models:
            List of model definitions (typically from ``load_models_from_directory``).

        Returns
        -------
        dict
            Summary with ``models_synced``, ``tests_created``, ``tests_deleted``.
        """
        models_synced = 0
        tests_created = 0
        tests_deleted = 0

        for model in sorted(models, key=lambda m: m.name):
            if not model.tests:
                continue

            deleted = await self._test_repo.delete_for_model(model.name)
            tests_deleted += deleted

            for test_def in model.tests:
                test_id = compute_deterministic_id(
                    model.name,
                    test_def.test_type.value,
                    test_def.column or "",
                    test_def.sql or "",
                )

                config: dict[str, Any] = {}
                if test_def.column is not None:
                    config["column"] = test_def.column
                if test_def.threshold is not None:
                    config["threshold"] = test_def.threshold
                if test_def.values is not None:
                    config["values"] = sorted(test_def.values)
                if test_def.sql is not None:
                    config["sql"] = test_def.sql

                await self._test_repo.save_test(
                    test_id=test_id,
                    model_name=model.name,
                    test_type=test_def.test_type.value,
                    test_config=config,
                    severity=test_def.severity.value,
                )
                tests_created += 1

            models_synced += 1

        return {
            "models_synced": models_synced,
            "tests_created": tests_created,
            "tests_deleted": tests_deleted,
        }
