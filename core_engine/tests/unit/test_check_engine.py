"""Unit tests for the IronLayer Check Engine.

Tests the core framework: models, registry, engine orchestrator,
and built-in check implementations (model tests, schema contracts).
"""

from __future__ import annotations

import pytest

from core_engine.checks.base import BaseCheck
from core_engine.checks.engine import CheckEngine, create_default_engine
from core_engine.checks.models import (
    CheckContext,
    CheckResult,
    CheckSeverity,
    CheckStatus,
    CheckSummary,
    CheckType,
)
from core_engine.checks.registry import CheckRegistry
from core_engine.models.model_definition import (
    ColumnContract,
    ModelDefinition,
    ModelKind,
    ModelTestDefinition,
    SchemaContractMode,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_model(
    name: str = "test_model",
    *,
    tests: list[ModelTestDefinition] | None = None,
    contract_mode: SchemaContractMode = SchemaContractMode.DISABLED,
    contract_columns: list[ColumnContract] | None = None,
    output_columns: list[str] | None = None,
) -> ModelDefinition:
    """Create a minimal ModelDefinition for testing."""
    return ModelDefinition(
        name=name,
        kind=ModelKind.FULL_REFRESH,
        file_path=f"models/{name}.sql",
        raw_sql=f"SELECT * FROM {name}",
        clean_sql=f"SELECT * FROM {name}",
        content_hash="abc123",
        tests=tests or [],
        contract_mode=contract_mode,
        contract_columns=contract_columns or [],
        output_columns=output_columns or [],
    )


class _PassingCheck(BaseCheck):
    """A check that always passes."""

    @property
    def check_type(self) -> CheckType:
        return CheckType.CUSTOM

    async def execute(self, context: CheckContext) -> list[CheckResult]:
        results = []
        models = context.models
        if context.model_names is not None:
            names = set(context.model_names)
            models = [m for m in models if m.name in names]
        for model in sorted(models, key=lambda m: m.name):
            results.append(
                CheckResult(
                    check_type=self.check_type,
                    model_name=model.name,
                    status=CheckStatus.PASS,
                    severity=CheckSeverity.LOW,
                    message="Custom check passed.",
                )
            )
        return results


class _FailingCheck(BaseCheck):
    """A check that always fails."""

    @property
    def check_type(self) -> CheckType:
        return CheckType.DATA_FRESHNESS

    async def execute(self, context: CheckContext) -> list[CheckResult]:
        results = []
        models = context.models
        if context.model_names is not None:
            names = set(context.model_names)
            models = [m for m in models if m.name in names]
        for model in sorted(models, key=lambda m: m.name):
            results.append(
                CheckResult(
                    check_type=self.check_type,
                    model_name=model.name,
                    status=CheckStatus.FAIL,
                    severity=CheckSeverity.HIGH,
                    message="Data is stale.",
                )
            )
        return results


class _ErrorCheck(BaseCheck):
    """A check that raises an exception."""

    @property
    def check_type(self) -> CheckType:
        return CheckType.VOLUME_ANOMALY

    async def execute(self, context: CheckContext) -> list[CheckResult]:
        raise RuntimeError("Unexpected error in check")


# ---------------------------------------------------------------------------
# CheckSummary tests
# ---------------------------------------------------------------------------


class TestCheckSummary:
    """Tests for CheckSummary.from_results()."""

    def test_empty_results(self):
        summary = CheckSummary.from_results([])
        assert summary.total == 0
        assert summary.passed == 0
        assert summary.failed == 0
        assert not summary.has_blocking_failures

    def test_all_pass(self):
        results = [
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="a",
                status=CheckStatus.PASS,
            ),
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="b",
                status=CheckStatus.PASS,
            ),
        ]
        summary = CheckSummary.from_results(results)
        assert summary.total == 2
        assert summary.passed == 2
        assert summary.failed == 0
        assert not summary.has_blocking_failures

    def test_mixed_results(self):
        results = [
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="a",
                status=CheckStatus.PASS,
            ),
            CheckResult(
                check_type=CheckType.SCHEMA_CONTRACT,
                model_name="b",
                status=CheckStatus.FAIL,
                severity=CheckSeverity.CRITICAL,
            ),
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="c",
                status=CheckStatus.WARN,
            ),
            CheckResult(
                check_type=CheckType.RECONCILIATION,
                model_name="d",
                status=CheckStatus.ERROR,
            ),
            CheckResult(
                check_type=CheckType.DATA_FRESHNESS,
                model_name="e",
                status=CheckStatus.SKIP,
            ),
        ]
        summary = CheckSummary.from_results(results)
        assert summary.total == 5
        assert summary.passed == 1
        assert summary.failed == 1
        assert summary.warned == 1
        assert summary.errored == 1
        assert summary.skipped == 1
        assert summary.blocking_failures == 1
        assert summary.has_blocking_failures

    def test_blocking_failures_only_critical_high(self):
        results = [
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="a",
                status=CheckStatus.FAIL,
                severity=CheckSeverity.MEDIUM,
            ),
            CheckResult(
                check_type=CheckType.MODEL_TEST,
                model_name="b",
                status=CheckStatus.FAIL,
                severity=CheckSeverity.LOW,
            ),
        ]
        summary = CheckSummary.from_results(results)
        assert summary.failed == 2
        assert summary.blocking_failures == 0
        assert not summary.has_blocking_failures

    def test_deterministic_ordering(self):
        results = [
            CheckResult(check_type=CheckType.SCHEMA_CONTRACT, model_name="z", status=CheckStatus.PASS),
            CheckResult(check_type=CheckType.MODEL_TEST, model_name="a", status=CheckStatus.PASS),
            CheckResult(check_type=CheckType.MODEL_TEST, model_name="m", status=CheckStatus.FAIL),
        ]
        summary = CheckSummary.from_results(results)
        names = [r.model_name for r in summary.results]
        assert names == sorted(names) or names == ["a", "m", "z"]

    def test_from_results_idempotent(self):
        results = [
            CheckResult(check_type=CheckType.MODEL_TEST, model_name="b", status=CheckStatus.PASS),
            CheckResult(check_type=CheckType.MODEL_TEST, model_name="a", status=CheckStatus.FAIL),
        ]
        s1 = CheckSummary.from_results(results)
        s2 = CheckSummary.from_results(results)
        assert s1.total == s2.total
        assert s1.passed == s2.passed
        assert s1.failed == s2.failed
        assert [r.model_name for r in s1.results] == [r.model_name for r in s2.results]


# ---------------------------------------------------------------------------
# CheckRegistry tests
# ---------------------------------------------------------------------------


class TestCheckRegistry:
    """Tests for CheckRegistry."""

    def test_register_and_get(self):
        registry = CheckRegistry()
        check = _PassingCheck()
        registry.register(check)
        assert registry.get(CheckType.CUSTOM) is check
        assert len(registry) == 1
        assert CheckType.CUSTOM in registry

    def test_register_duplicate_raises(self):
        registry = CheckRegistry()
        registry.register(_PassingCheck())
        with pytest.raises(ValueError, match="already registered"):
            registry.register(_PassingCheck())

    def test_unregister(self):
        registry = CheckRegistry()
        registry.register(_PassingCheck())
        registry.unregister(CheckType.CUSTOM)
        assert registry.get(CheckType.CUSTOM) is None
        assert len(registry) == 0

    def test_unregister_missing_raises(self):
        registry = CheckRegistry()
        with pytest.raises(KeyError, match="not registered"):
            registry.unregister(CheckType.CUSTOM)

    def test_get_all_sorted(self):
        registry = CheckRegistry()
        registry.register(_FailingCheck())  # DATA_FRESHNESS
        registry.register(_PassingCheck())  # CUSTOM
        all_checks = registry.get_all()
        types = [c.check_type for c in all_checks]
        assert types == sorted(types, key=lambda t: t.value)

    def test_get_types_sorted(self):
        registry = CheckRegistry()
        registry.register(_FailingCheck())
        registry.register(_PassingCheck())
        types = registry.get_types()
        assert types == sorted(types, key=lambda t: t.value)


# ---------------------------------------------------------------------------
# CheckEngine tests
# ---------------------------------------------------------------------------


class TestCheckEngine:
    """Tests for CheckEngine orchestration."""

    @pytest.mark.asyncio
    async def test_run_empty_registry(self):
        engine = CheckEngine()
        context = CheckContext(models=[_make_model("a")])
        summary = await engine.run(context)
        assert summary.total == 0

    @pytest.mark.asyncio
    async def test_run_passing_check(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        context = CheckContext(models=[_make_model("a"), _make_model("b")])
        summary = await engine.run(context)
        assert summary.total == 2
        assert summary.passed == 2
        assert not summary.has_blocking_failures

    @pytest.mark.asyncio
    async def test_run_failing_check(self):
        engine = CheckEngine()
        engine.register(_FailingCheck())
        context = CheckContext(models=[_make_model("a")])
        summary = await engine.run(context)
        assert summary.total == 1
        assert summary.failed == 1
        assert summary.has_blocking_failures

    @pytest.mark.asyncio
    async def test_run_error_check_graceful(self):
        engine = CheckEngine()
        engine.register(_ErrorCheck())
        context = CheckContext(models=[_make_model("a")])
        summary = await engine.run(context)
        assert summary.total == 1
        assert summary.errored == 1

    @pytest.mark.asyncio
    async def test_run_multiple_check_types(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        engine.register(_FailingCheck())
        context = CheckContext(models=[_make_model("a")])
        summary = await engine.run(context)
        assert summary.total == 2
        assert summary.passed == 1
        assert summary.failed == 1

    @pytest.mark.asyncio
    async def test_run_filtered_by_check_type(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        engine.register(_FailingCheck())
        context = CheckContext(
            models=[_make_model("a")],
            check_types=[CheckType.CUSTOM],
        )
        summary = await engine.run(context)
        assert summary.total == 1
        assert summary.passed == 1

    @pytest.mark.asyncio
    async def test_run_filtered_by_model_name(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        context = CheckContext(
            models=[_make_model("a"), _make_model("b"), _make_model("c")],
            model_names=["a", "c"],
        )
        summary = await engine.run(context)
        assert summary.total == 2
        names = {r.model_name for r in summary.results}
        assert names == {"a", "c"}

    @pytest.mark.asyncio
    async def test_deterministic_results(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        engine.register(_FailingCheck())
        context = CheckContext(
            models=[_make_model("c"), _make_model("a"), _make_model("b")],
        )
        s1 = await engine.run(context)
        s2 = await engine.run(context)
        r1 = [(r.model_name, r.check_type.value, r.status.value) for r in s1.results]
        r2 = [(r.model_name, r.check_type.value, r.status.value) for r in s2.results]
        assert r1 == r2

    def test_get_available_types(self):
        engine = CheckEngine()
        engine.register(_PassingCheck())
        engine.register(_FailingCheck())
        types = engine.get_available_types()
        assert CheckType.CUSTOM in types
        assert CheckType.DATA_FRESHNESS in types

    def test_create_default_engine(self):
        engine = create_default_engine()
        types = engine.get_available_types()
        assert CheckType.MODEL_TEST in types
        assert CheckType.SCHEMA_CONTRACT in types


# ---------------------------------------------------------------------------
# Built-in: SchemaContractCheck tests
# ---------------------------------------------------------------------------


class TestSchemaContractCheck:
    """Tests for the SchemaContractCheck built-in."""

    @pytest.mark.asyncio
    async def test_skip_when_disabled(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        model = _make_model("m", contract_mode=SchemaContractMode.DISABLED)
        context = CheckContext(models=[model])
        results = await check.execute(context)
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_skip_when_no_columns(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        model = _make_model("m", contract_mode=SchemaContractMode.STRICT)
        context = CheckContext(models=[model])
        results = await check.execute(context)
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_pass_when_contract_satisfied(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        model = _make_model(
            "m",
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="name", data_type="STRING"),
            ],
            output_columns=["id", "name"],
        )
        context = CheckContext(models=[model])
        results = await check.execute(context)
        assert len(results) == 1
        assert results[0].status == CheckStatus.PASS

    @pytest.mark.asyncio
    async def test_fail_when_column_removed(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        model = _make_model(
            "m",
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="name", data_type="STRING"),
            ],
            output_columns=["id"],  # "name" is missing
        )
        context = CheckContext(models=[model])
        results = await check.execute(context)
        failed = [r for r in results if r.status == CheckStatus.FAIL]
        assert len(failed) >= 1
        assert any("COLUMN_REMOVED" in r.detail for r in failed)

    @pytest.mark.asyncio
    async def test_warn_mode_downgrades_severity(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        model = _make_model(
            "m",
            contract_mode=SchemaContractMode.WARN,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=[],  # Column missing
        )
        context = CheckContext(models=[model])
        results = await check.execute(context)
        # In WARN mode, breaking violations should be WARN status, not FAIL.
        violations = [r for r in results if r.status in (CheckStatus.FAIL, CheckStatus.WARN)]
        assert len(violations) >= 1
        # Check that severity is downgraded from CRITICAL.
        for v in violations:
            if "COLUMN_REMOVED" in v.detail:
                assert v.status == CheckStatus.WARN
                assert v.severity == CheckSeverity.MEDIUM

    @pytest.mark.asyncio
    async def test_deterministic_ordering(self):
        from core_engine.checks.builtin.schema_contracts import SchemaContractCheck

        check = SchemaContractCheck()
        models = [
            _make_model("z", contract_mode=SchemaContractMode.STRICT, contract_columns=[ColumnContract(name="a", data_type="INT")], output_columns=["a"]),
            _make_model("a", contract_mode=SchemaContractMode.STRICT, contract_columns=[ColumnContract(name="b", data_type="INT")], output_columns=["b"]),
        ]
        context = CheckContext(models=models)
        r1 = await check.execute(context)
        r2 = await check.execute(context)
        assert [r.model_name for r in r1] == [r.model_name for r in r2]


# ---------------------------------------------------------------------------
# Built-in: ModelTestCheck tests
# ---------------------------------------------------------------------------


class TestModelTestCheck:
    """Tests for the ModelTestCheck built-in."""

    @pytest.mark.asyncio
    async def test_skip_when_no_tests(self):
        from core_engine.checks.builtin.model_tests import ModelTestCheck

        check = ModelTestCheck()
        model = _make_model("m")
        context = CheckContext(models=[model])
        results = await check.execute(context)
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_model_name_filter(self):
        from core_engine.checks.builtin.model_tests import ModelTestCheck

        check = ModelTestCheck()
        models = [_make_model("a"), _make_model("b"), _make_model("c")]
        context = CheckContext(models=models, model_names=["a", "c"])
        results = await check.execute(context)
        names = {r.model_name for r in results}
        assert names == {"a", "c"}
