"""Tests for contract violations flowing through the plan generation pipeline.

Verifies that when ``ContractValidationResult`` is passed into the interval
planner's ``generate_plan()``, violations are correctly embedded into each
``PlanStep.contract_violations`` list and the ``PlanSummary`` aggregation
fields (``contract_violations_count``, ``breaking_contract_violations``) are
computed accurately.
"""

from __future__ import annotations

from datetime import date

import networkx as nx
import pytest

from core_engine.contracts.schema_validator import (
    ContractValidationResult,
    ContractViolation,
    ViolationSeverity,
)
from core_engine.models.diff import DiffResult
from core_engine.models.model_definition import (
    ColumnContract,
    ModelDefinition,
    ModelKind,
    SchemaContractMode,
)
from core_engine.models.plan import Plan, PlanStep, PlanSummary
from core_engine.planner.interval_planner import PlannerConfig, generate_plan

# ---------------------------------------------------------------------------
# Fixed date for determinism
# ---------------------------------------------------------------------------

AS_OF = date(2024, 6, 15)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model(
    name: str,
    kind: ModelKind = ModelKind.FULL_REFRESH,
    contract_mode: SchemaContractMode = SchemaContractMode.DISABLED,
    contract_columns: list | None = None,
    output_columns: list | None = None,
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=kind,
        file_path=f"models/{name.replace('.', '/')}.sql",
        raw_sql=f"SELECT * FROM source_{name}",
        clean_sql=f"SELECT * FROM source_{name}",
        content_hash=f"hash_{name}",
        contract_mode=contract_mode,
        contract_columns=contract_columns or [],
        output_columns=output_columns or [],
    )


def _models_dict(*defs: ModelDefinition) -> dict[str, ModelDefinition]:
    return {m.name: m for m in defs}


def _diff(
    added: list[str] | None = None,
    removed: list[str] | None = None,
    modified: list[str] | None = None,
) -> DiffResult:
    return DiffResult(
        added_models=added or [],
        removed_models=removed or [],
        modified_models=modified or [],
    )


def _violation(
    model_name: str,
    column_name: str,
    violation_type: str = "COLUMN_REMOVED",
    severity: ViolationSeverity = ViolationSeverity.BREAKING,
    expected: str = "order_id: INT",
    actual: str = "(missing)",
    message: str = "",
) -> ContractViolation:
    return ContractViolation(
        model_name=model_name,
        column_name=column_name,
        violation_type=violation_type,
        severity=severity,
        expected=expected,
        actual=actual,
        message=message or f"Violation on {model_name}.{column_name}",
    )


def _contract_result(
    violations: list[ContractViolation] | None = None,
    models_checked: int = 1,
) -> ContractValidationResult:
    return ContractValidationResult(
        violations=violations or [],
        models_checked=models_checked,
    )


# ---------------------------------------------------------------------------
# 1. Plan without contract results (backward compatibility)
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Plans generated without contract_results behave identically to before."""

    def test_steps_have_empty_contract_violations_when_no_results(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )

        assert len(plan.steps) == 1
        assert plan.steps[0].contract_violations == []

    def test_summary_violations_zero_when_no_results(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
        )

        assert plan.summary.contract_violations_count == 0
        assert plan.summary.breaking_contract_violations == 0

    def test_explicit_none_contract_results_same_as_omitted(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=None,
        )

        assert plan.steps[0].contract_violations == []
        assert plan.summary.contract_violations_count == 0
        assert plan.summary.breaking_contract_violations == 0


# ---------------------------------------------------------------------------
# 2. Plan with contract results containing violations
# ---------------------------------------------------------------------------


class TestViolationsInPlanSteps:
    """Violations from ContractValidationResult appear in the correct steps."""

    def test_column_removed_violation_in_step(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        v = _violation(
            model_name="staging.orders",
            column_name="order_id",
            violation_type="COLUMN_REMOVED",
            severity=ViolationSeverity.BREAKING,
            expected="order_id: INT",
            actual="(missing)",
            message="Contracted column 'order_id' is missing from model output.",
        )
        cr = _contract_result(violations=[v])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert len(plan.steps[0].contract_violations) == 1
        cv = plan.steps[0].contract_violations[0]
        assert cv["violation_type"] == "COLUMN_REMOVED"
        assert cv["column_name"] == "order_id"

    def test_type_changed_violation_in_step(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        v = _violation(
            model_name="staging.orders",
            column_name="amount",
            violation_type="TYPE_CHANGED",
            severity=ViolationSeverity.BREAKING,
            expected="DECIMAL",
            actual="STRING",
            message="Column 'amount' type changed: contract declares DECIMAL, actual is STRING.",
        )
        cr = _contract_result(violations=[v])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        cv = plan.steps[0].contract_violations[0]
        assert cv["violation_type"] == "TYPE_CHANGED"
        assert cv["expected"] == "DECIMAL"
        assert cv["actual"] == "STRING"

    def test_multiple_violations_same_model(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="order_id",
                violation_type="COLUMN_REMOVED",
            ),
            _violation(
                model_name="staging.orders",
                column_name="amount",
                violation_type="TYPE_CHANGED",
                expected="DECIMAL",
                actual="STRING",
            ),
            _violation(
                model_name="staging.orders",
                column_name="status",
                violation_type="COLUMN_REMOVED",
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert len(plan.steps[0].contract_violations) == 3

    def test_violations_for_different_models_go_to_respective_steps(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders", "staging.customers"])

        violations = [
            _violation(model_name="staging.orders", column_name="order_id"),
            _violation(model_name="staging.customers", column_name="email"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        step_map = {s.model: s for s in plan.steps}
        assert len(step_map["staging.orders"].contract_violations) == 1
        assert step_map["staging.orders"].contract_violations[0]["column_name"] == "order_id"
        assert len(step_map["staging.customers"].contract_violations) == 1
        assert step_map["staging.customers"].contract_violations[0]["column_name"] == "email"

    def test_violation_dict_has_correct_keys(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        v = _violation(
            model_name="staging.orders",
            column_name="order_id",
            violation_type="COLUMN_REMOVED",
            severity=ViolationSeverity.BREAKING,
            expected="order_id: INT",
            actual="(missing)",
            message="Column removed.",
        )
        cr = _contract_result(violations=[v])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        cv = plan.steps[0].contract_violations[0]
        expected_keys = {"column_name", "violation_type", "severity", "expected", "actual", "message"}
        assert set(cv.keys()) == expected_keys

    def test_severity_stored_as_string_value(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        v = _violation(
            model_name="staging.orders",
            column_name="order_id",
            severity=ViolationSeverity.BREAKING,
        )
        cr = _contract_result(violations=[v])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        cv = plan.steps[0].contract_violations[0]
        assert cv["severity"] == "BREAKING"
        assert isinstance(cv["severity"], str)

    def test_info_severity_stored_correctly(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        v = _violation(
            model_name="staging.orders",
            column_name="new_col",
            violation_type="COLUMN_ADDED",
            severity=ViolationSeverity.INFO,
            expected="(not in contract)",
            actual="new_col",
        )
        cr = _contract_result(violations=[v])

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        cv = plan.steps[0].contract_violations[0]
        assert cv["severity"] == "INFO"


# ---------------------------------------------------------------------------
# 3. PlanSummary aggregation
# ---------------------------------------------------------------------------


class TestPlanSummaryAggregation:
    """Summary fields correctly aggregate violation counts across all steps."""

    def test_total_violations_sums_across_steps(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders", "staging.customers"])

        violations = [
            _violation(model_name="staging.orders", column_name="order_id"),
            _violation(model_name="staging.orders", column_name="amount"),
            _violation(model_name="staging.customers", column_name="email"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 3

    def test_breaking_violations_counts_only_breaking(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="order_id",
                severity=ViolationSeverity.BREAKING,
            ),
            _violation(
                model_name="staging.orders",
                column_name="new_col",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 2
        assert plan.summary.breaking_contract_violations == 1

    def test_mixed_severities_across_models(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders", "staging.customers"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="order_id",
                severity=ViolationSeverity.BREAKING,
            ),
            _violation(
                model_name="staging.orders",
                column_name="extra",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
            _violation(
                model_name="staging.customers",
                column_name="email",
                severity=ViolationSeverity.BREAKING,
            ),
            _violation(
                model_name="staging.customers",
                column_name="phone",
                violation_type="NULLABLE_TIGHTENED",
                severity=ViolationSeverity.WARNING,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 4
        assert plan.summary.breaking_contract_violations == 2

    def test_all_info_violations_zero_breaking(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="col_a",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
            _violation(
                model_name="staging.orders",
                column_name="col_b",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 2
        assert plan.summary.breaking_contract_violations == 0

    def test_all_breaking_violations(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="col_a",
                severity=ViolationSeverity.BREAKING,
            ),
            _violation(
                model_name="staging.orders",
                column_name="col_b",
                severity=ViolationSeverity.BREAKING,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 2
        assert plan.summary.breaking_contract_violations == 2


# ---------------------------------------------------------------------------
# 4. Models not in the plan
# ---------------------------------------------------------------------------


class TestViolationsForModelsNotInPlan:
    """Violations for models not in the plan's affected set are ignored."""

    def test_violation_for_unaffected_model_not_in_steps(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        # Only orders is modified; customers is unchanged.
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(model_name="staging.orders", column_name="order_id"),
            _violation(model_name="staging.customers", column_name="email"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        # Only staging.orders is in the plan.
        assert len(plan.steps) == 1
        assert plan.steps[0].model == "staging.orders"
        assert len(plan.steps[0].contract_violations) == 1
        assert plan.steps[0].contract_violations[0]["column_name"] == "order_id"

    def test_summary_excludes_violations_for_unaffected_models(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="order_id",
                severity=ViolationSeverity.BREAKING,
            ),
            _violation(
                model_name="staging.customers",
                column_name="email",
                severity=ViolationSeverity.BREAKING,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        # The customers violation is not included because that model is not in the plan.
        assert plan.summary.contract_violations_count == 1
        assert plan.summary.breaking_contract_violations == 1

    def test_violation_for_nonexistent_model_ignored(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(model_name="staging.orders", column_name="order_id"),
            _violation(model_name="nonexistent.model", column_name="foo"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert len(plan.steps) == 1
        assert len(plan.steps[0].contract_violations) == 1
        assert plan.summary.contract_violations_count == 1


# ---------------------------------------------------------------------------
# 5. Empty contract results
# ---------------------------------------------------------------------------


class TestEmptyContractResults:
    """ContractValidationResult with no violations yields clean plan steps."""

    def test_empty_violations_list_yields_empty_step_violations(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        cr = _contract_result(violations=[], models_checked=3)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.steps[0].contract_violations == []
        assert plan.summary.contract_violations_count == 0
        assert plan.summary.breaking_contract_violations == 0

    def test_empty_violations_with_multiple_models(self):
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders", "staging.customers"])

        cr = _contract_result(violations=[], models_checked=2)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        for step in plan.steps:
            assert step.contract_violations == []
        assert plan.summary.contract_violations_count == 0


# ---------------------------------------------------------------------------
# 6. Determinism
# ---------------------------------------------------------------------------


class TestContractViolationDeterminism:
    """Contract violations appear in deterministic order across runs."""

    def test_violations_sorted_by_model_then_column(self):
        """The real validation pipeline (validate_schema_contracts_batch)
        pre-sorts violations by (model_name, column_name, violation_type).
        The planner preserves that deterministic order via violations_for_model,
        which returns violations in their original list order filtered to the
        model.  We verify that pre-sorted input arrives at steps in the same
        deterministic order.
        """
        m_orders = _make_model("staging.orders")
        m_customers = _make_model("staging.customers")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        dag.add_node("staging.customers")
        diff = _diff(modified=["staging.orders", "staging.customers"])

        # Violations pre-sorted as validate_schema_contracts_batch would
        # produce: sorted by (model_name, column_name, violation_type).
        violations = [
            _violation(model_name="staging.customers", column_name="a_col"),
            _violation(model_name="staging.customers", column_name="b_col"),
            _violation(model_name="staging.orders", column_name="a_col"),
            _violation(model_name="staging.orders", column_name="z_col"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_orders, m_customers),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        # Steps are sorted by model name.
        assert plan.steps[0].model == "staging.customers"
        assert plan.steps[1].model == "staging.orders"

        # Violations arrive in the pre-sorted order established upstream.
        cust_cols = [v["column_name"] for v in plan.steps[0].contract_violations]
        assert cust_cols == ["a_col", "b_col"]

        order_cols = [v["column_name"] for v in plan.steps[1].contract_violations]
        assert order_cols == ["a_col", "z_col"]

    def test_identical_runs_produce_identical_violation_order(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(model_name="staging.orders", column_name="z_col"),
            _violation(model_name="staging.orders", column_name="a_col"),
            _violation(model_name="staging.orders", column_name="m_col"),
        ]
        cr = _contract_result(violations=violations)

        plan1 = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
            contract_results=cr,
        )
        plan2 = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan1.steps[0].contract_violations == plan2.steps[0].contract_violations
        assert plan1.plan_id == plan2.plan_id

    def test_plan_id_unchanged_by_contract_results(self):
        """Plan ID is computed from base/target/step IDs, not violations.

        Contract violations are embedded data, but the plan identity (plan_id)
        should remain deterministic from the same base, target, and model set.
        """
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        plan_without = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
            contract_results=None,
        )

        cr = _contract_result(
            violations=[
                _violation(model_name="staging.orders", column_name="order_id"),
            ]
        )
        plan_with = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            base="snap1",
            target="snap2",
            as_of_date=AS_OF,
            contract_results=cr,
        )

        # plan_id is derived from base + target + step_ids (which come from
        # model names and snapshot IDs), not from violation content.
        assert plan_without.plan_id == plan_with.plan_id


# ---------------------------------------------------------------------------
# 7. Downstream propagation with contract violations
# ---------------------------------------------------------------------------


class TestDownstreamWithContracts:
    """Violations attach to downstream steps correctly when DAG edges exist."""

    def test_downstream_step_gets_its_own_violations(self):
        m_source = _make_model("staging.source")
        m_derived = _make_model("staging.derived")
        dag = nx.DiGraph()
        dag.add_edge("staging.source", "staging.derived")
        diff = _diff(modified=["staging.source"])

        violations = [
            _violation(model_name="staging.source", column_name="id"),
            _violation(model_name="staging.derived", column_name="derived_col"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_source, m_derived),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        step_map = {s.model: s for s in plan.steps}
        assert len(step_map) == 2
        assert len(step_map["staging.source"].contract_violations) == 1
        assert step_map["staging.source"].contract_violations[0]["column_name"] == "id"
        assert len(step_map["staging.derived"].contract_violations) == 1
        assert step_map["staging.derived"].contract_violations[0]["column_name"] == "derived_col"

    def test_downstream_step_no_violations_when_only_upstream_has_them(self):
        m_source = _make_model("staging.source")
        m_derived = _make_model("staging.derived")
        dag = nx.DiGraph()
        dag.add_edge("staging.source", "staging.derived")
        diff = _diff(modified=["staging.source"])

        violations = [
            _violation(model_name="staging.source", column_name="id"),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m_source, m_derived),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        step_map = {s.model: s for s in plan.steps}
        assert len(step_map["staging.source"].contract_violations) == 1
        assert step_map["staging.derived"].contract_violations == []


# ---------------------------------------------------------------------------
# 8. Warning severity handling
# ---------------------------------------------------------------------------


class TestWarningSeverity:
    """WARNING severity is counted in total but not in breaking."""

    def test_warning_violation_in_total_not_breaking(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="phone",
                violation_type="NULLABLE_TIGHTENED",
                severity=ViolationSeverity.WARNING,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 1
        assert plan.summary.breaking_contract_violations == 0

    def test_warning_severity_stored_as_string(self):
        m = _make_model("staging.orders")
        dag = nx.DiGraph()
        dag.add_node("staging.orders")
        diff = _diff(modified=["staging.orders"])

        violations = [
            _violation(
                model_name="staging.orders",
                column_name="phone",
                severity=ViolationSeverity.WARNING,
            ),
        ]
        cr = _contract_result(violations=violations)

        plan = generate_plan(
            models=_models_dict(m),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.steps[0].contract_violations[0]["severity"] == "WARNING"


# ---------------------------------------------------------------------------
# 9. Large-scale aggregation
# ---------------------------------------------------------------------------


class TestLargeScaleAggregation:
    """Summaries aggregate correctly when many models and violations exist."""

    def test_many_models_many_violations(self):
        model_names = [f"staging.model_{i:03d}" for i in range(10)]
        models_list = [_make_model(name) for name in model_names]
        dag = nx.DiGraph()
        for name in model_names:
            dag.add_node(name)
        diff = _diff(modified=model_names)

        violations = []
        for name in model_names:
            violations.append(
                _violation(
                    model_name=name,
                    column_name="col_a",
                    severity=ViolationSeverity.BREAKING,
                )
            )
            violations.append(
                _violation(
                    model_name=name,
                    column_name="col_b",
                    violation_type="COLUMN_ADDED",
                    severity=ViolationSeverity.INFO,
                )
            )
        cr = _contract_result(violations=violations, models_checked=10)

        plan = generate_plan(
            models=_models_dict(*models_list),
            diff_result=diff,
            dag=dag,
            watermarks={},
            run_stats={},
            as_of_date=AS_OF,
            contract_results=cr,
        )

        assert plan.summary.contract_violations_count == 20
        assert plan.summary.breaking_contract_violations == 10
        for step in plan.steps:
            assert len(step.contract_violations) == 2
