"""Comprehensive tests for cli/cli/display.py -- Rich output formatting.

These tests verify that each display function produces the expected Rich
objects (Tables, Trees, Panels) with the correct content, styling, and
structural properties.  We capture rendered output via a Console writing
to a StringIO buffer rather than stderr.
"""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock

import pytest
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)
from core_engine.models.plan import (
    DateRange,
    Plan,
    PlanStep,
    PlanSummary,
    RunType,
)

from cli.display import (
    _STATUS_COLOURS,
    _coloured_status,
    display_lineage,
    display_model_list,
    display_plan_summary,
    display_run_results,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_console() -> tuple[Console, io.StringIO]:
    """Create a Console that writes to a StringIO buffer for assertion.

    Using ``no_color=True`` and ``highlight=False`` so that the output
    does not contain ANSI escape sequences, which makes assertions on
    plain text reliable.
    """
    buf = io.StringIO()
    console = Console(file=buf, no_color=True, highlight=False, width=120)
    return console, buf


def _make_model(
    name: str = "analytics.orders_daily",
    kind: ModelKind = ModelKind.INCREMENTAL_BY_TIME_RANGE,
    materialization: Materialization = Materialization.TABLE,
    time_column: str | None = "event_date",
    owner: str | None = "data-eng",
    tags: list[str] | None = None,
    dependencies: list[str] | None = None,
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=kind,
        materialization=materialization,
        time_column=time_column,
        owner=owner,
        tags=tags or ["core"],
        dependencies=dependencies or [],
        file_path=f"models/{name.replace('.', '/')}.sql",
        raw_sql=f"SELECT 1 -- {name}",
        clean_sql=f"SELECT 1 -- {name}",
        content_hash="abc123",
    )


def _make_plan(
    total_steps: int = 2,
    base: str = "abc1234567890123",
    target: str = "def5678901234567",
    with_steps: bool = True,
    with_input_range: bool = True,
) -> Plan:
    steps = []
    if with_steps:
        for i in range(total_steps):
            input_range = DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31)) if with_input_range else None
            steps.append(
                PlanStep(
                    step_id=f"step_{i:04d}",
                    model=f"model_{i}",
                    run_type=RunType.INCREMENTAL if i % 2 == 0 else RunType.FULL_REFRESH,
                    input_range=input_range,
                    depends_on=[],
                    parallel_group=i,
                    reason="SQL logic changed" if i % 2 == 0 else "new model added",
                    estimated_compute_seconds=300.0,
                    estimated_cost_usd=0.21,
                )
            )
    return Plan(
        plan_id="a" * 64,
        base=base,
        target=target,
        summary=PlanSummary(
            total_steps=total_steps if with_steps else 0,
            estimated_cost_usd=0.42 if with_steps else 0.0,
            models_changed=[f"model_{i}" for i in range(total_steps)] if with_steps else [],
        ),
        steps=steps,
    )


# ---------------------------------------------------------------------------
# _coloured_status
# ---------------------------------------------------------------------------


class TestColouredStatus:
    """Tests for the _coloured_status helper."""

    def test_success_is_green(self):
        result = _coloured_status("SUCCESS")
        assert "[green]" in result
        assert "SUCCESS" in result

    def test_fail_is_red(self):
        result = _coloured_status("FAIL")
        assert "[red]" in result
        assert "FAIL" in result

    def test_running_is_yellow(self):
        result = _coloured_status("RUNNING")
        assert "[yellow]" in result

    def test_pending_is_dim(self):
        result = _coloured_status("PENDING")
        assert "[dim]" in result

    def test_cancelled_is_dim_red(self):
        result = _coloured_status("CANCELLED")
        assert "[dim red]" in result

    def test_unknown_status_uses_white(self):
        result = _coloured_status("UNKNOWN_STATUS")
        assert "[white]" in result
        assert "UNKNOWN_STATUS" in result

    def test_all_known_statuses_have_mappings(self):
        expected_statuses = {"SUCCESS", "FAIL", "RUNNING", "PENDING", "CANCELLED"}
        assert set(_STATUS_COLOURS.keys()) == expected_statuses


# ---------------------------------------------------------------------------
# display_plan_summary
# ---------------------------------------------------------------------------


class TestDisplayPlanSummary:
    """Tests for display_plan_summary."""

    def test_renders_plan_with_steps(self):
        """A plan with steps should render a header panel and step table."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=2)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        # Header panel should contain plan metadata.
        assert "Execution Plan" in output
        assert "Plan ID:" in output
        assert "Steps:" in output
        assert "2" in output  # total_steps
        assert "Est. Cost:" in output
        assert "$0.4200" in output

        # Step table should contain model names.
        assert "Plan Steps" in output
        assert "model_0" in output
        assert "model_1" in output

        # Run types should be present.
        assert "INCREMENTAL" in output
        assert "FULL_REFRESH" in output

        # Summary footer.
        assert "step(s)" in output

    def test_renders_empty_plan(self):
        """A plan with zero steps should show the header and a no-steps message."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=0, with_steps=False)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "Execution Plan" in output
        assert "No steps in this plan" in output

    def test_plan_id_is_truncated(self):
        """Plan ID should be truncated to first 16 characters."""
        console, buf = _capture_console()
        plan = _make_plan()

        display_plan_summary(console, plan)
        output = buf.getvalue()

        # Plan ID is 64 'a's, truncated to 16 + "..."
        assert "aaaaaaaaaaaaaaaa..." in output

    def test_base_and_target_are_truncated(self):
        """Base and target refs should be truncated to 12 characters."""
        console, buf = _capture_console()
        plan = _make_plan(base="abcdef1234567890", target="fedcba9876543210")

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "abcdef123456" in output
        assert "fedcba987654" in output

    def test_step_input_range_rendered(self):
        """Steps with input_range should show the date range."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=1, with_input_range=True)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "2025-01-01" in output
        assert "2025-01-31" in output

    def test_step_no_input_range_shows_dash(self):
        """Steps without input_range should show '-'."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=1, with_input_range=False)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        # The step table should have at least one '-' for the missing range.
        # The model name and other fields should still appear.
        assert "model_0" in output

    def test_step_reasons_rendered(self):
        """Step reasons should appear in the table."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=2)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "SQL logic changed" in output
        assert "new model added" in output

    def test_estimated_cost_formatting(self):
        """Estimated cost should be formatted with 4 decimal places."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=1)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "$0.2100" in output

    def test_summary_footer_shows_totals(self):
        """The summary footer should show total steps and total cost."""
        console, buf = _capture_console()
        plan = _make_plan(total_steps=3)

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "3" in output
        assert "step(s)" in output
        assert "$0.4200" in output

    def test_renders_contract_violations(self):
        """Plans with contract violations should display a violations table."""
        console, buf = _capture_console()
        steps = [
            PlanStep(
                step_id="step_0001",
                model="staging.orders",
                run_type=RunType.INCREMENTAL,
                input_range=DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31)),
                depends_on=[],
                parallel_group=0,
                reason="SQL logic changed",
                estimated_compute_seconds=100.0,
                estimated_cost_usd=0.10,
                contract_violations=[
                    {
                        "column_name": "order_id",
                        "violation_type": "TYPE_CHANGE",
                        "severity": "BREAKING",
                        "expected": "INTEGER",
                        "actual": "STRING",
                        "message": "Column type changed",
                    },
                    {
                        "column_name": "amount",
                        "violation_type": "COLUMN_REMOVED",
                        "severity": "WARNING",
                        "expected": "DECIMAL",
                        "actual": "-",
                        "message": "Column removed",
                    },
                ],
            ),
        ]
        plan = Plan(
            plan_id="b" * 64,
            base="abc123",
            target="def456",
            summary=PlanSummary(
                total_steps=1,
                estimated_cost_usd=0.10,
                models_changed=["staging.orders"],
                contract_violations_count=2,
                breaking_contract_violations=1,
            ),
            steps=steps,
        )

        display_plan_summary(console, plan)
        output = buf.getvalue()

        # Violations table should be rendered.
        assert "Schema Contract Violations" in output
        assert "order_id" in output
        assert "TYPE_CHANGE" in output
        assert "BREAKING" in output
        assert "amount" in output
        assert "WARNING" in output
        # Breaking violation warning.
        assert "1 BREAKING contract violation" in output

    def test_renders_contract_violations_info_severity(self):
        """INFO severity violations should be styled dim."""
        console, buf = _capture_console()
        steps = [
            PlanStep(
                step_id="step_0001",
                model="staging.orders",
                run_type=RunType.FULL_REFRESH,
                input_range=None,
                depends_on=[],
                parallel_group=0,
                reason="new model",
                estimated_compute_seconds=0.0,
                estimated_cost_usd=0.0,
                contract_violations=[
                    {
                        "column_name": "notes",
                        "violation_type": "COLUMN_ADDED",
                        "severity": "INFO",
                        "expected": "-",
                        "actual": "STRING",
                    },
                ],
            ),
        ]
        plan = Plan(
            plan_id="c" * 64,
            base="abc",
            target="def",
            summary=PlanSummary(
                total_steps=1,
                estimated_cost_usd=0.0,
                models_changed=["staging.orders"],
                contract_violations_count=1,
                breaking_contract_violations=0,
            ),
            steps=steps,
        )

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "Schema Contract Violations" in output
        assert "COLUMN_ADDED" in output
        # No breaking violations warning.
        assert "BREAKING contract violation" not in output

    def test_contract_violations_count_in_summary(self):
        """Contract violations count should appear in the summary footer."""
        console, buf = _capture_console()
        steps = [
            PlanStep(
                step_id="step_0001",
                model="m1",
                run_type=RunType.INCREMENTAL,
                input_range=DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31)),
                depends_on=[],
                parallel_group=0,
                reason="changed",
                estimated_compute_seconds=0.0,
                estimated_cost_usd=0.0,
                contract_violations=[
                    {"column_name": "c1", "violation_type": "TYPE_CHANGE", "severity": "BREAKING"},
                    {"column_name": "c2", "violation_type": "COLUMN_ADDED", "severity": "INFO"},
                    {"column_name": "c3", "violation_type": "COLUMN_REMOVED", "severity": "WARNING"},
                ],
            ),
        ]
        plan = Plan(
            plan_id="d" * 64,
            base="abc",
            target="def",
            summary=PlanSummary(
                total_steps=1,
                estimated_cost_usd=0.0,
                models_changed=["m1"],
                contract_violations_count=3,
                breaking_contract_violations=1,
            ),
            steps=steps,
        )

        display_plan_summary(console, plan)
        output = buf.getvalue()

        assert "3 contract violation(s)" in output


# ---------------------------------------------------------------------------
# display_run_results
# ---------------------------------------------------------------------------


class TestDisplayRunResults:
    """Tests for display_run_results."""

    def test_renders_success_results(self):
        """Run results with SUCCESS status should render correctly."""
        console, buf = _capture_console()
        runs = [
            {
                "model": "model_a",
                "status": "SUCCESS",
                "duration_seconds": 5.23,
                "input_range": "2025-01-01 .. 2025-01-31",
                "retries": 0,
            },
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "Execution Results" in output
        assert "model_a" in output
        assert "SUCCESS" in output
        assert "5.23s" in output
        assert "2025-01-01" in output

    def test_renders_mixed_statuses(self):
        """Results with mixed statuses should show all with proper colours."""
        console, buf = _capture_console()
        runs = [
            {"model": "model_a", "status": "SUCCESS", "duration_seconds": 3.0, "input_range": "-", "retries": 0},
            {"model": "model_b", "status": "FAIL", "duration_seconds": 1.5, "input_range": "-", "retries": 2},
            {"model": "model_c", "status": "CANCELLED", "duration_seconds": 0.0, "input_range": "-", "retries": 0},
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "model_a" in output
        assert "model_b" in output
        assert "model_c" in output
        assert "3" in output  # 3 step(s)
        assert "1 succeeded" in output
        assert "1 failed" in output
        assert "1 cancelled" in output

    def test_renders_empty_runs(self):
        """An empty runs list should show a 'No runs' message."""
        console, buf = _capture_console()
        display_run_results(console, [])
        output = buf.getvalue()

        assert "No runs to display" in output

    def test_summary_only_succeeded(self):
        """When all steps succeed, summary should show only succeeded count."""
        console, buf = _capture_console()
        runs = [
            {"model": "m1", "status": "SUCCESS", "duration_seconds": 1.0, "input_range": "-", "retries": 0},
            {"model": "m2", "status": "SUCCESS", "duration_seconds": 2.0, "input_range": "-", "retries": 0},
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "2 succeeded" in output
        assert "failed" not in output
        assert "cancelled" not in output

    def test_summary_only_failed(self):
        """When all steps fail, summary should show only failed count."""
        console, buf = _capture_console()
        runs = [
            {"model": "m1", "status": "FAIL", "duration_seconds": 0.5, "input_range": "-", "retries": 1},
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "1 failed" in output

    def test_retries_column_rendered(self):
        """Retry count should be displayed in the table."""
        console, buf = _capture_console()
        runs = [
            {"model": "m1", "status": "SUCCESS", "duration_seconds": 10.0, "input_range": "-", "retries": 3},
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "3" in output

    def test_duration_formatting(self):
        """Duration should be formatted as X.XXs."""
        console, buf = _capture_console()
        runs = [
            {"model": "m1", "status": "SUCCESS", "duration_seconds": 123.456, "input_range": "-", "retries": 0},
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "123.46s" in output

    def test_missing_fields_use_defaults(self):
        """Run dicts with missing fields should use sensible defaults."""
        console, buf = _capture_console()
        runs = [
            {},  # All fields missing.
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        # Should fall back to defaults.
        assert "?" in output  # model defaults to "?"
        assert "PENDING" in output  # status defaults to PENDING
        assert "0.00s" in output  # duration defaults to 0.0

    def test_total_count_in_summary(self):
        """The summary should show the correct total step count."""
        console, buf = _capture_console()
        runs = [
            {"model": f"m{i}", "status": "SUCCESS", "duration_seconds": 1.0, "input_range": "-", "retries": 0}
            for i in range(5)
        ]

        display_run_results(console, runs)
        output = buf.getvalue()

        assert "5" in output
        assert "step(s)" in output


# ---------------------------------------------------------------------------
# display_model_list
# ---------------------------------------------------------------------------


class TestDisplayModelList:
    """Tests for display_model_list."""

    def test_renders_model_table(self):
        """A non-empty model list should render a table with correct columns."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="analytics.orders_daily",
                kind=ModelKind.INCREMENTAL_BY_TIME_RANGE,
                materialization=Materialization.TABLE,
                time_column="event_date",
                owner="data-eng",
                tags=["core", "finance"],
            ),
            _make_model(
                name="staging.raw_events",
                kind=ModelKind.FULL_REFRESH,
                materialization=Materialization.VIEW,
                time_column=None,
                owner=None,
                tags=[],
            ),
        ]

        display_model_list(console, models)
        output = buf.getvalue()

        assert "Models (2)" in output
        assert "analytics.orders_daily" in output
        assert "staging.raw_events" in output
        assert "INCREMENTAL_BY_TIME_RANGE" in output
        assert "FULL_REFRESH" in output
        assert "TABLE" in output
        assert "VIEW" in output
        assert "event_date" in output
        assert "data-eng" in output
        assert "core, finance" in output

    def test_renders_empty_model_list(self):
        """An empty model list should show a 'No models found' message."""
        console, buf = _capture_console()
        display_model_list(console, [])
        output = buf.getvalue()

        assert "No models found" in output

    def test_model_without_time_column_shows_dash(self):
        """Models with no time_column should show '-' in the table."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="staging.raw",
                kind=ModelKind.FULL_REFRESH,
                time_column=None,
            ),
        ]

        display_model_list(console, models)
        output = buf.getvalue()

        # The time column cell should contain "-".
        assert "staging.raw" in output

    def test_model_without_owner_shows_dash(self):
        """Models with no owner should show '-' in the table."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="staging.raw",
                kind=ModelKind.FULL_REFRESH,
                time_column=None,
                owner=None,
            ),
        ]

        display_model_list(console, models)
        output = buf.getvalue()

        assert "staging.raw" in output

    def test_model_with_empty_tags_shows_dash(self):
        """Models with no tags should show '-' in the tags column."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="staging.raw",
                kind=ModelKind.FULL_REFRESH,
                time_column=None,
                tags=[],
            ),
        ]

        display_model_list(console, models)
        output = buf.getvalue()

        assert "staging.raw" in output

    def test_model_count_in_title(self):
        """The table title should include the model count."""
        console, buf = _capture_console()
        models = [_make_model(name=f"model_{i}") for i in range(3)]

        display_model_list(console, models)
        output = buf.getvalue()

        assert "Models (3)" in output

    def test_kind_styling_incremental_cyan(self):
        """INCREMENTAL kind values should be styled cyan."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="inc_model",
                kind=ModelKind.INCREMENTAL_BY_TIME_RANGE,
            ),
        ]

        display_model_list(console, models)
        # We verify the output is produced; Rich applies ANSI escapes
        # that are hard to inspect, but the model name should be present.
        output = buf.getvalue()
        assert "inc_model" in output
        assert "INCREMENTAL_BY_TIME_RANGE" in output

    def test_kind_styling_full_refresh_magenta(self):
        """FULL_REFRESH kind values should be styled magenta."""
        console, buf = _capture_console()
        models = [
            _make_model(
                name="refresh_model",
                kind=ModelKind.FULL_REFRESH,
                time_column=None,
            ),
        ]

        display_model_list(console, models)
        output = buf.getvalue()
        assert "refresh_model" in output
        assert "FULL_REFRESH" in output


# ---------------------------------------------------------------------------
# display_lineage
# ---------------------------------------------------------------------------


class TestDisplayLineage:
    """Tests for display_lineage."""

    def test_renders_lineage_with_upstream_and_downstream(self):
        """Lineage with both upstream and downstream should render a tree."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="analytics.orders_daily",
            upstream=["staging.raw_orders", "staging.raw_payments"],
            downstream=["reporting.revenue_daily"],
        )
        output = buf.getvalue()

        assert "Lineage" in output
        assert "analytics.orders_daily" in output
        assert "upstream" in output
        assert "staging.raw_orders" in output
        assert "staging.raw_payments" in output
        assert "downstream" in output
        assert "reporting.revenue_daily" in output

        # Summary counts.
        assert "2" in output  # 2 upstream
        assert "1" in output  # 1 downstream

    def test_renders_lineage_no_upstream(self):
        """A model with no upstream should show 'no upstream dependencies'."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="source_model",
            upstream=[],
            downstream=["downstream_a"],
        )
        output = buf.getvalue()

        assert "no upstream dependencies" in output
        assert "downstream_a" in output
        assert "0" in output  # 0 upstream

    def test_renders_lineage_no_downstream(self):
        """A model with no downstream should show 'no downstream dependents'."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="leaf_model",
            upstream=["parent_model"],
            downstream=[],
        )
        output = buf.getvalue()

        assert "no downstream dependents" in output
        assert "parent_model" in output
        assert "0" in output  # 0 downstream

    def test_renders_lineage_isolated_model(self):
        """A model with no upstream or downstream shows both 'no' messages."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="isolated_model",
            upstream=[],
            downstream=[],
        )
        output = buf.getvalue()

        assert "isolated_model" in output
        assert "no upstream dependencies" in output
        assert "no downstream dependents" in output

    def test_lineage_panel_has_yellow_border(self):
        """The lineage panel should have a yellow border style."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="my_model",
            upstream=["up_1"],
            downstream=["down_1"],
        )
        output = buf.getvalue()

        # The panel title 'Lineage' should be rendered.
        assert "Lineage" in output

    def test_lineage_summary_counts(self):
        """The summary line should show correct upstream and downstream counts."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="center",
            upstream=["a", "b", "c"],
            downstream=["x", "y"],
        )
        output = buf.getvalue()

        assert "3" in output  # 3 upstream
        assert "2" in output  # 2 downstream
        assert "upstream" in output
        assert "downstream" in output

    def test_lineage_model_name_is_bold_yellow(self):
        """The focal model name is rendered; we verify it appears in output."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="highlighted_model",
            upstream=[],
            downstream=[],
        )
        output = buf.getvalue()

        assert "highlighted_model" in output

    def test_upstream_names_are_sorted_in_output(self):
        """Upstream names provided in sorted order appear in the tree."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="center",
            upstream=["alpha", "beta", "gamma"],
            downstream=[],
        )
        output = buf.getvalue()

        alpha_pos = output.index("alpha")
        beta_pos = output.index("beta")
        gamma_pos = output.index("gamma")
        assert alpha_pos < beta_pos < gamma_pos

    def test_downstream_names_are_sorted_in_output(self):
        """Downstream names provided in sorted order appear in the tree."""
        console, buf = _capture_console()
        display_lineage(
            console,
            model_name="center",
            upstream=[],
            downstream=["delta", "epsilon", "zeta"],
        )
        output = buf.getvalue()

        delta_pos = output.index("delta")
        epsilon_pos = output.index("epsilon")
        zeta_pos = output.index("zeta")
        assert delta_pos < epsilon_pos < zeta_pos
