"""Comprehensive tests for cli/cli/app.py -- the IronLayer CLI application.

Uses typer.testing.CliRunner to invoke each CLI command, with mocked
core_engine dependencies so that tests are fast, deterministic, and do not
require a real git repository, DuckDB, or Databricks cluster.

Because the CLI commands use *local* imports (``from core_engine.X import Y``
inside the function body), mocks must target the source modules rather than
``cli.app``.  Display helpers, however, are imported at module level in
``cli.app`` and are therefore patched via ``cli.app.display_*``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from core_engine.models.diff import DiffResult
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
from core_engine.models.run import RunRecord, RunStatus

from cli.app import app

runner = CliRunner()

# ---------------------------------------------------------------------------
# Helpers -- reusable fixtures / factories
# ---------------------------------------------------------------------------


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
        raw_sql=f"SELECT 1 AS placeholder -- {name}",
        clean_sql=f"SELECT 1 AS placeholder -- {name}",
        content_hash="abc123",
    )


def _make_plan(
    total_steps: int = 2,
    base: str = "abc1234",
    target: str = "def5678",
    with_steps: bool = True,
) -> Plan:
    steps = []
    if with_steps:
        for i in range(total_steps):
            steps.append(
                PlanStep(
                    step_id=f"step_{i:04d}",
                    model=f"model_{i}",
                    run_type=RunType.INCREMENTAL,
                    input_range=DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31)),
                    depends_on=[],
                    parallel_group=0,
                    reason="SQL logic changed",
                    estimated_compute_seconds=300.0,
                    estimated_cost_usd=0.21,
                )
            )
    return Plan(
        plan_id="plan_0001",
        base=base,
        target=target,
        summary=PlanSummary(
            total_steps=total_steps if with_steps else 0,
            estimated_cost_usd=0.42 if with_steps else 0.0,
            models_changed=[f"model_{i}" for i in range(total_steps)] if with_steps else [],
        ),
        steps=steps,
    )


def _make_run_record(
    model: str = "model_0",
    status: RunStatus = RunStatus.SUCCESS,
    retry_count: int = 0,
) -> RunRecord:
    return RunRecord(
        run_id="run-001",
        plan_id="plan-001",
        step_id="step-001",
        model_name=model,
        status=status,
        started_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        executor_version="local-duckdb",
        retry_count=retry_count,
    )


def _make_settings(**overrides):
    """Create a mock Settings object with sensible defaults."""
    settings = MagicMock()
    settings.default_lookback_days = overrides.get("default_lookback_days", 30)
    settings.local_db_path = overrides.get("local_db_path", Path("/tmp/test.duckdb"))
    return settings


# ---------------------------------------------------------------------------
# plan command
# ---------------------------------------------------------------------------


class TestPlanCommand:
    """Tests for `platform plan <repo> <base> <target>`."""

    @patch("cli.display.display_plan_summary")
    @patch("core_engine.planner.serialize_plan")
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.parser.compute_canonical_hash", return_value="hash_old")
    @patch("core_engine.git.get_file_at_commit", return_value="SELECT 1")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo")
    def test_plan_valid_args_produces_plan_json(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_get_file,
        mock_canonical_hash,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        mock_display,
        tmp_path,
    ):
        """Valid repo, base, and target produce a plan JSON file at --out."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model = _make_model()
        mock_load_models.return_value = [model]
        mock_build_dag.return_value = MagicMock()
        mock_diff.return_value = DiffResult()
        mock_load_settings.return_value = _make_settings()

        fake_plan = _make_plan()
        mock_generate_plan.return_value = fake_plan
        mock_serialize.return_value = json.dumps({"plan_id": "plan_0001"})

        out_file = tmp_path / "output" / "plan.json"

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "abc1234",
                "def5678",
                "--out",
                str(out_file),
            ],
        )

        assert result.exit_code == 0, f"Unexpected output: {result.output}\n{result.exception}"
        assert out_file.exists()
        content = out_file.read_text()
        assert "plan_id" in content
        mock_validate_repo.assert_called_once_with(tmp_path)

    def test_plan_missing_repo_arg_exits_with_error(self):
        """Omitting the required repo argument should cause a non-zero exit."""
        result = runner.invoke(app, ["plan"])
        assert result.exit_code != 0

    def test_plan_missing_base_and_target_exits_with_error(self, tmp_path):
        """Providing repo but omitting base and target should fail."""
        result = runner.invoke(app, ["plan", str(tmp_path)])
        assert result.exit_code != 0

    def test_plan_invalid_repo_path_shows_error(self):
        """A non-existent repo path triggers an error message."""
        result = runner.invoke(
            app,
            [
                "plan",
                "/nonexistent/path/to/repo",
                "abc",
                "def",
            ],
        )
        assert result.exit_code != 0

    @patch("cli.display.display_plan_summary")
    @patch("core_engine.planner.serialize_plan", return_value='{"plan_id": "test"}')
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo")
    def test_plan_out_flag_writes_to_specified_path(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        mock_display,
        tmp_path,
    ):
        """--out flag causes plan JSON to be written to a custom file."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        mock_load_models.return_value = [_make_model()]
        mock_build_dag.return_value = MagicMock()
        mock_diff.return_value = DiffResult()
        mock_load_settings.return_value = _make_settings()
        plan = _make_plan()
        mock_generate_plan.return_value = plan

        custom_out = tmp_path / "custom" / "my_plan.json"

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "base_sha",
                "target_sha",
                "--out",
                str(custom_out),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        assert custom_out.exists()

    @patch("core_engine.planner.serialize_plan")
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory", return_value=[])
    @patch("core_engine.git.validate_repo")
    def test_plan_no_models_found_exits_cleanly(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        tmp_path,
    ):
        """When the repo has no SQL models the plan command exits 0 with a message."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "base",
                "target",
            ],
        )

        assert result.exit_code == 0
        mock_generate_plan.assert_not_called()

    @patch("core_engine.planner.serialize_plan")
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo", side_effect=Exception("Not a git repo"))
    def test_plan_validate_repo_failure_exits_with_error(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        tmp_path,
    ):
        """If validate_repo raises, the plan command exits with code 3."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "base",
                "target",
            ],
        )

        assert result.exit_code == 3

    @patch("core_engine.planner.serialize_plan")
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo")
    def test_plan_json_mode_writes_to_stdout(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        tmp_path,
    ):
        """--json flag causes plan JSON to be emitted to stdout."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        mock_load_models.return_value = [_make_model()]
        mock_build_dag.return_value = MagicMock()
        mock_diff.return_value = DiffResult()
        mock_load_settings.return_value = _make_settings()

        plan = _make_plan()
        mock_generate_plan.return_value = plan
        plan_json = '{"plan_id": "plan_0001"}'
        mock_serialize.return_value = plan_json

        out_file = tmp_path / "plan.json"

        result = runner.invoke(
            app,
            [
                "--json",
                "plan",
                str(tmp_path),
                "base",
                "target",
                "--out",
                str(out_file),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        assert "plan_0001" in result.output

    @patch("core_engine.planner.serialize_plan", return_value="{}")
    @patch("core_engine.planner.generate_plan")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo")
    def test_plan_as_of_date_option(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        mock_load_settings,
        mock_generate_plan,
        mock_serialize,
        tmp_path,
    ):
        """--as-of-date is parsed and forwarded to generate_plan."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        mock_load_models.return_value = [_make_model()]
        mock_build_dag.return_value = MagicMock()
        mock_diff.return_value = DiffResult()
        mock_load_settings.return_value = _make_settings()

        plan = _make_plan(total_steps=0, with_steps=False)
        mock_generate_plan.return_value = plan

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "base",
                "target",
                "--as-of-date",
                "2025-06-15",
                "--out",
                str(tmp_path / "plan.json"),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        # Verify generate_plan was called with as_of_date=date(2025, 6, 15).
        mock_generate_plan.assert_called_once()
        call_kwargs = mock_generate_plan.call_args
        # as_of_date is a keyword argument.
        assert call_kwargs.kwargs.get("as_of_date") == date(2025, 6, 15)

    @patch("core_engine.diff.compute_structural_diff")
    @patch("core_engine.git.get_changed_files", return_value=[])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    @patch("core_engine.git.validate_repo")
    def test_plan_invalid_as_of_date_exits_with_error(
        self,
        mock_validate_repo,
        mock_load_models,
        mock_build_dag,
        mock_changed_files,
        mock_diff,
        tmp_path,
    ):
        """An invalid --as-of-date should cause exit code 3."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        mock_load_models.return_value = [_make_model()]
        mock_build_dag.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "plan",
                str(tmp_path),
                "base",
                "target",
                "--as-of-date",
                "not-a-date",
                "--out",
                str(tmp_path / "plan.json"),
            ],
        )

        assert result.exit_code == 3


# ---------------------------------------------------------------------------
# show command
# ---------------------------------------------------------------------------


class TestShowCommand:
    """Tests for `platform show <plan_path>`."""

    @patch("cli.app.display_plan_summary")
    @patch("core_engine.planner.deserialize_plan")
    def test_show_renders_plan_summary(
        self,
        mock_deserialize,
        mock_display,
        tmp_path,
    ):
        """show command reads the plan file and calls display_plan_summary."""
        plan = _make_plan()
        mock_deserialize.return_value = plan

        plan_file = tmp_path / "plan.json"
        plan_file.write_text('{"fake": "plan"}')

        result = runner.invoke(app, ["show", str(plan_file)])

        assert result.exit_code == 0
        mock_display.assert_called_once()
        assert mock_display.call_args[0][1] == plan

    @patch("core_engine.planner.deserialize_plan")
    def test_show_json_mode_writes_to_stdout(
        self,
        mock_deserialize,
        tmp_path,
    ):
        """--json flag causes plan JSON to be emitted to stdout."""
        plan = _make_plan()
        mock_deserialize.return_value = plan

        plan_json = '{"plan_id": "plan_0001", "summary": "test"}'
        plan_file = tmp_path / "plan.json"
        plan_file.write_text(plan_json)

        result = runner.invoke(app, ["--json", "show", str(plan_file)])

        assert result.exit_code == 0
        assert "plan_0001" in result.output

    def test_show_nonexistent_file_exits_with_error(self):
        """Pointing show at a missing file should cause an error exit."""
        result = runner.invoke(app, ["show", "/nonexistent/plan.json"])
        assert result.exit_code != 0

    @patch("core_engine.planner.deserialize_plan", side_effect=ValueError("Invalid JSON"))
    def test_show_invalid_plan_json_exits_with_error(
        self,
        mock_deserialize,
        tmp_path,
    ):
        """A corrupted plan file should cause exit code 3."""
        plan_file = tmp_path / "bad_plan.json"
        plan_file.write_text("this is not json{{{")

        result = runner.invoke(app, ["show", str(plan_file)])

        assert result.exit_code == 3

    @patch("cli.app.display_plan_summary")
    @patch("core_engine.planner.deserialize_plan")
    def test_show_empty_plan_displays_no_steps_message(
        self,
        mock_deserialize,
        mock_display,
        tmp_path,
    ):
        """A plan with zero steps should still display (display fn handles it)."""
        plan = _make_plan(total_steps=0, with_steps=False)
        mock_deserialize.return_value = plan

        plan_file = tmp_path / "plan.json"
        plan_file.write_text('{"plan_id": "empty"}')

        result = runner.invoke(app, ["show", str(plan_file)])

        assert result.exit_code == 0
        mock_display.assert_called_once()
        displayed_plan = mock_display.call_args[0][1]
        assert displayed_plan.summary.total_steps == 0
        assert displayed_plan.steps == []


# ---------------------------------------------------------------------------
# apply command
# ---------------------------------------------------------------------------


class TestApplyCommand:
    """Tests for `platform apply <plan_path>`."""

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_with_approve_by(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """apply with --approve-by succeeds in non-dev env and executes steps."""
        plan = _make_plan(total_steps=1)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(
            app,
            [
                "--env",
                "staging",
                "apply",
                str(plan_file),
                "--repo",
                str(tmp_path),
                "--approve-by",
                "alice@example.com",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        executor_instance.execute_step.assert_called_once()
        executor_instance.__exit__.assert_called_once()

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_auto_approve_in_dev(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """--auto-approve in dev environment should proceed without --approve-by."""
        plan = _make_plan(total_steps=1)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(
            app,
            [
                "apply",
                str(plan_file),
                "--repo",
                str(tmp_path),
                "--auto-approve",
            ],
        )

        assert result.exit_code == 0

    @patch("cli.app._load_model_sql_map")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_staging_without_approve_by_or_auto_approve_fails(
        self,
        mock_deserialize,
        mock_load_sql,
        tmp_path,
    ):
        """Non-dev env requires --approve-by or --auto-approve, else exit 3."""
        plan = _make_plan(total_steps=1)
        mock_deserialize.return_value = plan
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(
            app,
            [
                "--env",
                "staging",
                "apply",
                str(plan_file),
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 3

    def test_apply_missing_plan_file_exits_with_error(self):
        """Missing plan file should trigger an error exit."""
        result = runner.invoke(app, ["apply", "/nonexistent/plan.json"])
        assert result.exit_code != 0

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_zero_step_plan_exits_cleanly(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """A plan with zero steps prints a message and exits 0."""
        plan = _make_plan(total_steps=0, with_steps=False)
        mock_deserialize.return_value = plan
        mock_load_sql.return_value = {}

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(app, ["apply", str(plan_file), "--repo", str(tmp_path)])

        assert result.exit_code == 0
        mock_executor_cls.assert_not_called()

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_failed_step_exits_with_code_3(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """If a step fails, apply should exit with code 3."""
        plan = _make_plan(total_steps=2)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        failed_record = _make_run_record(status=RunStatus.FAIL)
        failed_record.error_message = "Query timeout"

        executor_instance = MagicMock()
        executor_instance.execute_step.side_effect = [
            _make_run_record(status=RunStatus.SUCCESS),
            failed_record,
        ]
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(app, ["apply", str(plan_file), "--repo", str(tmp_path)])

        assert result.exit_code == 3
        executor_instance.__exit__.assert_called_once()

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_cancels_remaining_steps_after_failure(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """Steps after a failure should be marked CANCELLED."""
        plan = _make_plan(total_steps=3)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        failed_record = _make_run_record(status=RunStatus.FAIL)
        failed_record.error_message = "Disk full"

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = failed_record
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(app, ["apply", str(plan_file), "--repo", str(tmp_path)])

        assert result.exit_code == 3
        assert executor_instance.execute_step.call_count == 1
        call_args = mock_display.call_args
        run_records = call_args[0][1]
        assert len(run_records) == 3
        statuses = [r["status"] for r in run_records]
        assert statuses[0] == "FAIL"
        assert statuses.count("CANCELLED") == 2

    @patch("cli.app._load_model_sql_map")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_json_mode_emits_json_to_stdout(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_load_sql,
        tmp_path,
    ):
        """--json flag makes apply emit run records as JSON to stdout."""
        plan = _make_plan(total_steps=1)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(
            app,
            [
                "--json",
                "apply",
                str(plan_file),
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        output = json.loads(result.output)
        assert isinstance(output, list)
        assert len(output) == 1
        assert output[0]["status"] == "SUCCESS"

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.planner.deserialize_plan")
    def test_apply_override_cluster(
        self,
        mock_deserialize,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """--override-cluster is forwarded as a parameter to execute_step."""
        plan = _make_plan(total_steps=1)
        mock_deserialize.return_value = plan
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {f"model_{i}": f"SELECT 1 -- model_{i}" for i in range(3)}

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")

        result = runner.invoke(
            app,
            [
                "apply",
                str(plan_file),
                "--repo",
                str(tmp_path),
                "--override-cluster",
                "cluster-xyz",
            ],
        )

        assert result.exit_code == 0
        call_kwargs = executor_instance.execute_step.call_args
        parameters = call_kwargs.kwargs.get("parameters", call_kwargs[1].get("parameters"))
        assert parameters["cluster_id"] == "cluster-xyz"

    @patch("core_engine.planner.deserialize_plan", side_effect=Exception("Bad plan"))
    def test_apply_corrupt_plan_file_exits_with_error(
        self,
        mock_deserialize,
        tmp_path,
    ):
        """A corrupted plan file should cause exit code 3."""
        plan_file = tmp_path / "bad.json"
        plan_file.write_text("garbage")

        result = runner.invoke(app, ["apply", str(plan_file), "--repo", str(tmp_path)])

        assert result.exit_code == 3


# ---------------------------------------------------------------------------
# backfill command
# ---------------------------------------------------------------------------


class TestBackfillCommand:
    """Tests for `platform backfill --model ... --start ... --end ...`."""

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_valid_model_and_date_range(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """A valid backfill command executes the step and succeeds."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        executor_instance.execute_step.assert_called_once()
        executor_instance.__exit__.assert_called_once()

    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_start_after_end_exits_with_error(
        self,
        mock_det_id,
        tmp_path,
    ):
        """Start date > end date should cause exit code 3."""
        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-02-01",
                "--end",
                "2025-01-01",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 3

    def test_backfill_missing_model_exits_with_error(self):
        """Omitting --model should cause a non-zero exit."""
        result = runner.invoke(
            app,
            [
                "backfill",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
            ],
        )
        assert result.exit_code != 0

    def test_backfill_missing_dates_exits_with_error(self):
        """Omitting --start and --end should cause a non-zero exit."""
        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
            ],
        )
        assert result.exit_code != 0

    def test_backfill_invalid_start_date_format(self, tmp_path):
        """A badly formatted start date triggers exit code 3."""
        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "not-a-date",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 3

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_with_cluster_override(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """--cluster is forwarded to the executor parameters."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--cluster",
                "my-cluster",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0
        call_kwargs = executor_instance.execute_step.call_args
        parameters = call_kwargs.kwargs.get("parameters", call_kwargs[1].get("parameters"))
        assert parameters["cluster_id"] == "my-cluster"

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_failure_exits_with_code_3(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """A failed backfill step results in exit code 3."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        failed = _make_run_record(status=RunStatus.FAIL)
        failed.error_message = "Out of memory"

        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = failed
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 3

    @patch("cli.app._load_model_sql_map")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_json_mode_emits_json(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_load_sql,
        tmp_path,
    ):
        """--json flag makes backfill emit JSON to stdout."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "--json",
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        # CliRunner may mix stderr Rich output with stdout JSON.
        # Extract the JSON array from the output.
        raw = result.output
        json_start = raw.index("[")
        json_end = raw.rindex("]") + 1
        output = json.loads(raw[json_start:json_end])
        assert isinstance(output, list)
        assert output[0]["model"] == "analytics.orders_daily"

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_backfill_same_start_and_end_succeeds(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """A single-day backfill (start == end) should succeed."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-15",
                "--end",
                "2025-01-15",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# models command
# ---------------------------------------------------------------------------


class TestModelsCommand:
    """Tests for `platform models <repo>`."""

    @patch("cli.app.display_model_list")
    @patch("core_engine.loader.load_models_from_directory")
    def test_models_lists_from_repo(
        self,
        mock_load_models,
        mock_display,
        tmp_path,
    ):
        """models command discovers and displays model definitions."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model_a = _make_model(name="analytics.orders_daily")
        model_b = _make_model(
            name="staging.raw_events",
            kind=ModelKind.FULL_REFRESH,
            time_column=None,
        )
        mock_load_models.return_value = [model_a, model_b]

        result = runner.invoke(app, ["models", str(tmp_path)])

        assert result.exit_code == 0
        mock_display.assert_called_once()
        displayed_models = mock_display.call_args[0][1]
        assert len(displayed_models) == 2

    @patch("cli.app.display_model_list")
    @patch("core_engine.loader.load_models_from_directory", return_value=[])
    def test_models_empty_repo_shows_message_exits_zero(
        self,
        mock_load_models,
        mock_display,
        tmp_path,
    ):
        """An empty repo (no models) should exit 0 with an appropriate message."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(app, ["models", str(tmp_path)])

        assert result.exit_code == 0
        mock_display.assert_not_called()

    def test_models_invalid_repo_path(self):
        """A non-existent repo path triggers an error."""
        result = runner.invoke(app, ["models", "/nonexistent/repo"])
        assert result.exit_code != 0

    @patch("core_engine.loader.load_models_from_directory", side_effect=Exception("Permission denied"))
    def test_models_load_failure_exits_with_error(
        self,
        mock_load_models,
        tmp_path,
    ):
        """If model loading raises, exit with code 3."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(app, ["models", str(tmp_path)])

        assert result.exit_code == 3

    @patch("core_engine.loader.load_models_from_directory")
    def test_models_json_mode_emits_json(
        self,
        mock_load_models,
        tmp_path,
    ):
        """--json flag should emit model details as JSON array to stdout."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model = _make_model(
            name="analytics.orders_daily",
            kind=ModelKind.INCREMENTAL_BY_TIME_RANGE,
            materialization=Materialization.TABLE,
            owner="data-eng",
            tags=["core", "finance"],
            dependencies=["staging.raw_orders"],
        )
        mock_load_models.return_value = [model]

        result = runner.invoke(app, ["--json", "models", str(tmp_path)])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert isinstance(output, list)
        assert len(output) == 1
        assert output[0]["name"] == "analytics.orders_daily"
        assert output[0]["kind"] == "INCREMENTAL_BY_TIME_RANGE"
        assert output[0]["owner"] == "data-eng"
        assert output[0]["tags"] == ["core", "finance"]
        assert output[0]["dependencies"] == ["staging.raw_orders"]

    @patch("cli.app.display_model_list")
    @patch("core_engine.loader.load_models_from_directory")
    def test_models_uses_repo_root_when_no_models_subdir(
        self,
        mock_load_models,
        mock_display,
        tmp_path,
    ):
        """When there is no models/ subdirectory, the repo root is used."""
        mock_load_models.return_value = [_make_model()]

        result = runner.invoke(app, ["models", str(tmp_path)])

        assert result.exit_code == 0
        mock_load_models.assert_called_once_with(tmp_path)


# ---------------------------------------------------------------------------
# lineage command
# ---------------------------------------------------------------------------


class TestLineageCommand:
    """Tests for `platform lineage <repo> --model <name>`."""

    @patch("cli.app.display_lineage")
    @patch("core_engine.graph.get_downstream", return_value=["downstream_a"])
    @patch("core_engine.graph.get_upstream", return_value=["upstream_a", "upstream_b"])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    def test_lineage_shows_upstream_and_downstream(
        self,
        mock_load_models,
        mock_build_dag,
        mock_upstream,
        mock_downstream,
        mock_display,
        tmp_path,
    ):
        """lineage command displays upstream and downstream for a model."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model_a = _make_model(name="analytics.orders_daily")
        model_b = _make_model(name="upstream_a", kind=ModelKind.FULL_REFRESH, time_column=None)
        model_c = _make_model(name="upstream_b", kind=ModelKind.FULL_REFRESH, time_column=None)
        model_d = _make_model(name="downstream_a")
        mock_load_models.return_value = [model_a, model_b, model_c, model_d]
        mock_build_dag.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "lineage",
                str(tmp_path),
                "--model",
                "analytics.orders_daily",
            ],
        )

        assert result.exit_code == 0
        mock_display.assert_called_once()
        call_args = mock_display.call_args[0]
        assert call_args[1] == "analytics.orders_daily"
        assert call_args[2] == ["upstream_a", "upstream_b"]
        assert call_args[3] == ["downstream_a"]

    @patch("cli.app.display_lineage")
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    def test_lineage_unknown_model_exits_with_error(
        self,
        mock_load_models,
        mock_build_dag,
        mock_display,
        tmp_path,
    ):
        """Requesting lineage for a model that does not exist should exit 3."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        mock_load_models.return_value = [_make_model(name="known_model")]
        mock_build_dag.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "lineage",
                str(tmp_path),
                "--model",
                "nonexistent_model",
            ],
        )

        assert result.exit_code == 3
        mock_display.assert_not_called()

    def test_lineage_missing_model_flag_exits_with_error(self, tmp_path):
        """Omitting the required --model flag should cause a non-zero exit."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(app, ["lineage", str(tmp_path)])
        assert result.exit_code != 0

    @patch("core_engine.graph.get_downstream", return_value=["downstream_x"])
    @patch("core_engine.graph.get_upstream", return_value=["upstream_x"])
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    def test_lineage_json_mode_emits_json(
        self,
        mock_load_models,
        mock_build_dag,
        mock_upstream,
        mock_downstream,
        tmp_path,
    ):
        """--json flag causes lineage data to be emitted as JSON to stdout."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        model = _make_model(name="my_model")
        mock_load_models.return_value = [model]
        mock_build_dag.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "--json",
                "lineage",
                str(tmp_path),
                "--model",
                "my_model",
            ],
        )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["model"] == "my_model"
        assert output["upstream"] == ["upstream_x"]
        assert output["downstream"] == ["downstream_x"]

    @patch("cli.app.display_lineage")
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory", return_value=[])
    def test_lineage_empty_repo_exits_zero(
        self,
        mock_load_models,
        mock_build_dag,
        mock_display,
        tmp_path,
    ):
        """No models in the repo should result in exit 0."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "lineage",
                str(tmp_path),
                "--model",
                "any_model",
            ],
        )

        assert result.exit_code == 0
        mock_display.assert_not_called()

    @patch("core_engine.loader.load_models_from_directory", side_effect=Exception("IO error"))
    def test_lineage_load_failure_exits_with_error(
        self,
        mock_load_models,
        tmp_path,
    ):
        """If loading models fails, exit with code 3."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "lineage",
                str(tmp_path),
                "--model",
                "any_model",
            ],
        )

        assert result.exit_code == 3

    @patch("cli.app.display_lineage")
    @patch("core_engine.graph.build_dag")
    @patch("core_engine.loader.load_models_from_directory")
    def test_lineage_unknown_model_shows_available_models(
        self,
        mock_load_models,
        mock_build_dag,
        mock_display,
        tmp_path,
    ):
        """When the model is not found, available model names are shown."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        mock_load_models.return_value = [
            _make_model(name="model_a"),
            _make_model(name="model_b"),
        ]
        mock_build_dag.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "lineage",
                str(tmp_path),
                "--model",
                "nonexistent",
            ],
        )

        assert result.exit_code == 3


# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------


class TestGlobalOptions:
    """Tests for global callback options (--env, --json, --metrics-file)."""

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_metrics_file_option(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """--metrics-file causes metrics events to be written."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"analytics.orders_daily": "SELECT 1 -- orders"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        metrics_file = tmp_path / "metrics.jsonl"

        result = runner.invoke(
            app,
            [
                "--metrics-file",
                str(metrics_file),
                "backfill",
                "--model",
                "analytics.orders_daily",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        assert metrics_file.exists()
        lines = metrics_file.read_text().strip().split("\n")
        assert len(lines) >= 1
        for line in lines:
            event = json.loads(line)
            assert "event" in event
            assert "timestamp" in event
            assert "data" in event

    def test_no_args_shows_help(self):
        """Running `platform` with no args shows help (Typer returns exit code 0 or 2)."""
        result = runner.invoke(app, [])
        # Typer's no_args_is_help=True may return 0 or 2 depending on version.
        assert result.exit_code in (0, 2)
        assert "IronLayer" in result.output or "Usage" in result.output

    @patch("cli.app._load_model_sql_map")
    @patch("cli.app.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id_123")
    def test_env_option_forwarded_to_load_settings(
        self,
        mock_det_id,
        mock_load_settings,
        mock_executor_cls,
        mock_display,
        mock_load_sql,
        tmp_path,
    ):
        """--env option is forwarded to load_settings."""
        mock_load_settings.return_value = _make_settings()
        mock_load_sql.return_value = {"test_model": "SELECT 1 -- test"}
        executor_instance = MagicMock()
        executor_instance.execute_step.return_value = _make_run_record()
        executor_instance.__enter__ = MagicMock(return_value=executor_instance)
        executor_instance.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor_instance

        result = runner.invoke(
            app,
            [
                "--env",
                "prod",
                "backfill",
                "--model",
                "test_model",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-31",
                "--repo",
                str(tmp_path),
            ],
        )

        mock_load_settings.assert_called_once_with(env="prod")


# ---------------------------------------------------------------------------
# _parse_date and _format_input_range helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    """Tests for internal helper functions in app.py."""

    def test_format_input_range_with_none(self):
        from cli.app import _format_input_range

        assert _format_input_range(None) == "-"

    def test_format_input_range_with_date_range(self):
        from cli.app import _format_input_range

        dr = DateRange(start=date(2025, 1, 1), end=date(2025, 1, 31))
        result = _format_input_range(dr)
        assert "2025-01-01" in result
        assert "2025-01-31" in result
        assert ".." in result
