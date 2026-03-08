"""Comprehensive tests for ``migrate from-sqlmesh`` CLI subcommand.

Covers the full lifecycle: project discovery, model loading, dry-run vs
real writes, JSON output, tag filtering, error handling (no config,
no models, SQLMeshLoadError, generic exceptions), and Python model
warnings.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)

from cli.app import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model(
    name: str = "staging.orders",
    kind: ModelKind = ModelKind.FULL_REFRESH,
    materialization: Materialization = Materialization.TABLE,
    raw_sql: str = "SELECT * FROM raw.orders",
    clean_sql: str = "SELECT * FROM raw.orders",
    file_path: str | None = None,
    tags: list[str] | None = None,
    dependencies: list[str] | None = None,
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=kind,
        materialization=materialization,
        file_path=file_path or f"models/{name.replace('.', '/')}.sql",
        raw_sql=raw_sql,
        clean_sql=clean_sql,
        tags=tags if tags is not None else [],
        dependencies=dependencies if dependencies is not None else [],
        content_hash="abc123",
    )


def _extract_json(raw: str) -> dict | list:
    """Extract the first JSON object or array from mixed CLI output."""
    for open_ch, close_ch in [("{", "}"), ("[", "]")]:
        start = raw.find(open_ch)
        if start == -1:
            continue
        end = raw.rfind(close_ch)
        if end == -1 or end <= start:
            continue
        return json.loads(raw[start : end + 1])
    raise ValueError(f"No JSON found in output: {raw[:200]!r}")


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


class TestMigrateFromSqlmeshHappy:
    """Happy-path tests for ``migrate from-sqlmesh``."""

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_discovers_and_converts(self, mock_discover, mock_load, tmp_path):
        """Valid SQLMesh project → IronLayer files written."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [
            _make_model(name="staging.orders"),
            _make_model(
                name="analytics.revenue",
                clean_sql="SELECT SUM(amount) FROM staging.orders",
                dependencies=["staging.orders"],
            ),
        ]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        orders_file = output_dir / "staging" / "orders.sql"
        revenue_file = output_dir / "analytics" / "revenue.sql"
        assert orders_file.exists()
        assert revenue_file.exists()

        content = orders_file.read_text(encoding="utf-8")
        assert "-- name: staging.orders" in content

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_dry_run_writes_no_files(self, mock_discover, mock_load, tmp_path):
        """--dry-run should report without writing files."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
                "--dry-run",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"
        if output_dir.exists():
            assert list(output_dir.rglob("*.sql")) == []

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_tag_filter_forwarded(self, mock_discover, mock_load, tmp_path):
        """--tag flag should be forwarded to load_models_from_sqlmesh_project."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name="staging.orders", tags=["core"])]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
                "--tag",
                "core",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"
        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args
        assert (
            call_kwargs.kwargs.get("tag_filter") == "core"
            or call_kwargs[1].get("tag_filter") == "core"
            or (len(call_kwargs.args) > 1 and call_kwargs.args[1] == "core")
        )

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_json_mode_emits_json(self, mock_discover, mock_load, tmp_path):
        """--json flag should emit migration report as JSON."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"
        output = _extract_json(result.output)
        assert "migrated" in output
        assert len(output["migrated"]) == 1
        assert output["migrated"][0]["name"] == "staging.orders"

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_json_dry_run_status(self, mock_discover, mock_load, tmp_path):
        """--json + --dry-run should report status as 'dry-run'."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert output["migrated"][0]["status"] == "dry-run"


# ---------------------------------------------------------------------------
# Python model warning
# ---------------------------------------------------------------------------


class TestSqlmeshPythonModelWarning:
    """Python models should generate a manual-conversion warning."""

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_python_model_produces_warning(self, mock_discover, mock_load, tmp_path):
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        python_model = _make_model(
            name="staging.py_model",
            raw_sql="-- Python model: some_func",
            clean_sql="-- Python model: some_func",
        )
        mock_load.return_value = [python_model]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert len(output["warnings"]) >= 1
        assert "Python model" in output["warnings"][0]
        assert "Manual SQL conversion" in output["warnings"][0]

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_normal_model_no_python_warning(self, mock_discover, mock_load, tmp_path):
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert len(output["warnings"]) == 0


# ---------------------------------------------------------------------------
# Error-path tests
# ---------------------------------------------------------------------------


class TestMigrateFromSqlmeshErrors:
    """Error handling for ``migrate from-sqlmesh``."""

    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project", return_value=None)
    def test_no_config_exits_3(self, mock_discover, tmp_path):
        """No config.yaml → exit code 3 with message."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()

        result = runner.invoke(
            app,
            ["migrate", "from-sqlmesh", str(project_dir)],
        )

        assert result.exit_code == 3
        assert "No SQLMesh config file found" in result.output

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project", return_value=[])
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_no_models_exits_0(self, mock_discover, mock_load, tmp_path):
        """Empty model list → exit code 0 with warning."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "config.yaml"

        result = runner.invoke(
            app,
            ["migrate", "from-sqlmesh", str(project_dir)],
        )

        assert result.exit_code == 0
        assert "No models found" in result.output

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project", return_value=[])
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_no_models_with_tag_shows_filter(self, mock_discover, mock_load, tmp_path):
        """No models with --tag filter → shows the tag used."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "config.yaml"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--tag",
                "niche",
            ],
        )

        assert result.exit_code == 0
        assert "niche" in result.output

    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_sqlmesh_load_error(self, mock_discover, tmp_path):
        """SQLMeshLoadError → exit code 3 with message."""
        from core_engine.loader.sqlmesh_loader import SQLMeshLoadError

        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "config.yaml"

        with patch(
            "core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project",
            side_effect=SQLMeshLoadError("bad config"),
        ):
            result = runner.invoke(
                app,
                ["migrate", "from-sqlmesh", str(project_dir)],
            )

        assert result.exit_code == 3
        assert "SQLMesh project error" in result.output

    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_generic_exception(self, mock_discover, tmp_path):
        """Unexpected exception → exit code 3 with generic message."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "config.yaml"

        with patch(
            "core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project",
            side_effect=RuntimeError("disk on fire"),
        ):
            result = runner.invoke(
                app,
                ["migrate", "from-sqlmesh", str(project_dir)],
            )

        assert result.exit_code == 3
        assert "Migration failed" in result.output

    def test_nonexistent_project_path(self):
        """Non-existent path → error exit."""
        result = runner.invoke(
            app,
            ["migrate", "from-sqlmesh", "/nonexistent/path/sqlmesh"],
        )
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Multi-model and output structure
# ---------------------------------------------------------------------------


class TestSqlmeshOutputStructure:
    """Verify output paths and multi-model handling."""

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_dotted_name_creates_directories(self, mock_discover, mock_load, tmp_path):
        """Model name dots → nested directory structure."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [
            _make_model(name="analytics.finance.revenue"),
        ]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        expected = output_dir / "analytics" / "finance" / "revenue.sql"
        assert expected.exists()

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_single_segment_name(self, mock_discover, mock_load, tmp_path):
        """Model with no dots → file at output root."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [
            _make_model(name="orders"),
        ]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        expected = output_dir / "orders.sql"
        assert expected.exists()

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_multiple_models_all_migrated(self, mock_discover, mock_load, tmp_path):
        """Multiple models → all migrated, JSON output includes all."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [_make_model(name=f"staging.model_{i}") for i in range(5)]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert len(output["migrated"]) == 5

    @patch("core_engine.loader.sqlmesh_loader.load_models_from_sqlmesh_project")
    @patch("core_engine.loader.sqlmesh_loader.discover_sqlmesh_project")
    def test_model_source_path_in_output(self, mock_discover, mock_load, tmp_path):
        """JSON output includes the source file_path for each model."""
        project_dir = tmp_path / "sqlmesh_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "config.yaml"
        mock_load.return_value = [
            _make_model(
                name="staging.orders",
                file_path="models/staging/orders.sql",
            ),
        ]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sqlmesh",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert output["migrated"][0]["source"] == "models/staging/orders.sql"
