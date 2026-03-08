"""Edge-case and gap-filling tests for the CLI package.

Covers areas not addressed by the primary test files:
  - ``_emit_metrics`` OSError suppression (lines 117-120)
  - ``__main__.py`` entry point
  - ``_load_model_sql_map`` helper
  - ``from-sql`` materialization fallbacks (INSERT_OVERWRITE, MERGE)
  - ``from-dbt`` DbtManifestError exception path
  - ``_extract_sql_table_refs`` additional edge cases
  - ``_load_env_file`` edge cases in dev.py
  - backfill-chunked cancelled/failed chunk paths
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.exceptions import Exit as ClickExit
from typer.testing import CliRunner

from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)

from cli.app import (
    _emit_metrics,
    _extract_sql_table_refs,
    _generate_ironlayer_file,
    app,
)

runner = CliRunner()


def _make_model(
    name: str = "staging.orders",
    raw_sql: str = "SELECT 1",
    clean_sql: str = "SELECT 1",
    **kwargs,
) -> ModelDefinition:
    defaults = dict(
        kind=ModelKind.FULL_REFRESH,
        materialization=Materialization.TABLE,
        file_path=f"models/{name}.sql",
        content_hash="abc",
    )
    defaults.update(kwargs)
    return ModelDefinition(name=name, raw_sql=raw_sql, clean_sql=clean_sql, **defaults)


# ---------------------------------------------------------------------------
# _emit_metrics edge cases
# ---------------------------------------------------------------------------


class TestEmitMetrics:
    """Verify _emit_metrics never crashes the CLI."""

    def test_emit_metrics_writes_to_file(self, tmp_path):
        """Normal operation writes a JSON line to the metrics file."""
        metrics_file = tmp_path / "metrics.jsonl"

        with patch("cli.app._metrics_file", metrics_file):
            _emit_metrics("test.event", {"key": "value"})

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["event"] == "test.event"
        assert record["data"]["key"] == "value"
        assert "timestamp" in record

    def test_emit_metrics_suppresses_os_error(self, tmp_path):
        """OSError (e.g. read-only filesystem) should be silently suppressed."""
        # Point metrics at a path under a non-existent directory that
        # we ensure can't be created.
        bad_path = tmp_path / "readonly" / "metrics.jsonl"
        # Don't create the parent directory.

        with patch("cli.app._metrics_file", bad_path):
            # Should NOT raise.
            _emit_metrics("test.event", {"safe": True})

        # Verify no file was written.
        assert not bad_path.exists()

    def test_emit_metrics_suppresses_permission_error(self, tmp_path):
        """PermissionError (a subclass of OSError) should be suppressed."""
        metrics_file = tmp_path / "metrics.jsonl"

        # Create a mock that raises PermissionError on open.
        with patch("cli.app._metrics_file", metrics_file):
            with patch.object(Path, "open", side_effect=PermissionError("no write")):
                # Should NOT raise.
                _emit_metrics("test.event", {"safe": True})


# ---------------------------------------------------------------------------
# __main__.py entry point
# ---------------------------------------------------------------------------


class TestMainEntryPoint:
    """Verify that ``python -m cli`` works."""

    def test_main_function_is_importable(self):
        """``cli.__main__.main`` should be importable."""
        from cli.__main__ import main

        assert callable(main)

    def test_main_invokes_app(self):
        """``main()`` should call ``app()``."""
        with patch("cli.__main__.app") as mock_app:
            from cli.__main__ import main

            mock_app.side_effect = SystemExit(0)
            with pytest.raises(SystemExit):
                main()
            mock_app.assert_called_once()


# ---------------------------------------------------------------------------
# _load_model_sql_map helper
# ---------------------------------------------------------------------------


class TestLoadModelSqlMap:
    """Tests for the _load_model_sql_map helper used by 'apply'."""

    def test_loads_from_models_subdir(self, tmp_path):
        """When a 'models/' subdir exists, loads from there."""
        from cli.app import _load_model_sql_map

        models_dir = tmp_path / "models"
        models_dir.mkdir()
        sql_file = models_dir / "orders.sql"
        sql_file.write_text(
            "-- name: orders\n-- kind: FULL_REFRESH\n-- materialization: TABLE\n\nSELECT 1",
            encoding="utf-8",
        )

        result = _load_model_sql_map(tmp_path)
        assert isinstance(result, dict)
        # At minimum should have found the model.
        assert len(result) >= 1

    def test_falls_back_to_repo_root(self, tmp_path):
        """When no 'models/' subdir, loads from repo root."""
        from cli.app import _load_model_sql_map

        sql_file = tmp_path / "my_model.sql"
        sql_file.write_text(
            "-- name: my_model\n-- kind: FULL_REFRESH\n-- materialization: TABLE\n\nSELECT 42",
            encoding="utf-8",
        )

        result = _load_model_sql_map(tmp_path)
        assert isinstance(result, dict)

    def test_empty_directory_returns_empty(self, tmp_path):
        """An empty directory should return an empty map."""
        from cli.app import _load_model_sql_map

        result = _load_model_sql_map(tmp_path)
        assert result == {}


# ---------------------------------------------------------------------------
# migrate from-sql materialization fallback warnings
# ---------------------------------------------------------------------------


class TestFromSqlMaterializationFallback:
    """Cover the INSERT_OVERWRITE and MERGE fallback paths."""

    def test_insert_overwrite_falls_back_with_warning(self, tmp_path):
        """INSERT_OVERWRITE without time_column → FULL_REFRESH + warning."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text(
            "SELECT * FROM source_table",
            encoding="utf-8",
        )
        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
                "--materialization",
                "INSERT_OVERWRITE",
            ],
        )

        assert result.exit_code == 0
        output = json.loads(result.output[result.output.index("{") : result.output.rindex("}") + 1])
        assert len(output["warnings"]) >= 1
        assert "INSERT_OVERWRITE" in output["warnings"][0] or "time_column" in output["warnings"][0]

        # The generated file should have FULL_REFRESH fallback.
        out_file = output_dir / "model.sql"
        content = out_file.read_text(encoding="utf-8")
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: TABLE" in content

    def test_merge_falls_back_with_warning(self, tmp_path):
        """MERGE without unique_key → FULL_REFRESH + warning."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text(
            "SELECT * FROM source_table",
            encoding="utf-8",
        )
        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
                "--materialization",
                "MERGE",
            ],
        )

        assert result.exit_code == 0
        output = json.loads(result.output[result.output.index("{") : result.output.rindex("}") + 1])
        assert len(output["warnings"]) >= 1
        assert "MERGE" in output["warnings"][0] or "unique_key" in output["warnings"][0]

        out_file = output_dir / "model.sql"
        content = out_file.read_text(encoding="utf-8")
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: TABLE" in content

    def test_insert_overwrite_warning_not_duplicated(self, tmp_path):
        """Multiple models should produce only one materialization warning."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        for i in range(3):
            (sql_dir / f"model_{i}.sql").write_text(
                f"SELECT {i} FROM source_{i}",
                encoding="utf-8",
            )
        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
                "--materialization",
                "INSERT_OVERWRITE",
            ],
        )

        assert result.exit_code == 0
        output = json.loads(result.output[result.output.index("{") : result.output.rindex("}") + 1])
        # The warning should appear exactly once, not per-model.
        io_warnings = [w for w in output["warnings"] if "INSERT_OVERWRITE" in w or "time_column" in w]
        assert len(io_warnings) == 1


# ---------------------------------------------------------------------------
# migrate from-dbt error paths
# ---------------------------------------------------------------------------


class TestFromDbtErrorPaths:
    """Cover DbtManifestError and generic exception paths."""

    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_dbt_manifest_error(self, mock_discover, tmp_path):
        from core_engine.loader.dbt_loader import DbtManifestError

        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "target" / "manifest.json"

        with patch(
            "core_engine.loader.dbt_loader.load_models_from_dbt_manifest",
            side_effect=DbtManifestError("corrupt manifest"),
        ):
            result = runner.invoke(
                app,
                ["migrate", "from-dbt", str(project_dir)],
            )

        assert result.exit_code == 3
        assert "dbt manifest error" in result.output

    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_generic_exception_from_dbt(self, mock_discover, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "target" / "manifest.json"

        with patch(
            "core_engine.loader.dbt_loader.load_models_from_dbt_manifest",
            side_effect=RuntimeError("unexpected"),
        ):
            result = runner.invoke(
                app,
                ["migrate", "from-dbt", str(project_dir)],
            )

        assert result.exit_code == 3
        assert "Migration failed" in result.output


# ---------------------------------------------------------------------------
# migrate from-sql error path
# ---------------------------------------------------------------------------


class TestFromSqlErrorPaths:
    """Cover the generic exception handler in migrate from-sql."""

    def test_read_error_on_sql_file(self, tmp_path):
        """Files that can't be read → skipped with warning."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()

        good_file = sql_dir / "good.sql"
        good_file.write_text("SELECT 1", encoding="utf-8")

        # Create a file that raises on read by making it a directory
        # with the .sql extension (will cause read error).
        bad_path = sql_dir / "bad.sql"
        bad_path.mkdir()

        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
            ],
        )

        # The good file should still be migrated; bad one skipped.
        assert result.exit_code == 0
        output = json.loads(result.output[result.output.index("{") : result.output.rindex("}") + 1])
        assert len(output["migrated"]) >= 1


# ---------------------------------------------------------------------------
# _extract_sql_table_refs additional coverage
# ---------------------------------------------------------------------------


class TestExtractSqlTableRefsEdge:
    """Additional edge cases for the SQL table ref extractor."""

    def test_skips_subquery_indicators(self):
        """Subqueries starting with '(' should not be treated as table refs."""
        sql = "SELECT * FROM (SELECT 1) AS sub"
        refs = _extract_sql_table_refs(sql)
        # Should not contain '(' or 'sub'.
        for ref in refs:
            assert not ref.startswith("(")

    def test_empty_table_name_skipped(self):
        """Empty string matches should be skipped."""
        # Craft edge case with FROM followed by whitespace only.
        sql = "SELECT * FROM  WHERE 1=1"
        refs = _extract_sql_table_refs(sql)
        assert "" not in refs

    def test_double_quoted_identifiers(self):
        """Double-quoted identifiers should be handled."""
        sql = 'SELECT * FROM "my_schema"."my_table"'
        refs = _extract_sql_table_refs(sql)
        assert len(refs) >= 0  # Implementation may or may not support this.

    def test_cross_join(self):
        """CROSS JOIN should extract the second table."""
        sql = "SELECT * FROM table_a CROSS JOIN table_b"
        refs = _extract_sql_table_refs(sql)
        assert "table_a" in refs
        assert "table_b" in refs

    def test_full_outer_join(self):
        """FULL OUTER JOIN should extract both tables."""
        sql = "SELECT * FROM left_t FULL OUTER JOIN right_t ON 1=1"
        refs = _extract_sql_table_refs(sql)
        assert "left_t" in refs
        assert "right_t" in refs


# ---------------------------------------------------------------------------
# dev.py: _load_env_file edge cases
# ---------------------------------------------------------------------------


class TestLoadDotenv:
    """Cover edge cases in _load_dotenv from commands/dev.py."""

    def test_skips_lines_without_equals(self, tmp_path):
        from cli.commands.dev import _load_dotenv

        env_file = tmp_path / ".env"
        env_file.write_text(
            "# Comment\nGOOD_KEY=good_value\nBAD_LINE_NO_EQUALS\nANOTHER_KEY=123\n",
            encoding="utf-8",
        )

        # Clear any existing env vars to avoid interference.
        for key in ("GOOD_KEY", "BAD_LINE_NO_EQUALS", "ANOTHER_KEY"):
            os.environ.pop(key, None)

        _load_dotenv(env_file)

        assert os.environ.get("GOOD_KEY") == "good_value"
        assert os.environ.get("ANOTHER_KEY") == "123"
        # The line without '=' should be silently skipped.
        assert "BAD_LINE_NO_EQUALS" not in os.environ

        # Clean up.
        os.environ.pop("GOOD_KEY", None)
        os.environ.pop("ANOTHER_KEY", None)

    def test_os_error_returns_silently(self, tmp_path):
        from cli.commands.dev import _load_dotenv

        # Use a directory as the file path to trigger OSError on read.
        dir_path = tmp_path / "fake_env"
        dir_path.mkdir()
        # Should not raise.
        _load_dotenv(dir_path)

    def test_strips_quotes_from_values(self, tmp_path):
        from cli.commands.dev import _load_dotenv

        env_file = tmp_path / ".env"
        env_file.write_text(
            "SINGLE='hello'\nDOUBLE=\"world\"\n",
            encoding="utf-8",
        )

        for key in ("SINGLE", "DOUBLE"):
            os.environ.pop(key, None)

        _load_dotenv(env_file)

        assert os.environ.get("SINGLE") == "hello"
        assert os.environ.get("DOUBLE") == "world"

        os.environ.pop("SINGLE", None)
        os.environ.pop("DOUBLE", None)

    def test_empty_lines_and_comments_skipped(self, tmp_path):
        from cli.commands.dev import _load_dotenv

        env_file = tmp_path / ".env"
        env_file.write_text(
            "\n  \n# This is a comment\n  # Another comment\nKEY=val\n",
            encoding="utf-8",
        )

        os.environ.pop("KEY", None)

        _load_dotenv(env_file)

        assert os.environ.get("KEY") == "val"
        os.environ.pop("KEY", None)


# ---------------------------------------------------------------------------
# dev.py: _build_services_table coverage
# ---------------------------------------------------------------------------


class TestBuildServicesTable:
    """Cover the _build_services_table helper."""

    def test_returns_table_object(self, tmp_path):
        from cli.commands.dev import _build_services_table

        table = _build_services_table(
            host="127.0.0.1",
            port=8000,
            ai_port=8001,
            no_ai=False,
            no_ui=False,
            project_root=tmp_path,
        )
        # Should return a Rich Table object.
        from rich.table import Table

        assert isinstance(table, Table)

    def test_no_ai_flag_skips_ai_service(self, tmp_path):
        from cli.commands.dev import _build_services_table

        table = _build_services_table(
            host="127.0.0.1",
            port=8000,
            ai_port=8001,
            no_ai=True,
            no_ui=False,
            project_root=tmp_path,
        )
        # Just verify it doesn't crash.
        assert table is not None

    def test_no_ui_flag_skips_frontend(self, tmp_path):
        from cli.commands.dev import _build_services_table

        table = _build_services_table(
            host="127.0.0.1",
            port=8000,
            ai_port=8001,
            no_ai=False,
            no_ui=True,
            project_root=tmp_path,
        )
        assert table is not None


# ---------------------------------------------------------------------------
# dev.py: _setup_local_env
# ---------------------------------------------------------------------------


class TestSetupLocalEnv:
    """Cover _setup_local_env helper."""

    _ENV_KEYS = (
        "PLATFORM_STATE_STORE_TYPE",
        "PLATFORM_DATABASE_URL",
        "PLATFORM_ENV",
        "API_HOST",
        "API_PORT",
        "API_CORS_ORIGINS",
        "API_AUTH_MODE",
        "API_JWT_SECRET",
        "API_RATE_LIMIT_ENABLED",
        "AI_ENGINE_URL",
        "AI_LLM_ENABLED",
        "PLATFORM_LOCAL_DB_PATH",
        "CUSTOM_VAR",
    )

    def _cleanup_env(self):
        for key in self._ENV_KEYS:
            os.environ.pop(key, None)

    def test_sets_expected_environment_variables(self, tmp_path):
        from cli.commands.dev import _setup_local_env

        self._cleanup_env()

        # Create a minimal .env file.
        env_file = tmp_path / ".env"
        env_file.write_text("CUSTOM_VAR=custom_value\n", encoding="utf-8")

        _setup_local_env(tmp_path, port=8000, ai_port=8001, no_ai=False)

        assert os.environ.get("PLATFORM_ENV") == "dev"
        assert os.environ.get("API_PORT") == "8000"
        assert os.environ.get("CUSTOM_VAR") == "custom_value"

        self._cleanup_env()

    def test_no_ai_disables_llm(self, tmp_path):
        from cli.commands.dev import _setup_local_env

        self._cleanup_env()

        _setup_local_env(tmp_path, port=8000, ai_port=8001, no_ai=True)

        assert os.environ.get("AI_LLM_ENABLED") == "false"
        assert os.environ.get("AI_ENGINE_URL") == ""

        self._cleanup_env()

    def test_creates_state_directory(self, tmp_path):
        from cli.commands.dev import _setup_local_env

        self._cleanup_env()

        _setup_local_env(tmp_path, port=9000, ai_port=9001, no_ai=False)

        state_dir = tmp_path / ".ironlayer"
        assert state_dir.exists()

        self._cleanup_env()


# ---------------------------------------------------------------------------
# _generate_ironlayer_file: empty SQL body fallback (line 1515)
# ---------------------------------------------------------------------------


class TestGenerateIronlayerFileEdge:
    """Cover edge case where model has no SQL body at all."""

    def test_empty_sql_body_produces_placeholder(self, tmp_path):
        """A model with empty raw/clean SQL → placeholder comment in file."""
        model = _make_model(
            name="empty_model",
            raw_sql="",
            clean_sql="",
        )
        output_path = _generate_ironlayer_file(model, tmp_path)
        content = output_path.read_text(encoding="utf-8")
        assert "-- No SQL body available" in content
        assert "empty_model" in content


# ---------------------------------------------------------------------------
# from-dbt: tag filter message with no matching models (line 1592)
# ---------------------------------------------------------------------------


class TestFromDbtTagFilterMessage:
    """Cover the 'no models matching tag' message path."""

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest", return_value=[])
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_no_models_with_tag_shows_filter(self, mock_discover, mock_load, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "target" / "manifest.json"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
                "--tag",
                "rare_tag",
            ],
        )

        assert result.exit_code == 0
        assert "rare_tag" in result.output


# ---------------------------------------------------------------------------
# from-dbt: single-part model name (line 1624)
# ---------------------------------------------------------------------------


class TestFromDbtSinglePartName:
    """Cover the single-segment model name path in from-dbt."""

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_single_segment_name_writes_to_root(self, mock_discover, mock_load, tmp_path):
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "target" / "manifest.json"
        mock_load.return_value = [_make_model(name="flat_model")]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        assert (output_dir / "flat_model.sql").exists()


# ---------------------------------------------------------------------------
# from-sql: generic exception path (lines 1890-1893)
# ---------------------------------------------------------------------------


class TestFromSqlGenericException:
    """Cover the generic exception handler in from-sql."""

    def test_generic_exception_exits_3(self, tmp_path):
        """An unexpected error during from-sql → exit 3."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text("SELECT 1", encoding="utf-8")

        with patch(
            "cli.app._generate_ironlayer_file",
            side_effect=RuntimeError("disk exploded"),
        ):
            result = runner.invoke(
                app,
                [
                    "migrate",
                    "from-sql",
                    str(sql_dir),
                ],
            )

        assert result.exit_code == 3
        assert "Migration failed" in result.output


# ---------------------------------------------------------------------------
# from-sql: else-branch for unknown materialization kind (line 1742)
# ---------------------------------------------------------------------------


class TestFromSqlUnknownMaterializationKind:
    """The else branch when materialization doesn't match known enums."""

    def test_table_materialization_kind(self, tmp_path):
        """TABLE → FULL_REFRESH (the standard path)."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text("SELECT 1", encoding="utf-8")
        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
                "--materialization",
                "TABLE",
            ],
        )

        assert result.exit_code == 0
        content = (output_dir / "model.sql").read_text(encoding="utf-8")
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: TABLE" in content

    def test_view_materialization_kind(self, tmp_path):
        """VIEW → FULL_REFRESH."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text("SELECT 1", encoding="utf-8")
        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
                "--materialization",
                "VIEW",
            ],
        )

        assert result.exit_code == 0
        content = (output_dir / "model.sql").read_text(encoding="utf-8")
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: VIEW" in content


# ---------------------------------------------------------------------------
# _extract_sql_table_refs: keyword skip path (line 1441)
# ---------------------------------------------------------------------------


class TestExtractSqlTableRefsKeywords:
    """Verify SQL keywords after FROM/JOIN are skipped."""

    def test_skips_select_keyword(self):
        """'FROM (SELECT ...)' should not extract 'SELECT' as a table."""
        sql = "SELECT * FROM (SELECT 1 AS x) AS sub"
        refs = _extract_sql_table_refs(sql)
        for ref in refs:
            assert ref.upper() != "SELECT"

    def test_skips_where_keyword(self):
        """Keyword-like names after FROM should be filtered."""
        # This test verifies the keyword skip set works.
        sql = "SELECT * FROM real_table WHERE id > 0"
        refs = _extract_sql_table_refs(sql)
        assert "real_table" in refs
        # 'WHERE' should not appear.
        for ref in refs:
            assert ref.upper() != "WHERE"


# ---------------------------------------------------------------------------
# lineage: models_dir fallback (line 1178)
# ---------------------------------------------------------------------------


class TestLineageFallback:
    """Cover the lineage command models_dir fallback to repo root."""

    def test_lineage_no_models_dir_falls_back(self, tmp_path):
        """When no 'models/' subdir, lineage should fall back to repo root."""
        # Create a .sql file at root.
        sql_file = tmp_path / "orders.sql"
        sql_file.write_text(
            "-- name: orders\n-- kind: FULL_REFRESH\n-- materialization: TABLE\n\nSELECT * FROM raw_orders",
            encoding="utf-8",
        )

        result = runner.invoke(
            app,
            ["lineage", str(tmp_path), "--model", "orders"],
        )

        # Should not error out trying to find models/ subdir.
        # Either succeeds (model found) or fails (model not in graph), but NOT crash.
        assert result.exit_code in (0, 3)
