"""Comprehensive tests for the ``migrate`` CLI command group.

Covers ``migrate from-dbt`` and ``migrate from-sql`` subcommands, the
``_generate_ironlayer_file`` helper, the ``_extract_sql_table_refs``
SQL dependency extractor, and the ``display_migration_report`` Rich
display function.

Tests use ``typer.testing.CliRunner`` for CLI invocations, ``tmp_path``
for file I/O, and mock the dbt loader to avoid needing real manifest
files for the from-dbt path.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console
from typer.testing import CliRunner

from core_engine.models.model_definition import (
    Materialization,
    ModelDefinition,
    ModelKind,
)

from cli.app import _extract_sql_table_refs, _generate_ironlayer_file, app
from cli.display import display_migration_report

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers -- reusable model factories
# ---------------------------------------------------------------------------


def _make_model(
    name: str = "staging.orders",
    kind: ModelKind = ModelKind.FULL_REFRESH,
    materialization: Materialization = Materialization.TABLE,
    time_column: str | None = None,
    unique_key: str | None = None,
    partition_by: str | None = None,
    incremental_strategy: str | None = None,
    owner: str | None = "data-team",
    tags: list[str] | None = None,
    dependencies: list[str] | None = None,
    raw_sql: str = "SELECT * FROM raw.orders",
    clean_sql: str = "SELECT * FROM raw.orders",
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=kind,
        materialization=materialization,
        time_column=time_column,
        unique_key=unique_key,
        partition_by=partition_by,
        incremental_strategy=incremental_strategy,
        owner=owner,
        tags=tags if tags is not None else ["staging"],
        dependencies=dependencies if dependencies is not None else ["raw.orders"],
        file_path=f"models/{name.replace('.', '/')}.sql",
        raw_sql=raw_sql,
        clean_sql=clean_sql,
        content_hash="abc123",
    )


def _capture_console() -> tuple[Console, io.StringIO]:
    """Create a Console that writes to a StringIO buffer for assertion."""
    buf = io.StringIO()
    console = Console(file=buf, no_color=True, highlight=False, width=120)
    return console, buf


def _extract_json_from_output(raw: str) -> dict | list:
    """Extract a JSON object or array from CliRunner output.

    CliRunner may mix Rich stderr output with JSON stdout.  This helper
    locates the first ``{`` or ``[`` and the matching closing delimiter,
    then parses the JSON substring.
    """
    # Try to find a JSON object first, then an array.
    for open_char, close_char in [("{", "}"), ("[", "]")]:
        start = raw.find(open_char)
        if start == -1:
            continue
        end = raw.rfind(close_char)
        if end == -1 or end <= start:
            continue
        return json.loads(raw[start : end + 1])
    raise ValueError(f"No JSON found in output: {raw[:200]!r}")


# ---------------------------------------------------------------------------
# _extract_sql_table_refs
# ---------------------------------------------------------------------------


class TestExtractSqlTableRefs:
    """Tests for the FROM/JOIN dependency extraction helper."""

    def test_simple_from_clause(self):
        sql = "SELECT * FROM my_schema.my_table WHERE id > 0"
        refs = _extract_sql_table_refs(sql)
        assert refs == ["my_schema.my_table"]

    def test_multiple_joins(self):
        sql = """
        SELECT a.id, b.name, c.value
        FROM schema_a.orders a
        JOIN schema_b.customers b ON a.cust_id = b.id
        LEFT JOIN schema_c.products c ON a.prod_id = c.id
        """
        refs = _extract_sql_table_refs(sql)
        assert refs == ["schema_a.orders", "schema_b.customers", "schema_c.products"]

    def test_deduplicates_references(self):
        sql = """
        SELECT * FROM users u
        JOIN users u2 ON u.manager_id = u2.id
        """
        refs = _extract_sql_table_refs(sql)
        assert refs == ["users"]

    def test_backtick_quoted_identifiers(self):
        sql = "SELECT * FROM `my_schema`.`my_table`"
        # The regex captures the backtick-quoted name as a whole
        refs = _extract_sql_table_refs(sql)
        # Backtick handling: the regex captures the full backtick content
        assert len(refs) >= 1

    def test_no_from_clause_returns_empty(self):
        sql = "SELECT 1 AS one"
        refs = _extract_sql_table_refs(sql)
        assert refs == []

    def test_skips_sql_keywords(self):
        """SQL keywords like SELECT should not be treated as table names."""
        # The regex looks for FROM/JOIN followed by identifiers, not keywords.
        sql = "SELECT 1"
        refs = _extract_sql_table_refs(sql)
        assert refs == []

    def test_case_insensitive_from_join(self):
        sql = "select * from My_Table join Other_Table on 1=1"
        refs = _extract_sql_table_refs(sql)
        assert "My_Table" in refs
        assert "Other_Table" in refs

    def test_unqualified_table_name(self):
        sql = "SELECT * FROM orders WHERE status = 'active'"
        refs = _extract_sql_table_refs(sql)
        assert refs == ["orders"]

    def test_sorted_output(self):
        sql = "SELECT * FROM z_table JOIN a_table ON 1=1"
        refs = _extract_sql_table_refs(sql)
        assert refs == ["a_table", "z_table"]

    def test_empty_sql_returns_empty(self):
        refs = _extract_sql_table_refs("")
        assert refs == []


# ---------------------------------------------------------------------------
# _generate_ironlayer_file
# ---------------------------------------------------------------------------


class TestGenerateIronlayerFile:
    """Tests for the IronLayer file generator."""

    def test_generates_file_with_header_and_sql(self, tmp_path):
        """Generated file should have -- key: value headers and SQL body."""
        model = _make_model(
            name="staging.orders",
            owner="data-team",
            tags=["staging", "core"],
            dependencies=["raw.orders"],
        )

        output_path = _generate_ironlayer_file(model, tmp_path)

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")

        # Verify header lines.
        assert "-- name: staging.orders" in content
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: TABLE" in content
        assert "-- owner: data-team" in content
        assert "-- tags: staging, core" in content
        assert "-- dependencies: raw.orders" in content

        # Verify SQL body is present.
        assert "SELECT * FROM raw.orders" in content

    def test_directory_structure_from_model_name(self, tmp_path):
        """Model name dots should become directory separators."""
        model = _make_model(name="analytics.finance.revenue")

        output_path = _generate_ironlayer_file(model, tmp_path)

        expected = tmp_path / "analytics" / "finance" / "revenue.sql"
        assert output_path == expected
        assert output_path.exists()

    def test_single_segment_name(self, tmp_path):
        """A model with no dot in the name should be written to root."""
        model = _make_model(name="my_model")

        output_path = _generate_ironlayer_file(model, tmp_path)

        expected = tmp_path / "my_model.sql"
        assert output_path == expected
        assert output_path.exists()

    def test_uses_original_sql_when_provided(self, tmp_path):
        """When original_sql is provided, it should be used as the body."""
        model = _make_model(clean_sql="SELECT 1", raw_sql="SELECT 2")

        output_path = _generate_ironlayer_file(model, tmp_path, original_sql="SELECT 42 AS answer")

        content = output_path.read_text(encoding="utf-8")
        assert "SELECT 42 AS answer" in content
        assert "SELECT 1" not in content

    def test_optional_fields_omitted_when_none(self, tmp_path):
        """Optional fields that are None should not appear in the header."""
        model = _make_model(
            time_column=None,
            unique_key=None,
            partition_by=None,
            incremental_strategy=None,
            owner=None,
            tags=[],
            dependencies=[],
        )

        output_path = _generate_ironlayer_file(model, tmp_path)
        content = output_path.read_text(encoding="utf-8")

        assert "-- time_column:" not in content
        assert "-- unique_key:" not in content
        assert "-- partition_by:" not in content
        assert "-- incremental_strategy:" not in content
        assert "-- owner:" not in content
        assert "-- tags:" not in content
        assert "-- dependencies:" not in content

    def test_all_optional_fields_present(self, tmp_path):
        """When all optional fields are set, they should all appear."""
        model = _make_model(
            kind=ModelKind.INCREMENTAL_BY_TIME_RANGE,
            materialization=Materialization.INSERT_OVERWRITE,
            time_column="event_date",
            unique_key="order_id",
            partition_by="event_date",
            incremental_strategy="insert_overwrite",
            owner="analytics",
            tags=["core", "sla"],
            dependencies=["raw.events"],
        )

        output_path = _generate_ironlayer_file(model, tmp_path)
        content = output_path.read_text(encoding="utf-8")

        assert "-- time_column: event_date" in content
        assert "-- unique_key: order_id" in content
        assert "-- partition_by: event_date" in content
        assert "-- incremental_strategy: insert_overwrite" in content
        assert "-- owner: analytics" in content
        assert "-- tags: core, sla" in content
        assert "-- dependencies: raw.events" in content

    def test_creates_parent_directories(self, tmp_path):
        """Parent directories should be created automatically."""
        model = _make_model(name="deeply.nested.model.name")

        output_path = _generate_ironlayer_file(model, tmp_path)

        assert output_path.exists()
        assert output_path == tmp_path / "deeply" / "nested" / "model" / "name.sql"


# ---------------------------------------------------------------------------
# migrate from-dbt (CLI)
# ---------------------------------------------------------------------------


class TestMigrateFromDbt:
    """Tests for `platform migrate from-dbt <project_path>`."""

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_discovers_and_converts(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """Valid dbt project should generate IronLayer model files."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        manifest_path = project_dir / "target" / "manifest.json"
        mock_discover.return_value = manifest_path

        model_a = _make_model(name="staging.orders", clean_sql="SELECT * FROM raw_orders")
        model_b = _make_model(
            name="analytics.revenue",
            clean_sql="SELECT SUM(amount) FROM staging.orders",
            dependencies=["staging.orders"],
        )
        mock_load.return_value = [model_a, model_b]

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

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        # Verify files were created.
        orders_file = output_dir / "staging" / "orders.sql"
        revenue_file = output_dir / "analytics" / "revenue.sql"
        assert orders_file.exists()
        assert revenue_file.exists()

        # Verify file content.
        orders_content = orders_file.read_text(encoding="utf-8")
        assert "-- name: staging.orders" in orders_content
        assert "SELECT * FROM raw_orders" in orders_content

        revenue_content = revenue_file.read_text(encoding="utf-8")
        assert "-- name: analytics.revenue" in revenue_content

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_dry_run_writes_no_files(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """--dry-run should report models without writing files."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "target" / "manifest.json"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
                "--output",
                str(output_dir),
                "--dry-run",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        # Output directory should not be created at all, or should be empty.
        if output_dir.exists():
            assert list(output_dir.rglob("*.sql")) == []

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_tag_filter(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """--tag flag should be forwarded as tag_filter to the loader."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "target" / "manifest.json"
        mock_load.return_value = [
            _make_model(name="staging.orders", tags=["core"]),
        ]

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
                "--output",
                str(output_dir),
                "--tag",
                "core",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        # Verify tag_filter was passed correctly.
        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args
        assert call_kwargs.kwargs.get("tag_filter") == ["core"]

    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest", return_value=None)
    def test_no_manifest_exits_with_error(
        self,
        mock_discover,
        tmp_path,
    ):
        """When no manifest.json is found, exit with code 3."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
            ],
        )

        assert result.exit_code == 3

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest", return_value=[])
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_no_models_found_exits_zero(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """When the manifest has no eligible models, exit 0 with a message."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        mock_discover.return_value = project_dir / "target" / "manifest.json"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                str(project_dir),
            ],
        )

        assert result.exit_code == 0

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_json_mode_emits_json(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """--json flag should emit migration report as JSON to stdout."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "target" / "manifest.json"
        mock_load.return_value = [_make_model(name="staging.orders")]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-dbt",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"
        output = _extract_json_from_output(result.output)
        assert "migrated" in output
        assert len(output["migrated"]) == 1
        assert output["migrated"][0]["name"] == "staging.orders"

    def test_nonexistent_project_path(self):
        """A non-existent project path should trigger an error exit."""
        result = runner.invoke(
            app,
            [
                "migrate",
                "from-dbt",
                "/nonexistent/path/dbt_project",
            ],
        )
        assert result.exit_code != 0

    @patch("core_engine.loader.dbt_loader.load_models_from_dbt_manifest")
    @patch("core_engine.loader.dbt_loader.discover_dbt_manifest")
    def test_skips_models_without_sql(
        self,
        mock_discover,
        mock_load,
        tmp_path,
    ):
        """Models with empty SQL content should be skipped with a warning."""
        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        output_dir = tmp_path / "output"

        mock_discover.return_value = project_dir / "target" / "manifest.json"

        empty_model = _make_model(
            name="staging.empty",
            clean_sql="",
            raw_sql="",
        )
        good_model = _make_model(name="staging.orders")
        mock_load.return_value = [empty_model, good_model]

        result = runner.invoke(
            app,
            [
                "--json",
                "migrate",
                "from-dbt",
                str(project_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        output = _extract_json_from_output(result.output)
        assert len(output["migrated"]) == 1
        assert len(output["skipped"]) == 1
        assert output["skipped"][0]["name"] == "staging.empty"
        assert len(output["warnings"]) >= 1


# ---------------------------------------------------------------------------
# migrate from-sql (CLI)
# ---------------------------------------------------------------------------


class TestMigrateFromSql:
    """Tests for `platform migrate from-sql <sql_dir>`."""

    def test_converts_sql_files(self, tmp_path):
        """SQL files should be converted to IronLayer format."""
        sql_dir = tmp_path / "sql_source"
        staging_dir = sql_dir / "staging"
        staging_dir.mkdir(parents=True)

        orders_sql = staging_dir / "orders.sql"
        orders_sql.write_text(
            "SELECT id, customer_id, amount FROM raw.orders WHERE status = 'active'",
            encoding="utf-8",
        )

        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        # Verify the output file was created.
        out_file = output_dir / "staging" / "orders.sql"
        assert out_file.exists()

        content = out_file.read_text(encoding="utf-8")
        assert "-- name: staging.orders" in content
        assert "-- kind: FULL_REFRESH" in content
        assert "-- materialization: TABLE" in content
        assert "SELECT id, customer_id, amount FROM raw.orders" in content

    def test_infers_dependencies(self, tmp_path):
        """FROM and JOIN clauses should be parsed as dependencies."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()

        sql_file = sql_dir / "report.sql"
        sql_file.write_text(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cust_id = c.id",
            encoding="utf-8",
        )

        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        out_file = output_dir / "report.sql"
        content = out_file.read_text(encoding="utf-8")
        assert "-- dependencies: customers, orders" in content

    def test_dry_run_writes_no_files(self, tmp_path):
        """--dry-run should not write files."""
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
                "--dry-run",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        if output_dir.exists():
            assert list(output_dir.rglob("*.sql")) == []

    def test_materialization_option(self, tmp_path):
        """--materialization should set the materialization in the header."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "my_view.sql").write_text("SELECT 1 AS one", encoding="utf-8")

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

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        out_file = output_dir / "my_view.sql"
        content = out_file.read_text(encoding="utf-8")
        assert "-- materialization: VIEW" in content
        assert "-- kind: FULL_REFRESH" in content

    def test_invalid_materialization_exits_with_error(self, tmp_path):
        """An invalid --materialization value should cause exit 3."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text("SELECT 1", encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--materialization",
                "INVALID_VALUE",
            ],
        )

        assert result.exit_code == 3

    def test_no_sql_files_exits_zero(self, tmp_path):
        """An empty directory should exit 0 with a message."""
        sql_dir = tmp_path / "empty_dir"
        sql_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
            ],
        )

        assert result.exit_code == 0

    def test_skips_empty_files(self, tmp_path):
        """Empty .sql files should be skipped."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "empty.sql").write_text("", encoding="utf-8")
        (sql_dir / "good.sql").write_text("SELECT 1", encoding="utf-8")

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

        assert result.exit_code == 0
        output = _extract_json_from_output(result.output)
        assert len(output["migrated"]) == 1
        assert len(output["skipped"]) == 1
        assert output["skipped"][0]["reason"] == "empty file"

    def test_nested_directory_structure(self, tmp_path):
        """Nested directories should produce dotted model names."""
        sql_dir = tmp_path / "sql_source"
        nested = sql_dir / "staging" / "finance"
        nested.mkdir(parents=True)
        (nested / "revenue.sql").write_text(
            "SELECT SUM(amount) FROM transactions",
            encoding="utf-8",
        )

        output_dir = tmp_path / "output"

        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                str(sql_dir),
                "--output",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\nException: {result.exception}"

        out_file = output_dir / "staging" / "finance" / "revenue.sql"
        assert out_file.exists()

        content = out_file.read_text(encoding="utf-8")
        assert "-- name: staging.finance.revenue" in content

    def test_json_mode_emits_json(self, tmp_path):
        """--json flag emits the migration report as JSON."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        (sql_dir / "model.sql").write_text("SELECT 1", encoding="utf-8")

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

        assert result.exit_code == 0
        output = _extract_json_from_output(result.output)
        assert "migrated" in output
        assert len(output["migrated"]) == 1

    def test_nonexistent_sql_dir(self):
        """A non-existent sql_dir path should trigger an error exit."""
        result = runner.invoke(
            app,
            [
                "migrate",
                "from-sql",
                "/nonexistent/path/sql_files",
            ],
        )
        assert result.exit_code != 0

    def test_multiple_files_all_migrated(self, tmp_path):
        """Multiple SQL files should all be migrated."""
        sql_dir = tmp_path / "sql_source"
        sql_dir.mkdir()
        for i in range(5):
            (sql_dir / f"model_{i}.sql").write_text(
                f"SELECT {i} AS val FROM source_{i}",
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
            ],
        )

        assert result.exit_code == 0
        output = _extract_json_from_output(result.output)
        assert len(output["migrated"]) == 5
        assert len(output["skipped"]) == 0

        # Verify all output files exist.
        for i in range(5):
            assert (output_dir / f"model_{i}.sql").exists()


# ---------------------------------------------------------------------------
# display_migration_report
# ---------------------------------------------------------------------------


class TestDisplayMigrationReport:
    """Tests for the migration report Rich display function."""

    def test_renders_migrated_models(self):
        """Migrated models should appear in the report table."""
        console, buf = _capture_console()
        migrated = [
            {
                "name": "staging.orders",
                "source": "models/orders.sql",
                "output": "/out/staging/orders.sql",
                "status": "migrated",
            },
            {
                "name": "analytics.revenue",
                "source": "models/revenue.sql",
                "output": "/out/analytics/revenue.sql",
                "status": "migrated",
            },
        ]

        display_migration_report(console, migrated, [], [])
        output = buf.getvalue()

        assert "Migration Report" in output
        assert "staging.orders" in output
        assert "analytics.revenue" in output
        assert "migrated" in output
        assert "2 migrated" in output

    def test_renders_skipped_models(self):
        """Skipped models should appear in the report."""
        console, buf = _capture_console()
        skipped = [
            {"name": "staging.empty", "source": "models/empty.sql", "reason": "no SQL content"},
        ]

        display_migration_report(console, [], skipped, [])
        output = buf.getvalue()

        assert "staging.empty" in output
        assert "no SQL content" in output
        assert "1 skipped" in output

    def test_renders_warnings(self):
        """Warnings should be displayed after the table."""
        console, buf = _capture_console()
        migrated = [
            {"name": "m1", "source": "m1.sql", "output": "/out/m1.sql", "status": "migrated"},
        ]
        warnings = ["Some models had issues.", "Check the output."]

        display_migration_report(console, migrated, [], warnings)
        output = buf.getvalue()

        assert "Warnings:" in output
        assert "Some models had issues." in output
        assert "Check the output." in output

    def test_empty_report(self):
        """When no models are found, a message should be shown."""
        console, buf = _capture_console()
        display_migration_report(console, [], [], [])
        output = buf.getvalue()

        assert "No models found to migrate" in output

    def test_mixed_migrated_and_skipped(self):
        """Both migrated and skipped counts should appear in summary."""
        console, buf = _capture_console()
        migrated = [
            {"name": "m1", "source": "m1.sql", "output": "/out/m1.sql", "status": "migrated"},
        ]
        skipped = [
            {"name": "m2", "source": "m2.sql", "reason": "empty file"},
        ]

        display_migration_report(console, migrated, skipped, [])
        output = buf.getvalue()

        assert "2 model(s) found" in output
        assert "1 migrated" in output
        assert "1 skipped" in output

    def test_dry_run_status(self):
        """Dry-run status should be displayed for dry-run entries."""
        console, buf = _capture_console()
        migrated = [
            {"name": "m1", "source": "m1.sql", "output": "/out/m1.sql", "status": "dry-run"},
        ]

        display_migration_report(console, migrated, [], [])
        output = buf.getvalue()

        assert "dry-run" in output

    def test_model_count_in_title(self):
        """The table title should include the total model count."""
        console, buf = _capture_console()
        migrated = [
            {"name": f"m{i}", "source": f"m{i}.sql", "output": f"/out/m{i}.sql", "status": "migrated"} for i in range(3)
        ]

        display_migration_report(console, migrated, [], [])
        output = buf.getvalue()

        assert "3 model(s)" in output


# ---------------------------------------------------------------------------
# migrate command group help
# ---------------------------------------------------------------------------


class TestMigrateHelp:
    """Tests for the migrate command group help and no-args behavior."""

    def test_migrate_no_args_shows_help(self):
        """Running `platform migrate` with no subcommand shows help."""
        result = runner.invoke(app, ["migrate"])
        assert result.exit_code in (0, 2)
        assert "from-dbt" in result.output or "from-sql" in result.output or "Usage" in result.output

    def test_migrate_help_flag(self):
        """Running `platform migrate --help` shows available subcommands."""
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0
        assert "from-dbt" in result.output
        assert "from-sql" in result.output
