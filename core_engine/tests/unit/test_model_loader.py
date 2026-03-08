"""Unit tests for core_engine.loader.model_loader."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from core_engine.loader.model_loader import (
    HeaderParseError,
    ModelLoadError,
    _compute_content_hash,
    _extract_sql_body,
    load_models_from_directory,
    parse_model_file,
    parse_yaml_header,
)
from core_engine.models.model_definition import ModelKind

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_HEADER_ALL_FIELDS = textwrap.dedent("""\
    -- name: analytics.orders_daily
    -- kind: FULL_REFRESH
    -- materialization: TABLE
    -- time_column: order_date
    -- unique_key: order_id
    -- partition_by: order_date
    -- incremental_strategy: merge
    -- owner: data-team
    -- tags: finance, daily
    -- dependencies: staging.raw_orders, staging.raw_customers

    SELECT * FROM staging.raw_orders
""")

MINIMAL_HEADER = textwrap.dedent("""\
    -- name: my_model
    -- kind: FULL_REFRESH

    SELECT 1
""")

SQL_WITH_REFS = textwrap.dedent("""\
    -- name: analytics.orders_daily
    -- kind: FULL_REFRESH

    SELECT *
    FROM {{ ref('staging_orders') }}
    JOIN {{ ref('staging_customers') }} ON 1=1
""")


# ---------------------------------------------------------------------------
# parse_yaml_header
# ---------------------------------------------------------------------------


class TestParseYamlHeader:
    def test_valid_header_all_fields(self):
        result = parse_yaml_header(VALID_HEADER_ALL_FIELDS)
        assert result["name"] == "analytics.orders_daily"
        assert result["kind"] == "FULL_REFRESH"
        assert result["materialization"] == "TABLE"
        assert result["time_column"] == "order_date"
        assert result["unique_key"] == "order_id"
        assert result["partition_by"] == "order_date"
        assert result["incremental_strategy"] == "merge"
        assert result["owner"] == "data-team"
        assert result["tags"] == ["finance", "daily"]
        assert result["dependencies"] == [
            "staging.raw_orders",
            "staging.raw_customers",
        ]

    def test_minimal_header(self):
        result = parse_yaml_header(MINIMAL_HEADER)
        assert result["name"] == "my_model"
        assert result["kind"] == "FULL_REFRESH"
        assert "materialization" not in result
        assert "tags" not in result

    def test_missing_name_raises_header_parse_error(self):
        sql = "-- kind: FULL_REFRESH\n\nSELECT 1"
        with pytest.raises(HeaderParseError, match="name"):
            parse_yaml_header(sql)

    def test_missing_kind_raises_header_parse_error(self):
        sql = "-- name: my_model\n\nSELECT 1"
        with pytest.raises(HeaderParseError, match="kind"):
            parse_yaml_header(sql)

    def test_missing_both_required_raises_header_parse_error(self):
        sql = "-- owner: somebody\n\nSELECT 1"
        with pytest.raises(HeaderParseError):
            parse_yaml_header(sql)

    def test_extra_whitespace_in_header(self):
        sql = textwrap.dedent("""\
            --  name:   my_model
            --  kind:   FULL_REFRESH

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert result["name"] == "my_model"
        assert result["kind"] == "FULL_REFRESH"

    def test_empty_file_raises_header_parse_error(self):
        with pytest.raises(HeaderParseError):
            parse_yaml_header("")

    def test_bare_comment_lines_do_not_terminate_header(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            --
            -- kind: FULL_REFRESH
            -- This is a note without a colon-value pattern
            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert result["name"] == "my_model"
        assert result["kind"] == "FULL_REFRESH"

    def test_unknown_fields_are_silently_ignored(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- unknown_field: some_value

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert "unknown_field" not in result
        assert result["name"] == "my_model"

    def test_empty_list_field_produces_empty_list(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- tags:

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert result.get("tags", []) == []


# ---------------------------------------------------------------------------
# _extract_sql_body
# ---------------------------------------------------------------------------


class TestExtractSqlBody:
    def test_strips_header_and_returns_body(self):
        body = _extract_sql_body(VALID_HEADER_ALL_FIELDS)
        assert body.startswith("SELECT")
        assert "-- name:" not in body

    def test_body_only_sql(self):
        sql = "SELECT 1"
        body = _extract_sql_body(sql)
        assert body == "SELECT 1"


# ---------------------------------------------------------------------------
# _compute_content_hash
# ---------------------------------------------------------------------------


class TestComputeContentHash:
    def test_deterministic(self):
        h1 = _compute_content_hash("SELECT 1")
        h2 = _compute_content_hash("SELECT 1")
        assert h1 == h2

    def test_different_sql_different_hash(self):
        h1 = _compute_content_hash("SELECT 1")
        h2 = _compute_content_hash("SELECT 2")
        assert h1 != h2

    def test_hash_is_64_hex_chars(self):
        h = _compute_content_hash("SELECT 1")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# parse_model_file
# ---------------------------------------------------------------------------


class TestParseModelFile:
    def test_valid_sql_file(self, tmp_path: Path):
        model_file = tmp_path / "orders.sql"
        model_file.write_text(VALID_HEADER_ALL_FIELDS, encoding="utf-8")

        model = parse_model_file(model_file)
        assert model.name == "analytics.orders_daily"
        assert model.kind == ModelKind.FULL_REFRESH
        assert model.file_path == str(model_file)
        assert model.content_hash != ""
        assert model.raw_sql == VALID_HEADER_ALL_FIELDS

    def test_ref_resolution(self, tmp_path: Path):
        model_file = tmp_path / "orders.sql"
        model_file.write_text(SQL_WITH_REFS, encoding="utf-8")

        registry = {
            "staging_orders": "staging.raw_orders",
            "staging_customers": "staging.raw_customers",
        }
        model = parse_model_file(model_file, model_registry=registry)
        assert "staging.raw_orders" in model.clean_sql
        assert "staging.raw_customers" in model.clean_sql
        assert "{{ ref(" not in model.clean_sql

    def test_missing_file_raises_model_load_error(self, tmp_path: Path):
        missing_file = tmp_path / "does_not_exist.sql"
        with pytest.raises(ModelLoadError):
            parse_model_file(missing_file)

    def test_incremental_by_time_range_requires_time_column(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: my_incr
            -- kind: INCREMENTAL_BY_TIME_RANGE

            SELECT 1
        """)
        model_file = tmp_path / "incr.sql"
        model_file.write_text(sql, encoding="utf-8")
        with pytest.raises(ValueError, match="time_column"):
            parse_model_file(model_file)

    def test_merge_by_key_requires_unique_key(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: my_merge
            -- kind: MERGE_BY_KEY

            SELECT 1
        """)
        model_file = tmp_path / "merge.sql"
        model_file.write_text(sql, encoding="utf-8")
        with pytest.raises(ValueError, match="unique_key"):
            parse_model_file(model_file)

    def test_incremental_by_time_range_with_time_column_succeeds(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: my_incr
            -- kind: INCREMENTAL_BY_TIME_RANGE
            -- time_column: created_at

            SELECT 1
        """)
        model_file = tmp_path / "incr.sql"
        model_file.write_text(sql, encoding="utf-8")
        model = parse_model_file(model_file)
        assert model.kind == ModelKind.INCREMENTAL_BY_TIME_RANGE
        assert model.time_column == "created_at"

    def test_merge_by_key_with_unique_key_succeeds(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: my_merge
            -- kind: MERGE_BY_KEY
            -- unique_key: id

            SELECT 1
        """)
        model_file = tmp_path / "merge.sql"
        model_file.write_text(sql, encoding="utf-8")
        model = parse_model_file(model_file)
        assert model.kind == ModelKind.MERGE_BY_KEY
        assert model.unique_key == "id"

    def test_no_registry_keeps_refs_unresolved(self, tmp_path: Path):
        model_file = tmp_path / "orders.sql"
        model_file.write_text(SQL_WITH_REFS, encoding="utf-8")
        model = parse_model_file(model_file, model_registry=None)
        # clean_sql should have the raw ref patterns as text (after header stripping)
        assert "ref(" in model.clean_sql or "staging_orders" in str(model.referenced_tables)

    def test_referenced_tables_populated(self, tmp_path: Path):
        model_file = tmp_path / "orders.sql"
        model_file.write_text(SQL_WITH_REFS, encoding="utf-8")
        model = parse_model_file(model_file, model_registry=None)
        assert "staging_orders" in model.referenced_tables
        assert "staging_customers" in model.referenced_tables


# ---------------------------------------------------------------------------
# load_models_from_directory
# ---------------------------------------------------------------------------


class TestLoadModelsFromDirectory:
    def test_loads_multiple_models(self, tmp_path: Path):
        (tmp_path / "a.sql").write_text(
            "-- name: model_a\n-- kind: FULL_REFRESH\n\nSELECT 1",
            encoding="utf-8",
        )
        (tmp_path / "b.sql").write_text(
            "-- name: model_b\n-- kind: FULL_REFRESH\n\nSELECT 2",
            encoding="utf-8",
        )
        models = load_models_from_directory(tmp_path)
        assert len(models) == 2
        names = [m.name for m in models]
        assert "model_a" in names
        assert "model_b" in names

    def test_sorted_by_name(self, tmp_path: Path):
        (tmp_path / "z.sql").write_text(
            "-- name: zzz_model\n-- kind: FULL_REFRESH\n\nSELECT 1",
            encoding="utf-8",
        )
        (tmp_path / "a.sql").write_text(
            "-- name: aaa_model\n-- kind: FULL_REFRESH\n\nSELECT 2",
            encoding="utf-8",
        )
        models = load_models_from_directory(tmp_path)
        assert models[0].name == "aaa_model"
        assert models[1].name == "zzz_model"

    def test_empty_directory_returns_empty(self, tmp_path: Path):
        models = load_models_from_directory(tmp_path)
        assert models == []

    def test_nested_directories(self, tmp_path: Path):
        subdir = tmp_path / "staging"
        subdir.mkdir()
        (subdir / "orders.sql").write_text(
            "-- name: staging.orders\n-- kind: FULL_REFRESH\n\nSELECT 1",
            encoding="utf-8",
        )
        (tmp_path / "top.sql").write_text(
            "-- name: top_model\n-- kind: FULL_REFRESH\n\nSELECT 2",
            encoding="utf-8",
        )
        models = load_models_from_directory(tmp_path)
        assert len(models) == 2

    def test_nonexistent_directory_raises_model_load_error(self, tmp_path: Path):
        fake_dir = tmp_path / "does_not_exist"
        with pytest.raises(ModelLoadError):
            load_models_from_directory(fake_dir)

    def test_ref_resolution_across_models(self, tmp_path: Path):
        (tmp_path / "source.sql").write_text(
            "-- name: staging.raw_orders\n-- kind: FULL_REFRESH\n\nSELECT 1",
            encoding="utf-8",
        )
        (tmp_path / "downstream.sql").write_text(
            textwrap.dedent("""\
                -- name: analytics.orders
                -- kind: FULL_REFRESH

                SELECT * FROM {{ ref('raw_orders') }}
            """),
            encoding="utf-8",
        )
        models = load_models_from_directory(tmp_path)
        analytics = next(m for m in models if m.name == "analytics.orders")
        assert "staging.raw_orders" in analytics.clean_sql
