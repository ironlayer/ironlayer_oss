"""Tests for SQLMesh project loader."""

from __future__ import annotations

import pytest
from pathlib import Path

from core_engine.loader.sqlmesh_loader import (
    SQLMeshLoadError,
    discover_sqlmesh_project,
    load_models_from_sqlmesh_project,
    _parse_model_block,
    _parse_sql_model_file,
    _KIND_MAP,
)
from core_engine.models.model_definition import ModelKind, Materialization

# ---------------------------------------------------------------------------
# discover_sqlmesh_project
# ---------------------------------------------------------------------------


class TestDiscoverSQLMeshProject:
    """Tests for project discovery."""

    def test_finds_config_yaml(self, tmp_path):
        (tmp_path / "config.yaml").write_text("model_defaults: {}")
        result = discover_sqlmesh_project(tmp_path)
        assert result is not None
        assert result.name == "config.yaml"

    def test_finds_config_yml(self, tmp_path):
        (tmp_path / "config.yml").write_text("model_defaults: {}")
        result = discover_sqlmesh_project(tmp_path)
        assert result is not None
        assert result.name == "config.yml"

    def test_finds_config_py(self, tmp_path):
        (tmp_path / "config.py").write_text("config = {}")
        result = discover_sqlmesh_project(tmp_path)
        assert result is not None
        assert result.name == "config.py"

    def test_prefers_yaml_over_yml(self, tmp_path):
        (tmp_path / "config.yaml").write_text("model_defaults: {}")
        (tmp_path / "config.yml").write_text("model_defaults: {}")
        result = discover_sqlmesh_project(tmp_path)
        assert result is not None
        assert result.name == "config.yaml"

    def test_returns_none_if_not_found(self, tmp_path):
        result = discover_sqlmesh_project(tmp_path)
        assert result is None


# ---------------------------------------------------------------------------
# _parse_model_block
# ---------------------------------------------------------------------------


class TestParseModelBlock:
    """Tests for MODEL block attribute extraction."""

    def test_extracts_name(self):
        block = "name my_schema.my_model,"
        attrs = _parse_model_block(block)
        assert attrs["name"] == "my_schema.my_model"

    def test_extracts_kind(self):
        block = "name m1, kind INCREMENTAL_BY_TIME_RANGE,"
        attrs = _parse_model_block(block)
        assert attrs["kind"] == "INCREMENTAL_BY_TIME_RANGE"

    def test_extracts_owner(self):
        block = "name m1, owner data-team,"
        attrs = _parse_model_block(block)
        assert attrs["owner"] == "data-team"

    def test_extracts_grain(self):
        block = "name m1, grain (order_id, product_id)"
        attrs = _parse_model_block(block)
        assert "order_id" in attrs["grain"]
        assert "product_id" in attrs["grain"]

    def test_extracts_tags(self):
        block = "name m1, tags ('daily', 'core')"
        attrs = _parse_model_block(block)
        assert "daily" in attrs["tags"]
        assert "core" in attrs["tags"]

    def test_extracts_time_column(self):
        block = "name m1, time_column event_date"
        attrs = _parse_model_block(block)
        assert attrs["time_column"] == "event_date"

    def test_extracts_depends_on(self):
        block = "name m1, depends_on ('staging.orders', 'staging.products')"
        attrs = _parse_model_block(block)
        assert "staging.orders" in attrs["depends_on"]
        assert "staging.products" in attrs["depends_on"]

    def test_empty_block(self):
        attrs = _parse_model_block("")
        assert len(attrs) == 0


# ---------------------------------------------------------------------------
# _KIND_MAP
# ---------------------------------------------------------------------------


class TestKindMap:
    """Tests for SQLMesh to IronLayer kind mapping."""

    def test_full_maps_correctly(self):
        kind, mat = _KIND_MAP["FULL"]
        assert kind == ModelKind.FULL_REFRESH
        assert mat == Materialization.TABLE

    def test_view_maps_correctly(self):
        kind, mat = _KIND_MAP["VIEW"]
        assert kind == ModelKind.FULL_REFRESH
        assert mat == Materialization.VIEW

    def test_incremental_by_time_range(self):
        kind, mat = _KIND_MAP["INCREMENTAL_BY_TIME_RANGE"]
        assert kind == ModelKind.INCREMENTAL_BY_TIME_RANGE
        assert mat == Materialization.INSERT_OVERWRITE

    def test_incremental_by_unique_key(self):
        kind, mat = _KIND_MAP["INCREMENTAL_BY_UNIQUE_KEY"]
        assert kind == ModelKind.MERGE_BY_KEY
        assert mat == Materialization.MERGE

    def test_scd_type_2(self):
        kind, mat = _KIND_MAP["SCD_TYPE_2"]
        assert kind == ModelKind.MERGE_BY_KEY
        assert mat == Materialization.MERGE

    def test_seed(self):
        kind, mat = _KIND_MAP["SEED"]
        assert kind == ModelKind.FULL_REFRESH
        assert mat == Materialization.TABLE


# ---------------------------------------------------------------------------
# load_models_from_sqlmesh_project (integration)
# ---------------------------------------------------------------------------


class TestLoadModelsFromSQLMeshProject:
    """Integration tests for loading a full SQLMesh project."""

    def _create_project(self, tmp_path, config_yaml="", model_files=None):
        """Helper to create a minimal SQLMesh project structure."""
        config_content = config_yaml or "model_defaults:\n  kind: FULL\n"
        (tmp_path / "config.yaml").write_text(config_content)
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        if model_files:
            for name, content in model_files.items():
                file_path = models_dir / name
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(content)
        return tmp_path

    def test_load_simple_model(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "orders.sql": ("MODEL (\n  name staging.orders,\n  kind FULL\n);\n\nSELECT * FROM raw.orders\n"),
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 1
        assert models[0].name == "staging.orders"
        assert models[0].kind == ModelKind.FULL_REFRESH

    def test_load_incremental_model(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "events.sql": (
                    "MODEL (\n"
                    "  name analytics.events,\n"
                    "  kind INCREMENTAL_BY_TIME_RANGE,\n"
                    "  time_column event_date\n"
                    ");\n\n"
                    "SELECT * FROM staging.events\n"
                    "WHERE event_date BETWEEN @start_date AND @end_date\n"
                ),
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 1
        assert models[0].kind == ModelKind.INCREMENTAL_BY_TIME_RANGE
        assert models[0].time_column == "event_date"

    def test_load_multiple_models(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": "MODEL (name a.m1, kind FULL);\nSELECT 1",
                "m2.sql": "MODEL (name a.m2, kind VIEW);\nSELECT 2",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 2
        names = [m.name for m in models]
        assert "a.m1" in names
        assert "a.m2" in names

    def test_models_sorted_by_name(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "z_model.sql": "MODEL (name z.model, kind FULL);\nSELECT 1",
                "a_model.sql": "MODEL (name a.model, kind FULL);\nSELECT 2",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert models[0].name < models[1].name

    def test_tag_filter(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": "MODEL (name m1, kind FULL, tags ('daily'));\nSELECT 1",
                "m2.sql": "MODEL (name m2, kind FULL, tags ('hourly'));\nSELECT 2",
            },
        )
        models = load_models_from_sqlmesh_project(project, tag_filter="daily")
        assert len(models) == 1
        assert models[0].name == "m1"

    def test_no_model_block_uses_filename(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "staging/orders.sql": "SELECT * FROM raw.orders",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 1
        assert models[0].name == "staging.orders"

    def test_empty_project(self, tmp_path):
        project = self._create_project(tmp_path)
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 0

    def test_nonexistent_path_raises(self, tmp_path):
        with pytest.raises(SQLMeshLoadError, match="does not exist"):
            load_models_from_sqlmesh_project(tmp_path / "nonexistent")

    def test_no_config_raises(self, tmp_path):
        (tmp_path / "models").mkdir()
        with pytest.raises(SQLMeshLoadError, match="No SQLMesh config"):
            load_models_from_sqlmesh_project(tmp_path)

    def test_model_with_owner(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": "MODEL (name m1, kind FULL, owner data-team);\nSELECT 1",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert models[0].owner == "data-team"

    def test_model_with_dependencies(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": (
                    "MODEL (\n"
                    "  name analytics.m1,\n"
                    "  kind FULL,\n"
                    "  depends_on ('staging.orders', 'staging.products')\n"
                    ");\n"
                    "SELECT * FROM staging.orders JOIN staging.products"
                ),
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert "staging.orders" in models[0].dependencies
        assert "staging.products" in models[0].dependencies

    def test_incremental_without_time_column_falls_back(self, tmp_path):
        """INCREMENTAL_BY_TIME_RANGE without time_column should fall back to FULL_REFRESH."""
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": "MODEL (name m1, kind INCREMENTAL_BY_TIME_RANGE);\nSELECT 1",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 1
        assert models[0].kind == ModelKind.FULL_REFRESH
        assert models[0].materialization == Materialization.TABLE

    def test_python_model_file(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "my_model.py": (
                    "from sqlmesh import model\n\n"
                    "@model('my_schema.my_model', kind='FULL', owner='analytics')\n"
                    "def execute(context):\n"
                    "    return context.query('SELECT 1')\n"
                ),
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 1
        assert models[0].name == "my_schema.my_model"
        assert models[0].owner == "analytics"

    def test_empty_sql_file_skipped(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "empty.sql": "",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert len(models) == 0

    def test_custom_model_paths(self, tmp_path):
        config = "model_defaults:\n  kind: FULL\nmodel_paths:\n  - sql_models\n"
        (tmp_path / "config.yaml").write_text(config)
        sql_dir = tmp_path / "sql_models"
        sql_dir.mkdir()
        (sql_dir / "m1.sql").write_text("MODEL (name m1, kind FULL);\nSELECT 1")
        models = load_models_from_sqlmesh_project(tmp_path)
        assert len(models) == 1

    def test_view_kind(self, tmp_path):
        project = self._create_project(
            tmp_path,
            model_files={
                "m1.sql": "MODEL (name m1, kind VIEW);\nSELECT 1",
            },
        )
        models = load_models_from_sqlmesh_project(project)
        assert models[0].kind == ModelKind.FULL_REFRESH
        assert models[0].materialization == Materialization.VIEW
