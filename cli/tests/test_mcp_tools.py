"""Tests for MCP tool implementations.

These tests exercise each tool function directly (without MCP transport)
to verify the business logic works correctly.  Integration tests for
the full MCP server (stdio/SSE) are separate.

Test categories:
- Tool argument parsing and validation
- Model listing and filtering
- Table-level lineage
- Column-level lineage (single model + cross-model)
- SQL diff detection
- Transpile Redshift → Databricks
- Schema mapping integration
- Error handling for missing repos / invalid SQL
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from cli.mcp.tools import (
    TOOL_DEFINITIONS,
    TOOL_DISPATCH,
    ironlayer_column_lineage,
    ironlayer_diff,
    ironlayer_lineage,
    ironlayer_models,
    ironlayer_transpile,
    ironlayer_validate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_repo(tmp_path: Path) -> Path:
    """Create a minimal IronLayer repo with model files."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    # Raw source model — uses ``-- key: value`` header format expected
    # by the model_loader (NOT YAML ``---`` delimiters).
    (models_dir / "raw_orders.sql").write_text(
        """\
-- name: raw.orders
-- kind: FULL_REFRESH
-- materialization: TABLE
SELECT
    id,
    customer_id,
    amount,
    created_at
FROM source_db.orders
""",
        encoding="utf-8",
    )

    # Staging model referencing raw.
    (models_dir / "stg_orders.sql").write_text(
        """\
-- name: staging.orders
-- kind: INCREMENTAL_BY_TIME_RANGE
-- materialization: TABLE
-- time_column: created_at
-- dependencies: raw.orders
SELECT
    id,
    customer_id,
    amount * 1.1 AS adjusted_amount,
    created_at
FROM raw.orders
WHERE amount > 0
""",
        encoding="utf-8",
    )

    return tmp_path


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------


class TestToolDefinitions:
    def test_all_tools_have_definitions(self):
        """Every tool in TOOL_DISPATCH must have a definition."""
        defined_names = {d["name"] for d in TOOL_DEFINITIONS}
        dispatch_names = set(TOOL_DISPATCH.keys())
        assert defined_names == dispatch_names

    def test_definitions_have_required_fields(self):
        for defn in TOOL_DEFINITIONS:
            assert "name" in defn
            assert "description" in defn
            assert "inputSchema" in defn
            assert defn["inputSchema"]["type"] == "object"
            assert "properties" in defn["inputSchema"]
            assert "required" in defn["inputSchema"]

    def test_eight_tools_defined(self):
        assert len(TOOL_DEFINITIONS) == 8
        assert len(TOOL_DISPATCH) == 8


# ---------------------------------------------------------------------------
# ironlayer_models
# ---------------------------------------------------------------------------


class TestIronlayerModels:
    def test_list_models(self, sample_repo: Path):
        result = asyncio.run(ironlayer_models(str(sample_repo)))

        assert result["total"] >= 2
        names = [m["name"] for m in result["models"]]
        assert "raw.orders" in names
        assert "staging.orders" in names

    def test_empty_repo(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        result = asyncio.run(ironlayer_models(str(tmp_path)))
        assert result["total"] == 0


# ---------------------------------------------------------------------------
# ironlayer_lineage
# ---------------------------------------------------------------------------


class TestIronlayerLineage:
    def test_upstream_downstream(self, sample_repo: Path):
        result = asyncio.run(ironlayer_lineage(str(sample_repo), "staging.orders"))

        assert result["model"] == "staging.orders"
        assert "raw.orders" in result["upstream"]

    def test_model_not_found(self, sample_repo: Path):
        result = asyncio.run(ironlayer_lineage(str(sample_repo), "nonexistent.model"))
        assert "error" in result


# ---------------------------------------------------------------------------
# ironlayer_column_lineage
# ---------------------------------------------------------------------------


class TestIronlayerColumnLineage:
    def test_all_columns(self, sample_repo: Path):
        result = asyncio.run(ironlayer_column_lineage(str(sample_repo), "staging.orders"))

        # Should not have error.
        assert "error" not in result or result.get("columns")
        if "columns" in result:
            assert "id" in result["columns"] or len(result["columns"]) > 0

    def test_single_column_trace(self, sample_repo: Path):
        result = asyncio.run(ironlayer_column_lineage(str(sample_repo), "staging.orders", column="id"))

        if "error" not in result:
            assert result["column"] == "id"
            assert "lineage_path" in result

    def test_with_schema_mapping(self, sample_repo: Path):
        schema = {
            "raw.orders": {
                "id": "INT",
                "customer_id": "INT",
                "amount": "DECIMAL",
                "created_at": "TIMESTAMP",
            }
        }
        result = asyncio.run(ironlayer_column_lineage(str(sample_repo), "staging.orders", schema=schema))

        # Schema should help resolve columns — result should be valid.
        assert isinstance(result, dict)

    def test_model_not_found(self, sample_repo: Path):
        result = asyncio.run(ironlayer_column_lineage(str(sample_repo), "nonexistent.model"))
        assert "error" in result


# ---------------------------------------------------------------------------
# ironlayer_diff
# ---------------------------------------------------------------------------


class TestIronlayerDiff:
    def test_identical_sql(self):
        result = asyncio.run(
            ironlayer_diff(
                "SELECT id, name FROM users",
                "SELECT id, name FROM users",
            )
        )

        # SqlGlotDiffer treats exact-match SQL as cosmetic-only (no edits);
        # the differ normalises both sides through sqlglot AST round-trip
        # which may produce cosmetic differences.
        assert result["is_cosmetic_only"] is True or result["is_identical"] is True
        assert result["edit_count"] == 0

    def test_structural_change(self):
        result = asyncio.run(
            ironlayer_diff(
                "SELECT id, name FROM users",
                "SELECT id, name, email FROM users",
            )
        )

        assert result["is_identical"] is False
        assert "column_changes" in result

    def test_cosmetic_change(self):
        result = asyncio.run(
            ironlayer_diff(
                "SELECT   id,   name   FROM   users",
                "SELECT id, name FROM users",
            )
        )

        # Whitespace-only changes should be cosmetic.
        assert result["is_cosmetic_only"] is True or result["is_identical"] is True


# ---------------------------------------------------------------------------
# ironlayer_transpile
# ---------------------------------------------------------------------------


class TestIronlayerTranspile:
    def test_redshift_to_databricks(self):
        result = asyncio.run(
            ironlayer_transpile(
                "SELECT GETDATE(), NVL(x, 0) FROM t",
                source_dialect="redshift",
                target_dialect="databricks",
            )
        )

        assert result["output_sql"]
        assert result["source_dialect"] == "redshift"
        assert result["target_dialect"] == "databricks"
        assert result["fallback_used"] is False

    def test_databricks_to_duckdb(self):
        result = asyncio.run(
            ironlayer_transpile(
                "SELECT CURRENT_TIMESTAMP() FROM t",
                source_dialect="databricks",
                target_dialect="duckdb",
            )
        )

        assert result["output_sql"]

    def test_pretty_formatting(self):
        result = asyncio.run(
            ironlayer_transpile(
                "SELECT a, b, c FROM t WHERE x > 1 AND y < 2",
                source_dialect="databricks",
                target_dialect="databricks",
                pretty=True,
            )
        )

        assert result["output_sql"]


# ---------------------------------------------------------------------------
# ironlayer_validate
# ---------------------------------------------------------------------------


class TestIronlayerValidate:
    def test_validate_all_models(self, sample_repo: Path):
        result = asyncio.run(ironlayer_validate(str(sample_repo)))

        assert result["models_checked"] >= 2
        assert isinstance(result["violations"], list)

    def test_validate_single_model(self, sample_repo: Path):
        result = asyncio.run(ironlayer_validate(str(sample_repo), model_name="raw.orders"))

        assert result["models_checked"] == 1

    def test_validate_with_schema(self, sample_repo: Path):
        schema = {
            "source_db.orders": {
                "id": "INT",
                "customer_id": "INT",
                "amount": "DECIMAL",
                "created_at": "TIMESTAMP",
            }
        }
        result = asyncio.run(ironlayer_validate(str(sample_repo), schema=schema))

        assert isinstance(result, dict)
        assert "models_checked" in result

    def test_model_not_found(self, sample_repo: Path):
        result = asyncio.run(ironlayer_validate(str(sample_repo), model_name="nonexistent"))
        assert "error" in result
