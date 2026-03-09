"""Tests for core_engine.executor.schema_introspector.

Covers:
- parse_describe_output: normal tables, metadata row skipping, empty input, special types
- compare_schemas: identical schemas, column removed/added, type changed, multiple drifts, empty schemas, case-insensitive
- compare_with_contracts: uses Phase 2 contract_columns from ModelDefinition
"""

from __future__ import annotations

from core_engine.executor.schema_introspector import (
    ColumnInfo,
    TableSchema,
    compare_schemas,
    compare_with_contracts,
    parse_describe_output,
)
from core_engine.models.model_definition import (
    ColumnContract,
    Materialization,
    ModelDefinition,
    ModelKind,
    SchemaContractMode,
)

# ---------------------------------------------------------------------------
# parse_describe_output
# ---------------------------------------------------------------------------


class TestParseDescribeOutput:
    """Tests for parsing Databricks DESCRIBE TABLE EXTENDED output."""

    def test_normal_table(self) -> None:
        """Standard table with multiple columns parses correctly."""
        raw = [
            {"col_name": "id", "data_type": "bigint", "comment": "Primary key"},
            {"col_name": "name", "data_type": "string", "comment": "User name"},
            {"col_name": "created_at", "data_type": "timestamp", "comment": ""},
        ]
        schema = parse_describe_output(raw)

        assert len(schema.columns) == 3
        assert schema.columns[0].name == "id"
        assert schema.columns[0].data_type == "bigint"
        assert schema.columns[0].comment == "Primary key"
        assert schema.columns[1].name == "name"
        assert schema.columns[1].data_type == "string"
        assert schema.columns[2].name == "created_at"
        assert schema.columns[2].data_type == "timestamp"
        assert schema.columns[2].comment is None  # Empty string -> None

    def test_metadata_rows_skipped(self) -> None:
        """Rows after the '#' separator are metadata and must be skipped."""
        raw = [
            {"col_name": "id", "data_type": "bigint", "comment": ""},
            {"col_name": "value", "data_type": "double", "comment": ""},
            {"col_name": "# Detailed Table Information", "data_type": "", "comment": ""},
            {"col_name": "Database", "data_type": "analytics", "comment": ""},
            {"col_name": "Table", "data_type": "metrics", "comment": ""},
        ]
        schema = parse_describe_output(raw)

        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"
        assert schema.columns[1].name == "value"

    def test_empty_input(self) -> None:
        """Empty input produces an empty schema."""
        schema = parse_describe_output([])
        assert len(schema.columns) == 0
        assert schema.table_name == ""

    def test_special_types(self) -> None:
        """Complex types like ARRAY, MAP, and STRUCT parse correctly."""
        raw = [
            {"col_name": "tags", "data_type": "array<string>", "comment": ""},
            {"col_name": "metadata", "data_type": "map<string,string>", "comment": ""},
            {"col_name": "address", "data_type": "struct<city:string,zip:int>", "comment": ""},
        ]
        schema = parse_describe_output(raw)

        assert len(schema.columns) == 3
        assert schema.columns[0].data_type == "array<string>"
        assert schema.columns[1].data_type == "map<string,string>"
        assert schema.columns[2].data_type == "struct<city:string,zip:int>"

    def test_empty_col_name_stops_parsing(self) -> None:
        """A row with an empty col_name terminates column collection."""
        raw = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "", "data_type": "", "comment": ""},
            {"col_name": "secret", "data_type": "string", "comment": ""},
        ]
        schema = parse_describe_output(raw)

        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"

    def test_whitespace_handling(self) -> None:
        """Leading/trailing whitespace in field values is stripped."""
        raw = [
            {"col_name": "  name  ", "data_type": "  string  ", "comment": "  a comment  "},
        ]
        schema = parse_describe_output(raw)

        assert schema.columns[0].name == "name"
        assert schema.columns[0].data_type == "string"
        assert schema.columns[0].comment == "a comment"


# ---------------------------------------------------------------------------
# compare_schemas
# ---------------------------------------------------------------------------


class TestCompareSchemas:
    """Tests for comparing expected vs actual schemas."""

    def _make_schema(self, table_name: str, cols: list[tuple[str, str]]) -> TableSchema:
        """Helper to build a TableSchema from (name, type) pairs."""
        return TableSchema(
            table_name=table_name,
            columns=[ColumnInfo(name=n, data_type=t) for n, t in cols],
        )

    def test_identical_schemas(self) -> None:
        """Identical schemas produce no drifts."""
        schema = self._make_schema("t", [("id", "INT"), ("name", "STRING")])
        drifts = compare_schemas(schema, schema)
        assert drifts == []

    def test_column_removed(self) -> None:
        """Column in expected but missing from actual -> COLUMN_REMOVED."""
        expected = self._make_schema("t", [("id", "INT"), ("name", "STRING")])
        actual = self._make_schema("t", [("id", "INT")])
        drifts = compare_schemas(expected, actual)

        assert len(drifts) == 1
        assert drifts[0].drift_type == "COLUMN_REMOVED"
        assert drifts[0].column_name == "name"

    def test_column_added(self) -> None:
        """Column in actual but not in expected -> COLUMN_ADDED."""
        expected = self._make_schema("t", [("id", "INT")])
        actual = self._make_schema("t", [("id", "INT"), ("extra", "STRING")])
        drifts = compare_schemas(expected, actual)

        assert len(drifts) == 1
        assert drifts[0].drift_type == "COLUMN_ADDED"
        assert drifts[0].column_name == "extra"

    def test_type_changed(self) -> None:
        """Column exists in both but with different types -> TYPE_CHANGED."""
        expected = self._make_schema("t", [("id", "INT"), ("amount", "FLOAT")])
        actual = self._make_schema("t", [("id", "INT"), ("amount", "DOUBLE")])
        drifts = compare_schemas(expected, actual)

        assert len(drifts) == 1
        assert drifts[0].drift_type == "TYPE_CHANGED"
        assert drifts[0].column_name == "amount"
        assert drifts[0].expected == "FLOAT"
        assert drifts[0].actual == "DOUBLE"

    def test_multiple_drifts(self) -> None:
        """Multiple drift types detected across columns."""
        expected = self._make_schema("t", [("id", "INT"), ("name", "STRING"), ("age", "INT")])
        actual = self._make_schema("t", [("id", "BIGINT"), ("email", "STRING")])
        drifts = compare_schemas(expected, actual)

        drift_types = sorted(d.drift_type for d in drifts)
        # age removed, name removed, email added, id type changed (INT vs BIGINT, but INT != BIGINT after normalization)
        assert "COLUMN_ADDED" in drift_types
        assert "COLUMN_REMOVED" in drift_types

    def test_empty_schemas(self) -> None:
        """Comparing two empty schemas produces no drifts."""
        empty = self._make_schema("t", [])
        drifts = compare_schemas(empty, empty)
        assert drifts == []

    def test_case_insensitive_column_names(self) -> None:
        """Column matching is case-insensitive."""
        expected = self._make_schema("t", [("ID", "INT"), ("Name", "STRING")])
        actual = self._make_schema("t", [("id", "INT"), ("name", "STRING")])
        drifts = compare_schemas(expected, actual)

        assert drifts == []

    def test_type_alias_normalization(self) -> None:
        """Type aliases like VARCHAR -> STRING are treated as equal."""
        expected = self._make_schema("t", [("col", "VARCHAR")])
        actual = self._make_schema("t", [("col", "STRING")])
        drifts = compare_schemas(expected, actual)

        assert drifts == []

    def test_integer_alias(self) -> None:
        """INTEGER and INT normalize to the same type."""
        expected = self._make_schema("t", [("col", "INTEGER")])
        actual = self._make_schema("t", [("col", "INT")])
        drifts = compare_schemas(expected, actual)

        assert drifts == []

    def test_deterministic_sort(self) -> None:
        """Results are sorted by (drift_type, column_name)."""
        expected = self._make_schema("t", [("z_col", "INT"), ("a_col", "STRING")])
        actual = self._make_schema("t", [("b_new", "INT"), ("c_new", "STRING")])
        drifts = compare_schemas(expected, actual)

        # All drift_types sorted, within each type by column_name.
        for i in range(len(drifts) - 1):
            key_i = (drifts[i].drift_type, drifts[i].column_name.lower())
            key_j = (drifts[i + 1].drift_type, drifts[i + 1].column_name.lower())
            assert key_i <= key_j


# ---------------------------------------------------------------------------
# compare_with_contracts
# ---------------------------------------------------------------------------


class TestCompareWithContracts:
    """Tests for comparing a ModelDefinition's contracts against actual schema."""

    def _make_model(
        self,
        name: str = "test.model",
        contract_columns: list[ColumnContract] | None = None,
        output_columns: list[str] | None = None,
        contract_mode: SchemaContractMode = SchemaContractMode.WARN,
    ) -> ModelDefinition:
        """Helper to build a ModelDefinition."""
        return ModelDefinition(
            name=name,
            kind=ModelKind.FULL_REFRESH,
            materialization=Materialization.TABLE,
            file_path="models/test.sql",
            raw_sql="SELECT 1",
            clean_sql="SELECT 1",
            contract_mode=contract_mode,
            contract_columns=contract_columns or [],
            output_columns=output_columns or [],
        )

    def _make_actual(self, cols: list[tuple[str, str]]) -> TableSchema:
        """Helper to build an actual TableSchema."""
        return TableSchema(
            table_name="warehouse.test_model",
            columns=[ColumnInfo(name=n, data_type=t) for n, t in cols],
        )

    def test_contract_columns_match(self) -> None:
        """When contract columns match actual columns, no drifts."""
        model = self._make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="name", data_type="STRING"),
            ]
        )
        actual = self._make_actual([("id", "INT"), ("name", "STRING")])
        drifts = compare_with_contracts(model, actual)

        assert drifts == []

    def test_contract_column_removed(self) -> None:
        """Contract column missing from actual -> COLUMN_REMOVED."""
        model = self._make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="deleted_col", data_type="STRING"),
            ]
        )
        actual = self._make_actual([("id", "INT")])
        drifts = compare_with_contracts(model, actual)

        removed = [d for d in drifts if d.drift_type == "COLUMN_REMOVED"]
        assert len(removed) == 1
        assert removed[0].column_name == "deleted_col"

    def test_contract_type_changed(self) -> None:
        """Contract type differs from actual -> TYPE_CHANGED."""
        model = self._make_model(
            contract_columns=[
                ColumnContract(name="amount", data_type="FLOAT"),
            ]
        )
        actual = self._make_actual([("amount", "DOUBLE")])
        drifts = compare_with_contracts(model, actual)

        changed = [d for d in drifts if d.drift_type == "TYPE_CHANGED"]
        assert len(changed) == 1
        assert changed[0].column_name == "amount"

    def test_fallback_to_output_columns(self) -> None:
        """Without contract_columns, uses output_columns for name-only checks."""
        model = self._make_model(
            output_columns=["id", "name", "email"],
        )
        actual = self._make_actual([("id", "INT"), ("name", "STRING")])
        drifts = compare_with_contracts(model, actual)

        removed = [d for d in drifts if d.drift_type == "COLUMN_REMOVED"]
        assert len(removed) == 1
        assert removed[0].column_name == "email"

    def test_no_columns_returns_empty(self) -> None:
        """No contract_columns and no output_columns -> empty result."""
        model = self._make_model()
        actual = self._make_actual([("id", "INT")])
        drifts = compare_with_contracts(model, actual)

        assert drifts == []

    def test_extra_actual_columns_detected(self) -> None:
        """Columns in actual but not in contract -> COLUMN_ADDED."""
        model = self._make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ]
        )
        actual = self._make_actual([("id", "INT"), ("extra", "STRING")])
        drifts = compare_with_contracts(model, actual)

        added = [d for d in drifts if d.drift_type == "COLUMN_ADDED"]
        assert len(added) == 1
        assert added[0].column_name == "extra"
