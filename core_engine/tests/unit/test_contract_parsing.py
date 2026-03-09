"""Unit tests for schema contract header parsing in core_engine.loader.model_loader."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from core_engine.loader.model_loader import (
    HeaderParseError,
    _parse_contract_columns,
    parse_model_file,
    parse_yaml_header,
)
from core_engine.models.model_definition import (
    ColumnContract,
    SchemaContractMode,
)

# ---------------------------------------------------------------------------
# _parse_contract_columns
# ---------------------------------------------------------------------------


class TestParseContractColumns:
    """Tests for the _parse_contract_columns() helper."""

    def test_single_column_with_type(self):
        result = _parse_contract_columns("id:INT")
        assert len(result) == 1
        assert result[0] == ColumnContract(name="id", data_type="INT", nullable=True)

    def test_single_column_with_not_null(self):
        result = _parse_contract_columns("id:INT:NOT_NULL")
        assert len(result) == 1
        assert result[0] == ColumnContract(name="id", data_type="INT", nullable=False)

    def test_multiple_columns(self):
        result = _parse_contract_columns("id:INT:NOT_NULL, name:STRING, created_at:TIMESTAMP:NOT_NULL")
        assert len(result) == 3
        assert result[0] == ColumnContract(name="id", data_type="INT", nullable=False)
        assert result[1] == ColumnContract(name="name", data_type="STRING", nullable=True)
        assert result[2] == ColumnContract(name="created_at", data_type="TIMESTAMP", nullable=False)

    def test_type_string(self):
        result = _parse_contract_columns("col:STRING")
        assert result[0].data_type == "STRING"

    def test_type_int(self):
        result = _parse_contract_columns("col:INT")
        assert result[0].data_type == "INT"

    def test_type_bigint(self):
        result = _parse_contract_columns("col:BIGINT")
        assert result[0].data_type == "BIGINT"

    def test_type_float(self):
        result = _parse_contract_columns("col:FLOAT")
        assert result[0].data_type == "FLOAT"

    def test_type_boolean(self):
        result = _parse_contract_columns("col:BOOLEAN")
        assert result[0].data_type == "BOOLEAN"

    def test_type_date(self):
        result = _parse_contract_columns("col:DATE")
        assert result[0].data_type == "DATE"

    def test_type_timestamp(self):
        result = _parse_contract_columns("col:TIMESTAMP")
        assert result[0].data_type == "TIMESTAMP"

    def test_type_decimal(self):
        result = _parse_contract_columns("col:DECIMAL")
        assert result[0].data_type == "DECIMAL"

    def test_empty_string_returns_empty_list(self):
        result = _parse_contract_columns("")
        assert result == []

    def test_whitespace_only_returns_empty_list(self):
        result = _parse_contract_columns("   ")
        assert result == []

    def test_whitespace_handling_in_entries(self):
        result = _parse_contract_columns("  id : INT : NOT_NULL ,  name : STRING  ")
        assert len(result) == 2
        assert result[0] == ColumnContract(name="id", data_type="INT", nullable=False)
        assert result[1] == ColumnContract(name="name", data_type="STRING", nullable=True)

    def test_trailing_comma_is_ignored(self):
        result = _parse_contract_columns("id:INT, name:STRING,")
        assert len(result) == 2

    def test_error_on_missing_type(self):
        with pytest.raises(HeaderParseError, match="expected"):
            _parse_contract_columns("id")

    def test_error_on_invalid_modifier(self):
        with pytest.raises(HeaderParseError, match="Expected 'NOT_NULL'"):
            _parse_contract_columns("id:INT:UNIQUE")

    def test_error_on_empty_column_name(self):
        with pytest.raises(HeaderParseError, match="Empty column name"):
            _parse_contract_columns(":INT")

    def test_error_on_empty_data_type(self):
        with pytest.raises(HeaderParseError, match="Empty data type"):
            _parse_contract_columns("id:")

    def test_type_normalised_to_uppercase(self):
        result = _parse_contract_columns("id:int")
        assert result[0].data_type == "INT"

    def test_modifier_case_insensitive(self):
        result = _parse_contract_columns("id:INT:not_null")
        assert result[0].nullable is False

    def test_single_column_nullable_defaults_true(self):
        result = _parse_contract_columns("name:STRING")
        assert result[0].nullable is True

    def test_many_columns_all_types(self):
        value = (
            "a:STRING, b:INT:NOT_NULL, c:BIGINT, d:FLOAT:NOT_NULL, "
            "e:BOOLEAN, f:DATE:NOT_NULL, g:TIMESTAMP, h:DECIMAL:NOT_NULL"
        )
        result = _parse_contract_columns(value)
        assert len(result) == 8
        assert result[0].data_type == "STRING"
        assert result[1].nullable is False
        assert result[2].data_type == "BIGINT"
        assert result[3].nullable is False
        assert result[4].data_type == "BOOLEAN"
        assert result[5].nullable is False
        assert result[6].data_type == "TIMESTAMP"
        assert result[7].nullable is False

    def test_error_on_only_name_no_colon(self):
        with pytest.raises(HeaderParseError, match="expected"):
            _parse_contract_columns("just_a_name")


# ---------------------------------------------------------------------------
# parse_yaml_header — contract-related fields
# ---------------------------------------------------------------------------


class TestParseYamlHeaderContracts:
    """Tests for contract fields returned by parse_yaml_header()."""

    def test_header_with_contract_mode(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- contract_mode: STRICT

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert result["contract_mode"] == "STRICT"

    def test_header_with_contract_columns(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- contract_columns: id:INT:NOT_NULL, name:STRING

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert isinstance(result["contract_columns"], list)
        assert len(result["contract_columns"]) == 2
        assert result["contract_columns"][0] == ColumnContract(name="id", data_type="INT", nullable=False)
        assert result["contract_columns"][1] == ColumnContract(name="name", data_type="STRING", nullable=True)

    def test_header_with_both_contract_mode_and_columns(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- contract_mode: WARN
            -- contract_columns: id:INT:NOT_NULL, amount:DECIMAL

            SELECT id, amount FROM raw
        """)
        result = parse_yaml_header(sql)
        assert result["contract_mode"] == "WARN"
        assert len(result["contract_columns"]) == 2

    def test_contract_columns_parsed_as_custom_field_not_list(self):
        """contract_columns is in _CUSTOM_PARSED_FIELDS, not _LIST_FIELDS.

        The result must be a list of ColumnContract objects (from custom
        parsing), not a list of raw comma-separated strings.
        """
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- contract_columns: id:INT

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert isinstance(result["contract_columns"][0], ColumnContract)

    def test_contract_mode_is_known_field(self):
        """contract_mode is in _KNOWN_FIELDS and therefore appears in the
        parsed header dict (not silently ignored)."""
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH
            -- contract_mode: DISABLED

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert "contract_mode" in result
        assert result["contract_mode"] == "DISABLED"

    def test_header_without_contract_fields(self):
        sql = textwrap.dedent("""\
            -- name: my_model
            -- kind: FULL_REFRESH

            SELECT 1
        """)
        result = parse_yaml_header(sql)
        assert "contract_mode" not in result
        assert "contract_columns" not in result


# ---------------------------------------------------------------------------
# parse_model_file — contract integration tests
# ---------------------------------------------------------------------------


class TestParseModelFileContracts:
    """Integration tests verifying that contract fields flow through
    parse_model_file() into ModelDefinition."""

    def test_model_with_contracts(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.orders
            -- kind: FULL_REFRESH
            -- contract_mode: STRICT
            -- contract_columns: id:INT:NOT_NULL, name:STRING, amount:DECIMAL

            SELECT id, name, amount FROM raw_orders
        """)
        model_file = tmp_path / "orders.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.STRICT
        assert len(model.contract_columns) == 3

    def test_model_without_contracts_defaults(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.simple
            -- kind: FULL_REFRESH

            SELECT 1
        """)
        model_file = tmp_path / "simple.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.DISABLED
        assert model.contract_columns == []

    def test_invalid_contract_mode_raises_header_parse_error(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.bad
            -- kind: FULL_REFRESH
            -- contract_mode: EXPLODE

            SELECT 1
        """)
        model_file = tmp_path / "bad.sql"
        model_file.write_text(sql)
        with pytest.raises(HeaderParseError, match="Invalid contract_mode"):
            parse_model_file(model_file)

    def test_contract_mode_case_insensitive_strict(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.ci
            -- kind: FULL_REFRESH
            -- contract_mode: strict

            SELECT 1
        """)
        model_file = tmp_path / "ci.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.STRICT

    def test_contract_mode_case_insensitive_warn(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.ci2
            -- kind: FULL_REFRESH
            -- contract_mode: Warn

            SELECT 1
        """)
        model_file = tmp_path / "ci2.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.WARN

    def test_contract_mode_disabled_explicit(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.disabled
            -- kind: FULL_REFRESH
            -- contract_mode: DISABLED

            SELECT 1
        """)
        model_file = tmp_path / "disabled.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.DISABLED

    def test_multiple_contract_columns_parsed_correctly(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.multi
            -- kind: FULL_REFRESH
            -- contract_mode: STRICT
            -- contract_columns: user_id:BIGINT:NOT_NULL, email:STRING, active:BOOLEAN, joined:DATE:NOT_NULL

            SELECT user_id, email, active, joined FROM users
        """)
        model_file = tmp_path / "multi.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert len(model.contract_columns) == 4
        assert model.contract_columns[0] == ColumnContract(name="user_id", data_type="BIGINT", nullable=False)
        assert model.contract_columns[1] == ColumnContract(name="email", data_type="STRING", nullable=True)
        assert model.contract_columns[2] == ColumnContract(name="active", data_type="BOOLEAN", nullable=True)
        assert model.contract_columns[3] == ColumnContract(name="joined", data_type="DATE", nullable=False)

    def test_contract_columns_without_contract_mode(self, tmp_path: Path):
        """contract_columns without an explicit contract_mode still produces
        a model with DISABLED mode and populated column contracts."""
        sql = textwrap.dedent("""\
            -- name: test.no_mode
            -- kind: FULL_REFRESH
            -- contract_columns: id:INT

            SELECT id FROM t
        """)
        model_file = tmp_path / "no_mode.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.DISABLED
        assert len(model.contract_columns) == 1
        assert model.contract_columns[0].name == "id"

    def test_contract_mode_without_columns(self, tmp_path: Path):
        """contract_mode without contract_columns gives STRICT mode and
        an empty column list."""
        sql = textwrap.dedent("""\
            -- name: test.no_cols
            -- kind: FULL_REFRESH
            -- contract_mode: STRICT

            SELECT 1
        """)
        model_file = tmp_path / "no_cols.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.contract_mode == SchemaContractMode.STRICT
        assert model.contract_columns == []

    def test_contract_fields_coexist_with_all_other_fields(self, tmp_path: Path):
        """Contracts integrate correctly alongside every other header field."""
        sql = textwrap.dedent("""\
            -- name: analytics.wide
            -- kind: FULL_REFRESH
            -- materialization: VIEW
            -- owner: data-team
            -- tags: finance, daily
            -- contract_mode: WARN
            -- contract_columns: revenue:DECIMAL:NOT_NULL, day:DATE

            SELECT revenue, day FROM raw.sales
        """)
        model_file = tmp_path / "wide.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        assert model.name == "analytics.wide"
        assert model.owner == "data-team"
        assert model.tags == ["finance", "daily"]
        assert model.contract_mode == SchemaContractMode.WARN
        assert len(model.contract_columns) == 2

    def test_model_definition_contract_columns_are_column_contract_instances(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.types
            -- kind: FULL_REFRESH
            -- contract_columns: a:STRING, b:INT:NOT_NULL

            SELECT a, b FROM t
        """)
        model_file = tmp_path / "types.sql"
        model_file.write_text(sql)
        model = parse_model_file(model_file)
        for col in model.contract_columns:
            assert isinstance(col, ColumnContract)

    def test_invalid_contract_columns_propagates_header_parse_error(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.bad_cols
            -- kind: FULL_REFRESH
            -- contract_columns: id

            SELECT 1
        """)
        model_file = tmp_path / "bad_cols.sql"
        model_file.write_text(sql)
        with pytest.raises(HeaderParseError, match="expected"):
            parse_model_file(model_file)

    def test_invalid_modifier_in_columns_propagates_header_parse_error(self, tmp_path: Path):
        sql = textwrap.dedent("""\
            -- name: test.bad_mod
            -- kind: FULL_REFRESH
            -- contract_columns: id:INT:PRIMARY_KEY

            SELECT 1
        """)
        model_file = tmp_path / "bad_mod.sql"
        model_file.write_text(sql)
        with pytest.raises(HeaderParseError, match="Expected 'NOT_NULL'"):
            parse_model_file(model_file)
