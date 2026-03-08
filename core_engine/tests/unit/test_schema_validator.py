"""Unit tests for core_engine.contracts.schema_validator."""

from __future__ import annotations

import pytest

from core_engine.contracts.schema_validator import (
    ContractValidationResult,
    ContractViolation,
    ViolationSeverity,
    _normalize_type,
    validate_schema_contract,
    validate_schema_contracts_batch,
)
from core_engine.models.model_definition import (
    ColumnContract,
    ModelDefinition,
    ModelKind,
    SchemaContractMode,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_model(
    name="test.model",
    contract_mode=SchemaContractMode.STRICT,
    contract_columns=None,
    output_columns=None,
    **kwargs,
) -> ModelDefinition:
    return ModelDefinition(
        name=name,
        kind=ModelKind.FULL_REFRESH,
        file_path="models/test.sql",
        raw_sql="SELECT 1",
        contract_mode=contract_mode,
        contract_columns=contract_columns or [],
        output_columns=output_columns or [],
        **kwargs,
    )


# ---------------------------------------------------------------------------
# ViolationSeverity enum
# ---------------------------------------------------------------------------


class TestViolationSeverity:
    def test_breaking_value(self):
        assert ViolationSeverity.BREAKING.value == "BREAKING"

    def test_warning_value(self):
        assert ViolationSeverity.WARNING.value == "WARNING"

    def test_info_value(self):
        assert ViolationSeverity.INFO.value == "INFO"

    def test_is_str_enum(self):
        assert isinstance(ViolationSeverity.BREAKING, str)


# ---------------------------------------------------------------------------
# _normalize_type
# ---------------------------------------------------------------------------


class TestNormalizeType:
    def test_varchar_to_string(self):
        assert _normalize_type("VARCHAR") == "STRING"

    def test_text_to_string(self):
        assert _normalize_type("TEXT") == "STRING"

    def test_char_to_string(self):
        assert _normalize_type("CHAR") == "STRING"

    def test_nvarchar_to_string(self):
        assert _normalize_type("NVARCHAR") == "STRING"

    def test_integer_to_int(self):
        assert _normalize_type("INTEGER") == "INT"

    def test_long_to_bigint(self):
        assert _normalize_type("LONG") == "BIGINT"

    def test_biginteger_to_bigint(self):
        assert _normalize_type("BIGINTEGER") == "BIGINT"

    def test_short_to_smallint(self):
        assert _normalize_type("SHORT") == "SMALLINT"

    def test_tinyint_to_smallint(self):
        assert _normalize_type("TINYINT") == "SMALLINT"

    def test_real_to_float(self):
        assert _normalize_type("REAL") == "FLOAT"

    def test_double_precision_to_double(self):
        assert _normalize_type("DOUBLE PRECISION") == "DOUBLE"

    def test_datetime_to_timestamp(self):
        assert _normalize_type("DATETIME") == "TIMESTAMP"

    def test_bool_to_boolean(self):
        assert _normalize_type("BOOL") == "BOOLEAN"

    def test_numeric_to_decimal(self):
        assert _normalize_type("NUMERIC") == "DECIMAL"

    def test_number_to_decimal(self):
        assert _normalize_type("NUMBER") == "DECIMAL"

    def test_unknown_type_passes_through(self):
        assert _normalize_type("BINARY") == "BINARY"

    def test_string_unchanged(self):
        assert _normalize_type("STRING") == "STRING"

    def test_int_unchanged(self):
        assert _normalize_type("INT") == "INT"

    def test_lowercase_input_uppercased(self):
        assert _normalize_type("varchar") == "STRING"

    def test_mixed_case_input(self):
        assert _normalize_type("Integer") == "INT"

    def test_whitespace_stripped(self):
        assert _normalize_type("  VARCHAR  ") == "STRING"

    def test_whitespace_on_unknown_type(self):
        assert _normalize_type("  BINARY  ") == "BINARY"


# ---------------------------------------------------------------------------
# ContractValidationResult properties
# ---------------------------------------------------------------------------


class TestContractValidationResult:
    def test_empty_result_no_violations(self):
        result = ContractValidationResult(violations=[], models_checked=0)
        assert result.has_breaking_violations is False
        assert result.breaking_count == 0
        assert result.warning_count == 0
        assert result.info_count == 0

    def test_has_breaking_violations_true(self):
        violation = ContractViolation(
            model_name="m",
            column_name="c",
            violation_type="COLUMN_REMOVED",
            severity=ViolationSeverity.BREAKING,
        )
        result = ContractValidationResult(violations=[violation], models_checked=1)
        assert result.has_breaking_violations is True

    def test_has_breaking_violations_false_with_info_only(self):
        violation = ContractViolation(
            model_name="m",
            column_name="c",
            violation_type="COLUMN_ADDED",
            severity=ViolationSeverity.INFO,
        )
        result = ContractValidationResult(violations=[violation], models_checked=1)
        assert result.has_breaking_violations is False

    def test_breaking_count(self):
        violations = [
            ContractViolation(
                model_name="m",
                column_name="a",
                violation_type="COLUMN_REMOVED",
                severity=ViolationSeverity.BREAKING,
            ),
            ContractViolation(
                model_name="m",
                column_name="b",
                violation_type="TYPE_CHANGED",
                severity=ViolationSeverity.BREAKING,
            ),
            ContractViolation(
                model_name="m",
                column_name="c",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=1)
        assert result.breaking_count == 2

    def test_warning_count(self):
        violations = [
            ContractViolation(
                model_name="m",
                column_name="a",
                violation_type="TEST",
                severity=ViolationSeverity.WARNING,
            ),
            ContractViolation(
                model_name="m",
                column_name="b",
                violation_type="TEST",
                severity=ViolationSeverity.WARNING,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=1)
        assert result.warning_count == 2

    def test_info_count(self):
        violations = [
            ContractViolation(
                model_name="m",
                column_name="x",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=1)
        assert result.info_count == 1

    def test_mixed_severity_counts(self):
        violations = [
            ContractViolation(
                model_name="m",
                column_name="a",
                violation_type="COLUMN_REMOVED",
                severity=ViolationSeverity.BREAKING,
            ),
            ContractViolation(
                model_name="m",
                column_name="b",
                violation_type="W",
                severity=ViolationSeverity.WARNING,
            ),
            ContractViolation(
                model_name="m",
                column_name="c",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=1)
        assert result.breaking_count == 1
        assert result.warning_count == 1
        assert result.info_count == 1

    def test_violations_for_model_filters_correctly(self):
        violations = [
            ContractViolation(
                model_name="alpha",
                column_name="c1",
                violation_type="COLUMN_REMOVED",
                severity=ViolationSeverity.BREAKING,
            ),
            ContractViolation(
                model_name="beta",
                column_name="c2",
                violation_type="COLUMN_ADDED",
                severity=ViolationSeverity.INFO,
            ),
            ContractViolation(
                model_name="alpha",
                column_name="c3",
                violation_type="TYPE_CHANGED",
                severity=ViolationSeverity.BREAKING,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=2)
        alpha_violations = result.violations_for_model("alpha")
        assert len(alpha_violations) == 2
        assert all(v.model_name == "alpha" for v in alpha_violations)

    def test_violations_for_model_returns_empty_for_unknown(self):
        violations = [
            ContractViolation(
                model_name="alpha",
                column_name="c1",
                violation_type="COLUMN_REMOVED",
                severity=ViolationSeverity.BREAKING,
            ),
        ]
        result = ContractValidationResult(violations=violations, models_checked=1)
        assert result.violations_for_model("nonexistent") == []


# ---------------------------------------------------------------------------
# validate_schema_contract — disabled / empty contracts
# ---------------------------------------------------------------------------


class TestValidateSchemaContractDisabledAndEmpty:
    def test_disabled_contract_returns_zero_models_checked(self):
        model = _make_model(contract_mode=SchemaContractMode.DISABLED)
        result = validate_schema_contract(model)
        assert result.models_checked == 0
        assert result.violations == []

    def test_disabled_contract_ignores_violations(self):
        model = _make_model(
            contract_mode=SchemaContractMode.DISABLED,
            contract_columns=[ColumnContract(name="id", data_type="INT")],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert result.models_checked == 0
        assert result.violations == []

    def test_empty_contract_columns_returns_no_violations(self):
        model = _make_model(
            contract_columns=[],
            output_columns=["id", "name"],
        )
        result = validate_schema_contract(model)
        assert result.models_checked == 1
        assert result.violations == []

    def test_empty_contract_columns_with_strict_mode(self):
        model = _make_model(
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[],
            output_columns=["col_a"],
        )
        result = validate_schema_contract(model)
        assert result.models_checked == 1
        assert result.has_breaking_violations is False


# ---------------------------------------------------------------------------
# validate_schema_contract — all columns match
# ---------------------------------------------------------------------------


class TestValidateSchemaContractAllMatch:
    def test_exact_column_match_no_violations(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="name", data_type="STRING"),
            ],
            output_columns=["id", "name"],
        )
        result = validate_schema_contract(model)
        assert result.violations == []
        assert result.models_checked == 1

    def test_exact_match_with_types_provided(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="name", data_type="STRING"),
            ],
            output_columns=["id", "name"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"id": "INT", "name": "STRING"},
        )
        assert result.violations == []

    def test_exact_match_with_nullability_provided(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=True),
                ColumnContract(name="name", data_type="STRING", nullable=True),
            ],
            output_columns=["id", "name"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": True, "name": True},
        )
        assert result.violations == []

    def test_type_aliases_match_no_violation(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INTEGER"),
                ColumnContract(name="label", data_type="VARCHAR"),
            ],
            output_columns=["id", "label"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"id": "INT", "label": "STRING"},
        )
        assert result.violations == []


# ---------------------------------------------------------------------------
# validate_schema_contract — COLUMN_REMOVED
# ---------------------------------------------------------------------------


class TestValidateSchemaContractColumnRemoved:
    def test_single_column_removed(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="deleted_col", data_type="STRING"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model)
        assert len(result.violations) == 1
        v = result.violations[0]
        assert v.violation_type == "COLUMN_REMOVED"
        assert v.severity == ViolationSeverity.BREAKING
        assert v.column_name == "deleted_col"
        assert v.actual == "(missing)"

    def test_all_contracted_columns_removed(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="a", data_type="INT"),
                ColumnContract(name="b", data_type="STRING"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert len(result.violations) == 2
        assert all(v.violation_type == "COLUMN_REMOVED" for v in result.violations)
        assert result.has_breaking_violations is True

    def test_column_removed_message_content(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="user_id", data_type="BIGINT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        v = result.violations[0]
        assert "user_id" in v.message
        assert "BIGINT" in v.message
        assert "missing" in v.message

    def test_column_removed_expected_field(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="amount", data_type="DECIMAL"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        v = result.violations[0]
        assert "amount" in v.expected
        assert "DECIMAL" in v.expected


# ---------------------------------------------------------------------------
# validate_schema_contract — TYPE_CHANGED
# ---------------------------------------------------------------------------


class TestValidateSchemaContractTypeChanged:
    def test_type_mismatch_string_to_int(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"id": "STRING"},
        )
        assert len(result.violations) == 1
        v = result.violations[0]
        assert v.violation_type == "TYPE_CHANGED"
        assert v.severity == ViolationSeverity.BREAKING
        assert v.expected == "INT"
        assert v.actual == "STRING"

    def test_type_mismatch_float_to_double(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="price", data_type="FLOAT"),
            ],
            output_columns=["price"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"price": "DOUBLE"},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "TYPE_CHANGED"

    def test_type_mismatch_boolean_to_int(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="active", data_type="BOOLEAN"),
            ],
            output_columns=["active"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"active": "INT"},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "TYPE_CHANGED"

    def test_no_type_violation_without_type_map(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model)
        assert result.violations == []

    def test_type_alias_normalization_prevents_false_positive(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="name", data_type="TEXT"),
            ],
            output_columns=["name"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"name": "VARCHAR"},
        )
        assert result.violations == []

    def test_type_alias_integer_vs_int(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="count", data_type="INTEGER"),
            ],
            output_columns=["count"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"count": "INT"},
        )
        assert result.violations == []

    def test_type_alias_long_vs_bigint(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="big_id", data_type="LONG"),
            ],
            output_columns=["big_id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"big_id": "BIGINT"},
        )
        assert result.violations == []

    def test_type_alias_datetime_vs_timestamp(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="created_at", data_type="DATETIME"),
            ],
            output_columns=["created_at"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"created_at": "TIMESTAMP"},
        )
        assert result.violations == []

    def test_type_alias_bool_vs_boolean(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="flag", data_type="BOOL"),
            ],
            output_columns=["flag"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"flag": "BOOLEAN"},
        )
        assert result.violations == []

    def test_type_alias_numeric_vs_decimal(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="amount", data_type="NUMERIC"),
            ],
            output_columns=["amount"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"amount": "DECIMAL"},
        )
        assert result.violations == []

    def test_type_alias_real_vs_float(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="score", data_type="REAL"),
            ],
            output_columns=["score"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"score": "FLOAT"},
        )
        assert result.violations == []

    def test_type_changed_message_content(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="status", data_type="STRING"),
            ],
            output_columns=["status"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"status": "INT"},
        )
        v = result.violations[0]
        assert "status" in v.message
        assert "STRING" in v.message
        assert "INT" in v.message

    def test_type_not_in_actual_types_map_no_violation(self):
        """Column exists in output but not in the types map -- skip type check."""
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"other_col": "STRING"},
        )
        assert result.violations == []


# ---------------------------------------------------------------------------
# validate_schema_contract — NULLABLE_TIGHTENED
# ---------------------------------------------------------------------------


class TestValidateSchemaContractNullableTightened:
    def test_contract_not_null_but_actual_nullable(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": True},
        )
        assert len(result.violations) == 1
        v = result.violations[0]
        assert v.violation_type == "NULLABLE_TIGHTENED"
        assert v.severity == ViolationSeverity.BREAKING
        assert v.expected == "NOT NULL"
        assert v.actual == "NULLABLE"

    def test_contract_nullable_actual_not_null_no_violation(self):
        """Contract allows NULLs, actual is NOT NULL. This is fine (stricter is safe)."""
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=True),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": False},
        )
        assert result.violations == []

    def test_both_nullable_no_violation(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=True),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": True},
        )
        assert result.violations == []

    def test_both_not_null_no_violation(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": False},
        )
        assert result.violations == []

    def test_no_nullability_violation_without_nullability_map(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model)
        assert result.violations == []

    def test_column_not_in_nullability_map_no_violation(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"other_col": True},
        )
        assert result.violations == []

    def test_nullable_tightened_message_content(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="user_id", data_type="INT", nullable=False),
            ],
            output_columns=["user_id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"user_id": True},
        )
        v = result.violations[0]
        assert "user_id" in v.message
        assert "NOT NULL" in v.message


# ---------------------------------------------------------------------------
# validate_schema_contract — COLUMN_ADDED
# ---------------------------------------------------------------------------


class TestValidateSchemaContractColumnAdded:
    def test_extra_column_produces_info_violation(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id", "extra_col"],
        )
        result = validate_schema_contract(model)
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert len(added) == 1
        assert added[0].severity == ViolationSeverity.INFO
        assert added[0].column_name == "extra_col"

    def test_multiple_extra_columns(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id", "extra_a", "extra_b"],
        )
        result = validate_schema_contract(model)
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert len(added) == 2

    def test_added_column_not_breaking(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id", "bonus"],
        )
        result = validate_schema_contract(model)
        assert result.has_breaking_violations is False
        assert result.info_count == 1

    def test_column_added_message_content(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id", "surprise"],
        )
        result = validate_schema_contract(model)
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert "surprise" in added[0].message
        assert "not declared" in added[0].message.lower() or "not" in added[0].message.lower()

    def test_column_added_expected_field(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id", "new_col"],
        )
        result = validate_schema_contract(model)
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert added[0].expected == "(not in contract)"


# ---------------------------------------------------------------------------
# validate_schema_contract — case insensitivity
# ---------------------------------------------------------------------------


class TestValidateSchemaContractCaseInsensitive:
    def test_column_match_case_insensitive(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="ID", data_type="INT"),
                ColumnContract(name="Name", data_type="STRING"),
            ],
            output_columns=["id", "name"],
        )
        result = validate_schema_contract(model)
        removed = [v for v in result.violations if v.violation_type == "COLUMN_REMOVED"]
        assert len(removed) == 0

    def test_type_lookup_case_insensitive(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="ID", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"id": "STRING"},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "TYPE_CHANGED"

    def test_nullability_lookup_case_insensitive(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="ID", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_nullability={"id": True},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "NULLABLE_TIGHTENED"

    def test_added_column_case_insensitive_no_false_positive(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="ID", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model)
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert len(added) == 0


# ---------------------------------------------------------------------------
# validate_schema_contract — multiple violations in one model
# ---------------------------------------------------------------------------


class TestValidateSchemaContractMultipleViolations:
    def test_removed_and_added_together(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
                ColumnContract(name="removed_col", data_type="STRING"),
            ],
            output_columns=["id", "new_col"],
        )
        result = validate_schema_contract(model)
        removed = [v for v in result.violations if v.violation_type == "COLUMN_REMOVED"]
        added = [v for v in result.violations if v.violation_type == "COLUMN_ADDED"]
        assert len(removed) == 1
        assert len(added) == 1
        assert removed[0].column_name == "removed_col"
        assert added[0].column_name == "new_col"

    def test_type_and_nullable_violations_together(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"id": "STRING"},
            actual_column_nullability={"id": True},
        )
        types = [v for v in result.violations if v.violation_type == "TYPE_CHANGED"]
        nulls = [v for v in result.violations if v.violation_type == "NULLABLE_TIGHTENED"]
        assert len(types) == 1
        assert len(nulls) == 1

    def test_all_violation_types_at_once(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="kept_col", data_type="INT", nullable=False),
                ColumnContract(name="removed_col", data_type="STRING"),
            ],
            output_columns=["kept_col", "extra_col"],
        )
        result = validate_schema_contract(
            model,
            actual_column_types={"kept_col": "STRING"},
            actual_column_nullability={"kept_col": True},
        )
        violation_types = {v.violation_type for v in result.violations}
        assert "COLUMN_REMOVED" in violation_types
        assert "TYPE_CHANGED" in violation_types
        assert "NULLABLE_TIGHTENED" in violation_types
        assert "COLUMN_ADDED" in violation_types

    def test_violations_sorted_deterministically(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="z_col", data_type="INT"),
                ColumnContract(name="a_col", data_type="STRING"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        names = [v.column_name for v in result.violations]
        assert names == sorted(names)


# ---------------------------------------------------------------------------
# validate_schema_contract — actual_columns parameter override
# ---------------------------------------------------------------------------


class TestValidateSchemaContractActualColumnsOverride:
    def test_actual_columns_overrides_output_columns(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model, actual_columns=[])
        removed = [v for v in result.violations if v.violation_type == "COLUMN_REMOVED"]
        assert len(removed) == 1

    def test_actual_columns_none_falls_back_to_output_columns(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model, actual_columns=None)
        assert result.violations == []


# ---------------------------------------------------------------------------
# validate_schema_contract — WARN vs STRICT mode
# ---------------------------------------------------------------------------


class TestValidateSchemaContractModes:
    def test_warn_mode_produces_violations(self):
        model = _make_model(
            contract_mode=SchemaContractMode.WARN,
            contract_columns=[
                ColumnContract(name="missing", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "COLUMN_REMOVED"

    def test_strict_mode_produces_violations(self):
        model = _make_model(
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="missing", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "COLUMN_REMOVED"

    def test_warn_and_strict_produce_same_violations(self):
        cols = [
            ColumnContract(name="id", data_type="INT"),
            ColumnContract(name="name", data_type="STRING"),
        ]
        warn_model = _make_model(
            name="warn_model",
            contract_mode=SchemaContractMode.WARN,
            contract_columns=cols,
            output_columns=[],
        )
        strict_model = _make_model(
            name="strict_model",
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=cols,
            output_columns=[],
        )
        warn_result = validate_schema_contract(warn_model)
        strict_result = validate_schema_contract(strict_model)
        assert len(warn_result.violations) == len(strict_result.violations)
        for wv, sv in zip(warn_result.violations, strict_result.violations):
            assert wv.violation_type == sv.violation_type
            assert wv.severity == sv.severity


# ---------------------------------------------------------------------------
# validate_schema_contract — edge cases
# ---------------------------------------------------------------------------


class TestValidateSchemaContractEdgeCases:
    def test_empty_output_columns_all_contracted_removed(self):
        model = _make_model(
            contract_columns=[
                ColumnContract(name="a", data_type="INT"),
                ColumnContract(name="b", data_type="STRING"),
                ColumnContract(name="c", data_type="BOOLEAN"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        removed = [v for v in result.violations if v.violation_type == "COLUMN_REMOVED"]
        assert len(removed) == 3

    def test_empty_output_and_empty_contract(self):
        model = _make_model(
            contract_columns=[],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert result.violations == []
        assert result.models_checked == 1

    def test_model_name_propagated_in_violations(self):
        model = _make_model(
            name="analytics.orders",
            contract_columns=[
                ColumnContract(name="gone", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contract(model)
        assert result.violations[0].model_name == "analytics.orders"

    def test_result_models_checked_is_one_for_active_contract(self):
        model = _make_model(
            contract_mode=SchemaContractMode.WARN,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contract(model)
        assert result.models_checked == 1


# ---------------------------------------------------------------------------
# validate_schema_contracts_batch
# ---------------------------------------------------------------------------


class TestValidateSchemaContractsBatch:
    def test_batch_empty_models_list(self):
        result = validate_schema_contracts_batch([])
        assert result.violations == []
        assert result.models_checked == 0

    def test_batch_single_model(self):
        model = _make_model(
            name="m1",
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contracts_batch([model])
        assert result.models_checked == 1
        assert result.violations == []

    def test_batch_multiple_models_with_violations(self):
        m1 = _make_model(
            name="model_a",
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=[],
        )
        m2 = _make_model(
            name="model_b",
            contract_columns=[
                ColumnContract(name="name", data_type="STRING"),
            ],
            output_columns=[],
        )
        result = validate_schema_contracts_batch([m1, m2])
        assert result.models_checked == 2
        assert len(result.violations) == 2
        model_names = {v.model_name for v in result.violations}
        assert model_names == {"model_a", "model_b"}

    def test_batch_skips_disabled_models(self):
        enabled = _make_model(
            name="enabled",
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=[],
        )
        disabled = _make_model(
            name="disabled",
            contract_mode=SchemaContractMode.DISABLED,
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contracts_batch([enabled, disabled])
        assert result.models_checked == 1
        assert all(v.model_name == "enabled" for v in result.violations)

    def test_batch_with_actual_columns_map(self):
        model = _make_model(
            name="m1",
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contracts_batch(
            [model],
            actual_columns_map={"m1": []},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "COLUMN_REMOVED"

    def test_batch_with_actual_types_map(self):
        model = _make_model(
            name="m1",
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contracts_batch(
            [model],
            actual_types_map={"m1": {"id": "STRING"}},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "TYPE_CHANGED"

    def test_batch_with_actual_nullability_map(self):
        model = _make_model(
            name="m1",
            contract_columns=[
                ColumnContract(name="id", data_type="INT", nullable=False),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contracts_batch(
            [model],
            actual_nullability_map={"m1": {"id": True}},
        )
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "NULLABLE_TIGHTENED"

    def test_batch_violations_sorted_globally(self):
        m_b = _make_model(
            name="b_model",
            contract_columns=[
                ColumnContract(name="col", data_type="INT"),
            ],
            output_columns=[],
        )
        m_a = _make_model(
            name="a_model",
            contract_columns=[
                ColumnContract(name="col", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contracts_batch([m_b, m_a])
        model_names = [v.model_name for v in result.violations]
        assert model_names == sorted(model_names)

    def test_batch_violations_for_model_works_across_batch(self):
        m1 = _make_model(
            name="alpha",
            contract_columns=[
                ColumnContract(name="x", data_type="INT"),
            ],
            output_columns=[],
        )
        m2 = _make_model(
            name="beta",
            contract_columns=[
                ColumnContract(name="y", data_type="STRING"),
            ],
            output_columns=[],
        )
        result = validate_schema_contracts_batch([m1, m2])
        alpha_v = result.violations_for_model("alpha")
        beta_v = result.violations_for_model("beta")
        assert len(alpha_v) == 1
        assert len(beta_v) == 1
        assert alpha_v[0].column_name == "x"
        assert beta_v[0].column_name == "y"

    def test_batch_mixed_modes(self):
        strict = _make_model(
            name="strict_m",
            contract_mode=SchemaContractMode.STRICT,
            contract_columns=[
                ColumnContract(name="a", data_type="INT"),
            ],
            output_columns=[],
        )
        warn = _make_model(
            name="warn_m",
            contract_mode=SchemaContractMode.WARN,
            contract_columns=[
                ColumnContract(name="b", data_type="INT"),
            ],
            output_columns=[],
        )
        disabled = _make_model(
            name="disabled_m",
            contract_mode=SchemaContractMode.DISABLED,
            contract_columns=[
                ColumnContract(name="c", data_type="INT"),
            ],
            output_columns=[],
        )
        result = validate_schema_contracts_batch([strict, warn, disabled])
        assert result.models_checked == 2
        assert len(result.violations) == 2
        model_names = {v.model_name for v in result.violations}
        assert "strict_m" in model_names
        assert "warn_m" in model_names
        assert "disabled_m" not in model_names

    def test_batch_model_not_in_columns_map_uses_output_columns(self):
        model = _make_model(
            name="m1",
            contract_columns=[
                ColumnContract(name="id", data_type="INT"),
            ],
            output_columns=["id"],
        )
        result = validate_schema_contracts_batch(
            [model],
            actual_columns_map={"other_model": ["col"]},
        )
        assert result.violations == []

    def test_batch_aggregates_models_checked(self):
        models = [
            _make_model(
                name=f"model_{i}",
                contract_mode=SchemaContractMode.STRICT,
                contract_columns=[ColumnContract(name="x", data_type="INT")],
                output_columns=["x"],
            )
            for i in range(5)
        ]
        result = validate_schema_contracts_batch(models)
        assert result.models_checked == 5
