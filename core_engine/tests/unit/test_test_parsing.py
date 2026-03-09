"""Tests for declarative test declaration parsing.

Covers:
- Individual test type parsing
- Multiple test parsing in one declaration
- Severity suffix (@WARN)
- Error cases: invalid types, missing args
- Full header integration: parsing SQL file with tests header line
"""

from __future__ import annotations

import pytest
from core_engine.loader.model_loader import (
    HeaderParseError,
    _parse_test_declarations,
    parse_yaml_header,
)
from core_engine.models.model_definition import (
    ModelTestDefinition,
    ModelTestType,
    TestSeverity,
)

# ---------------------------------------------------------------------------
# Individual test type parsing
# ---------------------------------------------------------------------------


class TestParseNotNull:
    """Parse not_null declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("not_null(id)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.NOT_NULL
        assert result[0].column == "id"
        assert result[0].severity == TestSeverity.BLOCK

    def test_with_spaces(self) -> None:
        result = _parse_test_declarations("  not_null( user_id )  ")
        assert len(result) == 1
        assert result[0].column == "user_id"


class TestParseUnique:
    """Parse unique declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("unique(email)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.UNIQUE
        assert result[0].column == "email"


class TestParseRowCountMin:
    """Parse row_count_min declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("row_count_min(100)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.ROW_COUNT_MIN
        assert result[0].threshold == 100

    def test_non_integer_raises(self) -> None:
        with pytest.raises(HeaderParseError, match="integer"):
            _parse_test_declarations("row_count_min(abc)")


class TestParseRowCountMax:
    """Parse row_count_max declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("row_count_max(5000)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.ROW_COUNT_MAX
        assert result[0].threshold == 5000


class TestParseAcceptedValues:
    """Parse accepted_values declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("accepted_values(status:active|inactive)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.ACCEPTED_VALUES
        assert result[0].column == "status"
        assert result[0].values == ["active", "inactive"]

    def test_three_values(self) -> None:
        result = _parse_test_declarations("accepted_values(status:active|inactive|pending)")
        assert len(result) == 1
        assert result[0].values == ["active", "inactive", "pending"]

    def test_values_sorted(self) -> None:
        """Parsed values are sorted for deterministic output."""
        result = _parse_test_declarations("accepted_values(level:high|low|medium)")
        assert result[0].values == ["high", "low", "medium"]

    def test_missing_colon_raises(self) -> None:
        with pytest.raises(HeaderParseError, match="column:val1"):
            _parse_test_declarations("accepted_values(active|inactive)")

    def test_empty_values_raises(self) -> None:
        with pytest.raises(HeaderParseError, match="No values"):
            _parse_test_declarations("accepted_values(col:)")


class TestParseCustomSQL:
    """Parse custom_sql declarations."""

    def test_simple(self) -> None:
        result = _parse_test_declarations("custom_sql(SELECT * FROM {model} WHERE x < 0)")
        assert len(result) == 1
        assert result[0].test_type == ModelTestType.CUSTOM_SQL
        assert result[0].sql == "SELECT * FROM {model} WHERE x < 0"

    def test_preserves_sql_content(self) -> None:
        """SQL with commas inside parens is preserved."""
        result = _parse_test_declarations("custom_sql(SELECT a, b FROM {model} WHERE a IN (1, 2, 3))")
        assert len(result) == 1
        assert "a, b" in (result[0].sql or "")
        assert "1, 2, 3" in (result[0].sql or "")

    def test_nested_parens(self) -> None:
        """Parentheses inside custom_sql are handled."""
        result = _parse_test_declarations(
            "custom_sql(SELECT COUNT(*) AS cnt FROM (SELECT * FROM {model}) sub WHERE cnt = 0)"
        )
        assert len(result) == 1
        assert "COUNT(*)" in (result[0].sql or "")


# ---------------------------------------------------------------------------
# Multiple test parsing
# ---------------------------------------------------------------------------


class TestMultipleParsing:
    """Parse multiple test declarations from one line."""

    def test_three_tests(self) -> None:
        result = _parse_test_declarations("not_null(id), unique(email), row_count_min(1)")
        assert len(result) == 3
        types = {t.test_type for t in result}
        assert types == {
            ModelTestType.NOT_NULL,
            ModelTestType.UNIQUE,
            ModelTestType.ROW_COUNT_MIN,
        }

    def test_deterministic_order(self) -> None:
        """Parsed tests are sorted deterministically."""
        result = _parse_test_declarations("unique(email), not_null(id), row_count_min(1)")
        assert result[0].test_type == ModelTestType.NOT_NULL
        assert result[1].test_type == ModelTestType.ROW_COUNT_MIN
        assert result[2].test_type == ModelTestType.UNIQUE

    def test_with_custom_sql_containing_commas(self) -> None:
        """Custom SQL with commas does not split incorrectly."""
        result = _parse_test_declarations("not_null(id), custom_sql(SELECT a, b FROM {model} WHERE c = 1)")
        assert len(result) == 2
        custom = [t for t in result if t.test_type == ModelTestType.CUSTOM_SQL]
        assert len(custom) == 1
        assert "a, b" in (custom[0].sql or "")


# ---------------------------------------------------------------------------
# Severity suffix
# ---------------------------------------------------------------------------


class TestSeveritySuffix:
    """Parse @WARN severity suffix."""

    def test_warn_suffix(self) -> None:
        result = _parse_test_declarations("not_null(id)@WARN")
        assert len(result) == 1
        assert result[0].severity == TestSeverity.WARN

    def test_block_is_default(self) -> None:
        result = _parse_test_declarations("not_null(id)")
        assert result[0].severity == TestSeverity.BLOCK

    def test_invalid_severity_raises(self) -> None:
        with pytest.raises(HeaderParseError, match="severity"):
            _parse_test_declarations("not_null(id)@CRITICAL")

    def test_mixed_severities(self) -> None:
        result = _parse_test_declarations("not_null(id)@WARN, unique(email)")
        warn_test = [t for t in result if t.test_type == ModelTestType.NOT_NULL][0]
        block_test = [t for t in result if t.test_type == ModelTestType.UNIQUE][0]
        assert warn_test.severity == TestSeverity.WARN
        assert block_test.severity == TestSeverity.BLOCK


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    """Invalid declarations raise HeaderParseError."""

    def test_unknown_test_type(self) -> None:
        with pytest.raises(HeaderParseError, match="Unknown test type"):
            _parse_test_declarations("bogus_test(id)")

    def test_missing_parens(self) -> None:
        with pytest.raises(HeaderParseError, match="test_type\\(arg\\)"):
            _parse_test_declarations("not_null id")

    def test_not_null_missing_column(self) -> None:
        with pytest.raises(HeaderParseError, match="column name"):
            _parse_test_declarations("not_null()")

    def test_row_count_min_missing_threshold(self) -> None:
        with pytest.raises(HeaderParseError, match="numeric threshold"):
            _parse_test_declarations("row_count_min()")

    def test_custom_sql_empty(self) -> None:
        with pytest.raises(HeaderParseError, match="SQL statement"):
            _parse_test_declarations("custom_sql()")

    def test_empty_string_returns_empty(self) -> None:
        result = _parse_test_declarations("")
        assert result == []

    def test_whitespace_only_returns_empty(self) -> None:
        result = _parse_test_declarations("   ")
        assert result == []


# ---------------------------------------------------------------------------
# Full header integration
# ---------------------------------------------------------------------------


class TestHeaderIntegration:
    """Parse a full SQL file header that includes a tests declaration."""

    def test_header_with_tests(self) -> None:
        sql = (
            "-- name: analytics.orders_daily\n"
            "-- kind: FULL_REFRESH\n"
            "-- tests: not_null(id), unique(id), row_count_min(1)\n"
            "\n"
            "SELECT * FROM raw.orders\n"
        )
        header = parse_yaml_header(sql)
        assert "tests" in header
        tests = header["tests"]
        assert len(tests) == 3
        assert all(isinstance(t, ModelTestDefinition) for t in tests)

    def test_header_without_tests(self) -> None:
        sql = "-- name: analytics.orders_daily\n-- kind: FULL_REFRESH\n\nSELECT * FROM raw.orders\n"
        header = parse_yaml_header(sql)
        assert "tests" not in header

    def test_header_with_accepted_values(self) -> None:
        sql = (
            "-- name: staging.users\n"
            "-- kind: FULL_REFRESH\n"
            "-- tests: accepted_values(status:active|inactive|suspended)\n"
            "\n"
            "SELECT * FROM raw.users\n"
        )
        header = parse_yaml_header(sql)
        tests = header["tests"]
        assert len(tests) == 1
        assert tests[0].test_type == ModelTestType.ACCEPTED_VALUES
        assert tests[0].values == ["active", "inactive", "suspended"]

    def test_header_with_severity(self) -> None:
        sql = (
            "-- name: staging.users\n"
            "-- kind: FULL_REFRESH\n"
            "-- tests: not_null(id), unique(email)@WARN\n"
            "\n"
            "SELECT * FROM raw.users\n"
        )
        header = parse_yaml_header(sql)
        tests = header["tests"]
        assert len(tests) == 2
        not_null = [t for t in tests if t.test_type == ModelTestType.NOT_NULL][0]
        unique = [t for t in tests if t.test_type == ModelTestType.UNIQUE][0]
        assert not_null.severity == TestSeverity.BLOCK
        assert unique.severity == TestSeverity.WARN
