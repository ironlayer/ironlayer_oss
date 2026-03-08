"""SQL injection prevention tests.

Validates that the test runner's identifier validation rejects unsafe SQL
identifiers (preventing injection through table/column names), validates
accepted values for SQL embedding, and that the repository's dialect-aware
upsert routes to the correct dialect backend.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core_engine.models.model_definition import ModelTestDefinition, ModelTestType
from core_engine.testing.test_runner import (
    ModelTestRunner,
    _validate_accepted_value,
    _validate_identifier,
)

# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------


class TestIdentifierValidation:
    """Verify SQL identifiers are validated against the allowlist regex."""

    def test_simple_valid_name(self) -> None:
        """Plain alphanumeric identifiers pass validation."""
        assert _validate_identifier("valid_table_name") == "valid_table_name"

    def test_leading_underscore(self) -> None:
        """Identifiers starting with underscore are valid."""
        assert _validate_identifier("_staging") == "_staging"

    def test_mixed_case_and_digits(self) -> None:
        """Mixed case and digits after first character are valid."""
        assert _validate_identifier("Orders2024") == "Orders2024"

    def test_schema_qualified_name(self) -> None:
        """Dotted names (schema.table.column) are allowed for qualified names."""
        assert _validate_identifier("staging.orders.order_id") == "staging.orders.order_id"

    def test_single_dot_qualified(self) -> None:
        """Two-part qualified names pass."""
        assert _validate_identifier("analytics.revenue") == "analytics.revenue"

    def test_semicolon_injection_raises(self) -> None:
        """Semicolons used for statement termination are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table; DROP TABLE users")

    def test_newline_injection_raises(self) -> None:
        """Newlines used to inject additional statements are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table\nSELECT")

    def test_space_injection_raises(self) -> None:
        """Spaces are not valid in identifiers."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table name")

    def test_dash_injection_raises(self) -> None:
        """SQL comment marker (--) is rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table--comment")

    def test_single_quote_injection_raises(self) -> None:
        """Single quotes for string escaping are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table'OR'1'='1")

    def test_parenthesis_injection_raises(self) -> None:
        """Parentheses are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table()")

    def test_empty_string_raises(self) -> None:
        """Empty identifiers are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("")

    def test_leading_digit_raises(self) -> None:
        """Identifiers starting with a digit are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("1table")

    def test_union_injection_raises(self) -> None:
        """UNION-based injection attempts are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("t UNION SELECT * FROM secrets")

    def test_backslash_injection_raises(self) -> None:
        """Backslash escape sequences are rejected."""
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("table\\0")


# ---------------------------------------------------------------------------
# Accepted value validation
# ---------------------------------------------------------------------------


class TestAcceptedValueValidation:
    """Verify that ACCEPTED_VALUES entries are safe for SQL embedding."""

    def test_simple_string_passes(self) -> None:
        """Normal string values are accepted."""
        assert _validate_accepted_value("active") == "active"

    def test_string_with_digits_passes(self) -> None:
        """Alphanumeric values are accepted."""
        assert _validate_accepted_value("status_2024") == "status_2024"

    def test_single_quote_raises(self) -> None:
        """Values containing single quotes are rejected (SQL string escape)."""
        with pytest.raises(ValueError, match="Unsafe accepted value"):
            _validate_accepted_value("it's")

    def test_backslash_raises(self) -> None:
        """Values containing backslashes are rejected."""
        with pytest.raises(ValueError, match="Unsafe accepted value"):
            _validate_accepted_value("value\\with\\backslash")

    def test_semicolon_raises(self) -> None:
        """Values containing semicolons are rejected."""
        with pytest.raises(ValueError, match="Unsafe accepted value"):
            _validate_accepted_value("value;DROP TABLE")

    def test_combined_injection_raises(self) -> None:
        """Values combining multiple injection vectors are rejected."""
        with pytest.raises(ValueError, match="Unsafe accepted value"):
            _validate_accepted_value("'; DROP TABLE users; --")

    def test_empty_string_passes(self) -> None:
        """Empty string is technically safe (no injection chars)."""
        assert _validate_accepted_value("") == ""

    def test_spaces_pass(self) -> None:
        """Spaces are allowed in accepted values (they are string literals)."""
        assert _validate_accepted_value("north america") == "north america"


# ---------------------------------------------------------------------------
# SQL generation rejects unsafe inputs
# ---------------------------------------------------------------------------


class TestGenerateTestSQLInjectionPrevention:
    """Verify that generate_test_sql rejects unsafe model names and columns."""

    def setup_method(self) -> None:
        self.runner = ModelTestRunner()

    def test_unsafe_model_name_rejected(self) -> None:
        """Model names containing injection vectors are rejected during SQL gen."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id")
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            self.runner.generate_test_sql(test, "orders; DROP TABLE users")

    def test_unsafe_column_name_rejected(self) -> None:
        """Column names containing injection vectors are rejected."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id; DROP TABLE")
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            self.runner.generate_test_sql(test, "orders")

    def test_unsafe_accepted_values_rejected(self) -> None:
        """Accepted values containing injection vectors are rejected."""
        test = ModelTestDefinition(
            test_type=ModelTestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "'; DELETE FROM users; --"],
        )
        with pytest.raises(ValueError, match="Unsafe accepted value"):
            self.runner.generate_test_sql(test, "orders")

    def test_safe_inputs_produce_valid_sql(self) -> None:
        """Safe identifiers produce correct SQL without any injection."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="user_id")
        sql = self.runner.generate_test_sql(test, "staging.orders")
        assert "staging.orders" in sql
        assert "user_id" in sql
        assert "IS NULL" in sql
        # No injection artifacts should appear
        assert "DROP" not in sql
        assert "DELETE" not in sql

    def test_newline_in_unique_column_rejected(self) -> None:
        """Newline injection in UNIQUE test column is rejected."""
        test = ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="id\nUNION SELECT")
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            self.runner.generate_test_sql(test, "t")

    def test_row_count_min_safe_model_name(self) -> None:
        """ROW_COUNT_MIN validates the model name even though there is no column."""
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=1)
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            self.runner.generate_test_sql(test, "t; SELECT 1")


# ---------------------------------------------------------------------------
# Dialect upsert
# ---------------------------------------------------------------------------


class TestDialectUpsert:
    """Verify _dialect_upsert routes to the correct dialect backend."""

    @pytest.mark.asyncio
    async def test_sqlite_dialect_uses_sqlite_insert(self) -> None:
        """When the session dialect is sqlite, sqlite_insert is used."""
        from core_engine.state.repository import _dialect_upsert

        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "sqlite"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind
        mock_session.execute = AsyncMock(return_value=MagicMock())

        # Use a real SQLAlchemy table for the sqlite insert to work
        from sqlalchemy import Column, Integer, MetaData, String, Table

        metadata = MetaData()
        test_table = Table(
            "test_table",
            metadata,
            Column("id", String, primary_key=True),
            Column("data", String),
        )

        values = {"id": "row1", "data": "value1"}
        index_elements = ["id"]
        update_columns = ["data"]

        await _dialect_upsert(mock_session, test_table, values, index_elements, update_columns)

        # Verify that execute was called (meaning the sqlite path ran)
        mock_session.execute.assert_awaited_once()
        # Verify the statement passed is a SQLite INSERT
        executed_stmt = mock_session.execute.call_args[0][0]
        stmt_str = str(executed_stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT" in stmt_str

    @pytest.mark.asyncio
    async def test_postgresql_dialect_uses_pg_insert(self) -> None:
        """When the session dialect is postgresql, pg_insert is used.

        We verify this by checking that session.execute is called with a
        statement produced via the PostgreSQL dialect path (the code uses
        stmt.excluded to build the set_ dict, which is specific to
        sqlalchemy.dialects.postgresql.insert).
        """
        from core_engine.state.repository import _dialect_upsert

        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind
        mock_session.execute = AsyncMock(return_value=MagicMock())

        # Use a real-enough SQLAlchemy table for the postgresql insert to work
        from sqlalchemy import Column, Integer, MetaData, String, Table

        metadata = MetaData()
        test_table = Table(
            "test_table",
            metadata,
            Column("id", String, primary_key=True),
            Column("data", String),
        )

        values = {"id": "row1", "data": "value1"}
        index_elements = ["id"]
        update_columns = ["data"]

        await _dialect_upsert(mock_session, test_table, values, index_elements, update_columns)

        # Verify that execute was called (meaning the pg path ran successfully)
        mock_session.execute.assert_awaited_once()
        # Verify the statement passed to execute is a PostgreSQL INSERT
        executed_stmt = mock_session.execute.call_args[0][0]
        # The compiled statement should contain ON CONFLICT (PostgreSQL specific)
        stmt_str = str(executed_stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT" in stmt_str

    @pytest.mark.asyncio
    async def test_unknown_dialect_falls_through_to_sqlite(self) -> None:
        """When the dialect is not postgresql, the sqlite (else) path is used as fallback."""
        from core_engine.state.repository import _dialect_upsert

        mock_session = AsyncMock()
        mock_bind = MagicMock()
        mock_dialect = MagicMock()
        mock_dialect.name = "mysql"  # Neither postgresql nor sqlite
        mock_bind.dialect = mock_dialect
        mock_session.get_bind.return_value = mock_bind
        mock_session.execute = AsyncMock(return_value=MagicMock())

        from sqlalchemy import Column, MetaData, String, Table

        metadata = MetaData()
        test_table = Table(
            "test_table",
            metadata,
            Column("id", String, primary_key=True),
            Column("data", String),
        )

        values = {"id": "row1", "data": "value1"}
        index_elements = ["id"]
        update_columns = ["data"]

        await _dialect_upsert(mock_session, test_table, values, index_elements, update_columns)

        # Should use the else (sqlite) path since dialect is not postgresql
        mock_session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# LIKE metacharacter escaping
# ---------------------------------------------------------------------------


class TestEscapeLike:
    """Verify _escape_like neutralises SQL LIKE wildcards."""

    def test_percent_escaped(self) -> None:
        from core_engine.state.repository import _escape_like

        assert _escape_like("100%") == "100\\%"

    def test_underscore_escaped(self) -> None:
        from core_engine.state.repository import _escape_like

        assert _escape_like("user_name") == "user\\_name"

    def test_backslash_escaped(self) -> None:
        from core_engine.state.repository import _escape_like

        assert _escape_like("path\\to") == "path\\\\to"

    def test_plain_string_unchanged(self) -> None:
        from core_engine.state.repository import _escape_like

        assert _escape_like("hello world") == "hello world"

    def test_combined_metacharacters(self) -> None:
        from core_engine.state.repository import _escape_like

        result = _escape_like("50%_discount\\sale")
        assert result == "50\\%\\_discount\\\\sale"

    def test_empty_string(self) -> None:
        from core_engine.state.repository import _escape_like

        assert _escape_like("") == ""
