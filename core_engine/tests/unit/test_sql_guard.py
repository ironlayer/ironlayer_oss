"""Unit tests for core_engine.parser.sql_guard."""

from __future__ import annotations

import pytest

from core_engine.parser.sql_guard import (
    DangerousOperation,
    SQLGuardConfig,
    SQLGuardViolation,
    Severity,
    UnsafeSQLError,
    assert_sql_safe,
    check_sql_safety,
)

# ---------------------------------------------------------------------------
# Safe queries should pass
# ---------------------------------------------------------------------------


class TestSafeQueries:
    def test_simple_select(self):
        violations = check_sql_safety("SELECT id, name FROM users")
        assert violations == []

    def test_select_with_join(self):
        sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
        violations = check_sql_safety(sql)
        assert violations == []

    def test_select_with_subquery(self):
        sql = "SELECT * FROM (  SELECT id, amount FROM orders WHERE amount > 100) sub WHERE sub.id > 10"
        violations = check_sql_safety(sql)
        assert violations == []

    def test_cte_with_safe_body(self):
        sql = (
            "WITH recent_orders AS ("
            "  SELECT id, customer_id, amount "
            "  FROM orders "
            "  WHERE order_date > '2024-01-01'"
            ") "
            "SELECT customer_id, SUM(amount) AS total "
            "FROM recent_orders "
            "GROUP BY customer_id"
        )
        violations = check_sql_safety(sql)
        assert violations == []

    def test_complex_window_functions(self):
        sql = (
            "SELECT "
            "  id, "
            "  customer_id, "
            "  amount, "
            "  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn, "
            "  SUM(amount) OVER (PARTITION BY customer_id) AS total "
            "FROM orders"
        )
        violations = check_sql_safety(sql)
        assert violations == []

    def test_insert_into(self):
        sql = "INSERT INTO staging.orders SELECT * FROM raw.orders"
        violations = check_sql_safety(sql)
        assert violations == []

    def test_create_or_replace_table_as_select(self):
        sql = "CREATE OR REPLACE TABLE analytics.summary AS SELECT 1 AS x"
        violations = check_sql_safety(sql)
        assert violations == []

    def test_delete_with_where_is_safe(self):
        sql = "DELETE FROM orders WHERE order_date < '2020-01-01'"
        violations = check_sql_safety(sql)
        assert violations == []


# ---------------------------------------------------------------------------
# DROP operations
# ---------------------------------------------------------------------------


class TestDropOperations:
    def test_drop_table_blocked(self):
        violations = check_sql_safety("DROP TABLE my_table")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DROP_TABLE
        assert violations[0].severity == Severity.CRITICAL

    def test_drop_table_if_exists_blocked(self):
        violations = check_sql_safety("DROP TABLE IF EXISTS my_table")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DROP_TABLE

    def test_drop_view_blocked(self):
        violations = check_sql_safety("DROP VIEW my_view")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DROP_VIEW
        assert violations[0].severity == Severity.CRITICAL

    def test_drop_schema_blocked(self):
        violations = check_sql_safety("DROP SCHEMA my_schema")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DROP_SCHEMA
        assert violations[0].severity == Severity.CRITICAL

    def test_drop_database_blocked_as_schema(self):
        violations = check_sql_safety("DROP DATABASE my_db")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DROP_SCHEMA


# ---------------------------------------------------------------------------
# TRUNCATE
# ---------------------------------------------------------------------------


class TestTruncate:
    def test_truncate_table_blocked(self):
        violations = check_sql_safety("TRUNCATE TABLE my_table")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.TRUNCATE
        assert violations[0].severity == Severity.CRITICAL


# ---------------------------------------------------------------------------
# DELETE without WHERE
# ---------------------------------------------------------------------------


class TestDeleteWithoutWhere:
    def test_delete_without_where_blocked(self):
        violations = check_sql_safety("DELETE FROM my_table")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.DELETE_WITHOUT_WHERE
        assert violations[0].severity == Severity.HIGH

    def test_delete_with_where_allowed(self):
        violations = check_sql_safety("DELETE FROM my_table WHERE status = 'expired'")
        assert violations == []


# ---------------------------------------------------------------------------
# ALTER TABLE DROP COLUMN
# ---------------------------------------------------------------------------


class TestAlterDropColumn:
    def test_alter_drop_columns_blocked(self):
        # sqlglot parses DROP COLUMNS (plural) as Alter with Drop actions.
        violations = check_sql_safety("ALTER TABLE my_table DROP COLUMNS (col1)")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.ALTER_DROP_COLUMN
        assert violations[0].severity == Severity.HIGH

    def test_alter_drop_column_command_fallback_blocked(self):
        # sqlglot may parse DROP COLUMN (singular) as a Command fallback.
        violations = check_sql_safety("ALTER TABLE my_table DROP COLUMN col1")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.ALTER_DROP_COLUMN


# ---------------------------------------------------------------------------
# GRANT / REVOKE
# ---------------------------------------------------------------------------


class TestGrantRevoke:
    def test_grant_blocked(self):
        violations = check_sql_safety("GRANT SELECT ON my_table TO user1")
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.GRANT
        assert violations[0].severity == Severity.CRITICAL

    def test_revoke_blocked(self):
        # sqlglot >=25 cannot parse REVOKE — it falls through to the
        # parse-error handler which surfaces it as RAW_EXEC / CRITICAL.
        violations = check_sql_safety("REVOKE SELECT ON my_table FROM user1")
        assert len(violations) == 1
        assert violations[0].operation in {
            DangerousOperation.REVOKE,
            DangerousOperation.RAW_EXEC,
        }
        assert violations[0].severity == Severity.CRITICAL


# ---------------------------------------------------------------------------
# INSERT OVERWRITE without partition
# ---------------------------------------------------------------------------


class TestInsertOverwrite:
    def test_insert_overwrite_without_partition_blocked(self):
        sql = "INSERT OVERWRITE TABLE my_table SELECT * FROM other_table"
        violations = check_sql_safety(sql)
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.INSERT_OVERWRITE_ALL
        assert violations[0].severity == Severity.HIGH


# ---------------------------------------------------------------------------
# Configuration overrides
# ---------------------------------------------------------------------------


class TestConfigOverrides:
    def test_maintenance_mode_allows_everything(self):
        config = SQLGuardConfig(maintenance_mode=True)
        violations = check_sql_safety("DROP TABLE foo; TRUNCATE TABLE bar", config)
        assert violations == []

    def test_disabled_allows_everything(self):
        config = SQLGuardConfig(enabled=False)
        violations = check_sql_safety("DROP TABLE foo", config)
        assert violations == []

    def test_allow_drop_allows_only_drop(self):
        config = SQLGuardConfig(allow_drop=True)

        # DROP should pass.
        violations = check_sql_safety("DROP TABLE foo", config)
        assert violations == []

        # TRUNCATE should still be blocked.
        violations = check_sql_safety("TRUNCATE TABLE foo", config)
        assert len(violations) == 1
        assert violations[0].operation == DangerousOperation.TRUNCATE

    def test_allow_truncate(self):
        config = SQLGuardConfig(allow_truncate=True)
        violations = check_sql_safety("TRUNCATE TABLE foo", config)
        assert violations == []

    def test_allow_alter_drop_column(self):
        config = SQLGuardConfig(allow_alter_drop_column=True)
        violations = check_sql_safety("ALTER TABLE foo DROP COLUMNS (bar)", config)
        assert violations == []

    def test_allow_delete_without_where(self):
        config = SQLGuardConfig(allow_delete_without_where=True)
        violations = check_sql_safety("DELETE FROM foo", config)
        assert violations == []

    def test_allowed_operations_explicit(self):
        config = SQLGuardConfig(allowed_operations={DangerousOperation.GRANT})
        violations = check_sql_safety("GRANT SELECT ON my_table TO user1", config)
        assert violations == []

        # REVOKE is NOT in the allow set.
        violations = check_sql_safety("REVOKE SELECT ON my_table FROM user1", config)
        assert len(violations) == 1


# ---------------------------------------------------------------------------
# Compound SQL (multiple statements)
# ---------------------------------------------------------------------------


class TestCompoundSQL:
    def test_multiple_violations_in_compound_sql(self):
        sql = "DROP TABLE foo; DELETE FROM bar; TRUNCATE TABLE baz"
        violations = check_sql_safety(sql)
        operations = {v.operation for v in violations}
        assert DangerousOperation.DROP_TABLE in operations
        assert DangerousOperation.DELETE_WITHOUT_WHERE in operations
        assert DangerousOperation.TRUNCATE in operations
        assert len(violations) == 3


# ---------------------------------------------------------------------------
# assert_sql_safe
# ---------------------------------------------------------------------------


class TestAssertSqlSafe:
    def test_safe_sql_does_not_raise(self):
        assert_sql_safe("SELECT 1 FROM dual")

    def test_critical_violation_raises(self):
        with pytest.raises(UnsafeSQLError) as exc_info:
            assert_sql_safe("DROP TABLE users")
        assert len(exc_info.value.violations) == 1
        assert exc_info.value.violations[0].severity == Severity.CRITICAL

    def test_non_critical_only_does_not_raise(self):
        # DELETE without WHERE is HIGH severity, not CRITICAL.
        # assert_sql_safe should log a warning but not raise.
        assert_sql_safe("DELETE FROM staging_table")

    def test_mixed_severities_raises_for_critical(self):
        sql = "DROP TABLE users; DELETE FROM orders"
        with pytest.raises(UnsafeSQLError) as exc_info:
            assert_sql_safe(sql)
        # Only the CRITICAL violation (DROP TABLE) should be in the error.
        assert all(v.severity == Severity.CRITICAL for v in exc_info.value.violations)

    def test_maintenance_mode_does_not_raise(self):
        config = SQLGuardConfig(maintenance_mode=True)
        assert_sql_safe("DROP TABLE users; TRUNCATE TABLE orders", config)


# ---------------------------------------------------------------------------
# UnsafeSQLError
# ---------------------------------------------------------------------------


class TestUnsafeSQLError:
    def test_error_message_contains_descriptions(self):
        violation = SQLGuardViolation(
            operation=DangerousOperation.DROP_TABLE,
            description="DROP TABLE detected on `users`",
            severity=Severity.CRITICAL,
        )
        err = UnsafeSQLError([violation])
        assert "DROP TABLE detected" in str(err)
        assert err.violations == [violation]

    def test_multiple_violations_in_message(self):
        v1 = SQLGuardViolation(
            operation=DangerousOperation.DROP_TABLE,
            description="DROP TABLE on `users`",
            severity=Severity.CRITICAL,
        )
        v2 = SQLGuardViolation(
            operation=DangerousOperation.TRUNCATE,
            description="TRUNCATE TABLE on `orders`",
            severity=Severity.CRITICAL,
        )
        err = UnsafeSQLError([v1, v2])
        assert "DROP TABLE on `users`" in str(err)
        assert "TRUNCATE TABLE on `orders`" in str(err)
