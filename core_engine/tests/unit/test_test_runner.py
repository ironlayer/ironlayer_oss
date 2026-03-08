"""Tests for the model test runner.

Covers:
- SQL generation for each test type
- DuckDB execution: passing and failing cases for all test types
- run_all_tests returns deterministically sorted results
- Error handling for execution failures
"""

from __future__ import annotations

import pytest

from core_engine.models.model_definition import (
    ModelTestDefinition,
    ModelTestType,
    TestSeverity,
)
from core_engine.testing.test_runner import ModelTestRunner, TestResult

# ---------------------------------------------------------------------------
# SQL generation tests
# ---------------------------------------------------------------------------


class TestGenerateTestSQL:
    """Verify SQL generation for each test type."""

    def setup_method(self) -> None:
        self.runner = ModelTestRunner()

    def test_not_null_sql(self) -> None:
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id")
        sql = self.runner.generate_test_sql(test, "orders")
        assert "IS NULL" in sql
        assert "orders" in sql
        assert "id" in sql

    def test_unique_sql(self) -> None:
        test = ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="email")
        sql = self.runner.generate_test_sql(test, "users")
        assert "GROUP BY" in sql
        assert "HAVING COUNT(*) > 1" in sql
        assert "email" in sql

    def test_row_count_min_sql(self) -> None:
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=100)
        sql = self.runner.generate_test_sql(test, "events")
        assert "100" in sql
        assert "events" in sql
        assert "cnt <" in sql

    def test_row_count_max_sql(self) -> None:
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MAX, threshold=1000)
        sql = self.runner.generate_test_sql(test, "events")
        assert "1000" in sql
        assert "cnt >" in sql

    def test_accepted_values_sql(self) -> None:
        test = ModelTestDefinition(
            test_type=ModelTestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "inactive"],
        )
        sql = self.runner.generate_test_sql(test, "accounts")
        assert "NOT IN" in sql
        assert "'active'" in sql
        assert "'inactive'" in sql
        assert "IS NOT NULL" in sql

    def test_accepted_values_sorted(self) -> None:
        """Values in SQL are sorted deterministically."""
        test = ModelTestDefinition(
            test_type=ModelTestType.ACCEPTED_VALUES,
            column="status",
            values=["z_value", "a_value", "m_value"],
        )
        sql = self.runner.generate_test_sql(test, "t")
        # Sorted order should be a_value, m_value, z_value
        a_pos = sql.index("'a_value'")
        m_pos = sql.index("'m_value'")
        z_pos = sql.index("'z_value'")
        assert a_pos < m_pos < z_pos

    def test_custom_sql(self) -> None:
        test = ModelTestDefinition(
            test_type=ModelTestType.CUSTOM_SQL,
            sql="SELECT * FROM {model} WHERE amount < 0",
        )
        sql = self.runner.generate_test_sql(test, "transactions")
        assert "transactions" in sql
        assert "amount < 0" in sql
        assert "{model}" not in sql

    def test_unknown_test_type_raises(self) -> None:
        """Unknown test types raise ValueError."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="x")
        # Monkey-patch to simulate unknown type
        test.test_type = "BOGUS"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unknown test type"):
            self.runner.generate_test_sql(test, "t")


# ---------------------------------------------------------------------------
# DuckDB execution tests
# ---------------------------------------------------------------------------


class TestDuckDBExecution:
    """Run actual tests against DuckDB with real data."""

    def setup_method(self) -> None:
        import duckdb

        self.conn = duckdb.connect(":memory:")
        # Create a test table with known data.
        self.conn.execute("""
            CREATE TABLE orders (
                id INTEGER,
                email VARCHAR,
                status VARCHAR,
                amount DOUBLE
            )
        """)
        self.conn.execute("""
            INSERT INTO orders VALUES
                (1, 'alice@example.com', 'active', 100.0),
                (2, 'bob@example.com', 'active', 50.0),
                (3, 'carol@example.com', 'inactive', -10.0),
                (4, NULL, 'active', 75.0),
                (5, 'alice@example.com', 'active', 200.0)
        """)
        self.runner = ModelTestRunner()

    def teardown_method(self) -> None:
        self.conn.close()

    @pytest.mark.asyncio
    async def test_not_null_passes(self) -> None:
        """NOT_NULL on 'id' column passes (no NULLs in id)."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id")
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True
        assert result.failure_message is None

    @pytest.mark.asyncio
    async def test_not_null_fails(self) -> None:
        """NOT_NULL on 'email' fails (row 4 has NULL email)."""
        test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="email")
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False
        assert "NOT_NULL" in (result.failure_message or "")

    @pytest.mark.asyncio
    async def test_unique_passes(self) -> None:
        """UNIQUE on 'id' passes (all IDs are unique)."""
        test = ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="id")
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_unique_fails(self) -> None:
        """UNIQUE on 'email' fails (alice@example.com appears twice)."""
        test = ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="email")
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_row_count_min_passes(self) -> None:
        """ROW_COUNT_MIN(3) passes (table has 5 rows)."""
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=3)
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_row_count_min_fails(self) -> None:
        """ROW_COUNT_MIN(10) fails (table only has 5 rows)."""
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=10)
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_row_count_max_passes(self) -> None:
        """ROW_COUNT_MAX(10) passes (table has 5 rows)."""
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MAX, threshold=10)
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_row_count_max_fails(self) -> None:
        """ROW_COUNT_MAX(3) fails (table has 5 rows)."""
        test = ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MAX, threshold=3)
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_accepted_values_passes(self) -> None:
        """ACCEPTED_VALUES on status with ['active', 'inactive'] passes."""
        test = ModelTestDefinition(
            test_type=ModelTestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "inactive"],
        )
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_accepted_values_fails(self) -> None:
        """ACCEPTED_VALUES on status with only ['active'] fails (inactive exists)."""
        test = ModelTestDefinition(
            test_type=ModelTestType.ACCEPTED_VALUES,
            column="status",
            values=["active"],
        )
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False

    @pytest.mark.asyncio
    async def test_custom_sql_passes(self) -> None:
        """Custom SQL asserting no rows with id > 100 passes."""
        test = ModelTestDefinition(
            test_type=ModelTestType.CUSTOM_SQL,
            sql="SELECT * FROM {model} WHERE id > 100",
        )
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_custom_sql_fails(self) -> None:
        """Custom SQL asserting no negative amounts fails (row 3 has -10)."""
        test = ModelTestDefinition(
            test_type=ModelTestType.CUSTOM_SQL,
            sql="SELECT * FROM {model} WHERE amount < 0",
        )
        result = await self.runner.run_test(test, "orders", duckdb_conn=self.conn)
        assert result.passed is False


# ---------------------------------------------------------------------------
# run_all_tests
# ---------------------------------------------------------------------------


class TestRunAllTests:
    """Verify run_all_tests returns sorted results."""

    def setup_method(self) -> None:
        import duckdb

        self.conn = duckdb.connect(":memory:")
        self.conn.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
        self.conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        self.runner = ModelTestRunner()

    def teardown_method(self) -> None:
        self.conn.close()

    @pytest.mark.asyncio
    async def test_results_are_sorted(self) -> None:
        """Results are sorted by (test_type, column)."""
        tests = [
            ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="name"),
            ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id"),
            ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=1),
        ]
        results = await self.runner.run_all_tests("t", tests, duckdb_conn=self.conn)
        assert len(results) == 3
        # NOT_NULL < ROW_COUNT_MIN < UNIQUE alphabetically
        assert results[0].test_type == "NOT_NULL"
        assert results[1].test_type == "ROW_COUNT_MIN"
        assert results[2].test_type == "UNIQUE"

    @pytest.mark.asyncio
    async def test_all_pass(self) -> None:
        """All tests pass when data meets assertions."""
        tests = [
            ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id"),
            ModelTestDefinition(test_type=ModelTestType.UNIQUE, column="id"),
            ModelTestDefinition(test_type=ModelTestType.ROW_COUNT_MIN, threshold=1),
        ]
        results = await self.runner.run_all_tests("t", tests, duckdb_conn=self.conn)
        assert all(r.passed for r in results)

    @pytest.mark.asyncio
    async def test_empty_tests_returns_empty(self) -> None:
        """Empty test list returns empty results."""
        results = await self.runner.run_all_tests("t", [], duckdb_conn=self.conn)
        assert results == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Verify graceful handling of execution errors."""

    @pytest.mark.asyncio
    async def test_sql_error_returns_failed_result(self) -> None:
        """SQL execution errors produce a failed TestResult, not an exception."""
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            runner = ModelTestRunner()
            test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="nonexistent")
            result = await runner.run_test(test, "no_such_table", duckdb_conn=conn)
            assert result.passed is False
            assert "error" in (result.failure_message or "").lower()
        finally:
            conn.close()

    @pytest.mark.asyncio
    async def test_result_has_duration(self) -> None:
        """Results include a non-negative duration."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        try:
            runner = ModelTestRunner()
            test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id")
            result = await runner.run_test(test, "t", duckdb_conn=conn)
            assert result.duration_ms >= 0
        finally:
            conn.close()

    @pytest.mark.asyncio
    async def test_deterministic_test_id(self) -> None:
        """Same inputs produce the same test_id."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        try:
            runner = ModelTestRunner()
            test = ModelTestDefinition(test_type=ModelTestType.NOT_NULL, column="id")
            r1 = await runner.run_test(test, "t", duckdb_conn=conn)
            r2 = await runner.run_test(test, "t", duckdb_conn=conn)
            assert r1.test_id == r2.test_id
        finally:
            conn.close()
