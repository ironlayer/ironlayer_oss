"""Unit tests for core_engine.diff.ast_diff."""

from __future__ import annotations

import pytest

from core_engine.diff.ast_diff import (
    compute_ast_diff,
    extract_changed_columns,
    is_cosmetic_only,
)
from core_engine.models.diff import ChangeType

# ---------------------------------------------------------------------------
# compute_ast_diff
# ---------------------------------------------------------------------------


class TestComputeAstDiff:
    def test_added_column(self):
        old_sql = "SELECT id, name FROM users"
        new_sql = "SELECT id, name, email FROM users"
        detail = compute_ast_diff(old_sql, new_sql)
        assert detail.change_type == ChangeType.MODIFIED
        assert "email" in detail.added_columns

    def test_removed_column(self):
        old_sql = "SELECT id, name, email FROM users"
        new_sql = "SELECT id, name FROM users"
        detail = compute_ast_diff(old_sql, new_sql)
        assert detail.change_type == ChangeType.MODIFIED
        assert "email" in detail.removed_columns

    def test_modified_expression(self):
        old_sql = "SELECT id, UPPER(name) AS name FROM users"
        new_sql = "SELECT id, LOWER(name) AS name FROM users"
        detail = compute_ast_diff(old_sql, new_sql)
        assert detail.change_type == ChangeType.MODIFIED

    def test_unchanged_sql(self):
        sql = "SELECT id, name FROM users"
        detail = compute_ast_diff(sql, sql)
        # Identical SQL should be cosmetic or no change.
        assert detail.change_type in (ChangeType.COSMETIC_ONLY, ChangeType.NO_CHANGE)

    def test_cosmetic_change_detected(self):
        old_sql = "select id, name from users"
        new_sql = "SELECT  id,  name  FROM  users"
        detail = compute_ast_diff(old_sql, new_sql)
        assert detail.change_type == ChangeType.COSMETIC_ONLY

    def test_added_table_is_semantic(self):
        old_sql = "SELECT * FROM users"
        new_sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        detail = compute_ast_diff(old_sql, new_sql)
        assert detail.change_type == ChangeType.MODIFIED


# ---------------------------------------------------------------------------
# is_cosmetic_only
# ---------------------------------------------------------------------------


class TestIsCosmeticOnly:
    def test_whitespace_only_true(self):
        old = "SELECT   id  FROM   users"
        new = "SELECT id FROM users"
        assert is_cosmetic_only(old, new) is True

    def test_casing_only_true(self):
        old = "select id from users"
        new = "SELECT id FROM users"
        assert is_cosmetic_only(old, new) is True

    def test_semantic_change_false(self):
        old = "SELECT id FROM users"
        new = "SELECT id, name FROM users"
        assert is_cosmetic_only(old, new) is False

    def test_identical_true(self):
        sql = "SELECT id FROM users"
        assert is_cosmetic_only(sql, sql) is True

    def test_invalid_sql_returns_false(self):
        # If either SQL is unparseable, should return False.
        assert is_cosmetic_only("GARBAGE", "SELECT 1") is False


# ---------------------------------------------------------------------------
# extract_changed_columns
# ---------------------------------------------------------------------------


class TestExtractChangedColumns:
    def test_identifies_added_column(self):
        old = "SELECT id, name FROM users"
        new = "SELECT id, name, email FROM users"
        changed = extract_changed_columns(old, new)
        assert "email" in changed

    def test_identifies_removed_column(self):
        old = "SELECT id, name, email FROM users"
        new = "SELECT id, name FROM users"
        changed = extract_changed_columns(old, new)
        assert "email" in changed

    def test_identifies_modified_column(self):
        old = "SELECT id, UPPER(name) AS formatted_name FROM users"
        new = "SELECT id, LOWER(name) AS formatted_name FROM users"
        changed = extract_changed_columns(old, new)
        assert "formatted_name" in changed

    def test_no_changes_returns_empty(self):
        sql = "SELECT id, name FROM users"
        changed = extract_changed_columns(sql, sql)
        assert changed == []

    def test_sorted_output(self):
        old = "SELECT id FROM users"
        new = "SELECT id, z_col, a_col FROM users"
        changed = extract_changed_columns(old, new)
        assert changed == sorted(changed)

    def test_invalid_sql_returns_empty(self):
        # Empty string genuinely fails to parse (sqlglot is permissive with
        # keyword-like identifiers such as "GARBAGE SQL").
        changed = extract_changed_columns("", "SELECT 1")
        assert changed == []
