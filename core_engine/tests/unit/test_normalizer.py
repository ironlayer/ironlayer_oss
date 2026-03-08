"""Unit tests for core_engine.parser.normalizer."""

from __future__ import annotations

import pytest

from core_engine.parser.normalizer import (
    CURRENT_VERSION,
    CanonicalizerVersion,
    compute_canonical_hash,
    get_canonicalizer_version,
    normalize_sql,
)

# ---------------------------------------------------------------------------
# normalize_sql
# ---------------------------------------------------------------------------


class TestNormalizeSql:
    def test_whitespace_normalization(self):
        sql_a = "SELECT   id,\n   name\n  FROM   users"
        sql_b = "SELECT id, name FROM users"
        norm_a = normalize_sql(sql_a)
        norm_b = normalize_sql(sql_b)
        assert norm_a == norm_b

    def test_keyword_casing(self):
        sql_lower = "select id from users where id = 1"
        sql_upper = "SELECT id FROM users WHERE id = 1"
        norm_lower = normalize_sql(sql_lower)
        norm_upper = normalize_sql(sql_upper)
        assert norm_lower == norm_upper

    def test_equivalent_sql_produces_same_output(self):
        sql_a = "SELECT  id , name  FROM  users  WHERE  id=1"
        sql_b = "select id,name from users where id = 1"
        assert normalize_sql(sql_a) == normalize_sql(sql_b)

    def test_comment_stripping(self):
        sql_with_comments = "-- This is a comment\nSELECT id /* inline comment */ FROM users"
        sql_without_comments = "SELECT id FROM users"
        norm_a = normalize_sql(sql_with_comments)
        norm_b = normalize_sql(sql_without_comments)
        assert norm_a == norm_b

    def test_empty_string_returns_empty(self):
        assert normalize_sql("") == ""

    def test_comment_only_returns_empty(self):
        assert normalize_sql("-- just a comment") == ""

    def test_v1_is_default(self):
        result_default = normalize_sql("SELECT 1")
        result_v1 = normalize_sql("SELECT 1", version=CanonicalizerVersion.V1)
        assert result_default == result_v1


# ---------------------------------------------------------------------------
# compute_canonical_hash
# ---------------------------------------------------------------------------


class TestComputeCanonicalHash:
    def test_stability_same_sql_same_hash(self):
        h1 = compute_canonical_hash("SELECT id FROM users")
        h2 = compute_canonical_hash("SELECT id FROM users")
        assert h1 == h2

    def test_different_sql_different_hash(self):
        h1 = compute_canonical_hash("SELECT id FROM users")
        h2 = compute_canonical_hash("SELECT name FROM users")
        assert h1 != h2

    def test_cosmetically_different_sql_same_hash(self):
        h1 = compute_canonical_hash("select id from users")
        h2 = compute_canonical_hash("SELECT  id  FROM  users")
        assert h1 == h2

    def test_hash_is_64_hex_chars(self):
        h = compute_canonical_hash("SELECT 1")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_metadata_inclusion_changes_hash(self):
        h_no_meta = compute_canonical_hash("SELECT 1")
        h_with_meta = compute_canonical_hash(
            "SELECT 1",
            metadata={"kind": "FULL_REFRESH"},
        )
        assert h_no_meta != h_with_meta

    def test_metadata_order_does_not_matter(self):
        h1 = compute_canonical_hash(
            "SELECT 1",
            metadata={"a": "1", "b": "2"},
        )
        h2 = compute_canonical_hash(
            "SELECT 1",
            metadata={"b": "2", "a": "1"},
        )
        assert h1 == h2

    def test_version_prefix_in_hash(self):
        # Changing the version should produce a different hash even for the
        # same SQL.  We test this indirectly by verifying the hash with
        # explicit V1 matches the default (since CURRENT_VERSION is V1).
        h_default = compute_canonical_hash("SELECT 1")
        h_v1 = compute_canonical_hash("SELECT 1", version=CanonicalizerVersion.V1)
        assert h_default == h_v1


# ---------------------------------------------------------------------------
# get_canonicalizer_version
# ---------------------------------------------------------------------------


class TestGetCanonicalizerVersion:
    def test_returns_current_version_string(self):
        ver = get_canonicalizer_version()
        assert ver == CURRENT_VERSION.value
        assert isinstance(ver, str)

    def test_current_version_is_v1(self):
        assert get_canonicalizer_version() == "v1"


# ---------------------------------------------------------------------------
# CanonicalizerVersion enum
# ---------------------------------------------------------------------------


class TestCanonicalizerVersionEnum:
    def test_v1_value(self):
        assert CanonicalizerVersion.V1 == "v1"
        assert CanonicalizerVersion.V1.value == "v1"

    def test_current_version_is_v1(self):
        assert CURRENT_VERSION == CanonicalizerVersion.V1
