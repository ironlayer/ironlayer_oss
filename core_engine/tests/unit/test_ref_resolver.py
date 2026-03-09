"""Unit tests for core_engine.loader.ref_resolver."""

from __future__ import annotations

import pytest
from core_engine.loader.ref_resolver import (
    UnresolvedRefError,
    build_model_registry,
    extract_ref_names,
    resolve_refs,
)
from core_engine.models.model_definition import (
    ModelDefinition,
    ModelKind,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model(name: str) -> ModelDefinition:
    """Create a minimal ModelDefinition for registry-building tests."""
    return ModelDefinition(
        name=name,
        kind=ModelKind.FULL_REFRESH,
        file_path=f"models/{name.replace('.', '/')}.sql",
        raw_sql=f"-- name: {name}\n-- kind: FULL_REFRESH\n\nSELECT 1",
    )


# ---------------------------------------------------------------------------
# build_model_registry
# ---------------------------------------------------------------------------


class TestBuildModelRegistry:
    def test_creates_correct_mapping_schema_qualified(self):
        models = [_make_model("analytics.orders_daily")]
        registry = build_model_registry(models)
        # Full canonical name maps to itself.
        assert registry["analytics.orders_daily"] == "analytics.orders_daily"
        # Short name also maps to canonical.
        assert registry["orders_daily"] == "analytics.orders_daily"

    def test_creates_correct_mapping_no_schema(self):
        models = [_make_model("orders_daily")]
        registry = build_model_registry(models)
        assert registry["orders_daily"] == "orders_daily"
        # No short-name entry since there is no dot.
        assert len(registry) == 1

    def test_multiple_models(self):
        models = [
            _make_model("staging.raw_orders"),
            _make_model("analytics.orders_daily"),
        ]
        registry = build_model_registry(models)
        assert registry["raw_orders"] == "staging.raw_orders"
        assert registry["orders_daily"] == "analytics.orders_daily"
        assert registry["staging.raw_orders"] == "staging.raw_orders"
        assert registry["analytics.orders_daily"] == "analytics.orders_daily"

    def test_short_name_collision_last_wins(self):
        models = [
            _make_model("staging.orders"),
            _make_model("analytics.orders"),
        ]
        registry = build_model_registry(models)
        # The last model in the list wins for the short name.
        assert registry["orders"] == "analytics.orders"
        # But both full names are present.
        assert registry["staging.orders"] == "staging.orders"
        assert registry["analytics.orders"] == "analytics.orders"

    def test_empty_model_list(self):
        registry = build_model_registry([])
        assert registry == {}


# ---------------------------------------------------------------------------
# resolve_refs
# ---------------------------------------------------------------------------


class TestResolveRefs:
    def test_single_ref_single_quoted(self):
        sql = "SELECT * FROM {{ ref('orders') }}"
        registry = {"orders": "staging.raw_orders"}
        result = resolve_refs(sql, registry)
        assert result == "SELECT * FROM staging.raw_orders"

    def test_single_ref_double_quoted(self):
        sql = 'SELECT * FROM {{ ref("orders") }}'
        registry = {"orders": "staging.raw_orders"}
        result = resolve_refs(sql, registry)
        assert result == "SELECT * FROM staging.raw_orders"

    def test_multiple_refs(self):
        sql = "SELECT * FROM {{ ref('orders') }} JOIN {{ ref('customers') }} ON 1=1"
        registry = {
            "orders": "staging.raw_orders",
            "customers": "staging.raw_customers",
        }
        result = resolve_refs(sql, registry)
        assert "staging.raw_orders" in result
        assert "staging.raw_customers" in result

    def test_ref_with_extra_whitespace(self):
        sql = "SELECT * FROM {{  ref( 'orders' )  }}"
        registry = {"orders": "staging.raw_orders"}
        result = resolve_refs(sql, registry)
        assert result == "SELECT * FROM staging.raw_orders"

    def test_missing_ref_raises_unresolved_ref_error(self):
        sql = "SELECT * FROM {{ ref('nonexistent') }}"
        registry = {"orders": "staging.raw_orders"}
        with pytest.raises(UnresolvedRefError, match="nonexistent"):
            resolve_refs(sql, registry)

    def test_no_refs_returns_unchanged(self):
        sql = "SELECT * FROM my_table"
        result = resolve_refs(sql, {})
        assert result == sql

    def test_dotted_ref_name(self):
        sql = "SELECT * FROM {{ ref('staging.orders') }}"
        registry = {"staging.orders": "staging.orders"}
        result = resolve_refs(sql, registry)
        assert result == "SELECT * FROM staging.orders"


# ---------------------------------------------------------------------------
# extract_ref_names
# ---------------------------------------------------------------------------


class TestExtractRefNames:
    def test_extracts_all_ref_names(self):
        sql = "SELECT * FROM {{ ref('orders') }} JOIN {{ ref('customers') }} ON 1=1"
        names = extract_ref_names(sql)
        assert names == ["orders", "customers"]

    def test_deduplicates(self):
        sql = "SELECT * FROM {{ ref('orders') }} UNION ALL SELECT * FROM {{ ref('orders') }}"
        names = extract_ref_names(sql)
        assert names == ["orders"]

    def test_preserves_first_occurrence_order(self):
        sql = "SELECT * FROM {{ ref('b_model') }} JOIN {{ ref('a_model') }} ON 1=1"
        names = extract_ref_names(sql)
        assert names == ["b_model", "a_model"]

    def test_no_refs_returns_empty(self):
        sql = "SELECT 1 FROM my_table"
        names = extract_ref_names(sql)
        assert names == []

    def test_double_quoted_ref(self):
        sql = 'SELECT * FROM {{ ref("my_model") }}'
        names = extract_ref_names(sql)
        assert names == ["my_model"]

    def test_mixed_quote_styles(self):
        sql = "SELECT * FROM {{ ref('model_a') }} JOIN {{ ref(\"model_b\") }} ON 1=1"
        names = extract_ref_names(sql)
        assert names == ["model_a", "model_b"]
