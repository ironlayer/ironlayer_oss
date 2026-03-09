"""Unit tests for core_engine.executor.cluster_templates."""

from __future__ import annotations

import pytest
from core_engine.executor.cluster_templates import (
    ClusterTemplates,
    get_cluster_spec,
    get_cost_rate,
)

# ---------------------------------------------------------------------------
# get_cluster_spec
# ---------------------------------------------------------------------------


class TestGetClusterSpec:
    @pytest.mark.parametrize("size", ["small", "medium", "large"])
    def test_valid_sizes(self, size: str):
        spec = get_cluster_spec(size)
        assert isinstance(spec, dict)
        assert "spark_version" in spec
        assert "node_type_id" in spec
        assert "num_workers" in spec

    def test_small_spec_values(self):
        spec = get_cluster_spec("small")
        assert spec["num_workers"] == 2
        assert spec["node_type_id"] == "Standard_DS3_v2"

    def test_medium_spec_values(self):
        spec = get_cluster_spec("medium")
        assert spec["num_workers"] == 8
        assert spec["node_type_id"] == "Standard_DS4_v2"

    def test_large_spec_values(self):
        spec = get_cluster_spec("large")
        assert spec["num_workers"] == 16
        assert spec["node_type_id"] == "Standard_DS5_v2"

    def test_case_insensitive(self):
        spec_lower = get_cluster_spec("small")
        spec_upper = get_cluster_spec("SMALL")
        assert spec_lower["num_workers"] == spec_upper["num_workers"]

    def test_overrides(self):
        overrides = {"num_workers": 4, "custom_key": "custom_value"}
        spec = get_cluster_spec("small", overrides=overrides)
        assert spec["num_workers"] == 4
        assert spec["custom_key"] == "custom_value"
        # Original keys should still be present.
        assert "spark_version" in spec

    def test_overrides_do_not_mutate_template(self):
        get_cluster_spec("small", overrides={"num_workers": 99})
        spec = get_cluster_spec("small")
        assert spec["num_workers"] == 2  # Original unchanged.

    def test_invalid_size_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown cluster size"):
            get_cluster_spec("xlarge")

    def test_deep_copy(self):
        spec1 = get_cluster_spec("small")
        spec2 = get_cluster_spec("small")
        spec1["spark_conf"]["test_key"] = "test_val"
        assert "test_key" not in spec2.get("spark_conf", {})


# ---------------------------------------------------------------------------
# get_cost_rate
# ---------------------------------------------------------------------------


class TestGetCostRate:
    @pytest.mark.parametrize(
        "size,expected",
        [
            ("small", 0.0007),
            ("medium", 0.0028),
            ("large", 0.0112),
        ],
    )
    def test_returns_rates_for_all_sizes(self, size: str, expected: float):
        assert get_cost_rate(size) == expected

    def test_case_insensitive(self):
        assert get_cost_rate("SMALL") == get_cost_rate("small")

    def test_invalid_size_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown cluster size"):
            get_cost_rate("xlarge")


# ---------------------------------------------------------------------------
# ClusterTemplates (OO facade)
# ---------------------------------------------------------------------------


class TestClusterTemplatesClass:
    def test_get_spec(self):
        spec = ClusterTemplates.get_spec("medium")
        assert spec["num_workers"] == 8

    def test_get_cost_rate(self):
        rate = ClusterTemplates.get_cost_rate("small")
        assert rate == 0.0007

    def test_available_sizes(self):
        sizes = ClusterTemplates.available_sizes()
        assert sizes == ["large", "medium", "small"]
