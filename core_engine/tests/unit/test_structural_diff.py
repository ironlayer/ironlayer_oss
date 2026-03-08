"""Unit tests for core_engine.diff.structural_diff."""

from __future__ import annotations

import pytest

from core_engine.diff.structural_diff import compute_structural_diff
from core_engine.models.diff import DiffResult

# ---------------------------------------------------------------------------
# compute_structural_diff
# ---------------------------------------------------------------------------


class TestComputeStructuralDiff:
    def test_added_models(self):
        previous: dict[str, str] = {}
        current = {"model_a": "hash_a", "model_b": "hash_b"}
        result = compute_structural_diff(previous, current)
        assert result.added_models == ["model_a", "model_b"]
        assert result.removed_models == []
        assert result.modified_models == []

    def test_removed_models(self):
        previous = {"model_a": "hash_a", "model_b": "hash_b"}
        current: dict[str, str] = {}
        result = compute_structural_diff(previous, current)
        assert result.added_models == []
        assert result.removed_models == ["model_a", "model_b"]
        assert result.modified_models == []

    def test_modified_models(self):
        previous = {"model_a": "hash_old"}
        current = {"model_a": "hash_new"}
        result = compute_structural_diff(previous, current)
        assert result.added_models == []
        assert result.removed_models == []
        assert result.modified_models == ["model_a"]
        assert "model_a" in result.hash_changes
        assert result.hash_changes["model_a"].old_hash == "hash_old"
        assert result.hash_changes["model_a"].new_hash == "hash_new"

    def test_unchanged_models(self):
        previous = {"model_a": "same_hash"}
        current = {"model_a": "same_hash"}
        result = compute_structural_diff(previous, current)
        assert result.added_models == []
        assert result.removed_models == []
        assert result.modified_models == []
        assert result.hash_changes == {}

    def test_empty_snapshots(self):
        result = compute_structural_diff({}, {})
        assert result.added_models == []
        assert result.removed_models == []
        assert result.modified_models == []

    def test_mixed_changes(self):
        previous = {
            "unchanged": "hash_same",
            "modified": "hash_old",
            "removed": "hash_rm",
        }
        current = {
            "unchanged": "hash_same",
            "modified": "hash_new",
            "added": "hash_add",
        }
        result = compute_structural_diff(previous, current)
        assert result.added_models == ["added"]
        assert result.removed_models == ["removed"]
        assert result.modified_models == ["modified"]
        assert result.hash_changes["modified"].old_hash == "hash_old"
        assert result.hash_changes["modified"].new_hash == "hash_new"

    def test_sorted_output_determinism(self):
        previous = {"z_model": "h1", "a_model": "h2", "m_model": "h3"}
        current = {
            "z_model": "h1_changed",
            "a_model": "h2_changed",
            "m_model": "h3_changed",
        }
        result = compute_structural_diff(previous, current)
        assert result.modified_models == ["a_model", "m_model", "z_model"]

    def test_added_sorted(self):
        current = {"z": "h1", "a": "h2", "m": "h3"}
        result = compute_structural_diff({}, current)
        assert result.added_models == ["a", "m", "z"]

    def test_removed_sorted(self):
        previous = {"z": "h1", "a": "h2", "m": "h3"}
        result = compute_structural_diff(previous, {})
        assert result.removed_models == ["a", "m", "z"]

    def test_result_type(self):
        result = compute_structural_diff({}, {})
        assert isinstance(result, DiffResult)

    def test_hash_changes_only_for_modified(self):
        previous = {"a": "h1", "b": "h2"}
        current = {"a": "h1_new", "c": "h3"}
        result = compute_structural_diff(previous, current)
        # hash_changes should only contain "a" (modified), not "b" (removed) or "c" (added).
        assert set(result.hash_changes.keys()) == {"a"}
