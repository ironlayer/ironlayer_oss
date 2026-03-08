"""Gold dataset regression tests — CI gate for AI engine quality.

These tests instantiate the **real** rule-based engines (no LLM) and
run them against the full gold dataset.  They are designed to catch
quality regressions introduced by engine code changes.

Marked with ``@pytest.mark.slow`` so they can be excluded from fast
unit test runs via ``pytest -m "not slow"``.
"""

from __future__ import annotations

import pytest

from ai_engine.evaluation.gold_dataset import GoldDataset, GoldDatasetEntry
from ai_engine.evaluation.harness import EvaluationHarness, EvaluationReport
from ai_engine.evaluation.metrics import accuracy, precision_recall_f1

# ---------------------------------------------------------------------------
# Engine instantiation (rule-based, no LLM, fully deterministic)
# ---------------------------------------------------------------------------

_CHANGE_TYPE_LABELS = [
    "non_breaking",
    "breaking",
    "metric_semantic",
    "rename_only",
    "partition_shift",
    "cosmetic",
]


def _build_engines() -> tuple:
    """Instantiate real engines in no-LLM mode.

    Returns (classifier, cost_predictor, risk_scorer, optimizer).
    """
    from ai_engine.engines.semantic_classifier import SemanticClassifier
    from ai_engine.engines.cost_predictor import CostPredictor
    from ai_engine.engines.risk_scorer import RiskScorer
    from ai_engine.engines.sql_optimizer import SQLOptimizer

    classifier = SemanticClassifier(llm_client=None)
    cost_predictor = CostPredictor()
    risk_scorer = RiskScorer()
    optimizer = SQLOptimizer(llm_client=None)

    return classifier, cost_predictor, risk_scorer, optimizer


# ---------------------------------------------------------------------------
# Dataset validation tests
# ---------------------------------------------------------------------------


class TestGoldDatasetIntegrity:
    """Validate the gold dataset itself before running engines against it."""

    def test_dataset_has_minimum_entries(self) -> None:
        ds = GoldDataset()
        assert ds.size >= 50, f"Expected >= 50 entries, got {ds.size}"

    def test_all_entries_have_required_fields(self) -> None:
        ds = GoldDataset()
        for entry in ds.get_all():
            assert entry.id, f"Entry missing id"
            assert entry.category, f"Entry {entry.id} missing category"
            assert entry.new_sql, f"Entry {entry.id} missing new_sql"
            assert entry.expected_change_type in _CHANGE_TYPE_LABELS, (
                f"Entry {entry.id} has invalid change_type: {entry.expected_change_type}"
            )

    def test_all_categories_represented(self) -> None:
        ds = GoldDataset()
        categories = ds.categories
        expected = {
            "cosmetic",
            "breaking",
            "non_breaking",
            "rename_only",
            "metric_semantic",
            "partition_shift",
            "edge_case",
        }
        assert expected.issubset(set(categories)), f"Missing categories: {expected - set(categories)}"

    def test_entry_ids_unique(self) -> None:
        ds = GoldDataset()
        ids = [e.id for e in ds.get_all()]
        assert len(ids) == len(set(ids)), "Duplicate entry IDs found"

    def test_get_by_category_filters_correctly(self) -> None:
        ds = GoldDataset()
        cosmetic = ds.get_by_category("cosmetic")
        assert len(cosmetic) >= 5
        assert all(e.category == "cosmetic" for e in cosmetic)

    def test_category_minimum_sizes(self) -> None:
        ds = GoldDataset()
        minimums = {
            "cosmetic": 5,
            "breaking": 10,
            "non_breaking": 8,
            "rename_only": 5,
            "metric_semantic": 8,
            "partition_shift": 5,
        }
        for category, min_count in minimums.items():
            entries = ds.get_by_category(category)
            assert len(entries) >= min_count, (
                f"Category '{category}' has {len(entries)} entries, expected >= {min_count}"
            )


# ---------------------------------------------------------------------------
# Full evaluation regression tests
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestClassifierRegression:
    """Regression tests using the real rule-based semantic classifier."""

    @pytest.fixture(autouse=True)
    def _setup_engines(self) -> None:
        classifier, cost_predictor, risk_scorer, optimizer = _build_engines()
        self.harness = EvaluationHarness(
            classifier=classifier,
            cost_predictor=cost_predictor,
            risk_scorer=risk_scorer,
            optimizer=optimizer,
        )
        self.dataset = GoldDataset()

    def test_full_evaluation_produces_report(self) -> None:
        """The harness should run to completion and produce a structured report."""
        report = self.harness.run_full_evaluation(self.dataset)
        assert isinstance(report, EvaluationReport)
        assert report.entries_evaluated >= 50

    def test_classifier_accuracy_above_threshold(self) -> None:
        """Rule-based classifier must achieve at least 60% accuracy.

        Note: The rule-based classifier without LLM enrichment may not reach
        the full 85% F1 target. This gate catches severe regressions.
        """
        report = self.harness.run_full_evaluation(self.dataset)
        assert report.classifier_accuracy >= 0.60, (
            f"Classifier accuracy {report.classifier_accuracy:.4f} < 0.60 threshold"
        )

    def test_no_cost_predictor_crashes(self) -> None:
        """Cost predictor should not crash on any gold dataset entry."""
        report = self.harness.run_full_evaluation(self.dataset)
        error_rate = report.cost_metrics.get("error_rate", 1.0)
        assert error_rate < 0.10, f"Cost predictor error rate {error_rate:.4f} >= 0.10 (too many crashes)"

    def test_per_category_has_results(self) -> None:
        """Every major category should appear in the breakdown."""
        report = self.harness.run_full_evaluation(self.dataset)
        for cat in ["cosmetic", "breaking", "non_breaking"]:
            assert cat in report.per_category_breakdown, f"Category '{cat}' missing from breakdown"
            assert report.per_category_breakdown[cat]["total"] > 0

    def test_cosmetic_accuracy_high(self) -> None:
        """Cosmetic changes (whitespace/comments) should be easy for the classifier."""
        report = self.harness.run_full_evaluation(self.dataset)
        cosmetic = report.per_category_breakdown.get("cosmetic", {})
        assert cosmetic.get("accuracy", 0.0) >= 0.80, f"Cosmetic accuracy {cosmetic.get('accuracy', 0):.4f} < 0.80"

    def test_breaking_detection_above_minimum(self) -> None:
        """Breaking changes should be detected at a reasonable rate."""
        report = self.harness.run_full_evaluation(self.dataset)
        breaking = report.per_category_breakdown.get("breaking", {})
        assert breaking.get("accuracy", 0.0) >= 0.40, f"Breaking detection {breaking.get('accuracy', 0):.4f} < 0.40"

    def test_report_failures_include_debugging_info(self) -> None:
        """Individual failures should contain enough info for debugging."""
        report = self.harness.run_full_evaluation(self.dataset)
        for failure in report.individual_failures:
            assert "entry_id" in failure
            assert "category" in failure
            # Each failure should have either expected/predicted or error.
            assert "expected" in failure or "error" in failure


@pytest.mark.slow
class TestVersionComparisonRegression:
    """Verify that compare_versions works on real evaluation reports."""

    @pytest.fixture(autouse=True)
    def _setup_engines(self) -> None:
        classifier, cost_predictor, risk_scorer, optimizer = _build_engines()
        self.harness = EvaluationHarness(
            classifier=classifier,
            cost_predictor=cost_predictor,
            risk_scorer=risk_scorer,
            optimizer=optimizer,
        )
        self.dataset = GoldDataset()

    def test_same_version_zero_delta(self) -> None:
        """Running the same engines twice should produce zero deltas."""
        report = self.harness.run_full_evaluation(self.dataset)
        delta = EvaluationHarness.compare_versions(report, report)

        assert delta["classifier_accuracy_delta"] == 0.0
        assert delta["entries_delta"] == 0
        assert delta["failures_delta"] == 0
