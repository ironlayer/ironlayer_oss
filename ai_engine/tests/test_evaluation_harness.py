"""Tests for the AI evaluation harness mechanics.

Validates that the EvaluationHarness correctly orchestrates engine calls,
computes metrics, produces structured reports, and performs version
comparison — all using mock engines to isolate harness logic.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from ai_engine.evaluation.gold_dataset import GoldDataset, GoldDatasetEntry
from ai_engine.evaluation.harness import EvaluationHarness, EvaluationReport
from ai_engine.evaluation.metrics import (
    accuracy,
    confidence_calibration,
    confusion_matrix,
    mean_absolute_error,
    precision_recall_f1,
)
from ai_engine.models.responses import (
    CostPredictResponse,
    OptimizeSQLResponse,
    RiskScoreResponse,
    SQLSuggestion,
    SemanticClassifyResponse,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_classifier(change_type: str = "non_breaking", confidence: float = 0.9):
    """Build a mock classifier that always returns the given type."""
    mock = MagicMock()
    mock.classify.return_value = SemanticClassifyResponse(
        change_type=change_type,
        confidence=confidence,
        requires_full_rebuild=False,
        impact_scope="minor",
    )
    return mock


def _make_cost_predictor(cost: float = 1.5):
    """Build a mock cost predictor."""
    mock = MagicMock()
    mock.predict.return_value = CostPredictResponse(
        estimated_runtime_minutes=2.0,
        estimated_cost_usd=cost,
        confidence=0.8,
        cost_lower_bound_usd=cost * 0.5,
        cost_upper_bound_usd=cost * 1.5,
        confidence_label="medium",
    )
    return mock


def _make_risk_scorer(approval_required: bool = False):
    """Build a mock risk scorer."""
    mock = MagicMock()
    mock.score.return_value = RiskScoreResponse(
        risk_score=3.0,
        business_critical=False,
        approval_required=approval_required,
        risk_factors=["moderate_downstream"],
    )
    return mock


def _make_optimizer(suggestions: list[str] | None = None):
    """Build a mock optimizer."""
    mock = MagicMock()
    sugs = [
        SQLSuggestion(
            suggestion_type=s,
            description=f"Suggestion: {s}",
            confidence=0.7,
        )
        for s in (suggestions or [])
    ]
    mock.optimize.return_value = OptimizeSQLResponse(suggestions=sugs)
    return mock


def _tiny_dataset() -> GoldDataset:
    """Return a small gold dataset with known entries for testing."""
    ds = GoldDataset()
    ds.ENTRIES = [
        GoldDatasetEntry(
            id="t_001",
            category="cosmetic",
            old_sql="SELECT id FROM t",
            new_sql="SELECT  id  FROM  t",
            expected_change_type="cosmetic",
            expected_confidence_min=0.8,
        ),
        GoldDatasetEntry(
            id="t_002",
            category="breaking",
            old_sql="SELECT id, name FROM t",
            new_sql="SELECT id FROM t",
            schema_diff={"removed": ["name"]},
            expected_change_type="breaking",
            expected_confidence_min=0.7,
            expected_full_rebuild=True,
            expected_risk_factors=["column_removed"],
        ),
        GoldDatasetEntry(
            id="t_003",
            category="non_breaking",
            old_sql="SELECT id FROM t",
            new_sql="SELECT id, email FROM t",
            schema_diff={"added": ["email"]},
            expected_change_type="non_breaking",
            expected_confidence_min=0.6,
            expected_suggestion_types=["add_column"],
        ),
    ]
    return ds


# ---------------------------------------------------------------------------
# Tests: harness report structure
# ---------------------------------------------------------------------------


class TestEvaluationReportStructure:
    """EvaluationReport must always be well-formed."""

    def test_default_report_is_failing(self) -> None:
        report = EvaluationReport()
        assert report.overall_pass is False
        assert report.entries_evaluated == 0
        assert report.individual_failures == []

    def test_report_serialises_to_dict(self) -> None:
        report = EvaluationReport(
            classifier_accuracy=0.9,
            entries_evaluated=50,
            overall_pass=True,
        )
        d = report.model_dump()
        assert d["classifier_accuracy"] == 0.9
        assert d["entries_evaluated"] == 50
        assert d["overall_pass"] is True


# ---------------------------------------------------------------------------
# Tests: harness evaluation mechanics
# ---------------------------------------------------------------------------


class TestHarnessEvaluation:
    """The harness must correctly dispatch to engines and aggregate results."""

    def test_perfect_classifier_produces_high_f1(self) -> None:
        """If the mock classifier always returns the correct type, F1 ≈ 1.0."""
        ds = _tiny_dataset()
        entries = ds.get_all()

        # Build a classifier that returns the correct answer for each entry.
        classifier = MagicMock()
        side_effects = []
        for entry in entries:
            side_effects.append(
                SemanticClassifyResponse(
                    change_type=entry.expected_change_type,
                    confidence=0.95,
                    requires_full_rebuild=entry.expected_full_rebuild,
                    impact_scope="mock",
                )
            )
        classifier.classify.side_effect = side_effects

        harness = EvaluationHarness(
            classifier=classifier,
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(ds)

        assert report.entries_evaluated == 3
        assert report.classifier_accuracy == 1.0
        assert report.individual_failures == []

    def test_wrong_classifier_produces_failures(self) -> None:
        """If the classifier always says 'cosmetic', non-cosmetic entries fail."""
        harness = EvaluationHarness(
            classifier=_make_classifier(change_type="cosmetic"),
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(_tiny_dataset())

        # 1 correct (cosmetic entry), 2 failures (breaking + non_breaking).
        assert report.entries_evaluated == 3
        assert len(report.individual_failures) == 2
        assert report.classifier_accuracy < 1.0

    def test_empty_dataset_returns_failing_report(self) -> None:
        """Empty dataset produces a non-passing report."""
        ds = GoldDataset()
        ds.ENTRIES = []

        harness = EvaluationHarness(
            classifier=_make_classifier(),
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(ds)

        assert report.overall_pass is False
        assert report.entries_evaluated == 0

    def test_classifier_exception_logged_as_failure(self) -> None:
        """If the classifier raises, the entry is logged as a failure."""
        classifier = MagicMock()
        classifier.classify.side_effect = RuntimeError("model failed")

        harness = EvaluationHarness(
            classifier=classifier,
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(_tiny_dataset())

        assert len(report.individual_failures) == 3
        assert any("error" in f for f in report.individual_failures)

    def test_cost_predictor_errors_tracked(self) -> None:
        """Cost predictor failures are counted in cost_metrics."""
        cost = MagicMock()
        cost.predict.side_effect = ValueError("bad input")

        harness = EvaluationHarness(
            classifier=_make_classifier(),
            cost_predictor=cost,
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(_tiny_dataset())

        assert report.cost_metrics["errors"] == 3
        assert report.cost_metrics["error_rate"] > 0

    def test_per_category_breakdown_computed(self) -> None:
        """Per-category accuracy should be tracked for each category."""
        harness = EvaluationHarness(
            classifier=_make_classifier(change_type="cosmetic"),
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=_make_optimizer(),
        )
        report = harness.run_full_evaluation(_tiny_dataset())

        assert "cosmetic" in report.per_category_breakdown
        assert report.per_category_breakdown["cosmetic"]["accuracy"] == 1.0
        assert report.per_category_breakdown["breaking"]["accuracy"] == 0.0

    def test_optimizer_suggestion_matching(self) -> None:
        """Optimizer match rate tracks expected suggestion type presence."""
        optimizer = _make_optimizer(suggestions=["add_column"])

        harness = EvaluationHarness(
            classifier=_make_classifier(),
            cost_predictor=_make_cost_predictor(),
            risk_scorer=_make_risk_scorer(),
            optimizer=optimizer,
        )
        report = harness.run_full_evaluation(_tiny_dataset())

        # t_003 expects "add_column" and optimizer returns it.
        assert report.optimizer_metrics["suggestion_match_rate"] > 0


# ---------------------------------------------------------------------------
# Tests: version comparison
# ---------------------------------------------------------------------------


class TestVersionComparison:
    """compare_versions() must return correct deltas."""

    def test_identical_reports_produce_zero_deltas(self) -> None:
        report = EvaluationReport(
            classifier_accuracy=0.85,
            entries_evaluated=50,
            overall_pass=True,
        )
        delta = EvaluationHarness.compare_versions(report, report)

        assert delta["classifier_accuracy_delta"] == 0.0
        assert delta["entries_delta"] == 0

    def test_improvement_shows_positive_delta(self) -> None:
        old = EvaluationReport(classifier_accuracy=0.80, entries_evaluated=50)
        new = EvaluationReport(classifier_accuracy=0.90, entries_evaluated=50)

        delta = EvaluationHarness.compare_versions(old, new)
        assert delta["classifier_accuracy_delta"] == 0.1

    def test_regression_shows_negative_delta(self) -> None:
        old = EvaluationReport(classifier_accuracy=0.90, entries_evaluated=50)
        new = EvaluationReport(classifier_accuracy=0.80, entries_evaluated=50)

        delta = EvaluationHarness.compare_versions(old, new)
        assert delta["classifier_accuracy_delta"] == pytest.approx(-0.1, abs=0.001)


# ---------------------------------------------------------------------------
# Tests: pure metric functions
# ---------------------------------------------------------------------------


class TestMetricFunctions:
    """Verify the metric utility functions independently."""

    def test_confusion_matrix_identity(self) -> None:
        labels = ["a", "b"]
        preds = ["a", "b", "a", "b"]
        actual = ["a", "b", "a", "b"]

        cm = confusion_matrix(preds, actual, labels)
        assert cm["a"]["a"] == 2
        assert cm["b"]["b"] == 2
        assert cm["a"]["b"] == 0
        assert cm["b"]["a"] == 0

    def test_precision_recall_f1_perfect(self) -> None:
        labels = ["x", "y"]
        preds = ["x", "y", "x", "y"]
        actual = ["x", "y", "x", "y"]

        result = precision_recall_f1(preds, actual, labels)
        assert result["x"]["f1"] == 1.0
        assert result["y"]["f1"] == 1.0
        assert result["macro"]["f1"] == 1.0

    def test_mean_absolute_error_zero(self) -> None:
        assert mean_absolute_error([1.0, 2.0], [1.0, 2.0]) == 0.0

    def test_mean_absolute_error_nonzero(self) -> None:
        assert mean_absolute_error([1.0, 3.0], [2.0, 1.0]) == 1.5

    def test_accuracy_perfect(self) -> None:
        assert accuracy(["a", "b"], ["a", "b"]) == 1.0

    def test_accuracy_none_correct(self) -> None:
        assert accuracy(["a", "a"], ["b", "b"]) == 0.0

    def test_confidence_calibration_all_correct(self) -> None:
        pairs = [(0.9, True), (0.85, True), (0.95, True)]
        cal = confidence_calibration(pairs)
        assert cal["0.8-1.0"] == 1.0

    def test_confidence_calibration_empty(self) -> None:
        cal = confidence_calibration([])
        assert all(v == 0.0 for v in cal.values())

    def test_length_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="same length"):
            accuracy(["a"], ["a", "b"])
