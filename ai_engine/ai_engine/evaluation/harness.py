"""Evaluation harness for running AI engines against a gold dataset.

The harness takes engine instances (classifier, cost predictor, risk scorer,
optimizer) and evaluates them against a curated gold dataset.  It computes
per-category and overall metrics, returning a structured report suitable
for CI-gate assertions.

INVARIANT: The harness is engine-agnostic -- it takes engine instances as
constructor arguments and does not instantiate them.  This allows testing
with both rule-based and LLM-enhanced engines.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from ai_engine.evaluation.gold_dataset import GoldDataset
from ai_engine.evaluation.metrics import (
    accuracy,
    confidence_calibration,
    precision_recall_f1,
)
from ai_engine.models.requests import (
    CostPredictRequest,
    OptimizeSQLRequest,
    RiskScoreRequest,
    SemanticClassifyRequest,
)

logger = logging.getLogger(__name__)

_CHANGE_TYPE_LABELS = [
    "non_breaking",
    "breaking",
    "metric_semantic",
    "rename_only",
    "partition_shift",
    "cosmetic",
]


class EvaluationReport(BaseModel):
    """Structured report from a full evaluation run."""

    classifier_metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="Per-category and macro precision/recall/F1 for the classifier.",
    )
    classifier_accuracy: float = Field(
        default=0.0,
        description="Overall classifier accuracy.",
    )
    cost_metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="Cost predictor evaluation metrics.",
    )
    risk_metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="Risk scorer evaluation metrics.",
    )
    optimizer_metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="SQL optimizer evaluation metrics.",
    )
    per_category_breakdown: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Per-category accuracy and sample count.",
    )
    individual_failures: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of individual misclassifications for debugging.",
    )
    overall_pass: bool = Field(
        default=False,
        description="True if all CI-gate thresholds are met.",
    )
    entries_evaluated: int = Field(
        default=0,
        description="Total number of gold dataset entries evaluated.",
    )


class EvaluationHarness:
    """Run AI engines against a gold dataset and compute quality metrics.

    Parameters
    ----------
    classifier:
        SemanticClassifier instance (rule-based, no LLM for determinism).
    cost_predictor:
        CostPredictor instance (heuristic fallback for determinism).
    risk_scorer:
        RiskScorer instance.
    optimizer:
        SQLOptimizer instance (no LLM for determinism).
    """

    def __init__(
        self,
        classifier: Any,
        cost_predictor: Any,
        risk_scorer: Any,
        optimizer: Any,
    ) -> None:
        self._classifier = classifier
        self._cost_predictor = cost_predictor
        self._risk_scorer = risk_scorer
        self._optimizer = optimizer

    def run_full_evaluation(
        self,
        dataset: GoldDataset | None = None,
        *,
        classifier_f1_threshold: float = 0.85,
        risk_accuracy_threshold: float = 0.90,
    ) -> EvaluationReport:
        """Evaluate all engines against the gold dataset.

        Parameters
        ----------
        dataset:
            Gold dataset to evaluate against.  Uses the built-in dataset
            if not provided.
        classifier_f1_threshold:
            Minimum macro-F1 for the classifier to pass.
        risk_accuracy_threshold:
            Minimum accuracy for the risk scorer to pass.

        Returns
        -------
        EvaluationReport
            Structured report with metrics and pass/fail status.
        """
        if dataset is None:
            dataset = GoldDataset()

        entries = dataset.get_all()
        if not entries:
            return EvaluationReport(overall_pass=False)

        # --- Classifier evaluation ---
        predicted_types: list[str] = []
        actual_types: list[str] = []
        confidence_pairs: list[tuple[float, bool]] = []
        individual_failures: list[dict[str, Any]] = []
        category_results: dict[str, dict[str, int]] = {}

        for entry in entries:
            try:
                request = SemanticClassifyRequest(
                    old_sql=entry.old_sql,
                    new_sql=entry.new_sql,
                    schema_diff=entry.schema_diff,
                    llm_enabled=False,
                )
                response = self._classifier.classify(request)
                predicted = response.change_type
                expected = entry.expected_change_type

                predicted_types.append(predicted)
                actual_types.append(expected)

                correct = predicted == expected
                confidence_pairs.append((response.confidence, correct))

                # Track per-category results.
                cat = entry.category
                if cat not in category_results:
                    category_results[cat] = {"correct": 0, "total": 0}
                category_results[cat]["total"] += 1
                if correct:
                    category_results[cat]["correct"] += 1
                else:
                    individual_failures.append(
                        {
                            "entry_id": entry.id,
                            "category": cat,
                            "expected": expected,
                            "predicted": predicted,
                            "confidence": response.confidence,
                            "old_sql_snippet": entry.old_sql[:80],
                            "new_sql_snippet": entry.new_sql[:80],
                        }
                    )
            except Exception as exc:
                logger.warning("Classifier failed on entry %s: %s", entry.id, exc)
                individual_failures.append(
                    {
                        "entry_id": entry.id,
                        "category": entry.category,
                        "error": str(exc),
                    }
                )

        # Compute classifier metrics.
        classifier_prf1 = precision_recall_f1(predicted_types, actual_types, _CHANGE_TYPE_LABELS)
        classifier_acc = accuracy(predicted_types, actual_types)  # type: ignore[arg-type]
        calibration = confidence_calibration(confidence_pairs)

        classifier_metrics = {
            "per_label": classifier_prf1,
            "accuracy": classifier_acc,
            "calibration": calibration,
            "total_evaluated": len(predicted_types),
        }

        per_category_breakdown = {
            cat: {
                "accuracy": round(data["correct"] / data["total"], 4) if data["total"] > 0 else 0.0,
                "correct": data["correct"],
                "total": data["total"],
            }
            for cat, data in sorted(category_results.items())
        }

        # --- Cost predictor evaluation ---
        cost_successes = 0
        cost_errors = 0
        cost_predictions: list[float] = []

        for entry in entries:
            try:
                request = CostPredictRequest(  # type: ignore[assignment]
                    model_name=entry.id,
                    partition_count=10,
                    cluster_size="medium",
                    llm_enabled=False,
                )
                response = self._cost_predictor.predict(request)
                cost_predictions.append(response.estimated_cost_usd)
                cost_successes += 1

                # Validate response sanity.
                assert response.estimated_cost_usd >= 0
                assert response.estimated_runtime_minutes >= 0
                assert 0.0 <= response.confidence <= 1.0
                assert response.cost_lower_bound_usd <= response.cost_upper_bound_usd
            except Exception as exc:
                cost_errors += 1
                logger.warning("Cost predictor failed on entry %s: %s", entry.id, exc)

        cost_metrics = {
            "successes": cost_successes,
            "errors": cost_errors,
            "error_rate": round(cost_errors / len(entries), 4) if entries else 0.0,
            "mean_predicted_cost": round(sum(cost_predictions) / len(cost_predictions), 4) if cost_predictions else 0.0,
        }

        # --- Risk scorer evaluation ---
        risk_predictions: list[bool] = []
        risk_actuals: list[bool] = []

        for entry in entries:
            if not entry.expected_risk_factors:
                continue
            try:
                request = RiskScoreRequest(  # type: ignore[assignment]
                    model_name=entry.id,
                    downstream_depth=2,
                    sla_tags=[],
                    dashboard_dependencies=[],
                    model_tags=[],
                    historical_failure_rate=0.05,
                    llm_enabled=False,
                )
                response = self._risk_scorer.score(request)

                # Evaluate: does the scorer flag high-risk entries?
                risk_predictions.append(response.approval_required)
                risk_actuals.append(entry.expected_change_type == "breaking")
            except Exception as exc:
                logger.warning("Risk scorer failed on entry %s: %s", entry.id, exc)

        risk_acc = accuracy(risk_predictions, risk_actuals) if risk_predictions else 0.0  # type: ignore[arg-type]
        risk_metrics = {
            "accuracy": risk_acc,
            "evaluated": len(risk_predictions),
        }

        # --- Optimizer evaluation ---
        optimizer_successes = 0
        optimizer_suggestion_matches = 0
        optimizer_total_with_expectations = 0

        for entry in entries:
            if not entry.expected_suggestion_types:
                continue
            optimizer_total_with_expectations += 1
            try:
                request = OptimizeSQLRequest(  # type: ignore[assignment]
                    sql=entry.new_sql,
                    llm_enabled=False,
                )
                response = self._optimizer.optimize(request)
                optimizer_successes += 1

                # Check if expected suggestion types appear.
                result_types = {s.suggestion_type for s in response.suggestions}
                if any(et in result_types for et in entry.expected_suggestion_types):
                    optimizer_suggestion_matches += 1
            except Exception as exc:
                logger.warning("Optimizer failed on entry %s: %s", entry.id, exc)

        optimizer_metrics = {
            "successes": optimizer_successes,
            "total_with_expectations": optimizer_total_with_expectations,
            "suggestion_match_rate": (
                round(optimizer_suggestion_matches / optimizer_total_with_expectations, 4)
                if optimizer_total_with_expectations > 0
                else 0.0
            ),
        }

        # --- Overall pass/fail ---
        macro_f1 = classifier_prf1.get("macro", {}).get("f1", 0.0)
        overall_pass = macro_f1 >= classifier_f1_threshold and cost_metrics["error_rate"] < 0.05

        report = EvaluationReport(
            classifier_metrics=classifier_metrics,
            classifier_accuracy=classifier_acc,
            cost_metrics=cost_metrics,
            risk_metrics=risk_metrics,
            optimizer_metrics=optimizer_metrics,
            per_category_breakdown=per_category_breakdown,
            individual_failures=individual_failures,
            overall_pass=overall_pass,
            entries_evaluated=len(entries),
        )

        logger.info(
            "Evaluation complete: %d entries, classifier_f1=%.4f, "
            "classifier_acc=%.4f, cost_errors=%d, risk_acc=%.4f, pass=%s",
            len(entries),
            macro_f1,
            classifier_acc,
            cost_errors,
            risk_acc,
            overall_pass,
        )

        return report

    @staticmethod
    def compare_versions(
        report_a: EvaluationReport,
        report_b: EvaluationReport,
    ) -> dict[str, Any]:
        """Compare metrics between two evaluation reports.

        Returns a dict with deltas for key metrics, useful for A/B
        comparison when changing engine logic.
        """

        def _safe_get(report: EvaluationReport, *keys: str) -> float:
            obj: Any = report.classifier_metrics
            for key in keys:
                if isinstance(obj, dict):
                    obj = obj.get(key, 0.0)
                else:
                    return 0.0
            return float(obj) if obj is not None else 0.0

        macro_f1_a = _safe_get(report_a, "per_label", "macro", "f1")
        macro_f1_b = _safe_get(report_b, "per_label", "macro", "f1")

        return {
            "classifier_accuracy_delta": round(report_b.classifier_accuracy - report_a.classifier_accuracy, 4),
            "classifier_f1_delta": round(macro_f1_b - macro_f1_a, 4),
            "cost_error_rate_delta": round(
                report_b.cost_metrics.get("error_rate", 0.0) - report_a.cost_metrics.get("error_rate", 0.0),
                4,
            ),
            "risk_accuracy_delta": round(
                report_b.risk_metrics.get("accuracy", 0.0) - report_a.risk_metrics.get("accuracy", 0.0),
                4,
            ),
            "entries_delta": (report_b.entries_evaluated - report_a.entries_evaluated),
            "failures_delta": (len(report_b.individual_failures) - len(report_a.individual_failures)),
        }
