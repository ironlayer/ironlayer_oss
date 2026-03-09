"""Tests for FailurePredictor and cost trend analysis."""

from __future__ import annotations

import pytest
from ai_engine.engines.failure_predictor import (
    FailurePredictor,
    RunHistory,
    _sigmoid,
    compute_cost_trend,
)

# ---------------------------------------------------------------------------
# _sigmoid
# ---------------------------------------------------------------------------


class TestSigmoid:
    """Tests for the sigmoid helper."""

    def test_midpoint_returns_half(self):
        assert abs(_sigmoid(0.0, midpoint=0.0) - 0.5) < 0.001

    def test_large_positive(self):
        assert _sigmoid(20.0) > 0.99

    def test_large_negative(self):
        assert _sigmoid(-20.0) < 0.01

    def test_steepness(self):
        # Higher steepness should give more extreme output
        gentle = _sigmoid(2.0, midpoint=0.0, steepness=0.5)
        steep = _sigmoid(2.0, midpoint=0.0, steepness=2.0)
        assert steep > gentle

    def test_midpoint_shift(self):
        val = _sigmoid(5.0, midpoint=5.0)
        assert abs(val - 0.5) < 0.001

    def test_overflow_protection(self):
        # Should not raise for extreme values
        assert _sigmoid(1000.0) > 0.99
        assert _sigmoid(-1000.0) < 0.01


# ---------------------------------------------------------------------------
# FailurePredictor
# ---------------------------------------------------------------------------


class TestFailurePredictor:
    """Tests for failure prediction logic."""

    @pytest.fixture
    def predictor(self):
        return FailurePredictor()

    def test_healthy_model(self, predictor):
        """Model with no failures should have low probability."""
        history = RunHistory(
            model_name="healthy_model",
            total_runs=100,
            failed_runs=0,
            recent_runs=10,
            recent_failures=0,
            consecutive_failures=0,
            avg_runtime_seconds=300,
            recent_avg_runtime_seconds=300,
        )
        pred = predictor.predict(history)
        assert pred.failure_probability < 0.15
        assert pred.risk_level == "low"
        assert pred.model_name == "healthy_model"

    def test_high_failure_rate(self, predictor):
        """Model with 50% failure rate should have high probability."""
        history = RunHistory(
            model_name="failing_model",
            total_runs=100,
            failed_runs=50,
            recent_runs=10,
            recent_failures=5,
            consecutive_failures=3,
        )
        pred = predictor.predict(history)
        assert pred.failure_probability > 0.5
        assert pred.risk_level == "high"
        assert any("failure rate" in f.lower() for f in pred.factors)

    def test_consecutive_failures(self, predictor):
        """Streak of failures should increase probability."""
        history_none = RunHistory(
            model_name="m1",
            total_runs=20,
            failed_runs=5,
            consecutive_failures=0,
        )
        history_streak = RunHistory(
            model_name="m1",
            total_runs=20,
            failed_runs=5,
            consecutive_failures=3,
        )
        pred_none = predictor.predict(history_none)
        pred_streak = predictor.predict(history_streak)
        assert pred_streak.failure_probability > pred_none.failure_probability

    def test_runtime_trend(self, predictor):
        """Growing runtime should contribute to risk."""
        # Baseline: moderate failure rate but no runtime trend
        history_baseline = RunHistory(
            model_name="m1",
            total_runs=50,
            failed_runs=10,
            avg_runtime_seconds=100,
            recent_avg_runtime_seconds=100,
            runtime_trend=0.0,
        )
        # Same model but with significant runtime growth
        history_trending = RunHistory(
            model_name="m1",
            total_runs=50,
            failed_runs=10,
            avg_runtime_seconds=100,
            recent_avg_runtime_seconds=200,
            runtime_trend=1.0,  # 100% growth
        )
        pred_baseline = predictor.predict(history_baseline)
        pred_trending = predictor.predict(history_trending)
        # Runtime trend should increase probability
        assert pred_trending.failure_probability > pred_baseline.failure_probability
        assert any("runtime" in f.lower() for f in pred_trending.factors)

    def test_shuffle_growth(self, predictor):
        """Shuffle volume growth should contribute to risk."""
        history = RunHistory(
            model_name="m1",
            total_runs=50,
            failed_runs=2,
            avg_shuffle_bytes=1_000_000,
            recent_avg_shuffle_bytes=3_000_000,
            shuffle_trend=2.0,  # 200% growth
        )
        pred = predictor.predict(history)
        assert any("shuffle" in f.lower() for f in pred.factors)

    def test_staleness(self, predictor):
        """Long time since last success should increase risk."""
        history = RunHistory(
            model_name="m1",
            total_runs=30,
            failed_runs=10,
            hours_since_last_success=720,  # 30 days
        )
        pred = predictor.predict(history)
        assert any("no successful run" in f.lower() for f in pred.factors)

    def test_recent_acceleration(self, predictor):
        """Recent failure rate higher than historical should flag."""
        history = RunHistory(
            model_name="m1",
            total_runs=100,
            failed_runs=5,  # 5% historical
            recent_runs=10,
            recent_failures=5,  # 50% recent
        )
        pred = predictor.predict(history)
        assert any("recent failure rate" in f.lower() for f in pred.factors)

    def test_empty_history(self, predictor):
        """Model with no runs should predict low risk."""
        history = RunHistory(model_name="new_model")
        pred = predictor.predict(history)
        assert pred.failure_probability < 0.2
        assert pred.risk_level == "low"

    def test_error_type_signal(self, predictor):
        """Non-transient error type should add signal."""
        history_transient = RunHistory(
            model_name="m1",
            total_runs=20,
            failed_runs=5,
            consecutive_failures=1,
            last_error_type="timeout",
        )
        history_permanent = RunHistory(
            model_name="m1",
            total_runs=20,
            failed_runs=5,
            consecutive_failures=1,
            last_error_type="SQL syntax error",
        )
        pred_transient = predictor.predict(history_transient)
        pred_permanent = predictor.predict(history_permanent)
        assert pred_permanent.failure_probability > pred_transient.failure_probability

    def test_custom_thresholds(self):
        """Custom thresholds should change risk level classification."""
        predictor = FailurePredictor(
            warning_threshold=0.1,
            critical_threshold=0.3,
        )
        history = RunHistory(
            model_name="m1",
            total_runs=50,
            failed_runs=10,
            consecutive_failures=2,
        )
        pred = predictor.predict(history)
        # With lower thresholds, moderate risk becomes "high"
        assert pred.risk_level in ("medium", "high")

    def test_suggested_actions_present(self, predictor):
        """Models with risk should get actionable suggestions."""
        history = RunHistory(
            model_name="m1",
            total_runs=50,
            failed_runs=25,
            consecutive_failures=3,
            hours_since_last_success=200,
        )
        pred = predictor.predict(history)
        assert len(pred.suggested_actions) > 0

    def test_predict_batch(self, predictor):
        """Batch prediction should sort by probability descending."""
        histories = [
            RunHistory(model_name="healthy", total_runs=100, failed_runs=0),
            RunHistory(model_name="failing", total_runs=100, failed_runs=50, consecutive_failures=5),
            RunHistory(model_name="moderate", total_runs=100, failed_runs=15),
        ]
        predictions = predictor.predict_batch(histories)
        assert len(predictions) == 3
        assert predictions[0].model_name == "failing"
        assert predictions[0].failure_probability >= predictions[1].failure_probability
        assert predictions[1].failure_probability >= predictions[2].failure_probability

    def test_probability_bounded(self, predictor):
        """Probability should always be between 0 and 1."""
        # Extreme case
        history = RunHistory(
            model_name="m1",
            total_runs=1000,
            failed_runs=999,
            consecutive_failures=100,
            runtime_trend=5.0,
            shuffle_trend=10.0,
            hours_since_last_success=10000,
            recent_runs=50,
            recent_failures=50,
            avg_runtime_seconds=100,
            recent_avg_runtime_seconds=500,
        )
        pred = predictor.predict(history)
        assert 0.0 <= pred.failure_probability <= 1.0


# ---------------------------------------------------------------------------
# compute_cost_trend
# ---------------------------------------------------------------------------


class TestComputeCostTrend:
    """Tests for cost trend analysis."""

    def test_stable_costs(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[1.0, 1.0, 1.0],
            historical_costs=[1.0, 1.0, 1.0],
        )
        assert trend.trend_direction == "stable"
        assert trend.alert is False
        assert abs(trend.cost_change_pct) < 0.05

    def test_increasing_costs(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[2.0, 2.0, 2.0],
            historical_costs=[1.0, 1.0, 1.0],
        )
        assert trend.trend_direction == "increasing"
        assert trend.cost_change_pct > 0.5
        assert trend.alert is True

    def test_decreasing_costs(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[0.5, 0.5],
            historical_costs=[1.0, 1.0],
        )
        assert trend.trend_direction == "decreasing"
        assert trend.cost_change_pct < -0.3
        assert trend.alert is False

    def test_empty_recent_costs(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[],
            historical_costs=[1.0, 1.0],
        )
        assert trend.current_avg_cost_usd == 0.0
        assert trend.trend_direction in ("stable", "decreasing")

    def test_empty_historical_costs(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[2.0],
            historical_costs=[],
        )
        assert trend.cost_change_pct == 1.0  # new cost where none existed

    def test_both_empty(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[],
            historical_costs=[],
        )
        assert trend.cost_change_pct == 0.0
        assert trend.alert is False

    def test_projected_monthly_cost(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[0.10, 0.10],
            historical_costs=[0.10],
            runs_per_month=30.0,
        )
        assert abs(trend.projected_monthly_cost_usd - 3.0) < 0.01

    def test_custom_alert_threshold(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[1.2],
            historical_costs=[1.0],
            alert_threshold_pct=0.1,  # 10% threshold
        )
        assert trend.alert is True  # 20% increase > 10% threshold

    def test_model_name_preserved(self):
        trend = compute_cost_trend(
            model_name="analytics.orders",
            recent_costs=[1.0],
            historical_costs=[1.0],
        )
        assert trend.model_name == "analytics.orders"

    def test_factors_contain_cost_info(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[5.0],
            historical_costs=[1.0],
        )
        assert len(trend.factors) > 0
        assert any("cost" in f.lower() or "$" in f for f in trend.factors)

    def test_alert_factor_present(self):
        trend = compute_cost_trend(
            model_name="m1",
            recent_costs=[5.0],
            historical_costs=[1.0],
        )
        assert any("ALERT" in f for f in trend.factors)
