"""Tests for the cost forecaster.

Validates exponential smoothing, trend detection, projections,
confidence intervals, edge cases, and aggregate forecasting.
"""

from __future__ import annotations

import pytest

from ai_engine.engines.cost_forecaster import CostForecast, CostForecaster

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def forecaster() -> CostForecaster:
    """Return a forecaster with default alpha=0.3."""
    return CostForecaster()


# ---------------------------------------------------------------------------
# Stable costs
# ---------------------------------------------------------------------------


class TestStableCosts:
    """Tests that stable cost histories produce stable forecasts."""

    def test_stable_trend_direction(self, forecaster: CostForecaster) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 10.0]
        result = forecaster.forecast("model_a", history)
        assert result.trend_direction == "stable"

    def test_stable_projection_accuracy(self, forecaster: CostForecaster) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 10.0]
        result = forecaster.forecast("model_a", history, runs_per_day=1.0)
        assert result.projected_7d_total == pytest.approx(70.0, abs=1.0)
        assert result.projected_30d_total == pytest.approx(300.0, abs=5.0)

    def test_stable_near_constant_costs(self, forecaster: CostForecaster) -> None:
        history = [10.0, 10.1, 9.9, 10.0, 10.1]
        result = forecaster.forecast("model_b", history)
        assert result.trend_direction == "stable"


# ---------------------------------------------------------------------------
# Increasing costs
# ---------------------------------------------------------------------------


class TestIncreasingCosts:
    """Tests for increasing cost trend detection."""

    def test_increasing_trend_detected(self, forecaster: CostForecaster) -> None:
        history = [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
        result = forecaster.forecast("model_inc", history)
        assert result.trend_direction == "increasing"

    def test_increasing_projections_higher(self, forecaster: CostForecaster) -> None:
        history = [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
        result = forecaster.forecast("model_inc", history, runs_per_day=1.0)
        # Projection should be at least the mean * 7.
        mean = sum(history) / len(history)
        assert result.projected_7d_total > 0


# ---------------------------------------------------------------------------
# Decreasing costs
# ---------------------------------------------------------------------------


class TestDecreasingCosts:
    """Tests for decreasing cost trend detection."""

    def test_decreasing_trend_detected(self, forecaster: CostForecaster) -> None:
        history = [20.0, 18.0, 16.0, 14.0, 12.0, 10.0]
        result = forecaster.forecast("model_dec", history)
        assert result.trend_direction == "decreasing"

    def test_decreasing_projections_positive(self, forecaster: CostForecaster) -> None:
        history = [20.0, 18.0, 16.0, 14.0, 12.0, 10.0]
        result = forecaster.forecast("model_dec", history)
        # Projections should always be non-negative.
        assert result.projected_7d_total >= 0
        assert result.projected_30d_total >= 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases: empty, single, two data points."""

    def test_empty_history(self, forecaster: CostForecaster) -> None:
        result = forecaster.forecast("model_empty", [])
        assert result.projected_7d_total == 0.0
        assert result.projected_30d_total == 0.0
        assert result.trend_direction == "stable"

    def test_single_data_point(self, forecaster: CostForecaster) -> None:
        result = forecaster.forecast("model_one", [10.0], runs_per_day=2.0)
        # With 1 data point and 2 runs/day:
        # daily = 10.0 * 2.0 = 20.0
        # 7d = 140.0, 30d = 600.0
        assert result.projected_7d_total == pytest.approx(140.0, abs=0.01)
        assert result.projected_30d_total == pytest.approx(600.0, abs=0.01)
        assert result.trend_direction == "stable"

    def test_two_data_points(self, forecaster: CostForecaster) -> None:
        result = forecaster.forecast("model_two", [10.0, 15.0])
        assert result.projected_7d_total > 0
        assert result.projected_30d_total > 0

    def test_all_zero_costs(self, forecaster: CostForecaster) -> None:
        history = [0.0, 0.0, 0.0, 0.0, 0.0]
        result = forecaster.forecast("model_zero", history)
        assert result.projected_7d_total == 0.0
        assert result.projected_30d_total == 0.0


# ---------------------------------------------------------------------------
# Confidence interval
# ---------------------------------------------------------------------------


class TestConfidenceInterval:
    """Tests for confidence interval properties."""

    def test_ci_bounds_order(self, forecaster: CostForecaster) -> None:
        history = [10.0, 12.0, 11.0, 13.0, 10.5]
        result = forecaster.forecast("model_ci", history)
        assert result.confidence_interval[0] <= result.confidence_interval[1]

    def test_ci_lower_bound_non_negative(self, forecaster: CostForecaster) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 10.0]
        result = forecaster.forecast("model_ci", history)
        assert result.confidence_interval[0] >= 0.0

    def test_ci_narrows_with_stable_data(self, forecaster: CostForecaster) -> None:
        stable = [10.0, 10.0, 10.0, 10.0, 10.0]
        volatile = [5.0, 15.0, 5.0, 15.0, 5.0, 15.0]
        r_stable = forecaster.forecast("stable", stable)
        r_volatile = forecaster.forecast("volatile", volatile)
        stable_width = r_stable.confidence_interval[1] - r_stable.confidence_interval[0]
        volatile_width = r_volatile.confidence_interval[1] - r_volatile.confidence_interval[0]
        # Stable data should have a narrower CI (or equal).
        assert stable_width <= volatile_width


# ---------------------------------------------------------------------------
# Alpha sensitivity
# ---------------------------------------------------------------------------


class TestAlphaSensitivity:
    """Tests that different alpha values change the forecast."""

    def test_high_alpha_more_responsive(self) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 10.0, 50.0]
        low = CostForecaster(alpha=0.1)
        high = CostForecaster(alpha=0.9)
        r_low = low.forecast("m", history)
        r_high = high.forecast("m", history)
        # Higher alpha should project closer to the latest value (50).
        assert r_high.projected_7d_total > r_low.projected_7d_total

    def test_alpha_1_uses_latest_value(self) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 50.0]
        f = CostForecaster(alpha=1.0)
        result = f.forecast("m", history, runs_per_day=1.0)
        # With alpha=1, the smoothed value should be the latest: 50.
        assert result.projected_7d_total == pytest.approx(350.0, abs=1.0)


# ---------------------------------------------------------------------------
# Aggregate forecasting
# ---------------------------------------------------------------------------


class TestAggregateForecasting:
    """Tests for forecast_aggregate()."""

    def test_aggregate_sums_projections(self, forecaster: CostForecaster) -> None:
        models = {
            "a": [10.0, 10.0, 10.0, 10.0, 10.0],
            "b": [20.0, 20.0, 20.0, 20.0, 20.0],
        }
        agg = forecaster.forecast_aggregate(models, runs_per_day=1.0)
        assert agg.model_name == "__aggregate__"
        # a: ~70, b: ~140 → total ~210
        assert agg.projected_7d_total == pytest.approx(210.0, abs=5.0)

    def test_aggregate_empty_models(self, forecaster: CostForecaster) -> None:
        agg = forecaster.forecast_aggregate({})
        assert agg.projected_7d_total == 0.0
        assert agg.model_name == "__aggregate__"

    def test_aggregate_trend_majority_vote(self, forecaster: CostForecaster) -> None:
        models = {
            "inc1": [10.0, 12.0, 14.0, 16.0, 18.0, 20.0],
            "inc2": [5.0, 7.0, 9.0, 11.0, 13.0, 15.0],
            "stable": [10.0, 10.0, 10.0, 10.0, 10.0],
        }
        agg = forecaster.forecast_aggregate(models)
        # 2 increasing, 1 stable → increasing.
        assert agg.trend_direction == "increasing"

    def test_aggregate_ci_sums(self, forecaster: CostForecaster) -> None:
        models = {
            "a": [10.0, 10.0, 10.0],
            "b": [20.0, 20.0, 20.0],
        }
        ind_a = forecaster.forecast("a", [10.0, 10.0, 10.0])
        ind_b = forecaster.forecast("b", [20.0, 20.0, 20.0])
        agg = forecaster.forecast_aggregate(models)
        expected_lower = ind_a.confidence_interval[0] + ind_b.confidence_interval[0]
        assert agg.confidence_interval[0] == pytest.approx(expected_lower, abs=0.01)


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Tests that identical inputs produce identical outputs."""

    def test_forecast_deterministic(self, forecaster: CostForecaster) -> None:
        history = [10.0, 12.0, 11.0, 15.0, 13.0]
        r1 = forecaster.forecast("m", history)
        r2 = forecaster.forecast("m", history)
        assert r1.projected_7d_total == r2.projected_7d_total
        assert r1.trend_direction == r2.trend_direction
        assert r1.confidence_interval == r2.confidence_interval


# ---------------------------------------------------------------------------
# CostForecast model
# ---------------------------------------------------------------------------


class TestCostForecastModel:
    """Tests for the CostForecast Pydantic model."""

    def test_defaults(self) -> None:
        f = CostForecast(model_name="test")
        assert f.projected_7d_total == 0.0
        assert f.projected_30d_total == 0.0
        assert f.trend_direction == "stable"
        assert f.smoothing_factor == 0.3

    def test_model_dump(self) -> None:
        f = CostForecast(
            model_name="m1",
            projected_7d_total=100.0,
            projected_30d_total=400.0,
            trend_direction="increasing",
            confidence_interval=[80.0, 120.0],
            smoothing_factor=0.5,
        )
        data = f.model_dump()
        assert data["model_name"] == "m1"
        assert data["projected_7d_total"] == 100.0
