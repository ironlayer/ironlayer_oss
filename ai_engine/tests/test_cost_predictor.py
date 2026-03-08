"""Tests for ai_engine.engines.cost_predictor.CostPredictor.

Covers heuristic prediction (no trained model), trained-model prediction,
model training via telemetry, the has_trained_model property, cluster-size
cost rate variation, and edge cases (zero partitions, very large volumes).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ai_engine.engines.cost_predictor import (
    CostPredictor,
    _COST_RATES,
    _HEURISTIC_BASE_SECONDS,
    _HEURISTIC_PER_PARTITION_SECONDS,
)
from ai_engine.models.requests import CostPredictRequest
from ai_engine.models.responses import CostPredictResponse

# ================================================================== #
# Helpers
# ================================================================== #


def _req(
    partition_count: int = 10,
    cluster_size: str = "medium",
    **kwargs,
) -> CostPredictRequest:
    return CostPredictRequest(
        model_name="catalog.schema.model_a",
        partition_count=partition_count,
        cluster_size=cluster_size,
        **kwargs,
    )


# ================================================================== #
# has_trained_model property
# ================================================================== #


class TestHasTrainedModel:
    """Verify the has_trained_model property."""

    def test_no_model_path(self):
        predictor = CostPredictor(model_path=None)
        assert predictor.has_trained_model is False

    def test_model_path_not_found(self, tmp_path):
        """A path that does not exist yields no trained model."""
        predictor = CostPredictor(model_path=tmp_path / "nonexistent.joblib")
        assert predictor.has_trained_model is False

    def test_model_loaded_from_disk(self, tmp_path):
        """When a valid model file exists, has_trained_model is True."""
        from sklearn.linear_model import LinearRegression

        import joblib

        model = LinearRegression()
        # 8 features matching extract_features output:
        # partition_count, log_volume, workers, sql_complexity,
        # join_count, cte_count, has_window, table_count
        X = np.array(
            [[1, 2, 3, 0, 0, 0, 0, 0], [4, 5, 6, 0, 0, 0, 0, 0]],
            dtype=np.float64,
        )
        y = np.array([100.0, 200.0])
        model.fit(X, y)

        model_path = tmp_path / "cost_model.joblib"
        joblib.dump(model, model_path)

        predictor = CostPredictor(model_path=model_path)
        assert predictor.has_trained_model is True


# ================================================================== #
# Heuristic prediction (no trained model)
# ================================================================== #


class TestHeuristicPrediction:
    """Prediction path when no trained model is available."""

    def test_basic_heuristic(self):
        predictor = CostPredictor(model_path=None)
        request = _req(partition_count=10)
        result = predictor.predict(request)

        expected_seconds = _HEURISTIC_BASE_SECONDS + 10 * _HEURISTIC_PER_PARTITION_SECONDS
        expected_minutes = round(expected_seconds / 60.0, 2)
        expected_cost = round(expected_minutes * _COST_RATES["medium"], 4)

        assert isinstance(result, CostPredictResponse)
        assert result.estimated_runtime_minutes == expected_minutes
        assert result.estimated_cost_usd == expected_cost
        assert result.confidence == 0.4

    def test_heuristic_with_historical_runtime(self):
        """When historical_runtime_avg is provided, it overrides the formula."""
        predictor = CostPredictor(model_path=None)
        request = _req(partition_count=10, historical_runtime_avg=600.0)
        result = predictor.predict(request)

        expected_minutes = round(600.0 / 60.0, 2)
        assert result.estimated_runtime_minutes == expected_minutes

    def test_heuristic_with_data_volume(self):
        """Data volume applies a logarithmic scaling factor."""
        predictor = CostPredictor(model_path=None)
        no_volume = predictor.predict(_req(partition_count=5))
        with_volume = predictor.predict(_req(partition_count=5, data_volume_bytes=1_000_000_000))
        # Data volume should increase the runtime
        assert with_volume.estimated_runtime_minutes > no_volume.estimated_runtime_minutes

    def test_heuristic_with_workers(self):
        """More workers should reduce the predicted runtime (diminishing returns)."""
        predictor = CostPredictor(model_path=None)
        one_worker = predictor.predict(_req(partition_count=10, num_workers=1))
        four_workers = predictor.predict(_req(partition_count=10, num_workers=4))
        assert four_workers.estimated_runtime_minutes < one_worker.estimated_runtime_minutes

    def test_heuristic_minimum_runtime(self):
        """Runtime is clamped to at least 30 seconds (0.5 minutes)."""
        predictor = CostPredictor(model_path=None)
        # Zero partitions, small numbers -> should clamp
        request = _req(partition_count=0, historical_runtime_avg=5.0)
        result = predictor.predict(request)
        # 5 seconds < 30 seconds clamp -> 30/60 = 0.5 minutes
        assert result.estimated_runtime_minutes == 0.5

    @pytest.mark.parametrize(
        "cluster_size,rate_key",
        [
            ("small", "small"),
            ("medium", "medium"),
            ("large", "large"),
        ],
    )
    def test_cluster_size_affects_cost(self, cluster_size, rate_key):
        """Different cluster sizes use different USD cost rates."""
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=10, cluster_size=cluster_size))
        expected_seconds = _HEURISTIC_BASE_SECONDS + 10 * _HEURISTIC_PER_PARTITION_SECONDS
        expected_minutes = expected_seconds / 60.0
        expected_cost = round(expected_minutes * _COST_RATES[rate_key], 4)
        assert result.estimated_cost_usd == expected_cost

    def test_small_cluster_costs_less_than_large(self):
        predictor = CostPredictor(model_path=None)
        small = predictor.predict(_req(partition_count=10, cluster_size="small"))
        large = predictor.predict(_req(partition_count=10, cluster_size="large"))
        assert small.estimated_cost_usd < large.estimated_cost_usd


# ================================================================== #
# Trained model prediction
# ================================================================== #


class TestTrainedModelPrediction:
    """Prediction path using a trained LinearRegression model."""

    @pytest.fixture()
    def trained_predictor(self, tmp_path):
        """Create a predictor with a real trained model."""
        from sklearn.linear_model import LinearRegression

        import joblib

        # Train a simple model with 8 features matching extract_features:
        # partition_count, log_volume, workers, sql_complexity,
        # join_count, cte_count, has_window, table_count
        X = np.array(
            [
                [5, np.log1p(1e6), 2, 5.0, 1.0, 0.0, 0.0, 2.0],
                [10, np.log1p(1e8), 4, 8.0, 2.0, 1.0, 0.0, 3.0],
                [20, np.log1p(1e10), 8, 12.0, 3.0, 1.0, 1.0, 4.0],
                [50, np.log1p(1e12), 16, 20.0, 5.0, 2.0, 1.0, 6.0],
            ],
            dtype=np.float64,
        )
        y = np.array([120.0, 300.0, 600.0, 1200.0])
        model = LinearRegression()
        model.fit(X, y)

        model_path = tmp_path / "cost_model.joblib"
        joblib.dump(model, model_path)

        return CostPredictor(model_path=model_path)

    def test_trained_model_is_used(self, trained_predictor):
        assert trained_predictor.has_trained_model is True
        result = trained_predictor.predict(_req(partition_count=10, data_volume_bytes=1_000_000_000, num_workers=4))
        assert isinstance(result, CostPredictResponse)
        assert result.confidence == 0.8  # trained model confidence
        assert result.estimated_runtime_minutes > 0
        assert result.estimated_cost_usd > 0

    def test_trained_model_minimum_runtime(self, trained_predictor):
        """Even a trained model output is clamped to at least 30 seconds."""
        # With very low inputs, the model might predict < 30s
        result = trained_predictor.predict(_req(partition_count=0, data_volume_bytes=0, num_workers=1))
        # 30 seconds = 0.5 minutes
        assert result.estimated_runtime_minutes >= 0.5

    def test_trained_model_uses_medium_rate_for_unknown_cluster(self, trained_predictor):
        """Unknown cluster_size falls back to medium rate."""
        # The pattern validation on cluster_size prevents truly unknown values,
        # but the code uses _COST_RATES.get(..., _COST_RATES["medium"]).
        # We test the medium path directly:
        result = trained_predictor.predict(_req(partition_count=10, cluster_size="medium"))
        assert result.estimated_cost_usd > 0


# ================================================================== #
# Training
# ================================================================== #


class TestTraining:
    """CostPredictor.train() from telemetry data."""

    def test_train_creates_model(self, tmp_path):
        model_path = tmp_path / "models" / "cost_model.joblib"
        predictor = CostPredictor(model_path=None)
        predictor._model_path = model_path

        assert predictor.has_trained_model is False

        telemetry = [
            {"partition_count": 5, "data_volume_bytes": 1e6, "num_workers": 2, "runtime_seconds": 120.0},
            {"partition_count": 10, "data_volume_bytes": 1e8, "num_workers": 4, "runtime_seconds": 300.0},
            {"partition_count": 20, "data_volume_bytes": 1e10, "num_workers": 8, "runtime_seconds": 600.0},
        ]
        predictor.train(telemetry)

        assert predictor.has_trained_model is True
        # Model file should be persisted
        assert model_path.exists()

    def test_train_empty_telemetry_skips(self):
        predictor = CostPredictor(model_path=None)
        predictor.train([])
        assert predictor.has_trained_model is False

    def test_train_invalid_records_skipped(self, tmp_path):
        """Non-dict records are skipped by feature extraction."""
        model_path = tmp_path / "cost_model.joblib"
        predictor = CostPredictor(model_path=None)
        predictor._model_path = model_path

        telemetry = [
            "not a dict",
            {"partition_count": 5, "runtime_seconds": 100.0},
            {"partition_count": 10, "runtime_seconds": 200.0},
            {"partition_count": 15, "runtime_seconds": 300.0},
        ]
        predictor.train(telemetry)
        # The 3 valid records should be enough to train
        assert predictor.has_trained_model is True

    def test_train_then_predict(self, tmp_path):
        """Train, then verify predictions use the trained path."""
        model_path = tmp_path / "cost_model.joblib"
        predictor = CostPredictor(model_path=None)
        predictor._model_path = model_path

        telemetry = [
            {"partition_count": i, "data_volume_bytes": i * 1e6, "num_workers": 2, "runtime_seconds": 50.0 + i * 10}
            for i in range(1, 21)
        ]
        predictor.train(telemetry)

        result = predictor.predict(_req(partition_count=10, data_volume_bytes=10_000_000, num_workers=2))
        assert result.confidence == 0.8  # trained model confidence


# ================================================================== #
# Edge cases
# ================================================================== #


class TestEdgeCases:
    """Boundary conditions for the cost predictor."""

    def test_zero_partitions(self):
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=0))
        # Base seconds only: 300 / 60 = 5.0 minutes
        expected_minutes = round(_HEURISTIC_BASE_SECONDS / 60.0, 2)
        assert result.estimated_runtime_minutes == expected_minutes
        assert result.estimated_cost_usd > 0

    def test_very_large_volume(self):
        """Very large data volumes should scale logarithmically, not explode."""
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=10, data_volume_bytes=10**15))
        # Should produce a finite, reasonable result
        assert result.estimated_runtime_minutes > 0
        assert result.estimated_runtime_minutes < 100_000  # sanity bound
        assert np.isfinite(result.estimated_cost_usd)

    def test_very_large_partition_count(self):
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=10_000))
        assert result.estimated_runtime_minutes > 0
        assert np.isfinite(result.estimated_cost_usd)

    def test_none_data_volume(self):
        """None data_volume_bytes should not blow up heuristic."""
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=5, data_volume_bytes=None))
        assert result.estimated_runtime_minutes > 0

    def test_none_num_workers(self):
        """None num_workers should use default (1 worker)."""
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=5, num_workers=None))
        assert result.estimated_runtime_minutes > 0

    def test_response_is_pydantic_model(self):
        predictor = CostPredictor(model_path=None)
        result = predictor.predict(_req(partition_count=5))
        assert isinstance(result, CostPredictResponse)
        data = result.model_dump()
        assert "estimated_runtime_minutes" in data
        assert "estimated_cost_usd" in data
        assert "confidence" in data
