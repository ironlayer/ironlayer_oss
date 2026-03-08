"""Cost prediction engine -- trained model with heuristic fallback.

Uses a scikit-learn ``LinearRegression`` trained on historical telemetry
when a model file is available, otherwise falls back to a deterministic
heuristic.  The engine **never** triggers any execution -- it only returns
advisory cost / runtime estimates.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from ai_engine.ml.cost_model import CostModelTrainer
from ai_engine.ml.feature_extractor import extract_features
from ai_engine.models.requests import CostPredictRequest
from ai_engine.models.responses import CostPredictResponse

logger = logging.getLogger(__name__)

# Cost per minute by cluster tier (USD, Databricks DBU-based estimates)
_COST_RATES: dict[str, float] = {
    "small": 0.042,
    "medium": 0.168,
    "large": 0.672,
}

# Heuristic constants
_HEURISTIC_BASE_SECONDS = 300.0
_HEURISTIC_PER_PARTITION_SECONDS = 30.0


def _compute_confidence_band(
    estimated_cost_usd: float,
    confidence: float,
) -> tuple[float, float, str]:
    """Compute cost confidence bands based on prediction confidence.

    Parameters
    ----------
    estimated_cost_usd:
        The point estimate cost.
    confidence:
        The confidence score (0.0 to 1.0).

    Returns
    -------
    tuple[float, float, str]
        (lower_bound, upper_bound, confidence_label)

    Confidence band widths:
    - Low confidence (< 0.5): +-40% of estimate
    - Medium confidence (0.5-0.79): +-20% of estimate
    - High confidence (>= 0.8): +-5% of estimate
    """
    if confidence >= 0.8:
        margin = 0.05
        label = "high"
    elif confidence >= 0.5:
        margin = 0.20
        label = "medium"
    else:
        margin = 0.40
        label = "low"

    lower = round(max(0.0, estimated_cost_usd * (1.0 - margin)), 6)
    upper = round(estimated_cost_usd * (1.0 + margin), 6)
    return lower, upper, label


class CostPredictor:
    """Predict execution cost and runtime for a model run."""

    def __init__(self, model_path: Path | None = None) -> None:
        self._model = None
        self._model_path = model_path

        if model_path is not None:
            self._model = CostModelTrainer.load(model_path)
            if self._model is not None:
                logger.info("Loaded trained cost model from %s", model_path)
            else:
                logger.info(
                    "No trained cost model at %s -- using heuristic fallback",
                    model_path,
                )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    @property
    def has_trained_model(self) -> bool:
        return self._model is not None

    def predict(self, request: CostPredictRequest) -> CostPredictResponse:
        """Return cost / runtime prediction for the requested model run."""
        if self._model is not None:
            return self._model_predict(request)
        return self._heuristic_predict(request)

    def train(self, telemetry_data: list[dict]) -> None:
        """Train (or retrain) the cost model from historical telemetry.

        After training the model is persisted to ``self._model_path``.
        """
        if not telemetry_data:
            logger.warning("No telemetry data provided -- skipping training")
            return

        features, targets = extract_features(telemetry_data)
        if features.shape[0] == 0:
            logger.warning("Feature extraction yielded 0 rows -- skipping training")
            return

        self._model = CostModelTrainer.train(features, targets)
        logger.info(
            "Trained cost model on %d samples (features=%d)",
            features.shape[0],
            features.shape[1],
        )

        if self._model_path is not None:
            self._model_path.parent.mkdir(parents=True, exist_ok=True)
            CostModelTrainer.save(self._model, self._model_path)
            logger.info("Saved cost model to %s", self._model_path)

    # ------------------------------------------------------------------
    # Trained-model path
    # ------------------------------------------------------------------

    def _model_predict(self, request: CostPredictRequest) -> CostPredictResponse:
        """Predict using the trained LinearRegression model.

        Constructs the same 8-feature vector that ``extract_features``
        produces during training:

          [partition_count, log1p_data_volume_bytes, num_workers,
           sql_complexity_score, join_count, cte_count,
           has_window_functions, distinct_table_count]

        SQL-complexity features default to 0.0 when not available at
        prediction time (the model was trained on telemetry that may or
        may not include SQL metadata).
        """
        assert self._model is not None

        volume_log = np.log1p(request.data_volume_bytes) if request.data_volume_bytes is not None else 0.0
        workers = float(max(request.num_workers, 1)) if request.num_workers is not None else 1.0

        features = np.array(
            [
                [
                    float(request.partition_count),
                    volume_log,
                    workers,
                    0.0,  # sql_complexity_score
                    0.0,  # join_count
                    0.0,  # cte_count
                    0.0,  # has_window_functions
                    0.0,  # distinct_table_count
                ]
            ],
            dtype=np.float64,
        )
        predicted_seconds = float(CostModelTrainer.predict(self._model, features)[0])
        # Clamp to sane range
        predicted_seconds = max(predicted_seconds, 30.0)

        runtime_minutes = predicted_seconds / 60.0
        cost_rate = _COST_RATES.get(request.cluster_size, _COST_RATES["medium"])
        cost_usd = runtime_minutes * cost_rate

        confidence = 0.8
        lower, upper, label = _compute_confidence_band(round(cost_usd, 4), confidence)

        return CostPredictResponse(
            estimated_runtime_minutes=round(runtime_minutes, 2),
            estimated_cost_usd=round(cost_usd, 4),
            confidence=confidence,
            cost_lower_bound_usd=lower,
            cost_upper_bound_usd=upper,
            confidence_label=label,
        )

    # ------------------------------------------------------------------
    # Heuristic fallback
    # ------------------------------------------------------------------

    def _heuristic_predict(
        self,
        request: CostPredictRequest,
    ) -> CostPredictResponse:
        """Deterministic heuristic when no trained model is available."""

        if request.historical_runtime_avg is not None and request.historical_runtime_avg > 0:
            predicted_seconds = request.historical_runtime_avg
        else:
            predicted_seconds = _HEURISTIC_BASE_SECONDS + request.partition_count * _HEURISTIC_PER_PARTITION_SECONDS

        # Scale by data volume if available (logarithmic factor)
        if request.data_volume_bytes is not None and request.data_volume_bytes > 0:
            volume_factor = 1.0 + np.log1p(request.data_volume_bytes) / 50.0
            predicted_seconds *= volume_factor

        # Scale by workers (diminishing returns)
        if request.num_workers is not None and request.num_workers > 1:
            parallelism_factor = 1.0 / (1.0 + np.log2(request.num_workers))
            predicted_seconds *= parallelism_factor

        predicted_seconds = max(predicted_seconds, 30.0)
        runtime_minutes = predicted_seconds / 60.0
        cost_rate = _COST_RATES.get(request.cluster_size, _COST_RATES["medium"])
        cost_usd = runtime_minutes * cost_rate

        confidence = 0.4
        lower, upper, label = _compute_confidence_band(round(cost_usd, 4), confidence)

        return CostPredictResponse(
            estimated_runtime_minutes=round(runtime_minutes, 2),
            estimated_cost_usd=round(cost_usd, 4),
            confidence=confidence,
            cost_lower_bound_usd=lower,
            cost_upper_bound_usd=upper,
            confidence_label=label,
        )
