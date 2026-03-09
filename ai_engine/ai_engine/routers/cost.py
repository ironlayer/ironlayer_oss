"""Router for cost prediction advisory."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.cost_predictor import CostPredictor
from ai_engine.models.requests import CostPredictRequest
from ai_engine.models.responses import CostPredictResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["cost"])

_predictor: CostPredictor | None = None
_cache: ResponseCache | None = None


def get_predictor() -> CostPredictor:
    """Return the module-level predictor instance."""
    if _predictor is None:
        raise RuntimeError("CostPredictor not initialised")
    return _predictor


def init_predictor(predictor: CostPredictor, cache: ResponseCache | None = None) -> None:
    """Called at startup to inject the predictor and optional cache."""
    global _predictor, _cache  # noqa: PLW0603
    _predictor = predictor
    _cache = cache


@router.post(
    "/predict_cost",
    response_model=CostPredictResponse,
    summary="Predict execution cost for a model run",
)
async def predict_cost(
    request: CostPredictRequest,
    predictor: Annotated[CostPredictor, Depends(get_predictor)],
) -> CostPredictResponse:
    """Return cost and runtime predictions.

    Advisory only -- does **not** trigger any execution.
    """
    # Check cache
    if _cache is not None:
        cache_key = ResponseCache.make_key(
            "predict_cost",
            request.model_dump(mode="json"),
        )
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    logger.info("Predicting cost for model=%s partitions=%d", request.model_name, request.partition_count)
    result = predictor.predict(request)

    # Store in cache
    if _cache is not None:
        _cache.put(cache_key, result, "predict_cost")

    return result
