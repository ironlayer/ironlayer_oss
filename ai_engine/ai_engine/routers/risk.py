"""Router for risk scoring advisory."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.risk_scorer import RiskScorer
from ai_engine.models.requests import RiskScoreRequest
from ai_engine.models.responses import RiskScoreResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["risk"])

_scorer: RiskScorer | None = None
_cache: ResponseCache | None = None


def get_scorer() -> RiskScorer:
    """Return the module-level scorer instance."""
    if _scorer is None:
        raise RuntimeError("RiskScorer not initialised")
    return _scorer


def init_scorer(scorer: RiskScorer, cache: ResponseCache | None = None) -> None:
    """Called at startup to inject the scorer and optional cache."""
    global _scorer, _cache  # noqa: PLW0603
    _scorer = scorer
    _cache = cache


@router.post(
    "/risk_score",
    response_model=RiskScoreResponse,
    summary="Score the risk of deploying a model change",
)
async def risk_score(
    request: RiskScoreRequest,
    scorer: Annotated[RiskScorer, Depends(get_scorer)],
) -> RiskScoreResponse:
    """Return a deterministic risk score with contributing factors.

    Advisory only -- does **not** approve or reject any plan.
    """
    # Check cache
    if _cache is not None:
        cache_key = ResponseCache.make_key(
            "risk_score",
            request.model_dump(mode="json"),
        )
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    logger.info("Scoring risk for model=%s downstream_depth=%d", request.model_name, request.downstream_depth)
    result = scorer.score(request)

    # Store in cache
    if _cache is not None:
        _cache.put(cache_key, result, "risk_score")

    return result
