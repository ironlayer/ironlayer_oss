"""Router for dependency fragility scoring endpoints."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from ai_engine.engines.fragility_scorer import FragilityScorer
from ai_engine.models.requests import FragilityBatchRequest, FragilityScoreRequest
from ai_engine.models.responses import (
    FragilityBatchResponse,
    FragilityScoreResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fragility"])

_scorer: FragilityScorer | None = None


def get_scorer() -> FragilityScorer:
    """Return the module-level fragility scorer instance."""
    if _scorer is None:
        raise RuntimeError("FragilityScorer not initialised")
    return _scorer


def init_scorer(scorer: FragilityScorer) -> None:
    """Called at startup to inject the scorer."""
    global _scorer  # noqa: PLW0603
    _scorer = scorer


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/fragility_score",
    response_model=FragilityScoreResponse,
    summary="Compute fragility score for a single model",
)
async def fragility_score(
    request: FragilityScoreRequest,
    scorer: Annotated[FragilityScorer, Depends(get_scorer)],
) -> FragilityScoreResponse:
    """Return the composite fragility score for a model in the DAG.

    The score combines own failure risk, upstream propagation risk,
    and downstream cascade risk into a single 0-10 metric.

    Advisory only -- does **not** block or modify any execution.
    """
    logger.info(
        "Computing fragility for model=%s dag_size=%d",
        request.model_name,
        len(request.dag),
    )

    result = scorer.compute_fragility(
        model_name=request.model_name,
        dag=request.dag,
        failure_predictions=request.failure_predictions,
    )

    return FragilityScoreResponse(
        model_name=result.model_name,
        own_risk=result.own_risk,
        upstream_risk=result.upstream_risk,
        cascade_risk=result.cascade_risk,
        fragility_score=result.fragility_score,
        critical_path=result.critical_path,
        risk_factors=result.risk_factors,
    )


@router.post(
    "/fragility_score/batch",
    response_model=FragilityBatchResponse,
    summary="Compute fragility scores for all models in a DAG",
)
async def fragility_score_batch(
    request: FragilityBatchRequest,
    scorer: Annotated[FragilityScorer, Depends(get_scorer)],
) -> FragilityBatchResponse:
    """Return fragility scores for every model in the DAG, sorted by
    descending fragility score.

    Advisory only -- does **not** block or modify any execution.
    """
    logger.info(
        "Computing batch fragility for dag_size=%d",
        len(request.dag),
    )

    results = scorer.compute_batch(
        dag=request.dag,
        failure_predictions=request.failure_predictions,
    )

    return FragilityBatchResponse(
        scores=[
            FragilityScoreResponse(
                model_name=r.model_name,
                own_risk=r.own_risk,
                upstream_risk=r.upstream_risk,
                cascade_risk=r.cascade_risk,
                fragility_score=r.fragility_score,
                critical_path=r.critical_path,
                risk_factors=r.risk_factors,
            )
            for r in results
        ]
    )
