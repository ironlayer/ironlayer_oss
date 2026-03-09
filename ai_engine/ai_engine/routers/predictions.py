"""Router for predictive intelligence endpoints."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, field_validator

from ai_engine.engines.failure_predictor import (
    FailurePredictor,
    RunHistory,
    compute_cost_trend,
)
from ai_engine.models.requests import _check_model_name

logger = logging.getLogger(__name__)

router = APIRouter(tags=["predictions"])

_predictor: FailurePredictor | None = None


def get_predictor() -> FailurePredictor:
    """Return the module-level failure predictor instance."""
    if _predictor is None:
        raise RuntimeError("FailurePredictor not initialised")
    return _predictor


def init_predictor(predictor: FailurePredictor) -> None:
    """Called at startup to inject the predictor."""
    global _predictor  # noqa: PLW0603
    _predictor = predictor


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class PredictFailureRequest(BaseModel):
    """Request body for ``POST /predict_failure``."""

    model_name: str = Field(..., description="Model to predict failure for.")
    total_runs: int = Field(default=0, ge=0)
    failed_runs: int = Field(default=0, ge=0)
    recent_runs: int = Field(default=0, ge=0, description="Runs in last 7 days.")
    recent_failures: int = Field(default=0, ge=0, description="Failures in last 7 days.")
    consecutive_failures: int = Field(default=0, ge=0)
    avg_runtime_seconds: float = Field(default=0.0, ge=0.0)
    recent_avg_runtime_seconds: float = Field(default=0.0, ge=0.0)
    runtime_trend: float = Field(default=0.0, description="% change in runtime (recent vs historical).")
    avg_shuffle_bytes: float = Field(default=0.0, ge=0.0)
    recent_avg_shuffle_bytes: float = Field(default=0.0, ge=0.0)
    shuffle_trend: float = Field(default=0.0, description="% change in shuffle volume.")
    hours_since_last_success: float = Field(default=0.0, ge=0.0)
    last_error_type: str | None = Field(default=None)
    tenant_id: str | None = Field(default=None, description="Tenant identifier (tracing).")

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        return _check_model_name(v)


class PredictFailureResponse(BaseModel):
    """Response from ``POST /predict_failure``."""

    model_name: str
    failure_probability: float = Field(ge=0.0, le=1.0)
    risk_level: str = Field(description="low, medium, or high")
    factors: list[str]
    suggested_actions: list[str]


# Maximum number of cost data points per list to prevent memory abuse.
_MAX_COST_ENTRIES: int = 1_000


class PredictCostTrendRequest(BaseModel):
    """Request body for ``POST /predict_cost_trend``."""

    model_name: str = Field(..., description="Model to analyse cost trend for.")
    recent_costs: list[float] = Field(..., description="Costs from the recent period (e.g. last 7 days).")
    historical_costs: list[float] = Field(..., description="Costs from the baseline period (e.g. last 30 days).")
    runs_per_month: float = Field(default=30.0, ge=0.0, description="Expected runs per month for projection.")
    alert_threshold_pct: float = Field(
        default=0.3,
        ge=0.0,
        le=5.0,
        description="Growth % threshold for alert (0.3 = 30%).",
    )
    tenant_id: str | None = Field(default=None, description="Tenant identifier (tracing).")

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        return _check_model_name(v)

    @field_validator("recent_costs")
    @classmethod
    def validate_recent_costs(cls, v: list[float]) -> list[float]:
        if len(v) > _MAX_COST_ENTRIES:
            raise ValueError(f"recent_costs has {len(v)} entries; maximum is {_MAX_COST_ENTRIES}")
        return v

    @field_validator("historical_costs")
    @classmethod
    def validate_historical_costs(cls, v: list[float]) -> list[float]:
        if len(v) > _MAX_COST_ENTRIES:
            raise ValueError(f"historical_costs has {len(v)} entries; maximum is {_MAX_COST_ENTRIES}")
        return v


class PredictCostTrendResponse(BaseModel):
    """Response from ``POST /predict_cost_trend``."""

    model_name: str
    current_avg_cost_usd: float
    previous_avg_cost_usd: float
    cost_change_pct: float
    projected_monthly_cost_usd: float
    trend_direction: str
    factors: list[str]
    alert: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/predict_failure",
    response_model=PredictFailureResponse,
    summary="Predict failure probability for a model",
)
async def predict_failure(
    request: PredictFailureRequest,
    predictor: Annotated[FailurePredictor, Depends(get_predictor)],
) -> PredictFailureResponse:
    """Return failure probability with contributing factors.

    Advisory only -- does **not** block or modify any execution.
    """
    logger.info(
        "Predicting failure for model=%s total_runs=%d failed=%d",
        request.model_name,
        request.total_runs,
        request.failed_runs,
    )

    history = RunHistory(
        model_name=request.model_name,
        total_runs=request.total_runs,
        failed_runs=request.failed_runs,
        recent_runs=request.recent_runs,
        recent_failures=request.recent_failures,
        consecutive_failures=request.consecutive_failures,
        avg_runtime_seconds=request.avg_runtime_seconds,
        recent_avg_runtime_seconds=request.recent_avg_runtime_seconds,
        runtime_trend=request.runtime_trend,
        avg_shuffle_bytes=request.avg_shuffle_bytes,
        recent_avg_shuffle_bytes=request.recent_avg_shuffle_bytes,
        shuffle_trend=request.shuffle_trend,
        hours_since_last_success=request.hours_since_last_success,
        last_error_type=request.last_error_type,
    )

    prediction = predictor.predict(history)

    return PredictFailureResponse(
        model_name=prediction.model_name,
        failure_probability=prediction.failure_probability,
        risk_level=prediction.risk_level,
        factors=prediction.factors,
        suggested_actions=prediction.suggested_actions,
    )


@router.post(
    "/predict_cost_trend",
    response_model=PredictCostTrendResponse,
    summary="Analyse cost trend for a model",
)
async def predict_cost_trend(
    request: PredictCostTrendRequest,
) -> PredictCostTrendResponse:
    """Return cost trend analysis with projections.

    Advisory only -- does **not** block or modify any execution.
    """
    logger.info(
        "Analysing cost trend for model=%s recent=%d historical=%d",
        request.model_name,
        len(request.recent_costs),
        len(request.historical_costs),
    )

    trend = compute_cost_trend(
        model_name=request.model_name,
        recent_costs=request.recent_costs,
        historical_costs=request.historical_costs,
        runs_per_month=request.runs_per_month,
        alert_threshold_pct=request.alert_threshold_pct,
    )

    return PredictCostTrendResponse(
        model_name=trend.model_name,
        current_avg_cost_usd=trend.current_avg_cost_usd,
        previous_avg_cost_usd=trend.previous_avg_cost_usd,
        cost_change_pct=trend.cost_change_pct,
        projected_monthly_cost_usd=trend.projected_monthly_cost_usd,
        trend_direction=trend.trend_direction,
        factors=trend.factors,
        alert=trend.alert,
    )
