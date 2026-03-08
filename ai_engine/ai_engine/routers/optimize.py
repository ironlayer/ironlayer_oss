"""Router for SQL optimisation advisory."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.sql_optimizer import SQLOptimizer
from ai_engine.models.requests import OptimizeSQLRequest
from ai_engine.models.responses import OptimizeSQLResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["optimize"])

_optimizer: SQLOptimizer | None = None
_cache: ResponseCache | None = None


def get_optimizer() -> SQLOptimizer:
    """Return the module-level optimizer instance."""
    assert _optimizer is not None, "SQLOptimizer not initialised"
    return _optimizer


def init_optimizer(optimizer: SQLOptimizer, cache: ResponseCache | None = None) -> None:
    """Called at startup to inject the optimizer and optional cache."""
    global _optimizer, _cache  # noqa: PLW0603
    _optimizer = optimizer
    _cache = cache


@router.post(
    "/optimize_sql",
    response_model=OptimizeSQLResponse,
    summary="Get SQL optimisation suggestions",
)
async def optimize_sql(
    request: OptimizeSQLRequest,
    optimizer: Annotated[SQLOptimizer, Depends(get_optimizer)],
) -> OptimizeSQLResponse:
    """Return optimisation suggestions for the given SQL.

    All suggestions are advisory and **must** be validated before use.
    The engine never mutates execution plans.
    """
    # Check cache
    if _cache is not None:
        cache_key = ResponseCache.make_key(
            "optimize_sql",
            request.model_dump(mode="json"),
        )
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    logger.info("Optimising SQL (length=%d)", len(request.sql))
    result = optimizer.optimize(request)

    # Store in cache
    if _cache is not None:
        _cache.put(cache_key, result, "optimize_sql")

    return result
