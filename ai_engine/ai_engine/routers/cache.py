"""Router for cache management endpoints."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from ai_engine.engines.cache import ResponseCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cache", tags=["cache"])

_cache: ResponseCache | None = None


def get_cache() -> ResponseCache:
    """Return the module-level cache instance."""
    if _cache is None:
        raise RuntimeError("ResponseCache not initialised")
    return _cache


def init_cache(cache: ResponseCache) -> None:
    """Called at startup to inject the cache."""
    global _cache  # noqa: PLW0603
    _cache = cache


@router.get(
    "/stats",
    summary="Get cache statistics",
)
async def cache_stats(
    cache: Annotated[ResponseCache, Depends(get_cache)],
) -> dict[str, Any]:
    """Return current cache hit/miss statistics and entry count."""
    return cache.stats


@router.post(
    "/invalidate",
    summary="Invalidate all cached responses",
)
async def invalidate_all(
    cache: Annotated[ResponseCache, Depends(get_cache)],
) -> dict[str, Any]:
    """Flush the entire response cache.

    Use after cost model retraining or prompt version changes.
    """
    removed = cache.invalidate_all()
    logger.info("Cache invalidated via API: %d entries removed", removed)
    return {"removed": removed}


@router.post(
    "/invalidate/{request_type}",
    summary="Invalidate cached responses by type",
)
async def invalidate_by_type(
    request_type: str,
    cache: Annotated[ResponseCache, Depends(get_cache)],
) -> dict[str, Any]:
    """Flush cached responses for a specific request type.

    Valid types: ``semantic_classify``, ``predict_cost``, ``risk_score``,
    ``optimize_sql``.
    """
    removed = cache.invalidate_by_type(request_type)
    logger.info("Cache invalidated for type=%s: %d entries removed", request_type, removed)
    return {"request_type": request_type, "removed": removed}
