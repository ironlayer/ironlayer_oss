"""Router for semantic classification advisory."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.semantic_classifier import SemanticClassifier
from ai_engine.models.requests import SemanticClassifyRequest
from ai_engine.models.responses import SemanticClassifyResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["semantic"])

_classifier: SemanticClassifier | None = None
_cache: ResponseCache | None = None


def get_classifier() -> SemanticClassifier:
    """Return the module-level classifier instance."""
    assert _classifier is not None, "SemanticClassifier not initialised"
    return _classifier


def init_classifier(classifier: SemanticClassifier, cache: ResponseCache | None = None) -> None:
    """Called at startup to inject the classifier and optional cache."""
    global _classifier, _cache  # noqa: PLW0603
    _classifier = classifier
    _cache = cache


@router.post(
    "/semantic_classify",
    response_model=SemanticClassifyResponse,
    summary="Classify a SQL change semantically",
)
async def semantic_classify(
    request: SemanticClassifyRequest,
    classifier: Annotated[SemanticClassifier, Depends(get_classifier)],
) -> SemanticClassifyResponse:
    """Classify a SQL model change and return advisory metadata.

    This endpoint **never** mutates execution plans.  It returns
    classification metadata only.
    """
    # Check cache
    if _cache is not None:
        cache_key = ResponseCache.make_key(
            "semantic_classify",
            request.model_dump(mode="json"),
        )
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    logger.info("Classifying change (old_sql length=%d, new_sql length=%d)", len(request.old_sql), len(request.new_sql))
    result = classifier.classify(request)

    # Store in cache
    if _cache is not None:
        _cache.put(cache_key, result, "semantic_classify")

    return result
