"""IronLayer AI Advisory Engine -- FastAPI application entry point.

This service provides **advisory-only** intelligence: semantic
classification, cost prediction, risk scoring, and SQL optimisation
suggestions.  It **never** mutates execution plans.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ai_engine import __version__
from ai_engine.config import AISettings, load_ai_settings
from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.cost_predictor import CostPredictor
from ai_engine.engines.failure_predictor import FailurePredictor
from ai_engine.engines.fragility_scorer import FragilityScorer
from ai_engine.engines.llm_client import LLMClient
from ai_engine.engines.risk_scorer import RiskScorer
from ai_engine.engines.semantic_classifier import SemanticClassifier
from ai_engine.engines.sql_optimizer import SQLOptimizer
from ai_engine.middleware import (
    AIRateLimitMiddleware,
    RequestSizeLimitMiddleware,
    SharedSecretMiddleware,
)
from ai_engine.routers import cache as cache_router
from ai_engine.routers import cost as cost_router
from ai_engine.routers import fragility as fragility_router
from ai_engine.routers import optimize as optimize_router
from ai_engine.routers import predictions as predictions_router
from ai_engine.routers import risk as risk_router
from ai_engine.routers import semantic as semantic_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application startup / shutdown lifecycle."""
    settings: AISettings = load_ai_settings()

    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    logger.info("Starting AI Advisory Engine v%s", __version__)

    # --- Initialise response cache ---
    cache = ResponseCache(
        enabled=settings.cache_enabled,
        max_entries=settings.cache_max_entries,
    )
    cache_router.init_cache(cache)

    # --- Initialise shared components ---
    llm_client = LLMClient(settings)

    classifier = SemanticClassifier(
        llm_client=llm_client if llm_client.enabled else None,
        confidence_threshold=settings.semantic_confidence_threshold,
    )
    semantic_router.init_classifier(classifier, cache=cache)

    predictor = CostPredictor(model_path=settings.cost_model_path)
    cost_router.init_predictor(predictor, cache=cache)

    scorer = RiskScorer(
        auto_approve_threshold=settings.risk_auto_approve_threshold,
        manual_review_threshold=settings.risk_manual_review_threshold,
    )
    risk_router.init_scorer(scorer, cache=cache)

    optimizer = SQLOptimizer(
        llm_client=llm_client if llm_client.enabled else None,
    )
    optimize_router.init_optimizer(optimizer, cache=cache)

    # --- Initialise failure predictor ---
    failure_predictor = FailurePredictor()
    predictions_router.init_predictor(failure_predictor)

    # --- Initialise fragility scorer ---
    fragility_scorer = FragilityScorer()
    fragility_router.init_scorer(fragility_scorer)
    logger.info("Fragility scorer initialised")

    logger.info(
        "Engine ready (LLM=%s, trained_cost_model=%s, cache=%s)",
        "enabled" if llm_client.enabled else "disabled",
        "yes" if predictor.has_trained_model else "no",
        "enabled" if cache._enabled else "disabled",
    )

    yield  # application runs here

    logger.info("Shutting down AI Advisory Engine")


def create_app() -> FastAPI:
    """Application factory."""
    app = FastAPI(
        title="IronLayer AI Advisory Engine",
        description=(
            "Advisory-only intelligence layer.  Returns semantic "
            "classification, cost predictions, risk scores, and SQL "
            "optimisation suggestions.  Never mutates execution plans."
        ),
        version=__version__,
        lifespan=lifespan,
    )

    platform_env = os.environ.get("PLATFORM_ENV", "development")

    # Parse allowed origins from env (comma-separated) or fall back to the
    # API service's default local address.
    allowed_origins_raw = os.environ.get("AI_ENGINE_ALLOWED_ORIGINS", "http://localhost:8000")
    allowed_origins = [origin.strip() for origin in allowed_origins_raw.split(",") if origin.strip()]

    # ---------------------------------------------------------------
    # Middleware registration.
    #
    # Starlette processes middleware in *reverse* registration order:
    # the last middleware added is the first to run on an incoming
    # request.  The desired request processing order is:
    #
    #   1. RequestSizeLimitMiddleware  -- reject oversized bodies early
    #   2. SharedSecretMiddleware      -- authenticate the caller
    #   3. AIRateLimitMiddleware       -- rate-limit per tenant
    #   4. CORSMiddleware              -- add CORS response headers
    #
    # So we register them in reverse (4 → 1):
    # ---------------------------------------------------------------

    # 4. CORS -- registered first so preflight responses always carry
    #    the correct Access-Control-* headers even when subsequent
    #    middleware short-circuits (401, 413, 429).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 3. Per-tenant rate limiting (default: 60 req/min/tenant).
    app.add_middleware(AIRateLimitMiddleware)

    # 2. Shared-secret authentication.
    app.add_middleware(SharedSecretMiddleware, platform_env=platform_env)

    # 1. Request body size guard (default: 1 MiB).
    app.add_middleware(RequestSizeLimitMiddleware)

    app.include_router(semantic_router.router)
    app.include_router(cost_router.router)
    app.include_router(risk_router.router)
    app.include_router(optimize_router.router)
    app.include_router(cache_router.router)
    app.include_router(predictions_router.router)
    app.include_router(fragility_router.router)

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    return app


app = create_app()
