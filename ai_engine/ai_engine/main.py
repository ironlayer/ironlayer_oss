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
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ai_engine import __version__
from ai_engine.config import AISettings, load_ai_settings
from ai_engine.engines.budget_guard import BudgetGuard
from ai_engine.engines.cache import ResponseCache
from ai_engine.engines.cost_predictor import CostPredictor
from ai_engine.engines.failure_predictor import FailurePredictor
from ai_engine.engines.fragility_scorer import FragilityScorer
from ai_engine.engines.in_memory_usage_repo import InMemoryLLMUsageRepo
from ai_engine.engines.llm_client import LLMClient
from ai_engine.engines.risk_scorer import RiskScorer
from ai_engine.engines.semantic_classifier import SemanticClassifier
from ai_engine.engines.sql_optimizer import SQLOptimizer
from ai_engine.middleware import (
    AIRateLimitMiddleware,
    RequestSizeLimitMiddleware,
    SharedSecretMiddleware,
)
from ai_engine.ml.model_registry import ModelRegistry
from ai_engine.routers import cache as cache_router
from ai_engine.routers import cost as cost_router
from ai_engine.routers import fragility as fragility_router
from ai_engine.routers import optimize as optimize_router
from ai_engine.routers import predictions as predictions_router
from ai_engine.routers import risk as risk_router
from ai_engine.routers import semantic as semantic_router

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OpenTelemetry (optional — no-ops when OTEL_EXPORTER_OTLP_ENDPOINT is unset)
# ---------------------------------------------------------------------------


def _configure_otel(app: FastAPI) -> None:
    """Wire OpenTelemetry SDK when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set.

    Lazy imports mean the OTel packages are only required when tracing is
    enabled.  When the env var is absent this function is a no-op.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    from opentelemetry import trace  # noqa: PLC0415
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # noqa: PLC0415
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # noqa: PLC0415
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource  # noqa: PLC0415
    from opentelemetry.sdk.trace import TracerProvider  # noqa: PLC0415
    from opentelemetry.sdk.trace.export import BatchSpanProcessor  # noqa: PLC0415

    resource = Resource.create({SERVICE_NAME: "ironlayer-ai"})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)

    logger.info("OpenTelemetry enabled — exporting traces to %s", endpoint)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application startup / shutdown lifecycle."""
    settings: AISettings = load_ai_settings()

    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    logger.info("Starting AI Advisory Engine v%s", __version__)
    # BL-077: Log the active LLM model at startup (validated against allowlist).
    logger.info("LLM model: %s (enabled=%s)", settings.llm_model, settings.llm_enabled)

    # --- Initialise response cache ---
    cache = ResponseCache(
        enabled=settings.cache_enabled,
        max_entries=settings.cache_max_entries,
    )
    cache_router.init_cache(cache)

    # --- Initialise LLM budget guard (platform-level; optional) ---
    budget_guard: BudgetGuard | None = None
    if settings.llm_daily_budget_usd is not None or settings.llm_monthly_budget_usd is not None:
        usage_repo = InMemoryLLMUsageRepo()
        budget_guard = BudgetGuard(
            usage_repo,
            tenant_id="__platform__",
            daily_budget_usd=settings.llm_daily_budget_usd,
            monthly_budget_usd=settings.llm_monthly_budget_usd,
        )
        logger.info(
            "LLM budget guard enabled (daily=$%s, monthly=$%s)",
            settings.llm_daily_budget_usd,
            settings.llm_monthly_budget_usd,
        )

    # --- Initialise shared components ---
    llm_client = LLMClient(settings, budget_guard=budget_guard)

    classifier = SemanticClassifier(
        llm_client=llm_client if llm_client.enabled else None,
        confidence_threshold=settings.semantic_confidence_threshold,
    )
    semantic_router.init_classifier(classifier, cache=cache)

    registry = ModelRegistry(
        models_dir=Path(__file__).parent / "ml" / "models"
    )
    # BL-100: Predictor is constructed here but does NOT load the model file.
    # Loading is deferred to the first predict() call (lazy loading) so that
    # startup / replica scale-up is fast.  The /readiness endpoint returns 503
    # until the first successful prediction warms the predictor.
    predictor = CostPredictor(model_path=settings.cost_model_path, registry=registry)
    cost_router.init_predictor(predictor, cache=cache)
    # BL-100: Expose predictor via app.state for the /readiness endpoint.
    app.state.predictor = predictor

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
        "Engine ready (LLM=%s, cost_model=lazy, cache=%s)",
        "enabled" if llm_client.enabled else "disabled",
        "enabled" if cache._enabled else "disabled",
    )
    logger.info(
        "Cost model loading deferred to first predict() call (BL-100: lazy loading)"
    )

    yield  # application runs here

    logger.info("Shutting down AI Advisory Engine")


def create_app() -> FastAPI:
    """Application factory."""
    platform_env = os.environ.get("PLATFORM_ENV", "development")
    _is_dev = platform_env == "development"

    app = FastAPI(
        title="IronLayer AI Advisory Engine",
        description=(
            "Advisory-only intelligence layer.  Returns semantic "
            "classification, cost predictions, risk scores, and SQL "
            "optimisation suggestions.  Never mutates execution plans."
        ),
        version=__version__,
        lifespan=lifespan,
        # Disable interactive docs outside dev — OpenAPI schema leaks routes,
        # parameter names, and response shapes to unauthenticated callers.
        docs_url="/docs" if _is_dev else None,
        redoc_url="/redoc" if _is_dev else None,
        openapi_url="/openapi.json" if _is_dev else None,
    )

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
    # BL-075: Restrict CORS to only the methods and headers the AI engine
    # actually uses.  Wildcards violate least-privilege and provide no safety
    # net if a new endpoint is added without an explicit method guard.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=[
            "Content-Type",
            "Authorization",
            "X-Tenant-Id",
            "X-Internal-Secret",
        ],
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

    @app.get("/readiness", tags=["system"])
    async def readiness(request: Request) -> dict[str, str]:
        """Return 200 once cost model loading has been attempted; 503 before.

        Kubernetes / load-balancer readiness probe target (BL-100).
        Returns 503 until the first :meth:`CostPredictor.predict` call
        has triggered model loading (or determined that heuristic mode
        is in effect).  After that the endpoint returns 200 permanently.
        """
        pred: CostPredictor = getattr(request.app.state, "predictor", None)
        if pred is None or not pred.is_ready:
            raise HTTPException(status_code=503, detail="Models not yet initialized")
        return {"status": "ready"}

    # -- Exception handlers --------------------------------------------------

    @app.exception_handler(Exception)
    async def catch_all_handler(request: Request, exc: Exception) -> JSONResponse:
        """Return a safe generic error; log the full traceback server-side."""
        logger.error(
            "Unhandled %s on %s: %s",
            type(exc).__name__,
            request.url.path,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    # Configures OTel tracing when OTEL_EXPORTER_OTLP_ENDPOINT is set.
    _configure_otel(app)

    return app


app = create_app()
