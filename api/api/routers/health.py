"""Health-check and readiness probe endpoints.

The ``/health`` endpoint (liveness) is registered under the versioned API
prefix (``/api/v1/health``).  The ``/ready`` endpoint is a Kubernetes-style
readiness probe registered at the application root (no version prefix) so
that orchestrators and load-balancers can gate traffic independently of the
API version.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api import __version__
from api.dependencies import AIClientDep, get_db_session

logger = logging.getLogger(__name__)

# Non-tenant-scoped session for health/readiness probes.
HealthSessionDep = Annotated[AsyncSession, Depends(get_db_session)]

# Short timeout for AI engine health check so probes respond quickly.
_AI_HEALTH_TIMEOUT = 2.0

router = APIRouter(tags=["health"])


async def _check_ai_health(ai_client: AIClientDep) -> bool:
    """Check AI engine health with a short timeout."""
    try:
        return await asyncio.wait_for(ai_client.health_check(), timeout=_AI_HEALTH_TIMEOUT)
    except (TimeoutError, Exception):
        return False


@router.get("/health")
async def health(
    session: HealthSessionDep,
    ai_client: AIClientDep,
) -> dict[str, Any]:
    """Return service health with optional dependency checks.

    The endpoint always returns HTTP 200 so that load-balancers see the
    service as alive.  The ``db`` and ``ai_engine`` fields indicate
    whether downstream dependencies are reachable.
    """
    result: dict[str, Any] = {
        "status": "healthy",
        "version": __version__,
        "db": "ok",
        "ai_engine": "ok",
    }

    # Database connectivity check.
    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("DB health check failed: %s", exc)
        result["db"] = "degraded"

    # AI engine connectivity check (graceful degradation, short timeout).
    if not await _check_ai_health(ai_client):
        result["ai_engine"] = "unavailable"

    return result


# ---------------------------------------------------------------------------
# Readiness probe (outside API versioning)
# ---------------------------------------------------------------------------

readiness_router = APIRouter(tags=["infrastructure"])


@readiness_router.get("/ready")
async def readiness_probe(
    session: HealthSessionDep,
    ai_client: AIClientDep,
) -> JSONResponse:
    """Kubernetes-style readiness probe.

    Checks:
    1. **Database connectivity** — executes ``SELECT 1``.
    2. **AI engine connectivity** — graceful; returns ``degraded`` if
       unavailable (the platform can still function without AI advisory).

    Returns HTTP 200 with ``"ready"`` or ``"degraded"`` status, or HTTP 503
    with ``"not_ready"`` if the database is unreachable.
    """
    checks: dict[str, str] = {
        "db": "ok",
        "ai_engine": "ok",
    }
    overall = "ready"

    # Database (critical — gates readiness).
    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Readiness: DB check failed: %s", exc)
        checks["db"] = "unavailable"
        overall = "not_ready"

    # AI engine (non-critical — degraded is acceptable, short timeout).
    if not await _check_ai_health(ai_client):
        checks["ai_engine"] = "unavailable"
        if overall == "ready":
            overall = "degraded"

    status_code = 200 if overall != "not_ready" else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall,
            "version": __version__,
            "checks": checks,
        },
    )
