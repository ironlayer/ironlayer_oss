"""Metering middleware -- records API_REQUEST events for usage tracking.

Captures every inbound HTTP request (excluding health/probe/docs paths)
as a metered ``API_REQUEST`` event.  The collector is resolved lazily at
request time because middleware instances are constructed in
``create_app()`` before the lifespan initialises the collector.
"""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# Paths that should NOT generate metering events (probes, docs, metrics).
_SKIP_PATHS: frozenset[str] = frozenset(
    {
        "/docs",
        "/redoc",
        "/openapi.json",
        "/favicon.ico",
        "/ready",
        "/metrics",
        "/api/v1/health",
    }
)


class MeteringMiddleware(BaseHTTPMiddleware):
    """Record an ``API_REQUEST`` usage event for every non-skipped request.

    The middleware is deliberately fault-tolerant: if the metering
    collector is unavailable (e.g. during startup or after shutdown),
    the request proceeds normally and the event is silently dropped.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        # Skip health probes, docs, and internal paths.
        path = request.url.path.rstrip("/") or "/"
        if path in _SKIP_PATHS:
            return response

        try:
            from core_engine.metering.events import UsageEventType

            from api.dependencies import get_metering_collector

            collector = get_metering_collector(request)
            tenant_id = getattr(request.state, "tenant_id", "anonymous")
            collector.record_event(
                tenant_id=tenant_id,
                event_type=UsageEventType.API_REQUEST,
                metadata={
                    "method": request.method,
                    "path": path,
                    "status_code": response.status_code,
                },
            )
        except RuntimeError:
            # Collector not initialised yet (startup) or already disposed
            # (shutdown).  Silently skip -- never block the request.
            pass
        except Exception:
            logger.debug("Metering middleware failed to record event", exc_info=True)

        return response
