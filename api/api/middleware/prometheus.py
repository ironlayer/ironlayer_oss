"""Prometheus metrics middleware for HTTP request instrumentation.

Exposes standard RED metrics (Rate, Errors, Duration) as Prometheus
counters and histograms.  Also provides application-specific gauges and
counters for plan runs, AI calls, and advisory lock tracking.

Path normalisation collapses path parameters (e.g. ``/plans/abc123`` ->
``/plans/{id}``) to prevent unbounded label cardinality.
"""

from __future__ import annotations

import logging
import re
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, Histogram

    HTTP_REQUESTS_TOTAL = Counter(
        "ironlayer_http_requests_total",
        "Total HTTP requests by method, path, and status code",
        ["method", "path", "status_code"],
    )

    HTTP_REQUEST_DURATION = Histogram(
        "ironlayer_http_request_duration_seconds",
        "HTTP request duration in seconds",
        ["method", "path"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    PLAN_RUNS_TOTAL = Counter(
        "ironlayer_plan_runs_total",
        "Total plan generation runs by outcome",
        ["outcome"],
    )

    AI_CALLS_TOTAL = Counter(
        "ironlayer_ai_calls_total",
        "Total AI advisory calls by call type and outcome",
        ["call_type", "outcome"],
    )

    ACTIVE_LOCKS = Gauge(
        "ironlayer_active_locks",
        "Number of currently held advisory locks",
    )

    OPERATION_DURATION = Histogram(
        "ironlayer_operation_duration_seconds",
        "Internal operation duration in seconds (profiled hot paths)",
        ["operation"],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    # BL-138: rate-limit rejection counter.  Uses the normalised path to
    # avoid unbounded label cardinality (same normalisation as the main
    # HTTP metrics middleware).
    RATE_LIMIT_REJECTED_TOTAL = Counter(
        "ironlayer_rate_limit_rejected_total",
        "Total requests rejected by the rate limiter",
        ["endpoint"],
    )

    # BL-140: event bus outbox backlog gauge.
    EVENT_BUS_OUTBOX_BACKLOG = Gauge(
        "ironlayer_event_bus_outbox_pending",
        "Number of outbox entries awaiting dispatch",
    )

    _METRICS_AVAILABLE = True

except ImportError:
    _METRICS_AVAILABLE = False
    logger.debug("prometheus_client not installed; metrics middleware disabled")


# ---------------------------------------------------------------------------
# Path normalisation: collapse UUIDs, hex IDs, and numeric segments
# ---------------------------------------------------------------------------

_PATH_PARAM_PATTERNS = [
    # UUIDs (8-4-4-4-12 hex format)
    (re.compile(r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"), "/{id}"),
    # Long hex strings (content hashes, plan IDs)
    (re.compile(r"/[0-9a-f]{12,64}"), "/{id}"),
    # Short alphanumeric IDs (e.g. plan-abc12345)
    (re.compile(r"/[a-z]+-[0-9a-f]{8,}"), "/{id}"),
    # Pure numeric segments
    (re.compile(r"/\d+"), "/{id}"),
]


def _normalise_path(path: str) -> str:
    """Collapse path parameters to prevent cardinality explosion."""
    for pattern, replacement in _PATH_PARAM_PATTERNS:
        path = pattern.sub(replacement, path)
    return path


# Paths excluded from metrics recording.
_SKIP_PATHS: frozenset[str] = frozenset({"/metrics", "/docs", "/redoc", "/openapi.json", "/favicon.ico"})


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Record HTTP request rate, error rate, and latency as Prometheus metrics.

    The middleware is a no-op if ``prometheus_client`` is not installed.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not _METRICS_AVAILABLE:
            return await call_next(request)

        path = request.url.path
        if path in _SKIP_PATHS:
            return await call_next(request)

        method = request.method
        normalised = _normalise_path(path)

        start = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start

        HTTP_REQUESTS_TOTAL.labels(
            method=method,
            path=normalised,
            status_code=str(response.status_code),
        ).inc()

        HTTP_REQUEST_DURATION.labels(
            method=method,
            path=normalised,
        ).observe(duration)

        return response
