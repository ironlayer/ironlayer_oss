"""Frontend error reporting endpoint (BL-156).

Receives structured error reports from the React ErrorBoundary and writes
them to the structured application log.  No database writes — the entries
are picked up by the Log Analytics ingestion pipeline (or any log aggregator).
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/errors", tags=["errors"])


class FrontendErrorReport(BaseModel):
    """Payload sent by the React ErrorBoundary."""

    error_message: str = Field(..., description="Error.message from the caught exception")
    component_stack: str = Field(default="", description="React componentStack from ErrorInfo")
    route: str = Field(default="", description="window.location.pathname at the time of the error")
    user_id: str | None = Field(default=None, description="Authenticated user ID (if known)")
    timestamp: str = Field(default="", description="ISO-8601 timestamp from the browser")
    extra: dict[str, Any] = Field(default_factory=dict, description="Any additional context")


@router.post("/frontend", status_code=204)
async def report_frontend_error(body: FrontendErrorReport) -> None:
    """Accept a structured error report from the frontend ErrorBoundary.

    The report is written to the structured application log so that it
    can be ingested by Azure Log Analytics, Grafana Loki, or any other
    log aggregator configured for this deployment.

    Returns HTTP 204 No Content on success.  Errors are not returned to
    the browser to avoid leaking server-side details.
    """
    logger.error(
        "Frontend error reported",
        extra={
            "event": "frontend_error",
            "error_message": body.error_message[:500],
            "component_stack": body.component_stack[:2000],
            "route": body.route[:200],
            "user_id": body.user_id,
            "timestamp": body.timestamp,
            "extra": body.extra,
        },
    )
