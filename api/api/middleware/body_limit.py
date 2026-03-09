"""Request body size limit middleware.

Rejects requests whose Content-Length exceeds a configured maximum to
mitigate DoS and memory exhaustion.  The check is performed using the
Content-Length header before any body is consumed.  Requests that omit
the header (e.g. chunked transfer-encoding) are allowed through; downstream
Pydantic validators enforce field-level limits where needed.
"""

from __future__ import annotations

import logging

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


class BodyLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests with Content-Length exceeding max_body_size bytes."""

    def __init__(self, app: ASGIApp, *, max_body_size: int = 1_048_576) -> None:
        super().__init__(app)
        self._max_size = max_body_size

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                length = int(content_length)
            except (ValueError, OverflowError):
                return JSONResponse(
                    {"detail": "Invalid Content-Length header"},
                    status_code=400,
                )
            if length > self._max_size:
                logger.warning(
                    "Rejected request to %s: Content-Length %d exceeds limit %d",
                    request.url.path,
                    length,
                    self._max_size,
                )
                return JSONResponse(
                    {"detail": f"Request body too large. Maximum: {self._max_size} bytes"},
                    status_code=413,
                )
        return await call_next(request)
