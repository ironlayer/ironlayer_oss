"""Request body size limit middleware.

Rejects requests whose body exceeds a configured maximum to mitigate
DoS and memory exhaustion.

Two-layer guard:
1. **Pre-read** — checks ``Content-Length`` header before any body is
   consumed (fast reject for well-behaved clients).
2. **Streaming** — wraps the ASGI receive channel to count bytes as
   they arrive, rejecting chunked-encoded requests that exceed the
   limit without relying on ``Content-Length``.
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
    """Reject requests whose body exceeds *max_body_size* bytes."""

    def __init__(self, app: ASGIApp, *, max_body_size: int = 1_048_576) -> None:
        super().__init__(app)
        self._max_size = max_body_size

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Layer 1: fast Content-Length check.
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

        # Layer 2: streaming byte counter for chunked requests.
        bytes_received = 0
        max_size = self._max_size
        original_receive = request.receive
        exceeded = False

        async def _counting_receive():
            nonlocal bytes_received, exceeded
            message = await original_receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"")
                bytes_received += len(body)
                if bytes_received > max_size:
                    exceeded = True
                    logger.warning(
                        "Rejected chunked request to %s: streamed %d bytes exceeds limit %d",
                        request.url.path,
                        bytes_received,
                        max_size,
                    )
                    return {"type": "http.request", "body": b"", "more_body": False}
            return message

        request._receive = _counting_receive
        response = await call_next(request)

        if exceeded:
            return JSONResponse(
                {"detail": f"Request body too large. Maximum: {self._max_size} bytes"},
                status_code=413,
            )

        return response
