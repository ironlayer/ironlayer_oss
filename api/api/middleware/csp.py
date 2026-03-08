"""Content Security Policy middleware."""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# Default CSP directives -- restrictive baseline.
_DEFAULT_CSP = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; "
    "font-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)


class ContentSecurityPolicyMiddleware(BaseHTTPMiddleware):
    """Adds Content-Security-Policy and other security headers to responses."""

    def __init__(self, app, *, csp_policy: str | None = None, api_url: str | None = None) -> None:
        super().__init__(app)
        policy = csp_policy or _DEFAULT_CSP
        if api_url:
            policy += f"; connect-src 'self' {api_url}"
        else:
            policy += "; connect-src 'self'"
        self._csp = policy

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = self._csp
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response
