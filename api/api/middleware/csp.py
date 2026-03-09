"""Content Security Policy middleware."""

from __future__ import annotations

import logging
import os

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# HSTS max-age: 2 years (≥ 1 year required for HSTS preload list).
# Applied only in non-dev environments — localhost does not use HTTPS.
_HSTS_VALUE = "max-age=63072000; includeSubDomains; preload"

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

        # Determine whether to add HSTS and upgrade-insecure-requests (BL-079, BL-104).
        # Skip on localhost/dev to avoid breaking HTTP-only development workflows.
        _platform_env = os.environ.get("PLATFORM_ENV", "development").lower()
        self._add_hsts = _platform_env not in ("development", "dev", "local")
        # BL-104: In non-dev environments, add upgrade-insecure-requests so that any
        # http:// subresource references are automatically upgraded to https://.
        # Belt-and-suspenders alongside HSTS.
        if self._add_hsts:
            self._csp += "; upgrade-insecure-requests"

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = self._csp
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        # BL-079: Add HSTS in non-dev environments to prevent SSL-stripping attacks.
        if self._add_hsts:
            response.headers["Strict-Transport-Security"] = _HSTS_VALUE
        return response
