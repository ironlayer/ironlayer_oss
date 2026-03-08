"""Tests for the W3C Trace Context middleware.

Covers traceparent parsing/generation, X-Trace-ID response header,
trace context propagation to logs and AI client, and W3C format compliance.
"""

from __future__ import annotations

import logging
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.middleware.trace_context import (
    TraceContextMiddleware,
    TraceLoggingFilter,
    _generate_span_id,
    _generate_trace_id,
    get_span_id,
    get_trace_context,
    get_trace_flags,
    get_trace_id,
    get_traceparent,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_TRACEPARENT = "00-4bf92f3577b16e8153e785e29fc5f28c-d75597dee50b0cac-01"
_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_HEX_16 = re.compile(r"^[0-9a-f]{16}$")


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------


class TestIDGeneration:
    def test_trace_id_length(self) -> None:
        tid = _generate_trace_id()
        assert _HEX_32.match(tid), f"Expected 32-hex trace_id, got: {tid}"

    def test_span_id_length(self) -> None:
        sid = _generate_span_id()
        assert _HEX_16.match(sid), f"Expected 16-hex span_id, got: {sid}"

    def test_uniqueness(self) -> None:
        ids = {_generate_trace_id() for _ in range(100)}
        assert len(ids) == 100, "trace_id generation should be unique"


# ---------------------------------------------------------------------------
# Traceparent parsing
# ---------------------------------------------------------------------------


class TestTraceparentParsing:
    def test_valid_traceparent(self) -> None:
        trace_id, span_id, flags = TraceContextMiddleware._parse_traceparent(_VALID_TRACEPARENT)
        assert trace_id == "4bf92f3577b16e8153e785e29fc5f28c"
        assert span_id == "d75597dee50b0cac"
        assert flags == "01"

    def test_empty_string(self) -> None:
        trace_id, span_id, flags = TraceContextMiddleware._parse_traceparent("")
        assert trace_id == ""
        assert span_id == ""

    def test_invalid_format(self) -> None:
        trace_id, span_id, flags = TraceContextMiddleware._parse_traceparent("not-a-traceparent")
        assert trace_id == ""

    def test_version_ff_invalid(self) -> None:
        header = "ff-4bf92f3577b16e8153e785e29fc5f28c-d75597dee50b0cac-01"
        trace_id, _, _ = TraceContextMiddleware._parse_traceparent(header)
        assert trace_id == "", "version ff should be rejected"

    def test_all_zero_trace_id(self) -> None:
        header = f"00-{'0' * 32}-d75597dee50b0cac-01"
        trace_id, _, _ = TraceContextMiddleware._parse_traceparent(header)
        assert trace_id == "", "all-zero trace_id should be rejected"

    def test_all_zero_span_id(self) -> None:
        header = f"00-4bf92f3577b16e8153e785e29fc5f28c-{'0' * 16}-01"
        trace_id, _, _ = TraceContextMiddleware._parse_traceparent(header)
        assert trace_id == "", "all-zero span_id should be rejected"

    def test_case_insensitive(self) -> None:
        header = "00-4BF92F3577B16E8153E785E29FC5F28C-D75597DEE50B0CAC-01"
        trace_id, span_id, flags = TraceContextMiddleware._parse_traceparent(header)
        assert trace_id == "4bf92f3577b16e8153e785e29fc5f28c"
        assert span_id == "d75597dee50b0cac"


# ---------------------------------------------------------------------------
# Middleware integration (using Starlette TestClient)
# ---------------------------------------------------------------------------


class TestMiddlewareIntegration:
    """Test middleware behaviour via the full ASGI application."""

    @pytest.fixture()
    def _app(self):
        """Create a minimal ASGI app with TraceContextMiddleware."""
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        async def handler(request):
            return JSONResponse(
                {
                    "trace_id": getattr(request.state, "trace_id", ""),
                    "span_id": getattr(request.state, "span_id", ""),
                    "parent_span_id": getattr(request.state, "parent_span_id", ""),
                }
            )

        app = Starlette(routes=[Route("/test", handler)])
        app.add_middleware(TraceContextMiddleware)
        return app

    @pytest.fixture()
    def client(self, _app):
        from starlette.testclient import TestClient

        return TestClient(_app)

    def test_no_traceparent_generates_new_ids(self, client) -> None:
        resp = client.get("/test")
        assert resp.status_code == 200
        data = resp.json()

        assert _HEX_32.match(data["trace_id"])
        assert _HEX_16.match(data["span_id"])
        assert data["parent_span_id"] == ""

        # Response header must include X-Trace-ID.
        assert "X-Trace-ID" in resp.headers
        assert resp.headers["X-Trace-ID"] == data["trace_id"]

    def test_valid_traceparent_parsed(self, client) -> None:
        resp = client.get("/test", headers={"traceparent": _VALID_TRACEPARENT})
        assert resp.status_code == 200
        data = resp.json()

        assert data["trace_id"] == "4bf92f3577b16e8153e785e29fc5f28c"
        # span_id should be a *new* span, not the parent.
        assert _HEX_16.match(data["span_id"])
        assert data["span_id"] != "d75597dee50b0cac"
        assert data["parent_span_id"] == "d75597dee50b0cac"

    def test_invalid_traceparent_generates_new(self, client) -> None:
        resp = client.get("/test", headers={"traceparent": "garbage"})
        data = resp.json()
        assert _HEX_32.match(data["trace_id"]), "should generate new on invalid"

    def test_x_trace_id_in_response(self, client) -> None:
        resp = client.get("/test")
        assert "X-Trace-ID" in resp.headers
        assert _HEX_32.match(resp.headers["X-Trace-ID"])


# ---------------------------------------------------------------------------
# TraceLoggingFilter
# ---------------------------------------------------------------------------


class TestTraceLoggingFilter:
    def test_injects_trace_fields(self) -> None:
        filt = TraceLoggingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )
        # Simulate context vars being set.
        from api.middleware.trace_context import _trace_id_var, _span_id_var

        token_t = _trace_id_var.set("abc123def456abc123def456abc123de")
        token_s = _span_id_var.set("1234567890abcdef")
        try:
            assert filt.filter(record) is True
            assert record.trace_id == "abc123def456abc123def456abc123de"  # type: ignore[attr-defined]
            assert record.span_id == "1234567890abcdef"  # type: ignore[attr-defined]
        finally:
            _trace_id_var.reset(token_t)
            _span_id_var.reset(token_s)

    def test_empty_when_no_context(self) -> None:
        filt = TraceLoggingFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )
        # Context vars default to empty string.
        from api.middleware.trace_context import _trace_id_var, _span_id_var

        token_t = _trace_id_var.set("")
        token_s = _span_id_var.set("")
        try:
            filt.filter(record)
            assert record.trace_id == ""  # type: ignore[attr-defined]
            assert record.span_id == ""  # type: ignore[attr-defined]
        finally:
            _trace_id_var.reset(token_t)
            _span_id_var.reset(token_s)


# ---------------------------------------------------------------------------
# get_traceparent helper
# ---------------------------------------------------------------------------


class TestGetTraceparent:
    def test_builds_w3c_format(self) -> None:
        from api.middleware.trace_context import _trace_id_var, _span_id_var, _trace_flags_var

        t1 = _trace_id_var.set("4bf92f3577b16e8153e785e29fc5f28c")
        t2 = _span_id_var.set("d75597dee50b0cac")
        t3 = _trace_flags_var.set("01")
        try:
            tp = get_traceparent()
            assert tp == "00-4bf92f3577b16e8153e785e29fc5f28c-d75597dee50b0cac-01"
        finally:
            _trace_id_var.reset(t1)
            _span_id_var.reset(t2)
            _trace_flags_var.reset(t3)

    def test_empty_when_no_context(self) -> None:
        from api.middleware.trace_context import _trace_id_var, _span_id_var

        t1 = _trace_id_var.set("")
        t2 = _span_id_var.set("")
        try:
            assert get_traceparent() == ""
        finally:
            _trace_id_var.reset(t1)
            _span_id_var.reset(t2)


# ---------------------------------------------------------------------------
# AI client forwards traceparent
# ---------------------------------------------------------------------------


class TestAIClientForwarding:
    @pytest.mark.asyncio
    async def test_traceparent_forwarded(self) -> None:
        """Verify the AI client includes traceparent in outgoing requests."""
        from api.middleware.trace_context import _trace_id_var, _span_id_var, _trace_flags_var

        t1 = _trace_id_var.set("4bf92f3577b16e8153e785e29fc5f28c")
        t2 = _span_id_var.set("d75597dee50b0cac")
        t3 = _trace_flags_var.set("01")

        try:
            import httpx
            from api.services.ai_client import AIServiceClient

            captured_headers: dict[str, str] = {}

            async def capture_transport(request: httpx.Request) -> httpx.Response:
                captured_headers.update(dict(request.headers))
                return httpx.Response(200, json={"ok": True})

            mock_transport = httpx.MockTransport(capture_transport)
            client = AIServiceClient.__new__(AIServiceClient)
            client._base_url = "http://test:8001"
            client._client = httpx.AsyncClient(
                base_url="http://test:8001",
                transport=mock_transport,
            )

            result = await client._post("/test", {"key": "value"})
            assert result == {"ok": True}
            assert "traceparent" in captured_headers
            assert captured_headers["traceparent"] == ("00-4bf92f3577b16e8153e785e29fc5f28c-d75597dee50b0cac-01")

            await client._client.aclose()
        finally:
            _trace_id_var.reset(t1)
            _span_id_var.reset(t2)
            _trace_flags_var.reset(t3)
