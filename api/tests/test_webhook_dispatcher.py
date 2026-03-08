"""Tests for the webhook dispatcher — HMAC signing, delivery, retries.

Uses httpx.MockTransport for deterministic HTTP simulation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from api.services.event_bus import EventPayload, EventType
from api.services.webhook_dispatcher import WebhookDispatcher

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def payload() -> EventPayload:
    """Sample event payload for all tests."""
    return EventPayload(
        event_type=EventType.PLAN_GENERATED,
        tenant_id="tenant-test",
        correlation_id="corr123abc",
        data={"plan_id": "plan_xyz"},
    )


def _make_transport(
    status_code: int = 200,
    *,
    side_effects: list[int] | None = None,
) -> httpx.MockTransport:
    """Create a mock transport that returns the given status codes.

    If ``side_effects`` is provided, each request gets the next status
    code in the list.  Otherwise every request gets ``status_code``.
    """
    call_index = {"i": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        idx = call_index["i"]
        call_index["i"] += 1
        code = side_effects[idx] if side_effects else status_code
        return httpx.Response(code, json={"ok": True})

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# HMAC Signing
# ---------------------------------------------------------------------------


class TestHMACSigning:
    """Tests for the _sign() and verify_signature() static methods."""

    def test_sign_returns_hex_digest(self) -> None:
        sig = WebhookDispatcher._sign("hello", "secret")
        # SHA-256 hex digest is 64 chars.
        assert len(sig) == 64

    def test_sign_matches_manual_hmac(self) -> None:
        body = '{"event_type": "plan.generated"}'
        secret = "my-webhook-secret"
        expected = hmac.new(
            secret.encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        assert WebhookDispatcher._sign(body, secret) == expected

    def test_sign_empty_secret_returns_empty(self) -> None:
        assert WebhookDispatcher._sign("body", "") == ""

    def test_verify_signature_valid(self) -> None:
        body = "test body"
        secret = "test-secret"
        sig = WebhookDispatcher._sign(body, secret)
        assert WebhookDispatcher.verify_signature(body, secret, sig) is True

    def test_verify_signature_invalid(self) -> None:
        body = "test body"
        secret = "test-secret"
        assert WebhookDispatcher.verify_signature(body, secret, "bad_sig") is False

    def test_verify_signature_different_secret(self) -> None:
        body = "test body"
        sig = WebhookDispatcher._sign(body, "secret-A")
        assert WebhookDispatcher.verify_signature(body, "secret-B", sig) is False


# ---------------------------------------------------------------------------
# Successful delivery
# ---------------------------------------------------------------------------


class TestSuccessfulDelivery:
    """Tests for successful webhook delivery."""

    @pytest.mark.asyncio
    async def test_deliver_success(self, payload: EventPayload) -> None:
        transport = _make_transport(200)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        results = await dispatcher.dispatch(
            payload,
            [{"url": "https://example.com/hook", "secret": "s3cret", "event_types": []}],
        )

        assert len(results) == 1
        assert results[0]["status"] == "delivered"
        assert results[0]["attempts"] == 1

    @pytest.mark.asyncio
    async def test_deliver_with_event_type_filter_match(self, payload: EventPayload) -> None:
        transport = _make_transport(200)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        results = await dispatcher.dispatch(
            payload,
            [
                {
                    "url": "https://example.com/hook",
                    "secret": "s3cret",
                    "event_types": ["plan.generated"],
                }
            ],
        )
        assert len(results) == 1
        assert results[0]["status"] == "delivered"

    @pytest.mark.asyncio
    async def test_deliver_skips_non_matching_event_type(self, payload: EventPayload) -> None:
        transport = _make_transport(200)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        results = await dispatcher.dispatch(
            payload,
            [
                {
                    "url": "https://example.com/hook",
                    "secret": "s3cret",
                    "event_types": ["run.completed"],
                }
            ],
        )
        # Should be empty — the subscription doesn't match.
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_deliver_headers_correct(self, payload: EventPayload) -> None:
        captured_requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        await dispatcher.dispatch(
            payload,
            [{"url": "https://example.com/hook", "secret": "mysecret", "event_types": []}],
        )

        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.headers["Content-Type"] == "application/json"
        assert req.headers["X-IronLayer-Event"] == "plan.generated"
        assert req.headers["X-IronLayer-Delivery"] == "corr123abc"
        assert len(req.headers["X-IronLayer-Signature"]) == 64  # HMAC hex

    @pytest.mark.asyncio
    async def test_deliver_multiple_subscriptions(self, payload: EventPayload) -> None:
        transport = _make_transport(200)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        # Patch SSRF validation since test URLs (a.com, b.com) won't resolve
        # in the test environment.  Delivery logic is the SUT here.
        with patch("api.services.webhook_dispatcher._validate_webhook_url"):
            results = await dispatcher.dispatch(
                payload,
                [
                    {"url": "https://a.com/hook", "secret": "s1", "event_types": []},
                    {"url": "https://b.com/hook", "secret": "s2", "event_types": []},
                ],
            )
        assert len(results) == 2
        assert all(r["status"] == "delivered" for r in results)


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


class TestRetryBehaviour:
    """Tests for exponential backoff and retry logic."""

    @pytest.mark.asyncio
    async def test_retry_on_server_error(self, payload: EventPayload) -> None:
        # 500, 500, 200 — success on third attempt.
        transport = _make_transport(side_effects=[500, 500, 200])
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        with patch("api.services.webhook_dispatcher.asyncio.sleep", new_callable=AsyncMock):
            results = await dispatcher.dispatch(
                payload,
                [{"url": "https://example.com/hook", "secret": "s", "event_types": []}],
            )

        assert results[0]["status"] == "delivered"
        assert results[0]["attempts"] == 3

    @pytest.mark.asyncio
    async def test_exhausted_retries(self, payload: EventPayload) -> None:
        transport = _make_transport(500)  # always fails
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        with patch("api.services.webhook_dispatcher.asyncio.sleep", new_callable=AsyncMock):
            results = await dispatcher.dispatch(
                payload,
                [{"url": "https://example.com/hook", "secret": "s", "event_types": []}],
            )

        assert results[0]["status"] == "failed"
        assert results[0]["attempts"] == 3
        assert "500" in results[0]["error"]


# ---------------------------------------------------------------------------
# Timeout and connection errors
# ---------------------------------------------------------------------------


class TestTimeoutAndErrors:
    """Tests for timeout and network error handling."""

    @pytest.mark.asyncio
    async def test_timeout_retries_and_fails(self, payload: EventPayload) -> None:
        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("Connection timed out")

        transport = httpx.MockTransport(timeout_handler)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        with patch("api.services.webhook_dispatcher.asyncio.sleep", new_callable=AsyncMock):
            results = await dispatcher.dispatch(
                payload,
                [{"url": "https://example.com/hook", "secret": "s", "event_types": []}],
            )

        assert results[0]["status"] == "failed"
        assert results[0]["error"] == "timeout"

    @pytest.mark.asyncio
    async def test_connection_error_retries(self, payload: EventPayload) -> None:
        def error_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

        transport = httpx.MockTransport(error_handler)
        client = httpx.AsyncClient(transport=transport)
        dispatcher = WebhookDispatcher(http_client=client)

        with patch("api.services.webhook_dispatcher.asyncio.sleep", new_callable=AsyncMock):
            results = await dispatcher.dispatch(
                payload,
                [{"url": "https://example.com/hook", "secret": "s", "event_types": []}],
            )

        assert results[0]["status"] == "failed"
        assert "Connection refused" in results[0]["error"]


# ---------------------------------------------------------------------------
# Client lifecycle
# ---------------------------------------------------------------------------


class TestClientLifecycle:
    """Tests for WebhookDispatcher client ownership."""

    @pytest.mark.asyncio
    async def test_close_owned_client(self) -> None:
        dispatcher = WebhookDispatcher()
        # Should not raise.
        await dispatcher.close()

    @pytest.mark.asyncio
    async def test_close_injected_client_not_closed(self) -> None:
        client = AsyncMock(spec=httpx.AsyncClient)
        dispatcher = WebhookDispatcher(http_client=client)
        await dispatcher.close()
        # Should not call aclose() on injected client.
        client.aclose.assert_not_called()
