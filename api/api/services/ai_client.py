"""HTTP client for the IronLayer AI advisory engine."""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, cast

import httpx

logger = logging.getLogger(__name__)

try:
    from api.middleware.prometheus import AI_CALLS_TOTAL

    _AI_METRICS = True
except ImportError:
    _AI_METRICS = False

# ---------------------------------------------------------------------------
# Prompt injection sanitization
# ---------------------------------------------------------------------------

_PROMPT_INJECTION_PATTERNS = re.compile(
    r"<\|system\|>|<\|user\|>|<\|assistant\|>|"
    r"Human:|Assistant:|"
    r"\[INST\]|\[/INST\]|"
    r"<<SYS>>|<</SYS>>|"
    r"<\|im_start\|>|<\|im_end\|>|"
    r"system\s*:|"
    r"\n\s*ignore\s+previous|"
    r"\n\s*forget\s+(previous|all|the\s+above)|"
    r"[\u0400-\u04ff\u0370-\u03ff]",
    re.IGNORECASE,
)
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")  # Keep \n, \t, \r
_MAX_FIELD_SIZE = 50 * 1024  # 50 KB per field


def _sanitize_ai_input(value: str, field_name: str = "input") -> str:
    """Sanitize user-derived context before sending to AI engine.

    Strips control characters (preserving newlines and tabs), removes
    known prompt injection role/delimiter markers, and truncates
    oversized inputs to prevent resource exhaustion.

    Parameters
    ----------
    value:
        Raw user-derived string to sanitize.
    field_name:
        Human-readable field name used in truncation messages.

    Returns
    -------
    str
        The sanitized string, safe for inclusion in AI engine payloads.
    """
    # Strip control characters (keep newlines, tabs, carriage returns).
    cleaned = _CONTROL_CHARS.sub("", value)
    # Strip prompt injection markers.
    cleaned = _PROMPT_INJECTION_PATTERNS.sub("[FILTERED]", cleaned)
    # Truncate to max size.
    if len(cleaned) > _MAX_FIELD_SIZE:
        cleaned = cleaned[:_MAX_FIELD_SIZE] + f"\n[TRUNCATED: {field_name} exceeded {_MAX_FIELD_SIZE} bytes]"
    return cleaned


def _sanitize_dict(d: dict[str, Any], parent_name: str = "dict") -> dict[str, Any]:
    """Recursively sanitize all string values in a dictionary.

    Non-string leaf values (int, float, bool, None) are passed through
    unchanged.  Nested dicts and lists are traversed recursively.

    Parameters
    ----------
    d:
        The dictionary to sanitize.
    parent_name:
        Context name for truncation messages.

    Returns
    -------
    dict
        A new dictionary with all string values sanitized.
    """
    result: dict[str, Any] = {}
    for key, val in d.items():
        field = f"{parent_name}.{key}"
        if isinstance(val, str):
            result[key] = _sanitize_ai_input(val, field)
        elif isinstance(val, dict):
            result[key] = _sanitize_dict(val, field)
        elif isinstance(val, list):
            result[key] = _sanitize_list(val, field)
        else:
            result[key] = val
    return result


def _sanitize_list(items: list[Any], parent_name: str = "list") -> list[Any]:
    """Recursively sanitize all string values in a list.

    Parameters
    ----------
    items:
        The list to sanitize.
    parent_name:
        Context name for truncation messages.

    Returns
    -------
    list
        A new list with all string values sanitized.
    """
    result: list[Any] = []
    for i, val in enumerate(items):
        field = f"{parent_name}[{i}]"
        if isinstance(val, str):
            result.append(_sanitize_ai_input(val, field))
        elif isinstance(val, dict):
            result.append(_sanitize_dict(val, field))
        elif isinstance(val, list):
            result.append(_sanitize_list(val, field))
        else:
            result.append(val)
    return result


class _CircuitBreaker:
    """Per-instance half-open circuit breaker for the AI advisory engine.

    Trips after ``fail_max`` consecutive *network-level* failures
    (``httpx.RequestError`` — timeouts, connection refused, DNS failures).
    HTTP error responses (4xx/5xx) do **not** trip the circuit because they
    indicate the server is reachable and responding, not that it is down.

    States
    ------
    closed    Normal operation; failures are counted.
    open      All requests are rejected immediately (returns ``None``).
              Transitions to ``half_open`` automatically after
              ``reset_timeout`` seconds.
    half_open Probe requests are allowed through (soft half-open; no
              concurrency gate).  Success → closed; failure → open
              (reset timeout restarts).
    """

    def __init__(self, fail_max: int = 5, reset_timeout: float = 30.0) -> None:
        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        self._failures = 0
        self._state = "closed"
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        """Return the current state, auto-transitioning open → half_open."""
        if self._state == "open":
            if time.monotonic() - self._opened_at >= self._reset_timeout:
                self._state = "half_open"
        return self._state

    def is_open(self) -> bool:
        """Return ``True`` if the circuit is open (request should be rejected)."""
        return self.state == "open"

    def on_success(self) -> None:
        """Record a successful or HTTP-level response (server is reachable)."""
        self._failures = 0
        self._state = "closed"

    def on_failure(self) -> None:
        """Record a network-level failure and possibly open the circuit."""
        self._failures += 1
        if self._state == "half_open" or self._failures >= self._fail_max:
            self._state = "open"
            self._opened_at = time.monotonic()


class AIServiceClient:
    """Thin async wrapper around the AI engine REST API.

    Every public method returns ``None`` on failure so that callers can
    degrade gracefully when the AI service is unavailable.  Errors are
    logged but never propagated.

    Parameters
    ----------
    base_url:
        Root URL of the AI engine (e.g. ``http://localhost:8001``).
    timeout:
        Per-request timeout in seconds.
    shared_secret:
        Shared secret for authenticating with the AI engine.  If not
        provided, the ``AI_ENGINE_SHARED_SECRET`` environment variable
        is read.  When empty, requests are sent without an auth header
        (the AI engine will reject them in non-dev environments).
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        shared_secret: str | None = None,
        *,
        circuit_breaker_fail_max: int = 5,
        circuit_breaker_reset_timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        _platform_env = os.environ.get("PLATFORM_ENV", "development")
        if _platform_env != "development" and self._base_url.startswith("http://"):
            raise ValueError(
                f"AI engine URL uses insecure HTTP scheme in {_platform_env} environment: {self._base_url!r}. "
                "Set AI_ENGINE_URL to an https:// URL for production deployments."
            )
        self._shared_secret = shared_secret or os.environ.get("AI_ENGINE_SHARED_SECRET", "")

        default_headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._shared_secret:
            default_headers["Authorization"] = f"Bearer {self._shared_secret}"

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout),
            headers=default_headers,
        )
        self._circuit_breaker = _CircuitBreaker(
            fail_max=circuit_breaker_fail_max,
            reset_timeout=circuit_breaker_reset_timeout,
        )

    # -- Advisory endpoints --------------------------------------------------

    async def semantic_classify(
        self,
        old_sql: str,
        new_sql: str,
        schema_diff: dict[str, Any] | None = None,
        *,
        tenant_id: str | None = None,
        llm_enabled: bool = True,
        api_key: str | None = None,
    ) -> dict[str, Any] | None:
        """Classify a SQL change semantically (e.g. column rename, filter tweak).

        Calls ``POST /semantic_classify`` on the AI engine.

        Parameters
        ----------
        api_key:
            Per-tenant LLM API key.  When provided, the AI engine uses this
            key for a one-shot Anthropic client instead of the platform key.
        """
        payload: dict[str, Any] = {
            "old_sql": _sanitize_ai_input(old_sql, "old_sql"),
            "new_sql": _sanitize_ai_input(new_sql, "new_sql"),
            "llm_enabled": llm_enabled,
        }
        if schema_diff is not None:
            payload["schema_diff"] = _sanitize_dict(schema_diff, "schema_diff")
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id
        if api_key is not None:
            payload["api_key"] = api_key
        return await self._post("/semantic_classify", payload)

    async def predict_cost(
        self,
        model_name: str,
        partition_count: int,
        historical_runtime_avg: float | None,
        data_volume: int | None,
        cluster_size: str,
        *,
        tenant_id: str | None = None,
        llm_enabled: bool = True,
    ) -> dict[str, Any] | None:
        """Predict execution cost for a model step.

        Calls ``POST /predict_cost`` on the AI engine.
        """
        payload: dict[str, Any] = {
            "model_name": _sanitize_ai_input(model_name, "model_name"),
            "partition_count": partition_count,
            "cluster_size": _sanitize_ai_input(cluster_size, "cluster_size"),
            "llm_enabled": llm_enabled,
        }
        if historical_runtime_avg is not None:
            payload["historical_runtime_avg"] = historical_runtime_avg
        if data_volume is not None:
            payload["data_volume_bytes"] = data_volume
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id
        return await self._post("/predict_cost", payload)

    async def score_risk(
        self,
        model_name: str,
        downstream_depth: int,
        sla_tags: list[str],
        dashboard_deps: list[str],
        model_tags: list[str],
        failure_rate: float,
        *,
        tenant_id: str | None = None,
        llm_enabled: bool = True,
    ) -> dict[str, Any] | None:
        """Score deployment risk for a model.

        Calls ``POST /risk_score`` on the AI engine.
        """
        payload: dict[str, Any] = {
            "model_name": _sanitize_ai_input(model_name, "model_name"),
            "downstream_depth": downstream_depth,
            "sla_tags": _sanitize_list(sla_tags, "sla_tags"),
            "dashboard_dependencies": _sanitize_list(dashboard_deps, "dashboard_deps"),
            "model_tags": _sanitize_list(model_tags, "model_tags"),
            "historical_failure_rate": failure_rate,
            "llm_enabled": llm_enabled,
        }
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id
        return await self._post("/risk_score", payload)

    async def optimize_sql(
        self,
        sql: str,
        stats: dict[str, Any] | None = None,
        *,
        tenant_id: str | None = None,
        llm_enabled: bool = True,
        api_key: str | None = None,
    ) -> dict[str, Any] | None:
        """Request SQL optimisation suggestions.

        Calls ``POST /optimize_sql`` on the AI engine.

        Parameters
        ----------
        api_key:
            Per-tenant LLM API key.  When provided, the AI engine uses this
            key for a one-shot Anthropic client instead of the platform key.
        """
        payload: dict[str, Any] = {
            "sql": _sanitize_ai_input(sql, "sql"),
            "llm_enabled": llm_enabled,
        }
        if stats is not None:
            payload["table_statistics"] = _sanitize_dict(stats, "table_statistics")
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id
        if api_key is not None:
            payload["api_key"] = api_key
        return await self._post("/optimize_sql", payload)

    # -- Lifecycle -----------------------------------------------------------

    async def health_check(self) -> bool:
        """Return ``True`` if the AI engine responds to a health ping."""
        try:
            resp = await self._client.get("/health")
            return resp.status_code == 200
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # -- Internal helpers ----------------------------------------------------

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Fire a POST request and return the JSON body, or ``None`` on error.

        Automatically forwards the W3C ``traceparent`` header if a trace
        context is active, enabling distributed tracing across services.

        The circuit breaker is checked before every request.  After
        ``circuit_breaker_fail_max`` consecutive network failures the circuit
        opens and all subsequent calls return ``None`` immediately without
        waiting for the 10 s timeout.  The circuit resets automatically after
        ``circuit_breaker_reset_timeout`` seconds.
        """
        # Circuit breaker: reject immediately when open.
        if self._circuit_breaker.is_open():
            logger.warning(
                "AI engine circuit breaker open — skipping request to %s", path
            )
            return None

        # Forward W3C traceparent for distributed tracing.
        extra_headers: dict[str, str] = {}
        try:
            from api.middleware.trace_context import get_traceparent

            traceparent = get_traceparent()
            if traceparent:
                extra_headers["traceparent"] = traceparent
        except ImportError:
            pass

        try:
            response = await self._client.post(path, json=payload, headers=extra_headers if extra_headers else None)
            response.raise_for_status()
            # Server responded — circuit is healthy regardless of status code.
            self._circuit_breaker.on_success()
            if _AI_METRICS:
                AI_CALLS_TOTAL.labels(call_type="advisory", outcome="success").inc()
            return cast(dict[str, Any], response.json())
        except httpx.HTTPStatusError as exc:
            # HTTP errors (4xx/5xx) mean the server is responding — do NOT trip
            # the circuit.  The server is reachable; the request was just invalid
            # or the engine returned an error response.
            self._circuit_breaker.on_success()
            if _AI_METRICS:
                AI_CALLS_TOTAL.labels(call_type="advisory", outcome="http_error").inc()
            logger.warning(
                "AI engine returned %d for %s: %s",
                exc.response.status_code,
                path,
                exc.response.text[:500],
            )
            return None
        except httpx.RequestError as exc:
            # Network-level failures (timeout, connection refused, DNS) trip the
            # circuit because they cause the full 10 s timeout to block.
            self._circuit_breaker.on_failure()
            if _AI_METRICS:
                AI_CALLS_TOTAL.labels(call_type="advisory", outcome="network_error").inc()
            logger.warning(
                "AI engine request to %s failed: %s",
                path,
                str(exc),
            )
            return None
