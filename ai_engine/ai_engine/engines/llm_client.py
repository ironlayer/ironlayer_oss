"""Optional LLM integration, fully feature-flagged.

When ``AI_ENGINE_LLM_ENABLED=false`` (default) every public method returns
*None* and no network calls are made.  When enabled, the client calls the
configured Anthropic model with structured prompts and hard timeouts.

All user-supplied text is scrubbed for PII before being sent to the LLM.
See :mod:`ai_engine.engines.pii_scrubber` for the scrubbing rules.

This module is a leaf dependency -- no other engine module imports it at
module level so that the ``anthropic`` package remains optional.

The client is **fully async** and uses the ``anthropic.AsyncAnthropic`` SDK.
All public methods are coroutines that must be awaited.  Callers in a sync
context (e.g. tests) should use ``asyncio.run()``.
"""

from __future__ import annotations

import json
import logging
import time as _time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from ai_engine.engines.budget_guard import BudgetExceededError, BudgetGuard, LLMUsage
from ai_engine.engines.pii_scrubber import (
    contains_pii,
    scrub_for_llm,
    scrub_sql_for_llm,
)
from ai_engine.engines.prompts import get_prompt

if TYPE_CHECKING:
    from ai_engine.config import AISettings

logger = logging.getLogger(__name__)


class LLMDisabledError(Exception):
    """Raised when an LLM call is attempted but the feature is disabled."""


# ---------------------------------------------------------------------------
# Output schema models (BL-050)
# ---------------------------------------------------------------------------
# These Pydantic models validate the JSON structures the LLM is expected to
# return.  Any response that does not conform to the schema is rejected and
# the caller receives ``None`` (same as any other LLM failure).  This
# prevents malformed or adversarially crafted LLM outputs from propagating
# into downstream processing as untyped dicts.


class _ClassifyChangeOutput(BaseModel):
    """Expected shape of the ``classify_change`` LLM response."""

    change_type: str
    confidence: float
    reasoning: str


class _OptimizationSuggestion(BaseModel):
    """Expected shape of a single ``suggest_optimization`` LLM item."""

    suggestion_type: str
    description: str
    rewritten_sql: str | None = None
    confidence: float


class LLMClient:
    """Thin async wrapper around the Anthropic SDK with fail-safe semantics.

    Supports two modes of operation:

    1. **Platform key (default)**: A global API key set via
       ``AI_ENGINE_LLM_API_KEY``.  Used as a fallback or for
       demo/trial tenants.
    2. **Per-tenant key**: Callers pass ``api_key`` on each request.
       The client creates a short-lived ``AsyncAnthropic`` client for that
       call and disposes it immediately.  This lets each tenant bring their
       own LLM API key without sharing credentials across tenants.

    All public methods are **async coroutines** that must be awaited.
    The async Anthropic SDK is used so that LLM I/O never blocks the
    FastAPI event loop.

    Budget enforcement is optional.  When a :class:`BudgetGuard` is
    supplied, :meth:`_call_llm` checks the budget *before* the call and
    records actual usage *after*.  The check and record are intentionally
    not held under the guard's lock during the LLM call itself: this keeps
    per-tenant concurrency high while still catching budget overruns in
    normal traffic patterns.  (Holding the lock for the full LLM call
    duration -- which can be several seconds -- would serialize all
    concurrent requests unnecessarily.)
    """

    def __init__(
        self,
        settings: AISettings,
        budget_guard: BudgetGuard | None = None,
    ) -> None:
        self._enabled = settings.llm_enabled
        self._model = settings.llm_model
        self._max_tokens = settings.llm_max_tokens
        self._timeout = settings.llm_timeout
        self._client: Any = None
        self._has_platform_key = False
        self._budget_guard = budget_guard

        if self._enabled:
            try:
                import anthropic  # noqa: WPS433 (optional import)

                api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else None
                if api_key:
                    self._client = anthropic.AsyncAnthropic(
                        api_key=api_key,
                        timeout=self._timeout,
                    )
                    self._has_platform_key = True
                    logger.info(
                        "LLM client initialised with platform key (model=%s, timeout=%.1fs)",
                        self._model,
                        self._timeout,
                    )
                else:
                    logger.info(
                        "LLM enabled but no platform key -- per-tenant keys required",
                    )
            except Exception:
                logger.warning(
                    "Failed to initialise Anthropic async client -- LLM features disabled",
                    exc_info=True,
                )
                self._enabled = False
                self._client = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def classify_change(
        self,
        old_sql: str,
        new_sql: str,
        context: str = "",
        *,
        llm_enabled: bool = True,
        api_key: str | None = None,
    ) -> dict | None:
        """Ask the LLM to classify a SQL change.

        Returns a dict with keys ``change_type``, ``confidence``,
        ``reasoning`` or *None* on any failure.

        Parameters
        ----------
        llm_enabled:
            Per-request override.  When ``False`` the call is skipped even
            if the global LLM flag is on (used for per-tenant opt-out).
        api_key:
            Per-tenant API key.  If provided, a temporary AsyncAnthropic
            client is created for this call.  Falls back to the platform key.
        """
        if not self._enabled or not llm_enabled:
            return None

        scrubbed_old = self._scrub_sql(old_sql, "classify_change.old_sql")
        scrubbed_new = self._scrub_sql(new_sql, "classify_change.new_sql")
        scrubbed_ctx = self._scrub_text(context, "classify_change.context") if context else ""

        prompt = get_prompt("classify_change_system")
        logger.info("LLM call: prompt_key=%s prompt_version=%s", prompt.key, prompt.version)

        user = f"OLD SQL:\n{scrubbed_old}\n\nNEW SQL:\n{scrubbed_new}"
        if scrubbed_ctx:
            user += f"\n\nADDITIONAL CONTEXT:\n{scrubbed_ctx}"

        try:
            raw = await self._call_llm(
                prompt.content, user, api_key=api_key, call_type="classify_change"
            )
            parsed = self._parse_json(raw)
            # Validate the response conforms to the expected schema (BL-050).
            validated = _ClassifyChangeOutput.model_validate(parsed)
            return validated.model_dump()
        except BudgetExceededError:
            logger.warning("LLM classify_change blocked by budget guard")
            return None
        except ValidationError as exc:
            logger.warning("LLM classify_change returned invalid schema: %s", exc)
            return None
        except Exception:
            logger.warning("LLM classify_change failed", exc_info=True)
            return None

    async def suggest_optimization(
        self,
        sql: str,
        context: str = "",
        *,
        llm_enabled: bool = True,
        api_key: str | None = None,
    ) -> list[dict] | None:
        """Ask the LLM for SQL optimisation suggestions.

        Returns a list of dicts with keys ``suggestion_type``,
        ``description``, ``rewritten_sql``, ``confidence`` or *None*.

        Parameters
        ----------
        llm_enabled:
            Per-request override.  When ``False`` the call is skipped even
            if the global LLM flag is on (used for per-tenant opt-out).
        api_key:
            Per-tenant API key.  If provided, a temporary AsyncAnthropic
            client is created for this call.  Falls back to the platform key.
        """
        if not self._enabled or not llm_enabled:
            return None

        scrubbed_sql = self._scrub_sql(sql, "suggest_optimization.sql")
        scrubbed_ctx = self._scrub_text(context, "suggest_optimization.context") if context else ""

        prompt = get_prompt("suggest_optimization_system")
        logger.info("LLM call: prompt_key=%s prompt_version=%s", prompt.key, prompt.version)

        user = f"SQL:\n{scrubbed_sql}"
        if scrubbed_ctx:
            user += f"\n\nCONTEXT:\n{scrubbed_ctx}"

        try:
            raw = await self._call_llm(
                prompt.content, user, api_key=api_key, call_type="suggest_optimization"
            )
            parsed = self._parse_json(raw)
            if not isinstance(parsed, list):
                logger.warning("LLM suggest_optimization returned non-list response; discarding")
                return None
            # Validate each item in the list against the expected schema (BL-050).
            validated: list[dict] = []
            for item in parsed:
                try:
                    validated.append(_OptimizationSuggestion.model_validate(item).model_dump())
                except ValidationError as item_exc:
                    logger.warning(
                        "LLM suggest_optimization item failed schema validation "
                        "(skipping): %s",
                        item_exc,
                    )
            return validated if validated else None
        except BudgetExceededError:
            logger.warning("LLM suggest_optimization blocked by budget guard")
            return None
        except Exception:
            logger.warning("LLM suggest_optimization failed", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _scrub_sql(self, sql: str, field_name: str) -> str:
        """Scrub a SQL string for PII before LLM submission."""
        if contains_pii(sql):
            logger.debug("PII detected and scrubbed from %s before LLM call", field_name)
        return scrub_sql_for_llm(sql)

    def _scrub_text(self, text: str, field_name: str) -> str:
        """Scrub a general text string for PII before LLM submission."""
        if contains_pii(text):
            logger.debug("PII detected and scrubbed from %s before LLM call", field_name)
        return scrub_for_llm(text)

    def _resolve_client(self, api_key: str | None) -> Any:
        """Return the AsyncAnthropic client to use for a request.

        If ``api_key`` is provided, creates a one-shot ``AsyncAnthropic``
        client scoped to that key.  Otherwise, falls back to the platform-
        level client.  Raises :class:`LLMDisabledError` if neither is
        available.
        """
        if api_key:
            try:
                import anthropic

                return anthropic.AsyncAnthropic(
                    api_key=api_key,
                    timeout=self._timeout,
                )
            except Exception as exc:
                logger.warning("Failed to create per-tenant AsyncAnthropic client: %s", exc)
                raise LLMDisabledError(
                    "Tenant-provided LLM API key is invalid or the Anthropic SDK "
                    "could not initialise. Check the key and try again."
                ) from exc

        if self._client is not None:
            return self._client

        raise LLMDisabledError(
            "No LLM API key available. Provide a key in Settings or contact your platform administrator."
        )

    async def _call_llm(
        self,
        system: str,
        user: str,
        *,
        api_key: str | None = None,
        call_type: str = "unknown",
    ) -> str:
        """Execute a single LLM call and return the text response.

        Uses ``AsyncAnthropic`` so the event loop is never blocked.

        When a :class:`BudgetGuard` is configured, the entire sequence of
        check → call → record executes atomically under the per-tenant
        ``asyncio.Lock`` via :meth:`BudgetGuard.guard_call`.  This eliminates
        the TOCTOU race where multiple concurrent requests could all pass the
        budget check before any spend has been committed.

        Parameters
        ----------
        api_key:
            Optional per-tenant API key.  When provided, a temporary
            ``AsyncAnthropic`` client is created for this call only.
        call_type:
            Label for the type of call (e.g. ``"classify_change"``),
            used for budget tracking.
        """
        client = self._resolve_client(api_key)

        # _response_holder captures the Anthropic response object from inside
        # the inner coroutine so we can extract the text after guard_call().
        # Using a list as a mutable closure cell avoids instance-level state
        # that would race under concurrent calls.
        _response_holder: list[Any] = []

        async def _make_request() -> LLMUsage:
            """Inner coroutine: perform the API call and return token usage."""
            start_ms = int(_time.monotonic() * 1000)
            response = await client.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                temperature=0.0,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            latency_ms = int(_time.monotonic() * 1000) - start_ms
            _response_holder.append(response)

            usage_obj = getattr(response, "usage", None)
            input_tokens = getattr(usage_obj, "input_tokens", 0) if usage_obj else 0
            output_tokens = getattr(usage_obj, "output_tokens", 0) if usage_obj else 0
            return LLMUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
            )

        if self._budget_guard is not None and self._budget_guard.has_budget:
            # Atomic: check budget, run the call, record usage — all under lock.
            await self._budget_guard.guard_call(
                _make_request,
                call_type=call_type,
                model_id=self._model,
            )
        else:
            # No budget enforcement — call directly.
            await _make_request()

        return _response_holder[0].content[0].text

    @staticmethod
    def _parse_json(raw: str) -> Any:
        """Best-effort JSON extraction from LLM output."""
        text = raw.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            first_newline = text.index("\n")
            last_fence = text.rfind("```")
            text = text[first_newline + 1 : last_fence].strip()
        return json.loads(text)
