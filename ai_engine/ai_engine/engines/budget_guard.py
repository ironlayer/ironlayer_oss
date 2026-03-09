"""LLM cost budget guardrails.

Enforces per-tenant daily and monthly spending limits on LLM API calls.

The correct atomic pattern is :meth:`BudgetGuard.guard_call`, which holds
the per-tenant lock across the full check → call → record sequence,
eliminating the TOCTOU race where multiple concurrent requests pass the
budget check before any usage has been committed:

    usage = await guard.guard_call(my_llm_coro, call_type="classify", model_id="...")

If you need finer control, acquire the lock manually via ``guard.lock``:

    async with guard.lock:
        await guard.check_budget()
        usage_info = await llm_call()
        await guard.record_usage(**usage_info)

Token-based cost estimation uses Anthropic Claude pricing:
    Input:  $3.00  / 1M tokens
    Output: $15.00 / 1M tokens
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-tenant asyncio locks for atomic budget enforcement
# ---------------------------------------------------------------------------

# BL-092: Use WeakValueDictionary so that locks for idle tenants are GC'd
# automatically when no coroutine holds a reference to them.  This bounds
# the dict size to the number of *concurrently active* tenants rather than
# the total number of tenants ever seen, preventing unbounded memory growth.
_TENANT_BUDGET_LOCKS: weakref.WeakValueDictionary[str, asyncio.Lock] = (
    weakref.WeakValueDictionary()
)


def _get_tenant_lock(tenant_id: str) -> asyncio.Lock:
    """Return (creating if needed) the asyncio.Lock for a tenant.

    Asyncio is single-threaded so plain dict access is safe without a
    meta-lock.  The lock is kept alive as long as the caller holds a
    reference to it; WeakValueDictionary releases the entry after all
    callers release their references.
    """
    lock = _TENANT_BUDGET_LOCKS.get(tenant_id)
    if lock is None:
        lock = asyncio.Lock()
        _TENANT_BUDGET_LOCKS[tenant_id] = lock
    return lock


# ---------------------------------------------------------------------------
# Pricing constants (Claude Sonnet 4 pricing, USD per token)
# ---------------------------------------------------------------------------

_COST_PER_INPUT_TOKEN = 3.0 / 1_000_000  # $3.00 / 1M
_COST_PER_OUTPUT_TOKEN = 15.0 / 1_000_000  # $15.00 / 1M


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class LLMUsage(NamedTuple):
    """Token usage and latency returned by the LLM callable passed to guard_call.

    Fields
    ------
    input_tokens:
        Number of prompt tokens consumed.
    output_tokens:
        Number of completion tokens produced.
    latency_ms:
        Wall-clock time of the LLM API call in milliseconds.
    """

    input_tokens: int
    output_tokens: int
    latency_ms: int


class BudgetExceededError(Exception):
    """Raised when a tenant's LLM budget would be exceeded."""

    def __init__(
        self,
        tenant_id: str,
        period: str,
        budget_usd: float,
        spent_usd: float,
    ) -> None:
        self.tenant_id = tenant_id
        self.period = period
        self.budget_usd = budget_usd
        self.spent_usd = spent_usd
        super().__init__(
            f"LLM {period} budget exceeded for tenant '{tenant_id}': "
            f"${spent_usd:.4f} spent of ${budget_usd:.4f} limit"
        )


def estimate_call_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate the USD cost of an LLM call from token counts."""
    return (input_tokens * _COST_PER_INPUT_TOKEN) + (output_tokens * _COST_PER_OUTPUT_TOKEN)


# ---------------------------------------------------------------------------
# BudgetGuard
# ---------------------------------------------------------------------------


class BudgetGuard:
    """Enforces per-tenant LLM spending limits.

    **Preferred usage — fully atomic via** :meth:`guard_call`::

        guard = BudgetGuard(usage_repo, tenant_id="t1", daily_budget_usd=10.0)

        async def my_llm_call() -> LLMUsage:
            t0 = time.monotonic()
            resp = await anthropic_client.messages.create(...)
            return LLMUsage(
                input_tokens=resp.usage.input_tokens,
                output_tokens=resp.usage.output_tokens,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )

        cost = await guard.guard_call(my_llm_call, call_type="classify", model_id="claude-sonnet-4")

    ``guard_call`` holds the per-tenant lock for the full
    check → call → record window, preventing TOCTOU races where
    concurrent requests all pass the budget check before any usage is
    recorded.

    **Lower-level API** (when you need to interleave your own logic)::

        async with guard.lock:
            await guard.check_budget()
            result = await llm_call()
            await guard.record_usage(...)

    Parameters
    ----------
    usage_repo:
        ``LLMUsageLogRepository`` for the tenant.
    tenant_id:
        Tenant identifier used for the per-tenant lock.
    daily_budget_usd:
        Maximum daily LLM spend (``None`` = unlimited).
    monthly_budget_usd:
        Maximum monthly LLM spend (``None`` = unlimited).
    """

    def __init__(
        self,
        usage_repo: Any,
        *,
        tenant_id: str = "default",
        daily_budget_usd: float | None = None,
        monthly_budget_usd: float | None = None,
    ) -> None:
        self._usage_repo = usage_repo
        self._tenant_id = tenant_id
        self._daily_budget = daily_budget_usd
        self._monthly_budget = monthly_budget_usd

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def has_budget(self) -> bool:
        """``True`` when at least one spending limit is configured."""
        return self._daily_budget is not None or self._monthly_budget is not None

    @property
    def lock(self) -> asyncio.Lock:
        """Per-tenant ``asyncio.Lock`` for atomic budget sequences.

        Prefer :meth:`guard_call` which acquires this lock automatically.
        Use this property only when you need explicit control over the
        critical section boundaries.
        """
        return _get_tenant_lock(self._tenant_id)

    # ------------------------------------------------------------------
    # Atomic high-level API
    # ------------------------------------------------------------------

    async def guard_call(
        self,
        call: Callable[[], Any],
        *,
        call_type: str,
        model_id: str,
    ) -> float:
        """Atomically check budget, run *call*, and record usage.

        This is the **recommended entry point** for budget-guarded LLM
        calls.  All three steps execute while holding the per-tenant lock,
        eliminating the TOCTOU race.

        Parameters
        ----------
        call:
            Async zero-argument callable that performs the LLM request and
            returns an :class:`LLMUsage` named tuple.  It is invoked after
            the budget check passes, so it may safely assume the call is
            within budget when it starts.
        call_type:
            Label for the type of call (e.g. ``"classify_change"``), used
            for usage logging and analytics.
        model_id:
            Model identifier (e.g. ``"claude-sonnet-4"``).

        Returns
        -------
        float
            Estimated USD cost of the completed call.

        Raises
        ------
        BudgetExceededError
            If the tenant has already exceeded its daily or monthly limit.
        Exception
            Any exception raised by *call* is re-raised after recording a
            failed usage entry (with zero token counts) so costs are still
            tracked even for errored calls.

        Examples
        --------
        ::

            async def my_llm() -> LLMUsage:
                t0 = time.monotonic()
                resp = await client.messages.create(...)
                return LLMUsage(
                    input_tokens=resp.usage.input_tokens,
                    output_tokens=resp.usage.output_tokens,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                )

            cost = await guard.guard_call(my_llm, call_type="plan", model_id="claude-sonnet-4")
        """
        async with _get_tenant_lock(self._tenant_id):
            # 1. Reject the call if budget is already exhausted.
            await self.check_budget()

            # 2. Run the actual LLM call; capture usage or failure.
            error_type: str | None = None
            usage: LLMUsage | None = None
            try:
                usage = await call()
            except Exception as exc:
                error_type = type(exc).__name__
                # Record the failed call (zero tokens) so failure rates are visible.
                await self.record_usage(
                    call_type=call_type,
                    model_id=model_id,
                    input_tokens=0,
                    output_tokens=0,
                    latency_ms=0,
                    success=False,
                    error_type=error_type,
                )
                raise

            # 3. Record successful usage while still holding the lock so no
            #    other coroutine for this tenant can check budget until the
            #    spend is committed to the repository.
            cost = await self.record_usage(
                call_type=call_type,
                model_id=model_id,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                latency_ms=usage.latency_ms,
                success=True,
            )
            return cost

    # ------------------------------------------------------------------
    # Lower-level primitives (use guard_call when possible)
    # ------------------------------------------------------------------

    async def check_budget(self) -> None:
        """Verify the tenant has not exceeded any configured budget.

        This method is **not** atomic by itself.  Use :meth:`guard_call`
        or acquire :attr:`lock` before calling this to prevent TOCTOU.

        Raises
        ------
        BudgetExceededError
            If daily or monthly budget would be exceeded.
        """
        now = datetime.now(UTC)

        # Daily check.
        if self._daily_budget is not None:
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            daily_spent = await self._usage_repo.get_period_cost(since=day_start)
            if daily_spent >= self._daily_budget:
                raise BudgetExceededError(
                    tenant_id=self._tenant_id,
                    period="daily",
                    budget_usd=self._daily_budget,
                    spent_usd=daily_spent,
                )

        # Monthly check.
        if self._monthly_budget is not None:
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_spent = await self._usage_repo.get_period_cost(since=month_start)
            if monthly_spent >= self._monthly_budget:
                raise BudgetExceededError(
                    tenant_id=self._tenant_id,
                    period="monthly",
                    budget_usd=self._monthly_budget,
                    spent_usd=monthly_spent,
                )

    async def record_usage(
        self,
        call_type: str,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        success: bool,
        error_type: str | None = None,
    ) -> float:
        """Record an LLM call and return the estimated cost.

        Always records regardless of budget state — call :meth:`check_budget`
        (or :meth:`guard_call`) before the LLM call.
        """
        cost = estimate_call_cost(input_tokens, output_tokens)

        await self._usage_repo.record_usage(
            call_type=call_type,
            model_id=model_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_usd=cost,
            latency_ms=latency_ms,
            success=success,
            error_type=error_type,
        )

        logger.info(
            "LLM usage: tenant=%s type=%s tokens=%d+%d cost=$%.6f latency=%dms",
            self._tenant_id,
            call_type,
            input_tokens,
            output_tokens,
            cost,
            latency_ms,
        )

        return cost

    async def get_budget_status(self) -> dict[str, Any]:
        """Return current budget utilisation as a plain dict."""
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        daily_spent = await self._usage_repo.get_period_cost(since=day_start)
        monthly_spent = await self._usage_repo.get_period_cost(since=month_start)

        daily_remaining = (
            max(0.0, self._daily_budget - daily_spent) if self._daily_budget is not None else None
        )
        monthly_remaining = (
            max(0.0, self._monthly_budget - monthly_spent)
            if self._monthly_budget is not None
            else None
        )

        exceeded = (
            (self._daily_budget is not None and daily_spent >= self._daily_budget)
            or (self._monthly_budget is not None and monthly_spent >= self._monthly_budget)
        )

        return {
            "daily_budget_usd": self._daily_budget,
            "daily_spent_usd": round(daily_spent, 6),
            "daily_remaining_usd": round(daily_remaining, 6) if daily_remaining is not None else None,
            "monthly_budget_usd": self._monthly_budget,
            "monthly_spent_usd": round(monthly_spent, 6),
            "monthly_remaining_usd": (
                round(monthly_remaining, 6) if monthly_remaining is not None else None
            ),
            "budget_exceeded": exceeded,
        }
