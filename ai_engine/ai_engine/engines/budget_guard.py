"""LLM cost budget guardrails.

Enforces per-tenant daily and monthly spending limits on LLM API calls.
The guard checks current period usage against configured budgets before
allowing a call, and records usage after each call completes.

Token-based cost estimation uses Anthropic Claude pricing:
    Input:  $3.00  / 1M tokens
    Output: $15.00 / 1M tokens
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pricing constants (Claude Sonnet 4 pricing, USD per token)
# ---------------------------------------------------------------------------

_COST_PER_INPUT_TOKEN = 3.0 / 1_000_000  # $3.00 / 1M
_COST_PER_OUTPUT_TOKEN = 15.0 / 1_000_000  # $15.00 / 1M


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
            f"LLM {period} budget exceeded for tenant '{tenant_id}': ${spent_usd:.4f} spent of ${budget_usd:.4f} limit"
        )


def estimate_call_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate cost of an LLM call from token counts."""
    return (input_tokens * _COST_PER_INPUT_TOKEN) + (output_tokens * _COST_PER_OUTPUT_TOKEN)


class BudgetGuard:
    """Enforces per-tenant LLM spending limits.

    Usage::

        guard = BudgetGuard(usage_repo, tenant_config)
        await guard.check_budget()          # raises BudgetExceededError
        response = llm_client.call(...)
        await guard.record_usage(...)

    Parameters
    ----------
    usage_repo:
        LLMUsageLogRepository for the tenant.
    daily_budget_usd:
        Maximum daily LLM spend (None = unlimited).
    monthly_budget_usd:
        Maximum monthly LLM spend (None = unlimited).
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

    @property
    def has_budget(self) -> bool:
        """Whether any budget limit is configured."""
        return self._daily_budget is not None or self._monthly_budget is not None

    async def check_budget(self) -> None:
        """Verify the tenant has not exceeded any configured budget.

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

        Always records regardless of budget state — the budget check
        should be done *before* the call.
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
        """Return current budget utilisation.

        Returns
        -------
        dict
            ``{"daily_budget_usd": float | None,
               "daily_spent_usd": float,
               "daily_remaining_usd": float | None,
               "monthly_budget_usd": float | None,
               "monthly_spent_usd": float,
               "monthly_remaining_usd": float | None,
               "budget_exceeded": bool}``
        """
        now = datetime.now(UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        daily_spent = await self._usage_repo.get_period_cost(since=day_start)
        monthly_spent = await self._usage_repo.get_period_cost(since=month_start)

        daily_remaining = max(0.0, self._daily_budget - daily_spent) if self._daily_budget is not None else None
        monthly_remaining = max(0.0, self._monthly_budget - monthly_spent) if self._monthly_budget is not None else None

        exceeded = False
        if self._daily_budget is not None and daily_spent >= self._daily_budget:
            exceeded = True
        if self._monthly_budget is not None and monthly_spent >= self._monthly_budget:
            exceeded = True

        return {
            "daily_budget_usd": self._daily_budget,
            "daily_spent_usd": round(daily_spent, 6),
            "daily_remaining_usd": round(daily_remaining, 6) if daily_remaining is not None else None,
            "monthly_budget_usd": self._monthly_budget,
            "monthly_spent_usd": round(monthly_spent, 6),
            "monthly_remaining_usd": round(monthly_remaining, 6) if monthly_remaining is not None else None,
            "budget_exceeded": exceeded,
        }
