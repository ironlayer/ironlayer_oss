"""Quota enforcement service for per-tenant usage limits.

Checks whether a tenant has exceeded their monthly quotas for plan runs,
AI calls, API requests, or LLM budget.  Quotas come from explicit
tenant_config overrides, then tier defaults, with ``None`` meaning unlimited.

Tier defaults::

    community:  plans=100,  ai=500,    api=10_000
    team:       plans=1_000, ai=5_000,  api=100_000
    enterprise: unlimited

Grace period: checks are pre-execution.  In-flight operations complete
even if a quota boundary is crossed during execution.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from core_engine.state.repository import (
    LLMUsageLogRepository,
    QuotaRepository,
    TenantConfigRepository,
    UserRepository,
)
from core_engine.state.tables import BillingCustomerTable
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tier defaults
# ---------------------------------------------------------------------------

_TIER_DEFAULTS: dict[str, dict[str, int | None]] = {
    "community": {
        "plan_quota_monthly": 100,
        "ai_quota_monthly": 500,
        "api_quota_monthly": 10_000,
        "max_seats": 1,
    },
    "team": {
        "plan_quota_monthly": 1_000,
        "ai_quota_monthly": 5_000,
        "api_quota_monthly": 100_000,
        "max_seats": 10,
    },
    "enterprise": {
        "plan_quota_monthly": None,
        "ai_quota_monthly": None,
        "api_quota_monthly": None,
        "max_seats": None,
    },
}


class QuotaService:
    """Pre-execution quota enforcement for tenant-scoped operations.

    Each ``check_*`` method returns ``(allowed, reason)`` where ``allowed``
    is ``True`` if the operation may proceed.
    """

    def __init__(self, session: AsyncSession, tenant_id: str) -> None:
        self._session = session
        self._tenant_id = tenant_id
        self._quota_repo = QuotaRepository(session, tenant_id)
        self._config_repo = TenantConfigRepository(session, tenant_id)
        self._llm_repo = LLMUsageLogRepository(session, tenant_id)

    async def _acquire_advisory_lock(self, event_type: str) -> None:
        """Acquire a PostgreSQL transaction-scoped advisory lock.

        The lock is keyed on ``(tenant_id, event_type)`` so that
        concurrent requests for the same tenant and event type are
        serialised while unrelated operations proceed in parallel.

        On non-PostgreSQL databases (e.g. SQLite in tests) this is a
        no-op because advisory locks are a PostgreSQL-specific feature.
        """
        lock_id = hash(f"quota_{self._tenant_id}_{event_type}") & 0x7FFFFFFF
        bind = self._session.get_bind()
        dialect_name = str(getattr(getattr(bind, "dialect", None), "name", ""))
        if "postgresql" in dialect_name:
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(:id)"),
                {"id": lock_id},
            )

    async def check_and_reserve(
        self,
        event_type: str,
    ) -> tuple[bool, str | None]:
        """Atomically check and reserve a quota slot for *event_type*.

        Acquires a PostgreSQL advisory lock scoped to the current
        transaction so that concurrent requests for the same tenant
        and event type cannot both pass the quota check.

        Parameters
        ----------
        event_type:
            One of ``"plan_run"``, ``"ai_call"``, ``"api_request"``.

        Returns
        -------
        tuple[bool, str | None]
            ``(allowed, reason)`` where *allowed* is ``True`` if the
            operation may proceed.
        """
        quota_field_map = {
            "plan_run": "plan_quota_monthly",
            "ai_call": "ai_quota_monthly",
            "api_request": "api_quota_monthly",
        }
        quota_field = quota_field_map.get(event_type)
        if quota_field is None:
            raise ValueError(f"Unknown event_type for quota reservation: {event_type}")

        # Serialise concurrent checks for the same tenant + event type.
        await self._acquire_advisory_lock(event_type)

        limit = await self._get_effective_quota(quota_field)
        if limit is None:
            return True, None

        current = await self._quota_repo.get_monthly_event_count(event_type)
        if current >= limit:
            msg = f"Monthly {event_type} quota exceeded ({current}/{limit}). Upgrade your plan for higher limits."
            logger.warning(
                "Quota exceeded: tenant=%s %s=%d/%d",
                self._tenant_id,
                event_type,
                current,
                limit,
            )
            return False, msg

        return True, None

    async def _get_effective_quota(self, quota_field: str) -> int | None:
        """Resolve effective quota: explicit config > tier default > unlimited.

        Returns ``None`` to indicate unlimited.
        """
        config = await self._config_repo.get()
        if config is not None:
            explicit = getattr(config, quota_field, None)
            if explicit is not None:
                return cast(int, explicit)

        # Fall back to tier default.
        tier = await self._get_plan_tier()
        defaults = _TIER_DEFAULTS.get(tier, _TIER_DEFAULTS["community"])
        return defaults.get(quota_field)

    async def _get_plan_tier(self) -> str:
        """Look up the billing plan tier for this tenant."""
        stmt = select(BillingCustomerTable.plan_tier).where(
            BillingCustomerTable.tenant_id == self._tenant_id,
        )
        result = await self._session.execute(stmt)
        tier = result.scalar_one_or_none()
        return tier or "community"

    async def check_plan_quota(self) -> tuple[bool, str | None]:
        """Check whether this tenant can run another plan this month.

        Acquires an advisory lock to prevent concurrent requests from
        both passing the quota boundary.
        """
        await self._acquire_advisory_lock("plan_run")

        limit = await self._get_effective_quota("plan_quota_monthly")
        if limit is None:
            return True, None

        current = await self._quota_repo.get_monthly_event_count("plan_run")
        if current >= limit:
            msg = f"Monthly plan run quota exceeded ({current}/{limit}). Upgrade your plan for higher limits."
            logger.warning("Quota exceeded: tenant=%s plan_runs=%d/%d", self._tenant_id, current, limit)
            return False, msg
        return True, None

    async def check_ai_quota(self) -> tuple[bool, str | None]:
        """Check whether this tenant can make another AI call this month.

        Acquires an advisory lock to prevent concurrent requests from
        both passing the quota boundary.
        """
        await self._acquire_advisory_lock("ai_call")

        limit = await self._get_effective_quota("ai_quota_monthly")
        if limit is None:
            return True, None

        current = await self._quota_repo.get_monthly_event_count("ai_call")
        if current >= limit:
            msg = f"Monthly AI call quota exceeded ({current}/{limit}). Upgrade your plan for higher limits."
            logger.warning("Quota exceeded: tenant=%s ai_calls=%d/%d", self._tenant_id, current, limit)
            return False, msg
        return True, None

    async def check_api_quota(self) -> tuple[bool, str | None]:
        """Check whether this tenant can make another API request this month.

        Acquires an advisory lock to prevent concurrent requests from
        both passing the quota boundary.
        """
        await self._acquire_advisory_lock("api_request")

        limit = await self._get_effective_quota("api_quota_monthly")
        if limit is None:
            return True, None

        current = await self._quota_repo.get_monthly_event_count("api_request")
        if current >= limit:
            msg = f"Monthly API request quota exceeded ({current}/{limit}). Upgrade your plan for higher limits."
            logger.warning("Quota exceeded: tenant=%s api_requests=%d/%d", self._tenant_id, current, limit)
            return False, msg
        return True, None

    # ------------------------------------------------------------------
    # Seat-based enforcement
    # ------------------------------------------------------------------

    async def check_seat_quota(self) -> tuple[bool, str | None]:
        """Check whether this tenant can add another user (seat).

        Acquires a PostgreSQL advisory lock keyed on ``"seat_check"`` so
        that two concurrent invite requests for the same tenant cannot
        both pass the seat-limit boundary.

        Returns
        -------
        tuple[bool, str | None]
            ``(True, None)`` if a new seat is available, or
            ``(False, reason)`` with a human-readable error message.
        """
        await self._acquire_advisory_lock("seat_check")

        limit = await self._get_effective_seat_limit()
        if limit is None:
            return True, None

        user_repo = UserRepository(self._session, tenant_id=self._tenant_id)
        current_count = await user_repo.count_by_tenant()
        if current_count >= limit:
            msg = (
                f"Seat limit reached ({current_count}/{limit}). "
                "Upgrade your plan or contact support for additional seats."
            )
            logger.warning(
                "Seat quota exceeded: tenant=%s seats=%d/%d",
                self._tenant_id,
                current_count,
                limit,
            )
            return False, msg
        return True, None

    async def _get_effective_seat_limit(self) -> int | None:
        """Resolve the effective seat limit for this tenant.

        Resolution order (first non-None wins):
        1. Explicit ``tenant_config.max_seats`` override.
        2. Tier default from ``_TIER_DEFAULTS``.
        3. ``None`` — unlimited.
        """
        config = await self._config_repo.get()
        if config is not None:
            explicit = getattr(config, "max_seats", None)
            if explicit is not None:
                return int(explicit)

        tier = await self._get_plan_tier()
        defaults = _TIER_DEFAULTS.get(tier, _TIER_DEFAULTS["community"])
        return defaults.get("max_seats")

    async def check_llm_budget(self) -> tuple[bool, str | None]:
        """Check whether LLM spending is within daily and monthly budgets.

        Returns ``(False, reason)`` with HTTP 402 semantics if budget is
        exceeded, allowing the caller to return a 402 Payment Required.
        """
        config = await self._config_repo.get()
        if config is None:
            return True, None

        # Daily budget check.
        if config.llm_daily_budget_usd is not None:
            daily_cost = await self._llm_repo.get_daily_cost()
            if daily_cost >= config.llm_daily_budget_usd:
                msg = (
                    f"Daily LLM budget exceeded (${daily_cost:.2f}/${config.llm_daily_budget_usd:.2f}). "
                    "Increase your budget in tenant settings."
                )
                logger.warning(
                    "LLM daily budget exceeded: tenant=%s cost=%.4f limit=%.2f",
                    self._tenant_id,
                    daily_cost,
                    config.llm_daily_budget_usd,
                )
                return False, msg

        # Monthly budget check.
        if config.llm_monthly_budget_usd is not None:
            monthly_cost = await self._llm_repo.get_monthly_cost()
            if monthly_cost >= config.llm_monthly_budget_usd:
                msg = (
                    f"Monthly LLM budget exceeded (${monthly_cost:.2f}/${config.llm_monthly_budget_usd:.2f}). "
                    "Increase your budget in tenant settings."
                )
                logger.warning(
                    "LLM monthly budget exceeded: tenant=%s cost=%.4f limit=%.2f",
                    self._tenant_id,
                    monthly_cost,
                    config.llm_monthly_budget_usd,
                )
                return False, msg

        return True, None

    async def get_usage_vs_limits(self) -> dict[str, Any]:
        """Return current usage alongside effective limits for all quota types.

        Used by the billing page to display progress bars.

        Returns
        -------
        dict
            ``{"quotas": [{"name": str, "used": int, "limit": int | None, "percentage": float | None}],
               "llm_budget": {"daily_used": float, "daily_limit": float | None,
                              "monthly_used": float, "monthly_limit": float | None}}``
        """
        usage = await self._quota_repo.get_current_usage()
        config = await self._config_repo.get()

        plan_limit = await self._get_effective_quota("plan_quota_monthly")
        ai_limit = await self._get_effective_quota("ai_quota_monthly")
        api_limit = await self._get_effective_quota("api_quota_monthly")

        def _make_quota(name: str, event_type: str, limit: int | None) -> dict[str, Any]:
            used = usage.get(event_type, 0)
            pct = round((used / limit) * 100, 1) if limit else None
            return {"name": name, "event_type": event_type, "used": used, "limit": limit, "percentage": pct}

        quotas = [
            _make_quota("Plan Runs", "plan_run", plan_limit),
            _make_quota("AI Calls", "ai_call", ai_limit),
            _make_quota("API Requests", "api_request", api_limit),
        ]

        daily_cost = await self._llm_repo.get_daily_cost()
        monthly_cost = await self._llm_repo.get_monthly_cost()

        llm_budget = {
            "daily_used_usd": round(daily_cost, 4),
            "daily_limit_usd": config.llm_daily_budget_usd if config else None,
            "monthly_used_usd": round(monthly_cost, 4),
            "monthly_limit_usd": config.llm_monthly_budget_usd if config else None,
        }

        # Seat information.
        seat_limit = await self._get_effective_seat_limit()
        user_repo = UserRepository(self._session, tenant_id=self._tenant_id)
        seats_used = await user_repo.count_by_tenant()
        seats = {
            "used": seats_used,
            "limit": seat_limit,
            "percentage": round((seats_used / seat_limit) * 100, 1) if seat_limit else None,
        }

        return {"quotas": quotas, "llm_budget": llm_budget, "seats": seats}
