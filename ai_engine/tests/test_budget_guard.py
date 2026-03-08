"""Tests for BudgetGuard and cost estimation."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from ai_engine.engines.budget_guard import (
    BudgetExceededError,
    BudgetGuard,
    estimate_call_cost,
    _COST_PER_INPUT_TOKEN,
    _COST_PER_OUTPUT_TOKEN,
)

# ---------------------------------------------------------------------------
# estimate_call_cost
# ---------------------------------------------------------------------------


class TestEstimateCallCost:
    """Tests for the cost estimation helper."""

    def test_zero_tokens(self):
        assert estimate_call_cost(0, 0) == 0.0

    def test_input_only(self):
        cost = estimate_call_cost(1_000_000, 0)
        assert abs(cost - 3.0) < 0.001  # $3.00 / 1M input tokens

    def test_output_only(self):
        cost = estimate_call_cost(0, 1_000_000)
        assert abs(cost - 15.0) < 0.001  # $15.00 / 1M output tokens

    def test_mixed_tokens(self):
        cost = estimate_call_cost(1000, 500)
        expected = (1000 * _COST_PER_INPUT_TOKEN) + (500 * _COST_PER_OUTPUT_TOKEN)
        assert abs(cost - expected) < 1e-9

    def test_typical_call(self):
        # Typical classify_change: ~2000 input, ~300 output
        cost = estimate_call_cost(2000, 300)
        assert cost > 0
        assert cost < 0.05  # Should be well under 5 cents


# ---------------------------------------------------------------------------
# BudgetGuard.has_budget
# ---------------------------------------------------------------------------


class TestBudgetGuardHasBudget:
    """Tests for the has_budget property."""

    def test_no_budgets(self):
        guard = BudgetGuard(
            AsyncMock(),
            tenant_id="t1",
        )
        assert guard.has_budget is False

    def test_daily_only(self):
        guard = BudgetGuard(
            AsyncMock(),
            tenant_id="t1",
            daily_budget_usd=1.0,
        )
        assert guard.has_budget is True

    def test_monthly_only(self):
        guard = BudgetGuard(
            AsyncMock(),
            tenant_id="t1",
            monthly_budget_usd=10.0,
        )
        assert guard.has_budget is True

    def test_both_budgets(self):
        guard = BudgetGuard(
            AsyncMock(),
            tenant_id="t1",
            daily_budget_usd=1.0,
            monthly_budget_usd=10.0,
        )
        assert guard.has_budget is True


# ---------------------------------------------------------------------------
# BudgetGuard.check_budget
# ---------------------------------------------------------------------------


class TestCheckBudget:
    """Tests for budget checking logic."""

    @pytest.mark.asyncio
    async def test_no_budget_always_passes(self):
        repo = AsyncMock()
        guard = BudgetGuard(repo, tenant_id="t1")
        await guard.check_budget()  # Should not raise

    @pytest.mark.asyncio
    async def test_daily_budget_under_limit(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=0.5)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
        )
        await guard.check_budget()  # Should not raise

    @pytest.mark.asyncio
    async def test_daily_budget_exceeded(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=1.5)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
        )
        with pytest.raises(BudgetExceededError) as exc_info:
            await guard.check_budget()
        assert exc_info.value.period == "daily"
        assert exc_info.value.budget_usd == 1.0
        assert exc_info.value.spent_usd == 1.5

    @pytest.mark.asyncio
    async def test_monthly_budget_under_limit(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=5.0)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            monthly_budget_usd=10.0,
        )
        await guard.check_budget()  # Should not raise

    @pytest.mark.asyncio
    async def test_monthly_budget_exceeded(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=15.0)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            monthly_budget_usd=10.0,
        )
        with pytest.raises(BudgetExceededError) as exc_info:
            await guard.check_budget()
        assert exc_info.value.period == "monthly"

    @pytest.mark.asyncio
    async def test_daily_exceeds_but_monthly_ok(self):
        """Daily budget should be checked first."""
        call_count = 0

        async def mock_get_period_cost(since):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # Daily check
                return 2.0  # Over daily budget
            return 5.0  # Under monthly budget

        repo = MagicMock()
        repo.get_period_cost = mock_get_period_cost
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
            monthly_budget_usd=10.0,
        )
        with pytest.raises(BudgetExceededError) as exc_info:
            await guard.check_budget()
        assert exc_info.value.period == "daily"

    @pytest.mark.asyncio
    async def test_daily_ok_but_monthly_exceeds(self):
        call_count = 0

        async def mock_get_period_cost(since):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # Daily check
                return 0.5  # Under daily budget
            return 15.0  # Over monthly budget

        repo = MagicMock()
        repo.get_period_cost = mock_get_period_cost
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
            monthly_budget_usd=10.0,
        )
        with pytest.raises(BudgetExceededError) as exc_info:
            await guard.check_budget()
        assert exc_info.value.period == "monthly"

    @pytest.mark.asyncio
    async def test_budget_at_exact_limit(self):
        """Spending exactly the limit should trigger the guard."""
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=1.0)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
        )
        with pytest.raises(BudgetExceededError):
            await guard.check_budget()


# ---------------------------------------------------------------------------
# BudgetGuard.record_usage
# ---------------------------------------------------------------------------


class TestRecordUsage:
    """Tests for usage recording."""

    @pytest.mark.asyncio
    async def test_records_usage(self):
        repo = MagicMock()
        repo.record_usage = AsyncMock()
        guard = BudgetGuard(repo, tenant_id="t1")

        cost = await guard.record_usage(
            call_type="classify_change",
            model_id="claude-sonnet-4-5-20250929",
            input_tokens=2000,
            output_tokens=300,
            latency_ms=1500,
            success=True,
        )

        assert cost > 0
        repo.record_usage.assert_called_once()
        call_kwargs = repo.record_usage.call_args.kwargs
        assert call_kwargs["call_type"] == "classify_change"
        assert call_kwargs["input_tokens"] == 2000
        assert call_kwargs["output_tokens"] == 300
        assert call_kwargs["success"] is True

    @pytest.mark.asyncio
    async def test_records_failed_call(self):
        repo = MagicMock()
        repo.record_usage = AsyncMock()
        guard = BudgetGuard(repo, tenant_id="t1")

        cost = await guard.record_usage(
            call_type="suggest_optimization",
            model_id="claude-sonnet-4-5-20250929",
            input_tokens=1000,
            output_tokens=0,
            latency_ms=5000,
            success=False,
            error_type="timeout",
        )

        # Even failed calls have input cost
        assert cost > 0
        call_kwargs = repo.record_usage.call_args.kwargs
        assert call_kwargs["success"] is False
        assert call_kwargs["error_type"] == "timeout"


# ---------------------------------------------------------------------------
# BudgetGuard.get_budget_status
# ---------------------------------------------------------------------------


class TestGetBudgetStatus:
    """Tests for budget status reporting."""

    @pytest.mark.asyncio
    async def test_no_budget_status(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=0.5)
        guard = BudgetGuard(repo, tenant_id="t1")

        status = await guard.get_budget_status()
        assert status["daily_budget_usd"] is None
        assert status["monthly_budget_usd"] is None
        assert status["budget_exceeded"] is False

    @pytest.mark.asyncio
    async def test_budget_status_under_limit(self):
        call_count = 0

        async def mock_get_period_cost(since):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # Daily
                return 0.3
            return 5.0  # Monthly

        repo = MagicMock()
        repo.get_period_cost = mock_get_period_cost
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
            monthly_budget_usd=10.0,
        )

        status = await guard.get_budget_status()
        assert status["daily_budget_usd"] == 1.0
        assert status["daily_spent_usd"] == 0.3
        assert status["daily_remaining_usd"] == 0.7
        assert status["monthly_budget_usd"] == 10.0
        assert status["monthly_spent_usd"] == 5.0
        assert status["monthly_remaining_usd"] == 5.0
        assert status["budget_exceeded"] is False

    @pytest.mark.asyncio
    async def test_budget_status_exceeded(self):
        repo = MagicMock()
        repo.get_period_cost = AsyncMock(return_value=2.0)
        guard = BudgetGuard(
            repo,
            tenant_id="t1",
            daily_budget_usd=1.0,
        )

        status = await guard.get_budget_status()
        assert status["budget_exceeded"] is True
        assert status["daily_remaining_usd"] == 0.0


# ---------------------------------------------------------------------------
# BudgetExceededError
# ---------------------------------------------------------------------------


class TestBudgetExceededError:
    """Tests for the exception class."""

    def test_attributes(self):
        err = BudgetExceededError(
            tenant_id="t1",
            period="daily",
            budget_usd=1.0,
            spent_usd=1.5,
        )
        assert err.tenant_id == "t1"
        assert err.period == "daily"
        assert err.budget_usd == 1.0
        assert err.spent_usd == 1.5
        assert "daily" in str(err)
        assert "t1" in str(err)
