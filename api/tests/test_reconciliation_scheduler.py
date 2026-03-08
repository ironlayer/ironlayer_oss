"""Tests for the reconciliation background scheduler.

Covers:
- compute_next_run for hourly, daily, weekly patterns
- compute_next_run with edge cases (exact match, past time)
- compute_next_run with invalid expressions
- Scheduler start/stop lifecycle
- _check_and_run_due_schedules with mocked repos
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.reconciliation_scheduler import (
    ReconciliationScheduler,
    compute_next_run,
)

# ---------------------------------------------------------------------------
# compute_next_run -- hourly
# ---------------------------------------------------------------------------


class TestComputeNextRunHourly:
    """Tests for hourly cron patterns: 'M * * * *'."""

    def test_hourly_future_minute(self) -> None:
        """Next run is within the current hour when minute is in the future."""
        from_time = datetime(2026, 2, 21, 14, 10, 0, tzinfo=timezone.utc)
        result = compute_next_run("30 * * * *", from_time)

        assert result == datetime(2026, 2, 21, 14, 30, 0, tzinfo=timezone.utc)

    def test_hourly_past_minute(self) -> None:
        """Next run rolls to the next hour when the minute has passed."""
        from_time = datetime(2026, 2, 21, 14, 45, 0, tzinfo=timezone.utc)
        result = compute_next_run("30 * * * *", from_time)

        assert result == datetime(2026, 2, 21, 15, 30, 0, tzinfo=timezone.utc)

    def test_hourly_exact_minute(self) -> None:
        """When from_time is exactly at the scheduled minute, rolls to next hour."""
        from_time = datetime(2026, 2, 21, 14, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 * * * *", from_time)

        assert result == datetime(2026, 2, 21, 15, 0, 0, tzinfo=timezone.utc)

    def test_hourly_midnight_rollover(self) -> None:
        """Hourly schedule at 23:50 rolls to next day 00:30."""
        from_time = datetime(2026, 2, 21, 23, 50, 0, tzinfo=timezone.utc)
        result = compute_next_run("30 * * * *", from_time)

        assert result.day == 22
        assert result.hour == 0
        assert result.minute == 30

    def test_hourly_minute_15(self) -> None:
        """Specific minute value (15) works correctly."""
        from_time = datetime(2026, 2, 21, 10, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("15 * * * *", from_time)

        assert result == datetime(2026, 2, 21, 10, 15, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# compute_next_run -- daily
# ---------------------------------------------------------------------------


class TestComputeNextRunDaily:
    """Tests for daily cron patterns: 'M H * * *'."""

    def test_daily_future_time(self) -> None:
        """Next run is today if the scheduled time hasn't passed."""
        from_time = datetime(2026, 2, 21, 6, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 12 * * *", from_time)

        assert result == datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)

    def test_daily_past_time(self) -> None:
        """Next run is tomorrow if the scheduled time has passed."""
        from_time = datetime(2026, 2, 21, 14, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 12 * * *", from_time)

        assert result == datetime(2026, 2, 22, 12, 0, 0, tzinfo=timezone.utc)

    def test_daily_exact_time(self) -> None:
        """Exactly at scheduled time rolls to next day."""
        from_time = datetime(2026, 2, 21, 0, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 0 * * *", from_time)

        assert result == datetime(2026, 2, 22, 0, 0, 0, tzinfo=timezone.utc)

    def test_daily_with_minute(self) -> None:
        """Daily at 3:30 AM."""
        from_time = datetime(2026, 2, 21, 1, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("30 3 * * *", from_time)

        assert result == datetime(2026, 2, 21, 3, 30, 0, tzinfo=timezone.utc)

    def test_daily_month_rollover(self) -> None:
        """Daily schedule at end of month rolls to next month."""
        from_time = datetime(2026, 2, 28, 23, 59, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 12 * * *", from_time)

        assert result.month == 3
        assert result.day == 1
        assert result.hour == 12


# ---------------------------------------------------------------------------
# compute_next_run -- weekly
# ---------------------------------------------------------------------------


class TestComputeNextRunWeekly:
    """Tests for weekly cron patterns: 'M H * * D'."""

    def test_weekly_sunday(self) -> None:
        """Schedule for Sunday (day 0) returns next Sunday."""
        # 2026-02-21 is a Saturday (weekday=5, Python)
        from_time = datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 0 * * 0", from_time)

        assert result.weekday() == 6  # Python: Sunday=6
        assert result == datetime(2026, 2, 22, 0, 0, 0, tzinfo=timezone.utc)

    def test_weekly_monday(self) -> None:
        """Schedule for Monday (day 1) computes correctly."""
        # 2026-02-21 is Saturday
        from_time = datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("0 9 * * 1", from_time)

        assert result.weekday() == 0  # Python: Monday=0
        assert result.hour == 9
        assert result.minute == 0

    def test_weekly_same_day_future_time(self) -> None:
        """When it's the target day but time is in the future, schedule is today."""
        # Create a Monday at 8:00
        from_time = datetime(2026, 2, 23, 8, 0, 0, tzinfo=timezone.utc)  # Monday
        result = compute_next_run("0 12 * * 1", from_time)

        # Should be today (Monday) at 12:00
        assert result == datetime(2026, 2, 23, 12, 0, 0, tzinfo=timezone.utc)

    def test_weekly_same_day_past_time(self) -> None:
        """When it's the target day but time has passed, schedule is next week."""
        # Create a Monday at 14:00
        from_time = datetime(2026, 2, 23, 14, 0, 0, tzinfo=timezone.utc)  # Monday
        result = compute_next_run("0 12 * * 1", from_time)

        # Should be next Monday at 12:00
        assert result == datetime(2026, 3, 2, 12, 0, 0, tzinfo=timezone.utc)

    def test_weekly_saturday(self) -> None:
        """Schedule for Saturday (day 6)."""
        from_time = datetime(2026, 2, 22, 12, 0, 0, tzinfo=timezone.utc)  # Sunday
        result = compute_next_run("0 8 * * 6", from_time)

        assert result.weekday() == 5  # Python: Saturday=5


# ---------------------------------------------------------------------------
# compute_next_run -- invalid expressions
# ---------------------------------------------------------------------------


class TestComputeNextRunInvalid:
    """Tests for invalid cron expressions."""

    def test_random_string_raises(self) -> None:
        """Arbitrary string raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported cron expression"):
            compute_next_run("not a cron", datetime.now(timezone.utc))

    def test_too_few_fields_raises(self) -> None:
        """Less than 5 fields raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported cron expression"):
            compute_next_run("0 *", datetime.now(timezone.utc))

    def test_too_many_fields_raises(self) -> None:
        """More than 5 fields raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported cron expression"):
            compute_next_run("0 * * * * *", datetime.now(timezone.utc))

    def test_complex_cron_unsupported(self) -> None:
        """Complex cron expressions (ranges, lists) are not supported."""
        with pytest.raises(ValueError, match="Unsupported cron expression"):
            compute_next_run("0,15,30,45 * * * *", datetime.now(timezone.utc))

    def test_empty_string_raises(self) -> None:
        """Empty string raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported cron expression"):
            compute_next_run("", datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------


class TestSchedulerLifecycle:
    """Tests for ReconciliationScheduler start/stop."""

    @pytest.mark.asyncio
    async def test_start_sets_running(self) -> None:
        """Starting the scheduler sets running=True and creates a task."""
        session_factory = AsyncMock()
        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        assert scheduler.running is False

        # Start and immediately stop to avoid infinite loop.
        await scheduler.start()
        assert scheduler.running is True
        assert scheduler._task is not None

        await scheduler.stop()
        assert scheduler.running is False
        assert scheduler._task is None

    @pytest.mark.asyncio
    async def test_stop_idempotent(self) -> None:
        """Stopping a non-running scheduler does not raise."""
        session_factory = AsyncMock()
        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        # Stop without starting -- should be safe.
        await scheduler.stop()
        assert scheduler.running is False

    @pytest.mark.asyncio
    async def test_start_twice_ignored(self) -> None:
        """Starting an already-running scheduler is a no-op."""
        session_factory = AsyncMock()
        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        await scheduler.start()
        first_task = scheduler._task

        await scheduler.start()  # Should be ignored.
        assert scheduler._task is first_task

        await scheduler.stop()


# ---------------------------------------------------------------------------
# _check_and_run_due_schedules
# ---------------------------------------------------------------------------


class TestCheckAndRunDueSchedules:
    """Tests for the schedule check loop internals."""

    @pytest.mark.asyncio
    async def test_due_schedule_executes(self) -> None:
        """A schedule with next_run_at in the past triggers execution."""
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        # Simulate async context manager for session_factory.
        session_cm = AsyncMock()
        session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        session_cm.__aexit__ = AsyncMock(return_value=False)

        session_factory = MagicMock()
        session_factory.return_value = session_cm

        past_time = datetime.now(timezone.utc) - timedelta(minutes=5)

        mock_schedule = MagicMock()
        mock_schedule.schedule_type = "run_reconciliation"
        mock_schedule.cron_expression = "0 * * * *"
        mock_schedule.next_run_at = past_time

        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        with (
            patch("api.services.reconciliation_scheduler.ReconciliationScheduleRepository") as MockRepo,
            patch("api.services.reconciliation_scheduler.ReconciliationService") as MockService,
        ):
            repo_instance = MockRepo.return_value
            repo_instance.get_all_enabled = AsyncMock(return_value=[mock_schedule])
            repo_instance.update_last_run = AsyncMock(return_value=True)

            service_instance = MockService.return_value
            service_instance.trigger_reconciliation = AsyncMock(return_value={"checked": 0})

            await scheduler._check_and_run_due_schedules()

            # Verify the service was called.
            service_instance.trigger_reconciliation.assert_awaited_once()
            # Verify timing was updated.
            repo_instance.update_last_run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_future_schedule_not_executed(self) -> None:
        """A schedule with next_run_at in the future is not executed."""
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        session_cm = AsyncMock()
        session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        session_cm.__aexit__ = AsyncMock(return_value=False)

        session_factory = MagicMock()
        session_factory.return_value = session_cm

        future_time = datetime.now(timezone.utc) + timedelta(hours=1)

        mock_schedule = MagicMock()
        mock_schedule.schedule_type = "schema_drift"
        mock_schedule.cron_expression = "0 * * * *"
        mock_schedule.next_run_at = future_time

        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        with (
            patch("api.services.reconciliation_scheduler.ReconciliationScheduleRepository") as MockRepo,
            patch("api.services.reconciliation_scheduler.ReconciliationService") as MockService,
        ):
            repo_instance = MockRepo.return_value
            repo_instance.get_all_enabled = AsyncMock(return_value=[mock_schedule])

            service_instance = MockService.return_value
            service_instance.check_all_schemas = AsyncMock()

            await scheduler._check_and_run_due_schedules()

            # Service should NOT have been called.
            service_instance.check_all_schemas.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_enabled_schedules(self) -> None:
        """When no enabled schedules exist, nothing happens."""
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        session_cm = AsyncMock()
        session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        session_cm.__aexit__ = AsyncMock(return_value=False)

        session_factory = MagicMock()
        session_factory.return_value = session_cm

        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        with patch("api.services.reconciliation_scheduler.ReconciliationScheduleRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.get_all_enabled = AsyncMock(return_value=[])

            await scheduler._check_and_run_due_schedules()
            # No error should occur.

    @pytest.mark.asyncio
    async def test_schema_drift_schedule_type(self) -> None:
        """schema_drift schedule type triggers check_all_schemas."""
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        session_cm = AsyncMock()
        session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        session_cm.__aexit__ = AsyncMock(return_value=False)

        session_factory = MagicMock()
        session_factory.return_value = session_cm

        past_time = datetime.now(timezone.utc) - timedelta(minutes=5)

        mock_schedule = MagicMock()
        mock_schedule.schedule_type = "schema_drift"
        mock_schedule.cron_expression = "0 0 * * *"
        mock_schedule.next_run_at = past_time

        scheduler = ReconciliationScheduler(session_factory, tenant_id="test")

        with (
            patch("api.services.reconciliation_scheduler.ReconciliationScheduleRepository") as MockRepo,
            patch("api.services.reconciliation_scheduler.ReconciliationService") as MockService,
        ):
            repo_instance = MockRepo.return_value
            repo_instance.get_all_enabled = AsyncMock(return_value=[mock_schedule])
            repo_instance.update_last_run = AsyncMock(return_value=True)

            service_instance = MockService.return_value
            service_instance.check_all_schemas = AsyncMock(return_value={"models_checked": 0})

            await scheduler._check_and_run_due_schedules()

            service_instance.check_all_schemas.assert_awaited_once()
