"""Tests for api/api/services/execution_service.py

Covers:
- Execute plan: local executor mode, records runs
- Update watermarks after successful execution
- Handle execution failures gracefully
- Idempotency: skip already-completed steps
- Lock acquisition and release for incremental runs
- Telemetry emission
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from api.config import APISettings
from api.middleware.rbac import Role
from api.services.execution_service import ExecutionService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(env: str = "dev") -> APISettings:
    return APISettings(
        platform_env=env,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
    )


def _make_plan_row(
    plan_id: str = "plan-001",
    plan_json_data: dict[str, Any] | None = None,
    approvals_json: str | None = None,
) -> MagicMock:
    if plan_json_data is None:
        plan_json_data = {
            "plan_id": plan_id,
            "steps": [
                {
                    "step_id": "step-001",
                    "model": "staging.orders",
                    "run_type": "FULL_REFRESH",
                    "input_range": None,
                    "depends_on": [],
                    "parallel_group": 0,
                    "reason": "SQL changed",
                },
            ],
            "summary": {
                "total_steps": 1,
                "estimated_cost_usd": 1.0,
                "models_changed": ["staging.orders"],
            },
        }
    row = MagicMock()
    row.plan_id = plan_id
    row.plan_json = json.dumps(plan_json_data)
    row.approvals_json = approvals_json
    row.auto_approved = False
    row.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    return row


# ---------------------------------------------------------------------------
# Execute plan: local executor mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_local_execution_success(mock_session: AsyncMock) -> None:
    """A single-step FULL_REFRESH plan executes locally in dev mode and records a run."""
    settings = _make_settings("dev")
    plan_row = _make_plan_row()

    service = ExecutionService(mock_session, settings)

    # Patch repositories on the service instance
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])  # no existing runs
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()

    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    # Ensure local execution path (no Databricks)
    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-001",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    run = results[0]
    assert run["model_name"] == "staging.orders"
    assert run["status"] == "SUCCESS"
    assert run["plan_id"] == "plan-001"
    assert run["step_id"] == "step-001"
    assert run["executor_version"] == "api-control-plane-0.1.0"

    # Verify run was persisted
    service._run_repo.create_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_plan_multi_step(mock_session: AsyncMock) -> None:
    """A two-step plan executes both steps in order."""
    settings = _make_settings("dev")
    plan_data = {
        "plan_id": "plan-multi",
        "steps": [
            {
                "step_id": "s1",
                "model": "staging.orders",
                "run_type": "FULL_REFRESH",
                "depends_on": [],
                "parallel_group": 0,
                "reason": "changed",
            },
            {
                "step_id": "s2",
                "model": "marts.revenue",
                "run_type": "FULL_REFRESH",
                "depends_on": ["s1"],
                "parallel_group": 1,
                "reason": "upstream changed",
            },
        ],
        "summary": {"total_steps": 2, "estimated_cost_usd": 2.0, "models_changed": ["staging.orders", "marts.revenue"]},
    }
    plan_row = _make_plan_row("plan-multi", plan_data)

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()
    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-multi",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 2
    assert results[0]["model_name"] == "staging.orders"
    assert results[1]["model_name"] == "marts.revenue"
    assert all(r["status"] == "SUCCESS" for r in results)

    # create_run called once per step
    assert service._run_repo.create_run.await_count == 2


# ---------------------------------------------------------------------------
# Update watermarks after successful execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_updates_watermark_on_incremental_success(
    mock_session: AsyncMock,
) -> None:
    """Successful incremental run updates the watermark."""
    settings = _make_settings("dev")
    plan_data = {
        "plan_id": "plan-inc",
        "steps": [
            {
                "step_id": "sinc",
                "model": "staging.orders",
                "run_type": "INCREMENTAL",
                "input_range": {"start": "2024-01-01", "end": "2024-01-31"},
                "depends_on": [],
                "parallel_group": 0,
                "reason": "incremental",
            },
        ],
        "summary": {"total_steps": 1, "estimated_cost_usd": 0.5, "models_changed": ["staging.orders"]},
    }
    plan_row = _make_plan_row("plan-inc", plan_data)

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()
    service._lock_repo = MagicMock()
    service._lock_repo.acquire_lock = AsyncMock(return_value=True)
    service._lock_repo.release_lock = AsyncMock()
    service._watermark_repo = MagicMock()
    service._watermark_repo.update_watermark = AsyncMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-inc",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    assert results[0]["status"] == "SUCCESS"

    # Watermark should have been updated
    service._watermark_repo.update_watermark.assert_awaited_once_with(
        model_name="staging.orders",
        partition_start=date(2024, 1, 1),
        partition_end=date(2024, 1, 31),
        row_count=None,
    )

    # Lock should have been acquired and released
    service._lock_repo.acquire_lock.assert_awaited_once()
    service._lock_repo.release_lock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Handle execution failures gracefully
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_execution_failure_records_fail_status(
    mock_session: AsyncMock,
) -> None:
    """When a step execution fails, the run is recorded with FAIL status."""
    settings = _make_settings("dev")
    plan_row = _make_plan_row()

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._watermark_repo.update_watermark = AsyncMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    # Force the local execution to fail
    with (
        patch.object(service, "_is_databricks_available", return_value=False),
        patch.object(
            service,
            "_execute_locally",
            AsyncMock(side_effect=RuntimeError("DuckDB crash")),
        ),
    ):
        results = await service.apply_plan(
            plan_id="plan-001",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    run = results[0]
    assert run["status"] == "FAIL"
    assert "DuckDB crash" in run["error_message"]

    # Run should still be persisted
    service._run_repo.create_run.assert_awaited_once()

    # Watermark should NOT have been updated on failure
    service._watermark_repo.update_watermark.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_plan_not_found_raises_value_error(
    mock_session: AsyncMock,
) -> None:
    """Applying a plan that does not exist raises ValueError."""
    settings = _make_settings("dev")
    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="Plan xyz not found"):
        await service.apply_plan(
            plan_id="xyz",
            approved_by=None,
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )


# ---------------------------------------------------------------------------
# Approval gate enforcement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_requires_approval_in_prod(
    mock_session: AsyncMock,
) -> None:
    """In non-dev environments, missing approvals raises PermissionError."""
    settings = _make_settings("production")
    plan_row = _make_plan_row(approvals_json=None)

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    with pytest.raises(PermissionError, match="no approvals"):
        await service.apply_plan(
            plan_id="plan-001",
            approved_by=None,
            cluster_override=None,
            auto_approve=False,
        )


@pytest.mark.asyncio
async def test_apply_plan_auto_approve_bypasses_gate_in_prod(
    mock_session: AsyncMock,
) -> None:
    """auto_approve=True bypasses the approval gate in production when caller is ADMIN."""
    settings = _make_settings("production")
    plan_row = _make_plan_row(approvals_json=None)

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()
    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-001",
            approved_by="admin",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    assert results[0]["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_apply_plan_auto_approve_denied_for_engineer(
    mock_session: AsyncMock,
) -> None:
    """auto_approve=True is rejected when the caller role is ENGINEER."""
    settings = _make_settings("dev")
    service = ExecutionService(mock_session, settings)

    with pytest.raises(PermissionError, match="auto_approve requires ADMIN role"):
        await service.apply_plan(
            plan_id="plan-001",
            approved_by="engineer-user",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ENGINEER,
        )


@pytest.mark.asyncio
async def test_apply_plan_auto_approve_denied_without_role(
    mock_session: AsyncMock,
) -> None:
    """auto_approve=True is rejected when no caller_role is provided."""
    settings = _make_settings("dev")
    service = ExecutionService(mock_session, settings)

    with pytest.raises(PermissionError, match="auto_approve requires ADMIN role"):
        await service.apply_plan(
            plan_id="plan-001",
            approved_by="unknown",
            cluster_override=None,
            auto_approve=True,
            caller_role=None,
        )


# ---------------------------------------------------------------------------
# Idempotency: skip completed steps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_skips_already_completed_step(
    mock_session: AsyncMock,
) -> None:
    """Steps that have already completed (SUCCESS) are skipped."""
    settings = _make_settings("dev")
    plan_row = _make_plan_row()

    # Simulate an existing completed run for step-001
    existing_run = MagicMock()
    existing_run.step_id = "step-001"
    existing_run.status = "SUCCESS"

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[existing_run])
    service._run_repo.create_run = AsyncMock()
    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-001",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    # The step was skipped -- no new run records returned
    assert len(results) == 0
    service._run_repo.create_run.assert_not_awaited()


# ---------------------------------------------------------------------------
# Lock acquisition failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_lock_failure_produces_cancelled_run(
    mock_session: AsyncMock,
) -> None:
    """If lock acquisition fails for an incremental step, CANCELLED run is recorded."""
    settings = _make_settings("dev")
    plan_data = {
        "plan_id": "plan-lock-fail",
        "steps": [
            {
                "step_id": "sinc",
                "model": "staging.orders",
                "run_type": "INCREMENTAL",
                "input_range": {"start": "2024-01-01", "end": "2024-01-31"},
                "depends_on": [],
                "parallel_group": 0,
                "reason": "incremental",
            },
        ],
        "summary": {"total_steps": 1, "estimated_cost_usd": 0.5, "models_changed": ["staging.orders"]},
    }
    plan_row = _make_plan_row("plan-lock-fail", plan_data)

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._lock_repo = MagicMock()
    service._lock_repo.acquire_lock = AsyncMock(return_value=False)  # lock fails
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-lock-fail",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    assert results[0]["status"] == "CANCELLED"
    assert "Lock acquisition failed" in results[0]["error_message"]


# ---------------------------------------------------------------------------
# Telemetry emission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_emits_telemetry(mock_session: AsyncMock) -> None:
    """Telemetry is recorded for every successfully executed step."""
    settings = _make_settings("dev")
    plan_row = _make_plan_row()

    service = ExecutionService(mock_session, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_by_plan = AsyncMock(return_value=[])
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()
    service._lock_repo = MagicMock()
    service._watermark_repo = MagicMock()
    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    with patch.object(service, "_is_databricks_available", return_value=False):
        results = await service.apply_plan(
            plan_id="plan-001",
            approved_by="tester",
            cluster_override=None,
            auto_approve=True,
            caller_role=Role.ADMIN,
        )

    assert len(results) == 1
    service._telemetry_repo.record.assert_awaited_once()
    telemetry_arg = service._telemetry_repo.record.call_args[0][0]
    assert telemetry_arg["model_name"] == "staging.orders"
    assert telemetry_arg["run_id"] == results[0]["run_id"]
    assert "runtime_seconds" in telemetry_arg


# ---------------------------------------------------------------------------
# _make_run_dict static method
# ---------------------------------------------------------------------------


def test_make_run_dict_defaults() -> None:
    """_make_run_dict fills in defaults for optional fields."""
    from core_engine.models.run import RunStatus

    result = ExecutionService._make_run_dict(
        plan_id="p1",
        step_id="s1",
        model_name="m1",
        status=RunStatus.SUCCESS,
    )

    assert result["plan_id"] == "p1"
    assert result["step_id"] == "s1"
    assert result["model_name"] == "m1"
    assert result["status"] == "SUCCESS"
    assert result["error_message"] is None
    assert result["input_range_start"] is None
    assert result["input_range_end"] is None
    assert result["logs_uri"] is None
    assert result["cluster_used"] is None
    assert result["retry_count"] == 0
    assert result["executor_version"] == "api-control-plane-0.1.0"
    # run_id is auto-generated UUID
    assert len(result["run_id"]) == 36  # UUID format


def test_make_run_dict_with_error() -> None:
    """_make_run_dict propagates error messages and date ranges."""
    from core_engine.models.run import RunStatus

    result = ExecutionService._make_run_dict(
        plan_id="p1",
        step_id="s1",
        model_name="m1",
        status=RunStatus.FAIL,
        error_message="Something went wrong",
        range_start=date(2024, 1, 1),
        range_end=date(2024, 1, 31),
        run_id="custom-run-id",
    )

    assert result["status"] == "FAIL"
    assert result["error_message"] == "Something went wrong"
    assert result["input_range_start"] == date(2024, 1, 1)
    assert result["input_range_end"] == date(2024, 1, 31)
    assert result["run_id"] == "custom-run-id"
