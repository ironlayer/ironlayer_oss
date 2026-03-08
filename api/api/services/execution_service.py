"""Orchestrates plan execution, backfills, and run recording."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

from core_engine.executor.cluster_templates import get_cost_rate
from core_engine.models.plan import DateRange, PlanStep, RunType, compute_deterministic_id
from core_engine.models.run import RunStatus
from core_engine.state.repository import (
    BackfillAuditRepository,
    BackfillCheckpointRepository,
    LockRepository,
    ModelRepository,
    PlanRepository,
    RunRepository,
    TelemetryRepository,
    WatermarkRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

from api.config import APISettings, PlatformEnv
from api.middleware.rbac import Role
from api.services.ai_feedback_service import AIFeedbackService

logger = logging.getLogger(__name__)

# Executor version tag written into every run record.
_EXECUTOR_VERSION = "api-control-plane-0.1.0"


class ExecutionService:
    """Execute plans and individual backfills against the configured backend.

    Parameters
    ----------
    session:
        Active database session.
    settings:
        Application configuration (determines which executor backend to use).
    """

    def __init__(
        self,
        session: AsyncSession,
        settings: APISettings,
        *,
        tenant_id: str = "default",
    ) -> None:
        self._session = session
        self._settings = settings
        self._tenant_id = tenant_id
        self._plan_repo = PlanRepository(session, tenant_id=tenant_id)
        self._run_repo = RunRepository(session, tenant_id=tenant_id)
        self._lock_repo = LockRepository(session, tenant_id=tenant_id)
        self._watermark_repo = WatermarkRepository(session, tenant_id=tenant_id)
        self._model_repo = ModelRepository(session, tenant_id=tenant_id)
        self._telemetry_repo = TelemetryRepository(session, tenant_id=tenant_id)
        self._feedback_service = AIFeedbackService(session, tenant_id=tenant_id)
        self._checkpoint_repo = BackfillCheckpointRepository(session, tenant_id=tenant_id)
        self._audit_repo = BackfillAuditRepository(session, tenant_id=tenant_id)

    # ------------------------------------------------------------------
    # Plan execution
    # ------------------------------------------------------------------

    async def apply_plan(
        self,
        plan_id: str,
        approved_by: str | None,
        cluster_override: str | None,
        auto_approve: bool,
        caller_role: Role | None = None,
    ) -> list[dict[str, Any]]:
        """Execute every step in a plan in topological order.

        Parameters
        ----------
        plan_id:
            The plan to execute.
        approved_by:
            User who authorised the execution.  Required in non-dev envs
            when *auto_approve* is ``False``.
        cluster_override:
            Optional cluster size override (``small``, ``medium``, ``large``).
        auto_approve:
            When ``True`` the approval gate is bypassed.  Requires ADMIN
            role.
        caller_role:
            The authenticated caller's role, used to enforce that only
            ADMIN users may use ``auto_approve``.

        Returns
        -------
        list[dict]
            A list of run record dictionaries, one per step.
        """
        # auto_approve privilege check: only ADMIN may bypass the approval
        # workflow.  This prevents ENGINEER/OPERATOR callers from skipping
        # the mandatory approval gate.
        if auto_approve:
            if caller_role != Role.ADMIN:
                raise PermissionError(
                    "auto_approve requires ADMIN role. "
                    f"Current role '{caller_role.name if caller_role else 'UNKNOWN'}' is insufficient."
                )
            # Additionally restrict auto_approve in production unless ADMIN.
            if self._settings.platform_env == PlatformEnv.PRODUCTION and caller_role != Role.ADMIN:
                raise PermissionError("auto_approve in production environment requires ADMIN role.")

        plan_row = await self._plan_repo.get_plan(plan_id)
        if plan_row is None:
            raise ValueError(f"Plan {plan_id} not found")

        # Approval gate: enforce in non-dev environments.
        if not auto_approve and self._settings.platform_env != PlatformEnv.DEV:
            approvals = json.loads(plan_row.approvals_json) if plan_row.approvals_json else []  # type: ignore[arg-type]
            if not approvals:
                raise PermissionError(
                    f"Plan {plan_id} has no approvals.  "
                    f"Approval is required in '{self._settings.platform_env}' environment."
                )

        plan_data = json.loads(plan_row.plan_json)  # type: ignore[arg-type]
        steps: list[dict[str, Any]] = plan_data.get("steps", [])

        run_records: list[dict[str, Any]] = []

        # Capture AI predictions from advisory_json before execution.
        try:
            await self._feedback_service.capture_predictions_from_plan(plan_id)
        except Exception:
            logger.warning(
                "Failed to capture AI predictions for plan %s",
                plan_id[:12],
                exc_info=True,
            )

        for step in steps:
            model_name: str = step["model"]
            step_id: str = step["step_id"]
            run_type: str = step.get("run_type", "FULL_REFRESH")
            input_range: dict[str, str] | None = step.get("input_range")

            # Idempotency: skip if a completed run already exists for this step.
            existing_runs = await self._run_repo.get_by_plan(plan_id)
            already_done = any(r.step_id == step_id and r.status == "SUCCESS" for r in existing_runs)
            if already_done:
                logger.info(
                    "Skipping step %s for %s: already completed",
                    step_id[:12],
                    model_name,
                )
                continue

            # Lock acquisition for incremental runs.
            range_start: date | None = None
            range_end: date | None = None
            locked = False
            if run_type == "INCREMENTAL" and input_range:
                range_start = date.fromisoformat(input_range["start"])
                range_end = date.fromisoformat(input_range["end"])
                locked = await self._lock_repo.acquire_lock(
                    model_name=model_name,
                    range_start=range_start,
                    range_end=range_end,
                    locked_by=approved_by or "api",
                )
                if not locked:
                    logger.warning(
                        "Could not acquire lock for %s [%s, %s]; skipping",
                        model_name,
                        range_start,
                        range_end,
                    )
                    run_records.append(
                        self._make_run_dict(
                            plan_id=plan_id,
                            step_id=step_id,
                            model_name=model_name,
                            status=RunStatus.CANCELLED,
                            error_message="Lock acquisition failed",
                            range_start=range_start,
                            range_end=range_end,
                        )
                    )
                    continue

            # Execute the step.  Wrap in try/finally to guarantee lock
            # release even when _execute_step() or downstream persistence
            # raises an unexpected exception.
            try:
                run_dict = await self._execute_step(
                    plan_id=plan_id,
                    step=step,
                    cluster_override=cluster_override,
                )
                run_records.append(run_dict)

                # Record the run in the database.
                await self._run_repo.create_run(run_dict)

                # Update watermark on success.
                if run_dict["status"] == RunStatus.SUCCESS.value and range_start and range_end:
                    await self._watermark_repo.update_watermark(
                        model_name=model_name,
                        partition_start=range_start,
                        partition_end=range_end,
                        row_count=None,
                    )

                # Emit telemetry.
                started = run_dict.get("started_at")
                finished = run_dict.get("finished_at")
                if started and finished:
                    runtime = (finished - started).total_seconds()
                    await self._telemetry_repo.record(
                        {
                            "run_id": run_dict["run_id"],
                            "model_name": model_name,
                            "runtime_seconds": runtime,
                            "shuffle_bytes": 0,
                            "input_rows": 0,
                            "output_rows": 0,
                            "partition_count": 1,
                        }
                    )

                # Compute and store cost from runtime x cluster rate.
                if run_dict["status"] == RunStatus.SUCCESS.value and started and finished:
                    cluster_size = cluster_override or "small"
                    try:
                        rate = get_cost_rate(cluster_size)
                        runtime = (finished - started).total_seconds()
                        computed_cost = runtime * rate
                        run_dict["cost_usd"] = computed_cost
                        await self._run_repo.update_cost(run_dict["run_id"], computed_cost)
                    except ValueError:
                        logger.warning(
                            "Unknown cluster size '%s'; skipping cost computation",
                            cluster_size,
                        )

                # Record AI feedback outcome.
                try:
                    await self._feedback_service.record_execution_outcome(
                        plan_id=plan_id,
                        step_id=step_id,
                        model_name=model_name,
                        run_dict=run_dict,
                    )
                except Exception:
                    logger.warning(
                        "Failed to record AI feedback for step %s",
                        step_id[:12],
                        exc_info=True,
                    )
            finally:
                # Release lock regardless of success or failure to prevent
                # orphan locks from blocking future runs.
                if locked and range_start and range_end:
                    await self._lock_repo.release_lock(
                        model_name=model_name,
                        range_start=range_start,
                        range_end=range_end,
                    )

        return run_records

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    async def backfill(
        self,
        model_name: str,
        start_date: str,
        end_date: str,
        cluster_size: str | None,
    ) -> dict[str, Any]:
        """Run a single-model backfill over a date range.

        Generates a synthetic one-step plan, checks locks, executes, and
        returns the plan together with its run record.
        """
        model_row = await self._model_repo.get(model_name)
        if model_row is None:
            raise ValueError(f"Model {model_name} not found")

        range_start = date.fromisoformat(start_date)
        range_end = date.fromisoformat(end_date)
        if range_start > range_end:
            raise ValueError("start_date must be <= end_date")

        # Lock check.
        is_locked = await self._lock_repo.check_lock(
            model_name=model_name,
            range_start=range_start,
            range_end=range_end,
        )
        if is_locked:
            raise RuntimeError(f"Model {model_name} is locked for range [{start_date}, {end_date}]")

        # Acquire lock.
        await self._lock_repo.acquire_lock(
            model_name=model_name,
            range_start=range_start,
            range_end=range_end,
            locked_by="backfill",
        )

        # Synthetic plan.
        step_id = compute_deterministic_id(model_name, start_date, end_date)
        plan_id = compute_deterministic_id("backfill", model_name, start_date, end_date)

        plan_dict: dict[str, Any] = {
            "plan_id": plan_id,
            "base": "backfill",
            "target": "backfill",
            "summary": {
                "total_steps": 1,
                "estimated_cost_usd": 0.0,
                "models_changed": [model_name],
            },
            "steps": [
                {
                    "step_id": step_id,
                    "model": model_name,
                    "run_type": "INCREMENTAL",
                    "input_range": {
                        "start": start_date,
                        "end": end_date,
                    },
                    "depends_on": [],
                    "parallel_group": 0,
                    "reason": "manual backfill",
                    "estimated_compute_seconds": 0.0,
                    "estimated_cost_usd": 0.0,
                }
            ],
        }

        # Persist the synthetic plan.
        await self._plan_repo.save_plan(
            plan_id=plan_id,
            base_sha="backfill",
            target_sha="backfill",
            plan_json=json.dumps(plan_dict),
        )

        # Execute.  Wrap in try/finally to guarantee lock release even
        # when _execute_step() or downstream persistence raises.
        step = plan_dict["steps"][0]
        try:
            run_dict = await self._execute_step(
                plan_id=plan_id,
                step=step,
                cluster_override=cluster_size,
            )
            await self._run_repo.create_run(run_dict)

            # Update watermark on success.
            if run_dict["status"] == RunStatus.SUCCESS.value:
                await self._watermark_repo.update_watermark(
                    model_name=model_name,
                    partition_start=range_start,
                    partition_end=range_end,
                    row_count=None,
                )

            # Compute and store cost from runtime x cluster rate.
            started = run_dict.get("started_at")
            finished = run_dict.get("finished_at")
            if run_dict["status"] == RunStatus.SUCCESS.value and started and finished:
                backfill_cluster = cluster_size or "small"
                try:
                    rate = get_cost_rate(backfill_cluster)
                    runtime = (finished - started).total_seconds()
                    computed_cost = runtime * rate
                    run_dict["cost_usd"] = computed_cost
                    await self._run_repo.update_cost(run_dict["run_id"], computed_cost)
                except ValueError:
                    logger.warning(
                        "Unknown cluster size '%s'; skipping cost computation",
                        backfill_cluster,
                    )
        finally:
            # Release lock regardless of success or failure to prevent
            # orphan locks from blocking future backfill runs.
            await self._lock_repo.release_lock(
                model_name=model_name,
                range_start=range_start,
                range_end=range_end,
            )

        return {"plan": plan_dict, "runs": [run_dict]}

    # ------------------------------------------------------------------
    # Chunked backfill
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_chunks(
        start: date,
        end: date,
        chunk_size_days: int,
    ) -> list[tuple[date, date]]:
        """Split a date range into day-aligned chunks.

        Each chunk is a ``(chunk_start, chunk_end)`` tuple where
        ``chunk_end`` is at most ``chunk_size_days - 1`` days after
        ``chunk_start``, and the last chunk ends on exactly ``end``.
        """
        chunks: list[tuple[date, date]] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=chunk_size_days - 1), end)
            chunks.append((cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)
        return chunks

    async def chunked_backfill(
        self,
        model_name: str,
        start_date: str,
        end_date: str,
        cluster_size: str | None = None,
        chunk_size_days: int = 7,
    ) -> dict[str, Any]:
        """Run a chunked backfill with checkpoint-based resume.

        Splits the requested date range into chunks (default 7 days each),
        executes them sequentially, and records progress in the
        ``backfill_checkpoints`` table.  If a chunk fails, the backfill
        is marked FAILED and can be resumed from the last completed chunk
        via :meth:`resume_backfill`.

        Parameters
        ----------
        model_name:
            Canonical model name to backfill.
        start_date:
            Start of the backfill range (YYYY-MM-DD, inclusive).
        end_date:
            End of the backfill range (YYYY-MM-DD, inclusive).
        cluster_size:
            Optional cluster size override.
        chunk_size_days:
            Number of days per chunk.  Must be >= 1.

        Returns
        -------
        dict
            ``{"backfill_id": str, "status": str, "completed_chunks": int,
               "total_chunks": int, "runs": list[dict]}``
        """
        model_row = await self._model_repo.get(model_name)
        if model_row is None:
            raise ValueError(f"Model {model_name} not found")

        range_start = date.fromisoformat(start_date)
        range_end = date.fromisoformat(end_date)
        if range_start > range_end:
            raise ValueError("start_date must be <= end_date")
        if chunk_size_days < 1:
            raise ValueError("chunk_size_days must be >= 1")

        # Compute chunks.
        chunks = self._compute_chunks(range_start, range_end, chunk_size_days)

        # Generate deterministic backfill ID.
        backfill_id = compute_deterministic_id(
            "chunked_backfill",
            model_name,
            start_date,
            end_date,
            str(chunk_size_days),
        )

        # Generate a plan ID for the overall backfill.
        plan_id = compute_deterministic_id(
            "chunked_backfill_plan",
            model_name,
            start_date,
            end_date,
        )

        # Create checkpoint record.
        await self._checkpoint_repo.create(
            backfill_id=backfill_id,
            model_name=model_name,
            overall_start=range_start,
            overall_end=range_end,
            chunk_size_days=chunk_size_days,
            total_chunks=len(chunks),
            cluster_size=cluster_size,
            plan_id=plan_id,
        )

        # Execute chunks sequentially.
        return await self._execute_chunks(
            backfill_id=backfill_id,
            model_name=model_name,
            plan_id=plan_id,
            chunks=chunks,
            cluster_size=cluster_size,
            start_chunk_index=0,
        )

    async def resume_backfill(
        self,
        backfill_id: str,
    ) -> dict[str, Any]:
        """Resume a previously failed or interrupted chunked backfill.

        Loads the checkpoint, verifies it is in a resumable state (FAILED
        or RUNNING), recomputes the remaining chunks, and continues
        execution from the first chunk after ``completed_through``.

        Parameters
        ----------
        backfill_id:
            The backfill identifier to resume.

        Returns
        -------
        dict
            Same structure as :meth:`chunked_backfill`.

        Raises
        ------
        ValueError
            If the backfill is not found or is not in a resumable state.
        """
        checkpoint = await self._checkpoint_repo.get(backfill_id)
        if checkpoint is None:
            raise ValueError(f"Backfill {backfill_id} not found")

        if checkpoint.status == "COMPLETED":
            raise ValueError(f"Backfill {backfill_id} is already completed; nothing to resume")

        if checkpoint.status not in ("FAILED", "RUNNING"):
            raise ValueError(
                f"Backfill {backfill_id} has unexpected status '{checkpoint.status}' and cannot be resumed"
            )

        # Determine resume point.
        if checkpoint.completed_through is not None:
            resume_start = checkpoint.completed_through + timedelta(days=1)
        else:
            resume_start = checkpoint.overall_start

        if resume_start > checkpoint.overall_end:
            # All chunks were actually completed -- mark it done.
            await self._checkpoint_repo.mark_completed(backfill_id)
            return {
                "backfill_id": backfill_id,
                "status": "COMPLETED",
                "completed_chunks": checkpoint.total_chunks,
                "total_chunks": checkpoint.total_chunks,
                "runs": [],
            }

        # Recompute remaining chunks.
        remaining_chunks = self._compute_chunks(
            resume_start,
            checkpoint.overall_end,
            checkpoint.chunk_size_days,
        )

        logger.info(
            "Resuming backfill %s from %s (%d chunks remaining)",
            backfill_id[:12],
            resume_start.isoformat(),
            len(remaining_chunks),
        )

        return await self._execute_chunks(
            backfill_id=backfill_id,
            model_name=checkpoint.model_name,
            plan_id=checkpoint.plan_id
            or compute_deterministic_id(
                "chunked_backfill_plan",
                checkpoint.model_name,
                checkpoint.overall_start.isoformat(),
                checkpoint.overall_end.isoformat(),
            ),
            chunks=remaining_chunks,
            cluster_size=checkpoint.cluster_size,
            start_chunk_index=checkpoint.completed_chunks,
        )

    async def _execute_chunks(
        self,
        backfill_id: str,
        model_name: str,
        plan_id: str,
        chunks: list[tuple[date, date]],
        cluster_size: str | None,
        start_chunk_index: int,
    ) -> dict[str, Any]:
        """Execute a series of date-range chunks sequentially.

        For each chunk:
        1. Build a synthetic step dict
        2. Acquire a lock on the chunk's date range
        3. Execute the step
        4. Record the run
        5. Update watermark on success
        6. Record chunk audit entry
        7. Update checkpoint progress
        8. Release the lock

        On failure, the chunk is recorded as FAILED, the checkpoint is
        marked FAILED, and the method returns immediately (no further
        chunks are executed).
        """
        checkpoint = await self._checkpoint_repo.get(backfill_id)
        total_chunks = checkpoint.total_chunks if checkpoint else len(chunks) + start_chunk_index
        run_dicts: list[dict[str, Any]] = []
        completed = start_chunk_index

        for i, (chunk_start, chunk_end) in enumerate(chunks):
            chunk_num = start_chunk_index + i + 1
            chunk_start_iso = chunk_start.isoformat()
            chunk_end_iso = chunk_end.isoformat()

            step_id = compute_deterministic_id(
                model_name,
                "chunk",
                chunk_start_iso,
                chunk_end_iso,
            )

            step: dict[str, Any] = {
                "step_id": step_id,
                "model": model_name,
                "run_type": "INCREMENTAL",
                "input_range": {
                    "start": chunk_start_iso,
                    "end": chunk_end_iso,
                },
                "depends_on": [],
                "parallel_group": 0,
                "reason": f"chunked backfill chunk {chunk_num}/{total_chunks}",
            }

            # Acquire lock for this chunk's range.
            locked = await self._lock_repo.acquire_lock(
                model_name=model_name,
                range_start=chunk_start,
                range_end=chunk_end,
                locked_by=f"chunked_backfill:{backfill_id[:12]}",
            )
            if not locked:
                error_msg = f"Lock acquisition failed for chunk [{chunk_start_iso}, {chunk_end_iso}]"
                await self._audit_repo.record_chunk(
                    backfill_id=backfill_id,
                    model_name=model_name,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    status="FAILED",
                    error_message=error_msg,
                )
                await self._checkpoint_repo.mark_failed(backfill_id, error_msg)
                return {
                    "backfill_id": backfill_id,
                    "status": "FAILED",
                    "completed_chunks": completed,
                    "total_chunks": total_chunks,
                    "runs": run_dicts,
                    "error": error_msg,
                }

            # Execute.  Wrap in try/finally to guarantee lock release even
            # when _execute_step() or downstream persistence raises.
            try:
                chunk_started = datetime.now(UTC)
                run_dict = await self._execute_step(
                    plan_id=plan_id,
                    step=step,
                    cluster_override=cluster_size,
                )
                chunk_finished = datetime.now(UTC)
                chunk_duration = (chunk_finished - chunk_started).total_seconds()

                # Record run.
                await self._run_repo.create_run(run_dict)
                run_dicts.append(run_dict)

                if run_dict["status"] == RunStatus.SUCCESS.value:
                    # Update watermark.
                    await self._watermark_repo.update_watermark(
                        model_name=model_name,
                        partition_start=chunk_start,
                        partition_end=chunk_end,
                        row_count=None,
                    )

                    # Compute cost.
                    started = run_dict.get("started_at")
                    finished = run_dict.get("finished_at")
                    if started and finished:
                        try:
                            rate = get_cost_rate(cluster_size or "small")
                            runtime = (finished - started).total_seconds()
                            computed_cost = runtime * rate
                            run_dict["cost_usd"] = computed_cost
                            await self._run_repo.update_cost(run_dict["run_id"], computed_cost)
                        except ValueError:
                            pass

                    # Audit: success.
                    await self._audit_repo.record_chunk(
                        backfill_id=backfill_id,
                        model_name=model_name,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                        status="SUCCESS",
                        run_id=run_dict["run_id"],
                        duration_seconds=chunk_duration,
                    )

                    # Advance checkpoint.
                    completed += 1
                    await self._checkpoint_repo.update_progress(
                        backfill_id=backfill_id,
                        completed_through=chunk_end,
                        completed_chunks=completed,
                    )
                else:
                    # Chunk failed: record audit, mark checkpoint failed, stop.
                    error_msg = run_dict.get("error_message", "Unknown error")
                    await self._audit_repo.record_chunk(
                        backfill_id=backfill_id,
                        model_name=model_name,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                        status="FAILED",
                        run_id=run_dict["run_id"],
                        error_message=error_msg,
                        duration_seconds=chunk_duration,
                    )
                    await self._checkpoint_repo.mark_failed(backfill_id, error_msg or "Chunk execution failed")

                    return {
                        "backfill_id": backfill_id,
                        "status": "FAILED",
                        "completed_chunks": completed,
                        "total_chunks": total_chunks,
                        "runs": run_dicts,
                        "error": error_msg,
                    }
            finally:
                # Release lock regardless of success or failure to prevent
                # orphan locks from blocking future chunk executions.
                await self._lock_repo.release_lock(
                    model_name=model_name,
                    range_start=chunk_start,
                    range_end=chunk_end,
                )

        # All chunks completed successfully.
        await self._checkpoint_repo.mark_completed(backfill_id)

        return {
            "backfill_id": backfill_id,
            "status": "COMPLETED",
            "completed_chunks": completed,
            "total_chunks": total_chunks,
            "runs": run_dicts,
        }

    async def get_backfill_status(
        self,
        backfill_id: str,
    ) -> dict[str, Any]:
        """Return the current status of a chunked backfill.

        Returns
        -------
        dict
            Checkpoint fields plus the list of audit entries.

        Raises
        ------
        ValueError
            If the backfill is not found.
        """
        checkpoint = await self._checkpoint_repo.get(backfill_id)
        if checkpoint is None:
            raise ValueError(f"Backfill {backfill_id} not found")

        audit_entries = await self._audit_repo.get_for_backfill(backfill_id)

        return {
            "backfill_id": checkpoint.backfill_id,
            "model_name": checkpoint.model_name,
            "overall_start": checkpoint.overall_start.isoformat(),
            "overall_end": checkpoint.overall_end.isoformat(),
            "completed_through": (checkpoint.completed_through.isoformat() if checkpoint.completed_through else None),
            "chunk_size_days": checkpoint.chunk_size_days,
            "status": checkpoint.status,
            "total_chunks": checkpoint.total_chunks,
            "completed_chunks": checkpoint.completed_chunks,
            "error_message": checkpoint.error_message,
            "cluster_size": checkpoint.cluster_size,
            "plan_id": checkpoint.plan_id,
            "created_at": checkpoint.created_at.isoformat(),
            "updated_at": checkpoint.updated_at.isoformat(),
            "chunks": [
                {
                    "chunk_start": entry.chunk_start.isoformat(),
                    "chunk_end": entry.chunk_end.isoformat(),
                    "status": entry.status,
                    "run_id": entry.run_id,
                    "error_message": entry.error_message,
                    "duration_seconds": entry.duration_seconds,
                    "executed_at": entry.executed_at.isoformat(),
                }
                for entry in audit_entries
            ],
        }

    async def get_backfill_history(
        self,
        model_name: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return backfill history for a model.

        Returns
        -------
        list[dict]
            List of backfill checkpoint summaries, newest first.
        """
        checkpoints = await self._checkpoint_repo.list_for_model(
            model_name,
            limit=limit,
        )
        return [
            {
                "backfill_id": cp.backfill_id,
                "model_name": cp.model_name,
                "overall_start": cp.overall_start.isoformat(),
                "overall_end": cp.overall_end.isoformat(),
                "completed_through": (cp.completed_through.isoformat() if cp.completed_through else None),
                "status": cp.status,
                "total_chunks": cp.total_chunks,
                "completed_chunks": cp.completed_chunks,
                "chunk_size_days": cp.chunk_size_days,
                "error_message": cp.error_message,
                "created_at": cp.created_at.isoformat(),
            }
            for cp in checkpoints
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _execute_step(
        self,
        plan_id: str,
        step: dict[str, Any],
        cluster_override: str | None,
    ) -> dict[str, Any]:
        """Execute a single step and return a run record dict.

        In development mode (or when Databricks is not configured) the
        step is executed locally via DuckDB.  In production the Databricks
        executor is used.
        """
        run_id = str(uuid4())
        model_name = step["model"]
        step_id = step["step_id"]
        started_at = datetime.now(UTC)

        input_range = step.get("input_range")
        range_start = date.fromisoformat(input_range["start"]) if input_range else None
        range_end = date.fromisoformat(input_range["end"]) if input_range else None

        logger.info(
            "Executing step %s for model %s (run %s)",
            step_id[:12],
            model_name,
            run_id[:12],
        )

        status = RunStatus.SUCCESS
        error_message: str | None = None

        try:
            # Attempt Databricks execution if configured.
            if self._is_databricks_available():
                await self._execute_on_databricks(
                    step=step,
                    cluster_override=cluster_override,
                )
            else:
                await self._execute_locally(step=step)
        except Exception as exc:
            logger.error(
                "Step %s failed for model %s: %s",
                step_id[:12],
                model_name,
                exc,
            )
            status = RunStatus.FAIL
            error_message = str(exc)[:2000]

        finished_at = datetime.now(UTC)

        return self._make_run_dict(
            plan_id=plan_id,
            step_id=step_id,
            model_name=model_name,
            status=status,
            error_message=error_message,
            range_start=range_start,
            range_end=range_end,
            started_at=started_at,
            finished_at=finished_at,
            run_id=run_id,
        )

    def _is_databricks_available(self) -> bool:
        """Check whether Databricks credentials are configured."""
        # The API layer defers to core_engine config for executor selection.
        # For the control plane, we detect via env var presence.
        import os

        return bool(os.environ.get("PLATFORM_DATABRICKS_HOST") and os.environ.get("PLATFORM_DATABRICKS_TOKEN"))

    async def _execute_on_databricks(
        self,
        step: dict[str, Any],
        cluster_override: str | None,
    ) -> None:
        """Submit a step to Databricks and poll to completion."""
        import os

        from core_engine.executor import DatabricksExecutor

        host = os.environ["PLATFORM_DATABRICKS_HOST"]
        token = os.environ["PLATFORM_DATABRICKS_TOKEN"]
        warehouse_id = os.environ.get("PLATFORM_DATABRICKS_WAREHOUSE_ID")

        executor = DatabricksExecutor(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            default_cluster_size=cluster_override or "small",
        )

        plan_step = PlanStep(
            step_id=step["step_id"],
            model=step["model"],
            run_type=RunType(step.get("run_type", "FULL_REFRESH")),
            input_range=(DateRange(**step["input_range"]) if step.get("input_range") else None),
            depends_on=step.get("depends_on", []),
            parallel_group=step.get("parallel_group", 0),
            reason=step.get("reason", ""),
        )

        # The model SQL would normally be loaded from the repo; for the
        # control plane we pass the model name as the query identifier.
        result = executor.execute_step(
            step=plan_step,
            sql=step["model"],
            parameters={},
        )

        if result.status == RunStatus.FAIL:
            raise RuntimeError(result.error_message or "Databricks step failed")

    async def _execute_locally(self, step: dict[str, Any]) -> None:
        """Execute a step using the local DuckDB executor.

        This is used in development environments where Databricks is
        not configured.  The execution transpiles the model SQL from
        Databricks dialect to DuckDB dialect via sqlglot, applies the
        SQL guard to the final SQL, and runs it against an in-memory
        DuckDB instance.

        If the model SQL references tables that do not exist in DuckDB,
        EXPLAIN is used as a validation fallback to at least prove the
        SQL parses and has a valid plan shape.
        """
        import duckdb
        from core_engine.parser.sql_guard import assert_sql_safe
        from core_engine.sql_toolkit import Dialect, get_sql_toolkit

        model_name = step["model"]
        step_id = step["step_id"]

        logger.info(
            "Local DuckDB execution for model %s step %s",
            model_name,
            step_id[:12],
        )

        # Retrieve the model's canonical SQL from the model_versions table.
        # The model_name is used as a lookup key.  If the model's SQL is
        # not available (e.g. the step was synthesized for a backfill), fall
        # back to a validation-only pass.
        model_sql: str | None = None
        try:
            from core_engine.state.tables import ModelVersionTable
            from sqlalchemy import select as sa_select

            stmt = (
                sa_select(ModelVersionTable.canonical_sql)
                .where(
                    ModelVersionTable.tenant_id == self._tenant_id,
                    ModelVersionTable.model_name == model_name,
                )
                .order_by(ModelVersionTable.created_at.desc())
                .limit(1)
            )
            result = await self._session.execute(stmt)
            row = result.scalar_one_or_none()
            if row:
                model_sql = row
        except Exception:
            logger.debug(
                "Could not fetch canonical SQL for model %s; proceeding with validation-only mode",
                model_name,
                exc_info=True,
            )

        if not model_sql:
            logger.info(
                "No SQL found for model %s; local execution completed (validation-only)",
                model_name,
            )
            return

        # Apply SQL guard to the model SQL before execution.
        assert_sql_safe(model_sql)

        # Transpile from Databricks dialect to DuckDB.
        tk = get_sql_toolkit()
        try:
            transpile_result = tk.transpiler.transpile(
                model_sql,
                Dialect.DATABRICKS,
                Dialect.DUCKDB,
            )
            duckdb_sql = transpile_result.output_sql
            if not duckdb_sql:
                raise RuntimeError(f"SQL transpilation produced no output for model {model_name}")
        except Exception as exc:
            raise RuntimeError(f"SQL transpilation failed for model {model_name}: {exc}") from exc

        # Execute in a restricted DuckDB sandbox.
        conn = duckdb.connect(":memory:")
        try:
            # Restrict external access.
            try:
                conn.execute("SET enable_external_access = false")
            except duckdb.Error:
                logger.debug("Could not disable external access in local DuckDB")

            # Attempt direct execution.
            try:
                conn.execute(duckdb_sql)
                logger.info(
                    "Local DuckDB execution succeeded for model %s",
                    model_name,
                )
                return
            except duckdb.Error as exec_err:
                # If tables don't exist, fall back to EXPLAIN for validation.
                logger.debug(
                    "Direct execution failed for model %s (%s); trying EXPLAIN",
                    model_name,
                    exec_err,
                )

            try:
                conn.execute(f"EXPLAIN {duckdb_sql}")
                logger.info(
                    "Local DuckDB EXPLAIN validation passed for model %s",
                    model_name,
                )
            except duckdb.Error:
                # Final fallback: parse validation via sql_toolkit.
                try:
                    tk.parser.parse_one(duckdb_sql, Dialect.DUCKDB)
                    logger.info(
                        "Local parse validation passed for model %s",
                        model_name,
                    )
                except Exception as parse_err:
                    raise RuntimeError(
                        f"Local DuckDB validation failed for model {model_name}: {parse_err}"
                    ) from parse_err
        finally:
            conn.close()

    @staticmethod
    def _make_run_dict(
        plan_id: str,
        step_id: str,
        model_name: str,
        status: RunStatus,
        error_message: str | None = None,
        range_start: date | None = None,
        range_end: date | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Assemble a run record dictionary ready for persistence."""
        now = datetime.now(UTC)
        return {
            "run_id": run_id or str(uuid4()),
            "plan_id": plan_id,
            "step_id": step_id,
            "model_name": model_name,
            "status": status.value,
            "started_at": started_at or now,
            "finished_at": finished_at or now,
            "input_range_start": range_start,
            "input_range_end": range_end,
            "error_message": error_message,
            "logs_uri": None,
            "cluster_used": None,
            "executor_version": _EXECUTOR_VERSION,
            "retry_count": 0,
            "cost_usd": None,
            "external_run_id": None,
        }
