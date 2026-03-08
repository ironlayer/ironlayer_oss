"""Tests for chunked backfill with checkpoint-based resume.

Covers:
- _compute_chunks(): date range splitting into day-aligned chunks
- chunked_backfill(): full execution, checkpoint creation, audit trail
- Mid-run failure: creates FAILED checkpoint, stops at failed chunk
- resume_backfill(): resumes from last completed chunk
- resume_backfill(): edge cases (already completed, not found)
- get_backfill_status(): returns checkpoint + audit entries
- get_backfill_history(): returns checkpoint summaries
- Lock acquisition failure mid-backfill
- Router endpoints: POST /chunked, POST /{id}/resume, GET /status/{id}, GET /history/{model}
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from api.config import APISettings
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


def _make_model_row(model_name: str = "staging.orders") -> MagicMock:
    row = MagicMock()
    row.model_name = model_name
    row.tenant_id = "default"
    row.kind = "sql"
    row.materialization = "incremental"
    row.time_column = "order_date"
    return row


def _make_checkpoint_row(
    backfill_id: str = "bf-001",
    model_name: str = "staging.orders",
    overall_start: date = date(2024, 1, 1),
    overall_end: date = date(2024, 1, 21),
    completed_through: date | None = None,
    chunk_size_days: int = 7,
    status: str = "RUNNING",
    total_chunks: int = 3,
    completed_chunks: int = 0,
    error_message: str | None = None,
    cluster_size: str | None = None,
    plan_id: str | None = "plan-bf-001",
) -> MagicMock:
    row = MagicMock()
    row.backfill_id = backfill_id
    row.model_name = model_name
    row.overall_start = overall_start
    row.overall_end = overall_end
    row.completed_through = completed_through
    row.chunk_size_days = chunk_size_days
    row.status = status
    row.total_chunks = total_chunks
    row.completed_chunks = completed_chunks
    row.error_message = error_message
    row.cluster_size = cluster_size
    row.plan_id = plan_id
    row.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    row.updated_at = datetime(2024, 6, 15, 12, 5, 0, tzinfo=timezone.utc)
    return row


def _make_audit_entry(
    chunk_start: date,
    chunk_end: date,
    status: str = "SUCCESS",
    run_id: str | None = "run-001",
    error_message: str | None = None,
    duration_seconds: float | None = 30.0,
) -> MagicMock:
    entry = MagicMock()
    entry.chunk_start = chunk_start
    entry.chunk_end = chunk_end
    entry.status = status
    entry.run_id = run_id
    entry.error_message = error_message
    entry.duration_seconds = duration_seconds
    entry.executed_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    return entry


def _setup_service_for_chunked(
    mock_session: AsyncMock,
    model_row: MagicMock | None = None,
    lock_returns: bool = True,
) -> ExecutionService:
    """Create an ExecutionService with mocked repositories for chunked backfill."""
    settings = _make_settings("dev")
    service = ExecutionService(mock_session, settings)

    service._model_repo = MagicMock()
    service._model_repo.get = AsyncMock(return_value=model_row or _make_model_row())

    service._plan_repo = MagicMock()
    service._plan_repo.save_plan = AsyncMock()

    service._run_repo = MagicMock()
    service._run_repo.create_run = AsyncMock()
    service._run_repo.update_cost = AsyncMock()

    service._lock_repo = MagicMock()
    service._lock_repo.acquire_lock = AsyncMock(return_value=lock_returns)
    service._lock_repo.release_lock = AsyncMock()
    service._lock_repo.check_lock = AsyncMock(return_value=False)

    service._watermark_repo = MagicMock()
    service._watermark_repo.update_watermark = AsyncMock()

    service._telemetry_repo = MagicMock()
    service._telemetry_repo.record = AsyncMock()

    service._checkpoint_repo = MagicMock()
    service._checkpoint_repo.create = AsyncMock()
    service._checkpoint_repo.get = AsyncMock(return_value=_make_checkpoint_row())
    service._checkpoint_repo.update_progress = AsyncMock()
    service._checkpoint_repo.mark_completed = AsyncMock()
    service._checkpoint_repo.mark_failed = AsyncMock()
    service._checkpoint_repo.get_resumable = AsyncMock(return_value=[])
    service._checkpoint_repo.list_for_model = AsyncMock(return_value=[])

    service._audit_repo = MagicMock()
    service._audit_repo.record_chunk = AsyncMock()
    service._audit_repo.get_history = AsyncMock(return_value=[])
    service._audit_repo.get_for_backfill = AsyncMock(return_value=[])

    return service


# ---------------------------------------------------------------------------
# _compute_chunks() — static method tests
# ---------------------------------------------------------------------------


class TestComputeChunks:
    """Verify date range splitting into day-aligned chunks."""

    def test_exact_multiple(self):
        """21-day range with 7-day chunks produces exactly 3 chunks."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 1, 21),
            chunk_size_days=7,
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2024, 1, 1), date(2024, 1, 7))
        assert chunks[1] == (date(2024, 1, 8), date(2024, 1, 14))
        assert chunks[2] == (date(2024, 1, 15), date(2024, 1, 21))

    def test_non_exact_multiple(self):
        """10-day range with 7-day chunks produces 2 chunks (7+3)."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 1, 10),
            chunk_size_days=7,
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2024, 1, 1), date(2024, 1, 7))
        assert chunks[1] == (date(2024, 1, 8), date(2024, 1, 10))

    def test_single_day(self):
        """Single-day range produces one chunk."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 1, 1),
            chunk_size_days=7,
        )
        assert len(chunks) == 1
        assert chunks[0] == (date(2024, 1, 1), date(2024, 1, 1))

    def test_chunk_size_one(self):
        """Chunk size of 1 produces one chunk per day."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 1, 5),
            chunk_size_days=1,
        )
        assert len(chunks) == 5
        for i, (s, e) in enumerate(chunks):
            assert s == date(2024, 1, 1 + i)
            assert s == e  # Each chunk is one day.

    def test_chunk_larger_than_range(self):
        """Chunk size larger than the range produces one chunk."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 1, 5),
            chunk_size_days=30,
        )
        assert len(chunks) == 1
        assert chunks[0] == (date(2024, 1, 1), date(2024, 1, 5))

    def test_month_boundaries(self):
        """Chunks correctly cross month boundaries."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 28),
            date(2024, 2, 10),
            chunk_size_days=7,
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2024, 1, 28), date(2024, 2, 3))
        assert chunks[1] == (date(2024, 2, 4), date(2024, 2, 10))

    def test_large_range(self):
        """365-day range with 30-day chunks produces correct count."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 12, 31),
            chunk_size_days=30,
        )
        # 366 days (leap year) / 30 = 13 chunks (12 full + 1 partial).
        assert len(chunks) == 13
        # First chunk starts on Jan 1.
        assert chunks[0][0] == date(2024, 1, 1)
        # Last chunk ends on Dec 31.
        assert chunks[-1][1] == date(2024, 12, 31)

    def test_contiguous_coverage(self):
        """All chunks are contiguous (no gaps or overlaps)."""
        chunks = ExecutionService._compute_chunks(
            date(2024, 1, 1),
            date(2024, 3, 15),
            chunk_size_days=10,
        )
        for i in range(len(chunks) - 1):
            _, end = chunks[i]
            next_start, _ = chunks[i + 1]
            from datetime import timedelta

            assert next_start == end + timedelta(days=1), (
                f"Gap between chunk {i} ending {end} and chunk {i + 1} starting {next_start}"
            )


# ---------------------------------------------------------------------------
# chunked_backfill() — full execution flow
# ---------------------------------------------------------------------------


class TestChunkedBackfill:
    """Test chunked_backfill() method on ExecutionService."""

    @pytest.mark.asyncio
    async def test_successful_chunked_backfill(self, mock_session: AsyncMock):
        """All chunks succeed: checkpoint created, progress updated, marked completed."""
        service = _setup_service_for_chunked(mock_session)

        with patch.object(service, "_is_databricks_available", return_value=False):
            result = await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-21",
                chunk_size_days=7,
            )

        assert result["status"] == "COMPLETED"
        assert result["completed_chunks"] == 3
        assert result["total_chunks"] == 3
        assert len(result["runs"]) == 3
        assert "backfill_id" in result

        # Checkpoint lifecycle.
        service._checkpoint_repo.create.assert_awaited_once()
        create_call = service._checkpoint_repo.create.call_args
        assert create_call.kwargs["total_chunks"] == 3
        assert create_call.kwargs["chunk_size_days"] == 7

        # Progress updated 3 times (once per chunk).
        assert service._checkpoint_repo.update_progress.await_count == 3

        # Marked completed at the end.
        service._checkpoint_repo.mark_completed.assert_awaited_once()

        # 3 audit entries recorded (all SUCCESS).
        assert service._audit_repo.record_chunk.await_count == 3

        # 3 locks acquired and released.
        assert service._lock_repo.acquire_lock.await_count == 3
        assert service._lock_repo.release_lock.await_count == 3

        # 3 watermarks updated.
        assert service._watermark_repo.update_watermark.await_count == 3

    @pytest.mark.asyncio
    async def test_chunked_backfill_model_not_found(self, mock_session: AsyncMock):
        """ValueError raised when model does not exist."""
        service = _setup_service_for_chunked(mock_session)
        service._model_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="Model.*not found"):
            await service.chunked_backfill(
                model_name="nonexistent.model",
                start_date="2024-01-01",
                end_date="2024-01-21",
            )

    @pytest.mark.asyncio
    async def test_chunked_backfill_invalid_dates(self, mock_session: AsyncMock):
        """ValueError raised when start_date > end_date."""
        service = _setup_service_for_chunked(mock_session)

        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-21",
                end_date="2024-01-01",
            )

    @pytest.mark.asyncio
    async def test_chunked_backfill_invalid_chunk_size(self, mock_session: AsyncMock):
        """ValueError raised when chunk_size_days < 1."""
        service = _setup_service_for_chunked(mock_session)

        with pytest.raises(ValueError, match="chunk_size_days must be >= 1"):
            await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-21",
                chunk_size_days=0,
            )

    @pytest.mark.asyncio
    async def test_chunked_backfill_single_day(self, mock_session: AsyncMock):
        """Single-day backfill produces one chunk."""
        service = _setup_service_for_chunked(mock_session)

        # Override checkpoint mock to return single-chunk checkpoint.
        service._checkpoint_repo.get = AsyncMock(
            return_value=_make_checkpoint_row(total_chunks=1),
        )

        with patch.object(service, "_is_databricks_available", return_value=False):
            result = await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-01",
                chunk_size_days=7,
            )

        assert result["status"] == "COMPLETED"
        assert result["completed_chunks"] == 1
        assert result["total_chunks"] == 1


# ---------------------------------------------------------------------------
# Mid-run failure and checkpoint
# ---------------------------------------------------------------------------


class TestChunkedBackfillFailure:
    """Test failure scenarios during chunked backfill execution."""

    @pytest.mark.asyncio
    async def test_failure_on_second_chunk(self, mock_session: AsyncMock):
        """When second chunk fails, checkpoint is FAILED with 1 completed chunk."""
        service = _setup_service_for_chunked(mock_session)

        call_count = 0

        async def _execute_locally_side_effect(step):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("OOM on worker node")

        with (
            patch.object(service, "_is_databricks_available", return_value=False),
            patch.object(service, "_execute_locally", side_effect=_execute_locally_side_effect),
        ):
            result = await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-21",
                chunk_size_days=7,
            )

        assert result["status"] == "FAILED"
        assert result["completed_chunks"] == 1
        assert result["total_chunks"] == 3
        assert len(result["runs"]) == 2  # First success + second failure
        assert "error" in result

        # Checkpoint marked failed.
        service._checkpoint_repo.mark_failed.assert_awaited_once()

        # Only 1 progress update (the successful chunk).
        assert service._checkpoint_repo.update_progress.await_count == 1

        # 2 audit entries: 1 SUCCESS + 1 FAILED.
        assert service._audit_repo.record_chunk.await_count == 2

        # Watermark updated only for the successful first chunk.
        assert service._watermark_repo.update_watermark.await_count == 1

    @pytest.mark.asyncio
    async def test_failure_on_first_chunk(self, mock_session: AsyncMock):
        """When first chunk fails, checkpoint is FAILED with 0 completed chunks."""
        service = _setup_service_for_chunked(mock_session)

        with (
            patch.object(service, "_is_databricks_available", return_value=False),
            patch.object(
                service,
                "_execute_locally",
                AsyncMock(side_effect=RuntimeError("Cluster unavailable")),
            ),
        ):
            result = await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-21",
                chunk_size_days=7,
            )

        assert result["status"] == "FAILED"
        assert result["completed_chunks"] == 0
        assert len(result["runs"]) == 1

    @pytest.mark.asyncio
    async def test_lock_failure_mid_backfill(self, mock_session: AsyncMock):
        """Lock failure on second chunk fails the backfill."""
        service = _setup_service_for_chunked(mock_session)

        lock_call_count = 0

        async def _lock_side_effect(*args, **kwargs):
            nonlocal lock_call_count
            lock_call_count += 1
            if lock_call_count == 2:
                return False
            return True

        service._lock_repo.acquire_lock = AsyncMock(side_effect=_lock_side_effect)

        with patch.object(service, "_is_databricks_available", return_value=False):
            result = await service.chunked_backfill(
                model_name="staging.orders",
                start_date="2024-01-01",
                end_date="2024-01-21",
                chunk_size_days=7,
            )

        assert result["status"] == "FAILED"
        assert result["completed_chunks"] == 1
        assert "Lock acquisition failed" in result.get("error", "")

        # Checkpoint marked failed.
        service._checkpoint_repo.mark_failed.assert_awaited_once()


# ---------------------------------------------------------------------------
# resume_backfill()
# ---------------------------------------------------------------------------


class TestResumeBackfill:
    """Test resume_backfill() method on ExecutionService."""

    @pytest.mark.asyncio
    async def test_resume_from_failed_checkpoint(self, mock_session: AsyncMock):
        """Resume picks up from completed_through + 1 day."""
        service = _setup_service_for_chunked(mock_session)

        # Simulate checkpoint that completed chunk 1 (Jan 1-7) then failed.
        failed_checkpoint = _make_checkpoint_row(
            backfill_id="bf-resume",
            status="FAILED",
            completed_through=date(2024, 1, 7),
            completed_chunks=1,
            total_chunks=3,
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
            error_message="Previous failure",
        )
        service._checkpoint_repo.get = AsyncMock(return_value=failed_checkpoint)

        with patch.object(service, "_is_databricks_available", return_value=False):
            result = await service.resume_backfill("bf-resume")

        assert result["status"] == "COMPLETED"
        assert result["backfill_id"] == "bf-resume"
        # Should have executed 2 remaining chunks (Jan 8-14, Jan 15-21).
        assert len(result["runs"]) == 2

        # Checkpoint marked completed.
        service._checkpoint_repo.mark_completed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_resume_not_found(self, mock_session: AsyncMock):
        """ValueError raised when backfill does not exist."""
        service = _setup_service_for_chunked(mock_session)
        service._checkpoint_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="Backfill.*not found"):
            await service.resume_backfill("nonexistent")

    @pytest.mark.asyncio
    async def test_resume_already_completed(self, mock_session: AsyncMock):
        """ValueError raised when backfill is already completed."""
        service = _setup_service_for_chunked(mock_session)
        service._checkpoint_repo.get = AsyncMock(return_value=_make_checkpoint_row(status="COMPLETED"))

        with pytest.raises(ValueError, match="already completed"):
            await service.resume_backfill("bf-completed")

    @pytest.mark.asyncio
    async def test_resume_with_no_completed_chunks(self, mock_session: AsyncMock):
        """Resume from the very beginning when completed_through is None."""
        service = _setup_service_for_chunked(mock_session)

        # Checkpoint failed before any chunk completed.
        failed_checkpoint = _make_checkpoint_row(
            backfill_id="bf-fresh-fail",
            status="FAILED",
            completed_through=None,
            completed_chunks=0,
            total_chunks=3,
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
        )
        service._checkpoint_repo.get = AsyncMock(return_value=failed_checkpoint)

        with patch.object(service, "_is_databricks_available", return_value=False):
            result = await service.resume_backfill("bf-fresh-fail")

        assert result["status"] == "COMPLETED"
        # All 3 chunks should be executed.
        assert len(result["runs"]) == 3

    @pytest.mark.asyncio
    async def test_resume_all_chunks_already_done(self, mock_session: AsyncMock):
        """Resume when completed_through >= overall_end marks as completed immediately."""
        service = _setup_service_for_chunked(mock_session)

        checkpoint = _make_checkpoint_row(
            backfill_id="bf-actually-done",
            status="FAILED",
            completed_through=date(2024, 1, 21),
            completed_chunks=3,
            total_chunks=3,
            overall_start=date(2024, 1, 1),
            overall_end=date(2024, 1, 21),
        )
        service._checkpoint_repo.get = AsyncMock(return_value=checkpoint)

        result = await service.resume_backfill("bf-actually-done")

        assert result["status"] == "COMPLETED"
        assert len(result["runs"]) == 0
        service._checkpoint_repo.mark_completed.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_backfill_status()
# ---------------------------------------------------------------------------


class TestGetBackfillStatus:
    """Test get_backfill_status() method on ExecutionService."""

    @pytest.mark.asyncio
    async def test_returns_checkpoint_with_audit_entries(
        self,
        mock_session: AsyncMock,
    ):
        service = _setup_service_for_chunked(mock_session)

        checkpoint = _make_checkpoint_row(
            backfill_id="bf-status",
            status="RUNNING",
            completed_through=date(2024, 1, 7),
            completed_chunks=1,
        )
        service._checkpoint_repo.get = AsyncMock(return_value=checkpoint)

        audit_entries = [
            _make_audit_entry(
                chunk_start=date(2024, 1, 1),
                chunk_end=date(2024, 1, 7),
                status="SUCCESS",
            ),
        ]
        service._audit_repo.get_for_backfill = AsyncMock(return_value=audit_entries)

        result = await service.get_backfill_status("bf-status")

        assert result["backfill_id"] == "bf-status"
        assert result["status"] == "RUNNING"
        assert result["completed_through"] == "2024-01-07"
        assert result["completed_chunks"] == 1
        assert len(result["chunks"]) == 1
        assert result["chunks"][0]["status"] == "SUCCESS"
        assert result["chunks"][0]["chunk_start"] == "2024-01-01"

    @pytest.mark.asyncio
    async def test_not_found_raises_value_error(self, mock_session: AsyncMock):
        service = _setup_service_for_chunked(mock_session)
        service._checkpoint_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="Backfill.*not found"):
            await service.get_backfill_status("nonexistent")


# ---------------------------------------------------------------------------
# get_backfill_history()
# ---------------------------------------------------------------------------


class TestGetBackfillHistory:
    """Test get_backfill_history() method on ExecutionService."""

    @pytest.mark.asyncio
    async def test_returns_checkpoint_summaries(self, mock_session: AsyncMock):
        service = _setup_service_for_chunked(mock_session)

        checkpoints = [
            _make_checkpoint_row(backfill_id="bf-1", status="COMPLETED"),
            _make_checkpoint_row(backfill_id="bf-2", status="FAILED"),
        ]
        service._checkpoint_repo.list_for_model = AsyncMock(
            return_value=checkpoints,
        )

        result = await service.get_backfill_history("staging.orders", limit=10)

        assert len(result) == 2
        assert result[0]["backfill_id"] == "bf-1"
        assert result[0]["status"] == "COMPLETED"
        assert result[1]["backfill_id"] == "bf-2"
        assert result[1]["status"] == "FAILED"

    @pytest.mark.asyncio
    async def test_returns_empty_for_unknown_model(self, mock_session: AsyncMock):
        service = _setup_service_for_chunked(mock_session)
        service._checkpoint_repo.list_for_model = AsyncMock(return_value=[])

        result = await service.get_backfill_history("nonexistent.model")
        assert result == []


# ---------------------------------------------------------------------------
# Router endpoint tests
# ---------------------------------------------------------------------------


class TestBackfillChunkedEndpoint:
    """Test POST /backfills/chunked router endpoint."""

    @pytest.mark.asyncio
    async def test_chunked_backfill_endpoint(self, client):
        """POST /backfills/chunked returns chunked backfill result."""
        mock_result = {
            "backfill_id": "bf-endpoint",
            "status": "COMPLETED",
            "completed_chunks": 3,
            "total_chunks": 3,
            "runs": [],
        }

        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.chunked_backfill = AsyncMock(return_value=mock_result)
            MockService.return_value = instance

            resp = await client.post(
                "/api/v1/backfills/chunked",
                json={
                    "model_name": "staging.orders",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-21",
                    "chunk_size_days": 7,
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["backfill_id"] == "bf-endpoint"
        assert data["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_chunked_backfill_validation_error(self, client):
        """POST /backfills/chunked returns 400 on ValueError."""
        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.chunked_backfill = AsyncMock(
                side_effect=ValueError("Model not found"),
            )
            MockService.return_value = instance

            resp = await client.post(
                "/api/v1/backfills/chunked",
                json={
                    "model_name": "nonexistent.model",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-21",
                },
            )

        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_chunked_backfill_conflict(self, client):
        """POST /backfills/chunked returns 409 on RuntimeError."""
        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.chunked_backfill = AsyncMock(
                side_effect=RuntimeError("Model is locked"),
            )
            MockService.return_value = instance

            resp = await client.post(
                "/api/v1/backfills/chunked",
                json={
                    "model_name": "staging.orders",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-21",
                },
            )

        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_chunked_backfill_invalid_chunk_size(self, client):
        """POST /backfills/chunked rejects chunk_size_days < 1 via Pydantic validation."""
        resp = await client.post(
            "/api/v1/backfills/chunked",
            json={
                "model_name": "staging.orders",
                "start_date": "2024-01-01",
                "end_date": "2024-01-21",
                "chunk_size_days": 0,
            },
        )
        assert resp.status_code == 422  # Pydantic validation error


class TestResumeBackfillEndpoint:
    """Test POST /backfills/{backfill_id}/resume router endpoint."""

    @pytest.mark.asyncio
    async def test_resume_endpoint(self, client):
        """POST /backfills/{id}/resume returns resumed backfill result."""
        mock_result = {
            "backfill_id": "bf-resume",
            "status": "COMPLETED",
            "completed_chunks": 3,
            "total_chunks": 3,
            "runs": [],
        }

        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.resume_backfill = AsyncMock(return_value=mock_result)
            MockService.return_value = instance

            resp = await client.post("/api/v1/backfills/bf-resume/resume")

        assert resp.status_code == 200
        data = resp.json()
        assert data["backfill_id"] == "bf-resume"

    @pytest.mark.asyncio
    async def test_resume_not_found(self, client):
        """POST /backfills/{id}/resume returns 400 when not found."""
        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.resume_backfill = AsyncMock(
                side_effect=ValueError("Backfill not found"),
            )
            MockService.return_value = instance

            resp = await client.post("/api/v1/backfills/nonexistent/resume")

        assert resp.status_code == 400


class TestBackfillStatusEndpoint:
    """Test GET /backfills/status/{backfill_id} router endpoint."""

    @pytest.mark.asyncio
    async def test_status_endpoint(self, client):
        """GET /backfills/status/{id} returns status with audit chunks."""
        mock_result = {
            "backfill_id": "bf-status",
            "model_name": "staging.orders",
            "status": "RUNNING",
            "completed_chunks": 1,
            "total_chunks": 3,
            "chunks": [
                {
                    "chunk_start": "2024-01-01",
                    "chunk_end": "2024-01-07",
                    "status": "SUCCESS",
                    "run_id": "run-1",
                    "error_message": None,
                    "duration_seconds": 30.0,
                    "executed_at": "2024-06-15T12:00:00+00:00",
                },
            ],
        }

        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.get_backfill_status = AsyncMock(return_value=mock_result)
            MockService.return_value = instance

            resp = await client.get("/api/v1/backfills/status/bf-status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["backfill_id"] == "bf-status"
        assert len(data["chunks"]) == 1

    @pytest.mark.asyncio
    async def test_status_not_found(self, client):
        """GET /backfills/status/{id} returns 404 when not found."""
        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.get_backfill_status = AsyncMock(
                side_effect=ValueError("Backfill not found"),
            )
            MockService.return_value = instance

            resp = await client.get("/api/v1/backfills/status/nonexistent")

        assert resp.status_code == 404


class TestBackfillHistoryEndpoint:
    """Test GET /backfills/history/{model_name} router endpoint."""

    @pytest.mark.asyncio
    async def test_history_endpoint(self, client):
        """GET /backfills/history/{model} returns checkpoint summaries."""
        mock_result = [
            {
                "backfill_id": "bf-1",
                "model_name": "staging.orders",
                "status": "COMPLETED",
                "total_chunks": 3,
                "completed_chunks": 3,
            },
        ]

        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.get_backfill_history = AsyncMock(return_value=mock_result)
            MockService.return_value = instance

            resp = await client.get("/api/v1/backfills/history/staging.orders")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["backfill_id"] == "bf-1"

    @pytest.mark.asyncio
    async def test_history_with_limit(self, client):
        """GET /backfills/history/{model}?limit=5 passes limit parameter."""
        with patch(
            "api.routers.backfills.ExecutionService",
        ) as MockService:
            instance = MagicMock()
            instance.get_backfill_history = AsyncMock(return_value=[])
            MockService.return_value = instance

            resp = await client.get(
                "/api/v1/backfills/history/staging.orders?limit=5",
            )

        assert resp.status_code == 200
