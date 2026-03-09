"""Tests for api/api/services/plan_service.py

Covers:
- generate_augmented_plan: generates plan augmentation with AI advisory
- AI failure: returns plan without advisory (graceful degradation)
- get_plan: returns plan data or None
- list_plans: pagination and summary extraction
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.config import APISettings
from api.services.plan_service import PlanService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings() -> APISettings:
    return APISettings(
        platform_env="dev",
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
    )


def _make_plan_row(
    plan_id: str = "plan-aug-001",
    plan_data: dict[str, Any] | None = None,
    approvals: list[dict[str, str]] | None = None,
    auto_approved: bool = False,
) -> MagicMock:
    if plan_data is None:
        plan_data = {
            "plan_id": plan_id,
            "base": "sha-base",
            "target": "sha-target",
            "summary": {
                "total_steps": 1,
                "estimated_cost_usd": 2.0,
                "models_changed": ["staging.orders"],
            },
            "steps": [
                {
                    "step_id": "step-001",
                    "model": "staging.orders",
                    "run_type": "FULL_REFRESH",
                    "reason": "SQL body changed",
                    "depends_on": [],
                    "parallel_group": 0,
                },
            ],
        }
    row = MagicMock()
    row.plan_id = plan_id
    row.plan_json = json.dumps(plan_data)
    row.approvals_json = json.dumps(approvals) if approvals else None
    row.auto_approved = auto_approved
    row.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    row.base_sha = "sha-base"
    row.target_sha = "sha-target"
    return row


def _make_model_row(name: str = "staging.orders", tags: str | None = None) -> MagicMock:
    row = MagicMock()
    row.model_name = name
    row.tags = tags or json.dumps(["staging"])
    return row


# ---------------------------------------------------------------------------
# generate_augmented_plan: success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_augmented_plan_success(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """Augmented plan attaches AI advisory data for each model step."""
    settings = _make_settings()
    plan_row = _make_plan_row()

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(
        return_value={"staging.orders": {"avg_runtime_seconds": 45.0, "avg_cost_usd": None, "run_count": 5}}
    )
    service._run_repo.get_failure_rates_batch = AsyncMock(
        return_value={"staging.orders": 0.1}
    )

    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(
        return_value={"staging.orders": _make_model_row()}
    )

    result = await service.generate_augmented_plan("plan-aug-001")

    assert "advisory" in result
    assert result["advisory"] is not None
    assert "staging.orders" in result["advisory"]

    advisory = result["advisory"]["staging.orders"]
    assert "semantic_classification" in advisory
    assert "cost_prediction" in advisory
    assert "risk_score" in advisory

    # Verify AI client methods were called
    mock_ai_client.semantic_classify.assert_awaited_once()
    mock_ai_client.predict_cost.assert_awaited_once()
    mock_ai_client.score_risk.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_augmented_plan_multiple_steps(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """Augmented plan processes all steps and keyed by model name."""
    settings = _make_settings()
    plan_data = {
        "plan_id": "plan-multi",
        "steps": [
            {"step_id": "s1", "model": "staging.orders", "reason": "changed"},
            {"step_id": "s2", "model": "marts.revenue", "reason": "upstream"},
        ],
        "summary": {"total_steps": 2, "estimated_cost_usd": 3.0, "models_changed": ["staging.orders", "marts.revenue"]},
    }
    plan_row = _make_plan_row("plan-multi", plan_data)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(return_value={})
    service._run_repo.get_failure_rates_batch = AsyncMock(
        return_value={"staging.orders": 0.0, "marts.revenue": 0.0}
    )
    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(
        return_value={
            "staging.orders": _make_model_row("staging.orders"),
            "marts.revenue": _make_model_row("marts.revenue"),
        }
    )

    result = await service.generate_augmented_plan("plan-multi")

    assert result["advisory"] is not None
    assert "staging.orders" in result["advisory"]
    assert "marts.revenue" in result["advisory"]

    # Called once per step for each AI method
    assert mock_ai_client.semantic_classify.await_count == 2
    assert mock_ai_client.predict_cost.await_count == 2
    assert mock_ai_client.score_risk.await_count == 2


# ---------------------------------------------------------------------------
# generate_augmented_plan: AI failure -> graceful degradation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_augmented_plan_ai_failure_returns_plan_without_advisory(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """If all AI calls return None, advisory is set to None."""
    settings = _make_settings()
    plan_row = _make_plan_row()

    # Simulate AI service being down - all methods return None
    mock_ai_client.semantic_classify = AsyncMock(return_value=None)
    mock_ai_client.predict_cost = AsyncMock(return_value=None)
    mock_ai_client.score_risk = AsyncMock(return_value=None)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(return_value={})
    service._run_repo.get_failure_rates_batch = AsyncMock(return_value={"staging.orders": 0.0})
    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(
        return_value={"staging.orders": _make_model_row()}
    )

    result = await service.generate_augmented_plan("plan-aug-001")

    # Plan data is still returned
    assert result["plan_id"] == "plan-aug-001"
    assert result["steps"] is not None

    # Advisory is None because no AI data was available
    assert result["advisory"] is None


@pytest.mark.asyncio
async def test_generate_augmented_plan_partial_ai_failure(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """If some AI calls fail, available data is still included in advisory."""
    settings = _make_settings()
    plan_row = _make_plan_row()

    # Only semantic_classify succeeds
    mock_ai_client.semantic_classify = AsyncMock(return_value={"change_type": "non_breaking", "confidence": 0.9})
    mock_ai_client.predict_cost = AsyncMock(return_value=None)
    mock_ai_client.score_risk = AsyncMock(return_value=None)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(return_value={})
    service._run_repo.get_failure_rates_batch = AsyncMock(return_value={"staging.orders": 0.0})
    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(
        return_value={"staging.orders": _make_model_row()}
    )

    result = await service.generate_augmented_plan("plan-aug-001")

    assert result["advisory"] is not None
    advisory = result["advisory"]["staging.orders"]
    assert "semantic_classification" in advisory
    assert "cost_prediction" not in advisory
    assert "risk_score" not in advisory


# ---------------------------------------------------------------------------
# generate_augmented_plan: plan not found
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_augmented_plan_not_found(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """ValueError raised when plan_id does not exist."""
    settings = _make_settings()

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="Plan nonexistent not found"):
        await service.generate_augmented_plan("nonexistent")


# ---------------------------------------------------------------------------
# get_plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_plan_existing(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """get_plan returns plan data with approvals and metadata."""
    settings = _make_settings()
    approvals = [{"user": "reviewer", "comment": "LGTM", "at": "2024-06-15T12:30:00+00:00"}]
    plan_row = _make_plan_row(approvals=approvals, auto_approved=True)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    result = await service.get_plan("plan-aug-001")

    assert result is not None
    assert result["plan_id"] == "plan-aug-001"
    assert result["approvals"] == approvals
    assert result["auto_approved"] is True
    assert result["created_at"] == "2024-06-15T12:00:00+00:00"


@pytest.mark.asyncio
async def test_get_plan_not_found(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """get_plan returns None for missing plans."""
    settings = _make_settings()

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=None)

    result = await service.get_plan("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_get_plan_no_approvals(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """get_plan returns empty approvals list when none exist."""
    settings = _make_settings()
    plan_row = _make_plan_row(approvals=None, auto_approved=False)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    result = await service.get_plan("plan-aug-001")

    assert result is not None
    assert result["approvals"] == []
    assert result["auto_approved"] is False


# ---------------------------------------------------------------------------
# list_plans
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_plans_returns_summaries(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """list_plans extracts summary fields from plan JSON."""
    settings = _make_settings()
    rows = [
        _make_plan_row("plan-1"),
        _make_plan_row("plan-2"),
    ]

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.list_recent = AsyncMock(return_value=rows)

    result = await service.list_plans(limit=10, offset=0)

    assert len(result) == 2
    assert result[0]["plan_id"] == "plan-1"
    assert result[1]["plan_id"] == "plan-2"
    assert result[0]["total_steps"] == 1
    assert result[0]["estimated_cost_usd"] == 2.0
    assert result[0]["models_changed"] == ["staging.orders"]
    assert result[0]["created_at"] == "2024-06-15T12:00:00+00:00"


@pytest.mark.asyncio
async def test_list_plans_empty(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """list_plans returns empty list when no plans exist."""
    settings = _make_settings()

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.list_recent = AsyncMock(return_value=[])

    result = await service.list_plans(limit=10, offset=0)
    assert result == []


@pytest.mark.asyncio
async def test_list_plans_pagination_offset(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """list_plans delegates offset to the repository for SQL-level pagination."""
    settings = _make_settings()
    # The repository is now called with limit and offset; it returns only the
    # page the DB selected — the service no longer slices in Python.
    rows = [
        _make_plan_row("plan-2"),
    ]

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.list_recent = AsyncMock(return_value=rows)

    result = await service.list_plans(limit=1, offset=1)

    assert len(result) == 1
    assert result[0]["plan_id"] == "plan-2"

    # list_recent is called with both limit and offset at SQL level
    service._plan_repo.list_recent.assert_awaited_once_with(limit=1, offset=1)


# ---------------------------------------------------------------------------
# generate_augmented_plan: step with empty model name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_augmented_plan_skips_empty_model_name(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """Steps without a model name are skipped during augmentation."""
    settings = _make_settings()
    plan_data = {
        "plan_id": "plan-empty",
        "steps": [
            {"step_id": "s1", "model": "", "reason": "no model"},
        ],
        "summary": {"total_steps": 1, "estimated_cost_usd": 0, "models_changed": []},
    }
    plan_row = _make_plan_row("plan-empty", plan_data)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)
    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(return_value={})
    service._run_repo.get_failure_rates_batch = AsyncMock(return_value={})
    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(return_value={})

    result = await service.generate_augmented_plan("plan-empty")

    # No advisory data since the only step had an empty model name
    assert result["advisory"] is None

    # AI client should not have been called
    mock_ai_client.semantic_classify.assert_not_awaited()
    mock_ai_client.predict_cost.assert_not_awaited()
    mock_ai_client.score_risk.assert_not_awaited()


# ---------------------------------------------------------------------------
# BL-062: batch pre-loading in generate_augmented_plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_augmented_plan_uses_batch_preload(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """generate_augmented_plan pre-loads all data via batch methods (not N+1)."""
    settings = _make_settings()
    plan_data = {
        "plan_id": "plan-batch",
        "steps": [
            {"step_id": "s1", "model": "m.alpha", "reason": "changed"},
            {"step_id": "s2", "model": "m.beta", "reason": "upstream"},
        ],
        "summary": {"total_steps": 2, "estimated_cost_usd": 0, "models_changed": ["m.alpha", "m.beta"]},
    }
    plan_row = _make_plan_row("plan-batch", plan_data)

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

    service._run_repo = MagicMock()
    service._run_repo.get_historical_stats_batch = AsyncMock(
        return_value={
            "m.alpha": {"avg_runtime_seconds": 10.0, "avg_cost_usd": 0.5, "run_count": 3},
            "m.beta": {"avg_runtime_seconds": 20.0, "avg_cost_usd": 1.0, "run_count": 2},
        }
    )
    service._run_repo.get_failure_rates_batch = AsyncMock(
        return_value={"m.alpha": 0.0, "m.beta": 0.25}
    )

    service._model_repo = MagicMock()
    service._model_repo.get_models_batch = AsyncMock(
        return_value={
            "m.alpha": _make_model_row("m.alpha"),
            "m.beta": _make_model_row("m.beta"),
        }
    )

    result = await service.generate_augmented_plan("plan-batch")

    # Each step produces advisory data.
    assert result["advisory"] is not None
    assert "m.alpha" in result["advisory"]
    assert "m.beta" in result["advisory"]

    # Batch methods were each called exactly once (not once per step).
    service._run_repo.get_historical_stats_batch.assert_awaited_once()
    service._run_repo.get_failure_rates_batch.assert_awaited_once()
    service._model_repo.get_models_batch.assert_awaited_once()


# ---------------------------------------------------------------------------
# BL-063: list_plans passes offset to repository (SQL-level pagination)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_plans_passes_limit_and_offset_to_repo(
    mock_session: AsyncMock,
    mock_ai_client: AsyncMock,
) -> None:
    """list_plans forwards both limit and offset to the repository for SQL pagination."""
    settings = _make_settings()

    service = PlanService(mock_session, mock_ai_client, settings)
    service._plan_repo = MagicMock()
    service._plan_repo.list_recent = AsyncMock(return_value=[_make_plan_row("plan-x")])

    result = await service.list_plans(limit=5, offset=3)

    assert len(result) == 1
    # Verify offset is passed to the repo, not handled in Python.
    service._plan_repo.list_recent.assert_awaited_once_with(limit=5, offset=3)
