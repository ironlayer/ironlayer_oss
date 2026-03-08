"""Tests for AIFeedbackService and accuracy helpers."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from api.services.ai_feedback_service import (
    AIFeedbackService,
    _compute_cost_accuracy,
    _compute_risk_accuracy,
)

# ---------------------------------------------------------------------------
# Accuracy helper tests
# ---------------------------------------------------------------------------


class TestComputeCostAccuracy:
    """Tests for _compute_cost_accuracy."""

    def test_exact_match(self):
        assert _compute_cost_accuracy(1.0, 1.0) == 1.0

    def test_both_zero(self):
        assert _compute_cost_accuracy(0.0, 0.0) == 1.0

    def test_predicted_double(self):
        # |2.0 - 1.0| / max(2.0, 1.0) = 0.5 => accuracy = 0.5
        assert _compute_cost_accuracy(2.0, 1.0) == 0.5

    def test_actual_double(self):
        assert _compute_cost_accuracy(1.0, 2.0) == 0.5

    def test_large_overestimate(self):
        # |10.0 - 1.0| / 10.0 = 0.9 => accuracy = 0.1
        result = _compute_cost_accuracy(10.0, 1.0)
        assert abs(result - 0.1) < 0.001

    def test_clamps_to_zero(self):
        assert _compute_cost_accuracy(100.0, 0.001) >= 0.0

    def test_small_values_use_floor(self):
        # Both near zero but not exactly zero
        result = _compute_cost_accuracy(0.001, 0.002)
        assert 0.0 <= result <= 1.0

    def test_negative_protection(self):
        # Negative costs should not produce accuracy > 1
        result = _compute_cost_accuracy(-1.0, 1.0)
        assert 0.0 <= result <= 1.0


class TestComputeRiskAccuracy:
    """Tests for _compute_risk_accuracy."""

    def test_low_risk_success(self):
        assert _compute_risk_accuracy(0.0, True) == 1.0

    def test_high_risk_failure(self):
        assert _compute_risk_accuracy(10.0, False) == 1.0

    def test_high_risk_success(self):
        assert _compute_risk_accuracy(10.0, True) == 0.0

    def test_low_risk_failure(self):
        assert _compute_risk_accuracy(0.0, False) == 0.0

    def test_medium_risk_success(self):
        assert _compute_risk_accuracy(5.0, True) == 0.5

    def test_medium_risk_failure(self):
        assert _compute_risk_accuracy(5.0, False) == 0.5

    def test_clamps_above_10(self):
        # Risk > 10 should be clamped to 10
        assert _compute_risk_accuracy(15.0, False) == 1.0
        assert _compute_risk_accuracy(15.0, True) == 0.0

    def test_clamps_below_0(self):
        # Risk < 0 should be clamped to 0
        assert _compute_risk_accuracy(-5.0, True) == 1.0
        assert _compute_risk_accuracy(-5.0, False) == 0.0


# ---------------------------------------------------------------------------
# Service tests
# ---------------------------------------------------------------------------


class TestAIFeedbackService:
    """Tests for AIFeedbackService."""

    @pytest.fixture
    def service(self):
        session = AsyncMock()
        svc = AIFeedbackService(session, tenant_id="test-tenant")
        svc._feedback_repo = MagicMock()
        svc._plan_repo = MagicMock()
        svc._run_repo = MagicMock()
        # All repo methods must be async
        svc._feedback_repo.record_prediction = AsyncMock()
        svc._feedback_repo.record_outcome = AsyncMock()
        svc._feedback_repo.mark_accepted = AsyncMock(return_value=True)
        svc._feedback_repo.get_accuracy_stats = AsyncMock(
            return_value={
                "avg_accuracy": 0.75,
                "acceptance_rate": 0.8,
                "record_count": 10,
            }
        )
        svc._feedback_repo.get_training_data = AsyncMock(return_value=[])
        return svc

    # -- capture_predictions_from_plan --

    @pytest.mark.asyncio
    async def test_capture_predictions_plan_not_found(self, service):
        service._plan_repo.get_plan = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="not found"):
            await service.capture_predictions_from_plan("missing")

    @pytest.mark.asyncio
    async def test_capture_predictions_no_advisory(self, service):
        plan_row = MagicMock()
        plan_row.advisory_json = None
        plan_row.plan_json = {"steps": []}
        service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

        count = await service.capture_predictions_from_plan("plan-1")
        assert count == 0

    @pytest.mark.asyncio
    async def test_capture_predictions_with_cost_and_risk(self, service):
        plan_row = MagicMock()
        plan_row.plan_json = {
            "steps": [
                {"step_id": "step-1", "model": "model_a"},
            ]
        }
        plan_row.advisory_json = {
            "steps": {
                "step-1": {
                    "model": "model_a",
                    "cost": {"estimated_cost_usd": 1.5},
                    "risk": {"risk_score": 3.0},
                }
            }
        }
        service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

        count = await service.capture_predictions_from_plan("plan-1")
        assert count == 2
        assert service._feedback_repo.record_prediction.call_count == 2

    @pytest.mark.asyncio
    async def test_capture_predictions_with_classification(self, service):
        plan_row = MagicMock()
        plan_row.plan_json = {"steps": [{"step_id": "s1", "model": "m1"}]}
        plan_row.advisory_json = {
            "steps": {
                "s1": {
                    "classification": {"change_type": "schema", "confidence": 0.9},
                }
            }
        }
        service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

        count = await service.capture_predictions_from_plan("plan-2")
        assert count == 1

    @pytest.mark.asyncio
    async def test_capture_predictions_all_three_types(self, service):
        plan_row = MagicMock()
        plan_row.plan_json = {"steps": [{"step_id": "s1", "model": "m1"}]}
        plan_row.advisory_json = {
            "steps": {
                "s1": {
                    "cost": {"estimated_cost_usd": 2.0},
                    "risk": {"risk_score": 4.0},
                    "classification": {"change_type": "logic"},
                }
            }
        }
        service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

        count = await service.capture_predictions_from_plan("plan-3")
        assert count == 3
        assert service._feedback_repo.record_prediction.call_count == 3

    @pytest.mark.asyncio
    async def test_capture_predictions_string_json(self, service):
        """advisory_json stored as a JSON string should be handled."""
        import json

        plan_row = MagicMock()
        plan_row.plan_json = json.dumps({"steps": [{"step_id": "s1", "model": "m1"}]})
        plan_row.advisory_json = json.dumps(
            {
                "steps": {
                    "s1": {
                        "cost": {"estimated_cost_usd": 1.0},
                    }
                }
            }
        )
        service._plan_repo.get_plan = AsyncMock(return_value=plan_row)

        count = await service.capture_predictions_from_plan("plan-str")
        assert count == 1

    # -- record_suggestion_feedback --

    @pytest.mark.asyncio
    async def test_record_suggestion_feedback(self, service):
        feedbacks = [
            {"step_id": "s1", "model_name": "m1", "feedback_type": "cost", "accepted": True},
            {"step_id": "s2", "model_name": "m2", "feedback_type": "risk", "accepted": False},
        ]
        updated = await service.record_suggestion_feedback("plan-1", feedbacks)
        assert updated == 2
        assert service._feedback_repo.mark_accepted.call_count == 2

    @pytest.mark.asyncio
    async def test_record_suggestion_feedback_partial_update(self, service):
        """When some mark_accepted calls return False, count only successes."""
        service._feedback_repo.mark_accepted = AsyncMock(side_effect=[True, False, True])
        feedbacks = [
            {"step_id": "s1", "model_name": "m1", "feedback_type": "cost", "accepted": True},
            {"step_id": "s2", "model_name": "m2", "feedback_type": "risk", "accepted": False},
            {"step_id": "s3", "model_name": "m3", "feedback_type": "classification", "accepted": True},
        ]
        updated = await service.record_suggestion_feedback("plan-1", feedbacks)
        assert updated == 2

    @pytest.mark.asyncio
    async def test_record_suggestion_feedback_empty(self, service):
        updated = await service.record_suggestion_feedback("plan-1", [])
        assert updated == 0

    # -- get_accuracy_stats --

    @pytest.mark.asyncio
    async def test_get_accuracy_stats(self, service):
        stats = await service.get_accuracy_stats(feedback_type="cost")
        assert stats["avg_accuracy"] == 0.75
        assert stats["acceptance_rate"] == 0.8
        assert stats["record_count"] == 10

    @pytest.mark.asyncio
    async def test_get_accuracy_stats_with_model_filter(self, service):
        await service.get_accuracy_stats(feedback_type="risk", model_name="my_model")
        service._feedback_repo.get_accuracy_stats.assert_called_once_with(
            feedback_type="risk",
            model_name="my_model",
        )

    # -- get_training_data --

    @pytest.mark.asyncio
    async def test_get_training_data(self, service):
        data = await service.get_training_data("cost")
        assert data == []
        service._feedback_repo.get_training_data.assert_called_once_with(
            feedback_type="cost",
            limit=1000,
        )

    @pytest.mark.asyncio
    async def test_get_training_data_custom_limit(self, service):
        await service.get_training_data("risk", limit=50)
        service._feedback_repo.get_training_data.assert_called_once_with(
            feedback_type="risk",
            limit=50,
        )


# ---------------------------------------------------------------------------
# record_execution_outcome tests
# ---------------------------------------------------------------------------


class TestRecordExecutionOutcome:
    """Tests for record_execution_outcome with real accuracy computation."""

    @pytest.fixture
    def service(self):
        session = AsyncMock()
        svc = AIFeedbackService(session, tenant_id="test-tenant")
        svc._feedback_repo = MagicMock()
        svc._plan_repo = MagicMock()
        svc._run_repo = MagicMock()
        svc._feedback_repo.record_outcome = AsyncMock()
        return svc

    @pytest.mark.asyncio
    async def test_records_cost_outcome_without_prediction(self, service):
        """When no cost prediction exists, still records outcome."""
        # Mock the session query to return no prediction
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        service._session.execute = AsyncMock(return_value=mock_result)

        run_dict = {
            "status": "SUCCESS",
            "cost_usd": 1.5,
            "started_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
            "error_message": None,
        }

        updated = await service.record_execution_outcome(
            plan_id="p1",
            step_id="s1",
            model_name="m1",
            run_dict=run_dict,
        )
        # Cost outcome recorded (no risk prediction found)
        assert updated >= 1
        assert service._feedback_repo.record_outcome.called

    @pytest.mark.asyncio
    async def test_records_cost_outcome_with_prediction(self, service):
        """When cost prediction exists, computes accuracy."""
        # First query returns cost prediction, second returns no risk prediction
        cost_pred_row = MagicMock()
        cost_pred_row.prediction_json = {"estimated_cost_usd": 2.0}

        result_cost = MagicMock()
        result_cost.scalar_one_or_none.return_value = cost_pred_row

        result_risk = MagicMock()
        result_risk.scalar_one_or_none.return_value = None

        service._session.execute = AsyncMock(side_effect=[result_cost, result_risk])

        run_dict = {
            "status": "SUCCESS",
            "cost_usd": 1.5,
            "started_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
            "error_message": None,
        }

        updated = await service.record_execution_outcome(
            plan_id="p1",
            step_id="s1",
            model_name="m1",
            run_dict=run_dict,
        )
        assert updated == 1

        # Verify record_outcome was called with an accuracy score
        call_args = service._feedback_repo.record_outcome.call_args_list[0]
        assert call_args.kwargs.get("accuracy_score") is not None or (
            len(call_args.args) > 5 and call_args.args[5] is not None
        )

    @pytest.mark.asyncio
    async def test_records_risk_outcome_with_prediction(self, service):
        """When risk prediction exists, computes risk accuracy."""
        # cost_usd is None so cost prediction lookup is SKIPPED.
        # Only one _session.execute call: for risk prediction lookup.
        risk_pred_row = MagicMock()
        risk_pred_row.prediction_json = {"risk_score": 2.0}
        risk_result = MagicMock()
        risk_result.scalar_one_or_none.return_value = risk_pred_row

        service._session.execute = AsyncMock(return_value=risk_result)

        run_dict = {
            "status": "SUCCESS",
            "cost_usd": None,
            "started_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            "error_message": None,
        }

        updated = await service.record_execution_outcome(
            plan_id="p1",
            step_id="s1",
            model_name="m1",
            run_dict=run_dict,
        )
        # Cost + risk outcomes
        assert updated == 2
        assert service._feedback_repo.record_outcome.call_count == 2

    @pytest.mark.asyncio
    async def test_failed_run_outcome(self, service):
        """A failed run should still record outcome and compute risk accuracy."""
        # cost_usd is None so cost prediction lookup is SKIPPED.
        # Only one _session.execute call: for risk prediction lookup.
        risk_pred_row = MagicMock()
        risk_pred_row.prediction_json = {"risk_score": 8.0}
        risk_result = MagicMock()
        risk_result.scalar_one_or_none.return_value = risk_pred_row

        service._session.execute = AsyncMock(return_value=risk_result)

        run_dict = {
            "status": "FAIL",
            "cost_usd": None,
            "started_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
            "error_message": "SQL syntax error",
        }

        updated = await service.record_execution_outcome(
            plan_id="p1",
            step_id="s1",
            model_name="m1",
            run_dict=run_dict,
        )
        assert updated == 2

        # Risk accuracy for failed run with high risk should be high
        risk_call = service._feedback_repo.record_outcome.call_args_list[1]
        risk_accuracy = risk_call.kwargs.get("accuracy_score")
        assert risk_accuracy is not None
        assert risk_accuracy == 0.8  # risk_score=8 / 10 for failure
