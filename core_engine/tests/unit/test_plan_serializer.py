"""Unit tests for core_engine.planner.plan_serializer."""

from __future__ import annotations

import json

import pytest
from core_engine.models.plan import Plan, PlanStep, PlanSummary, RunType
from core_engine.planner.plan_serializer import (
    deserialize_plan,
    serialize_plan,
    validate_plan_schema,
)
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_plan() -> Plan:
    """Create a simple plan for testing serialisation."""
    step = PlanStep(
        step_id="abc123deadbeef" * 4 + "00000000",
        model="analytics.orders",
        run_type=RunType.FULL_REFRESH,
        depends_on=[],
        parallel_group=1,
        reason="SQL logic changed",
        estimated_compute_seconds=120.0,
        estimated_cost_usd=0.084,
    )
    summary = PlanSummary(
        total_steps=1,
        estimated_cost_usd=0.084,
        models_changed=["analytics.orders"],
    )
    return Plan(
        plan_id="plan" + "a" * 60,
        base="snap_base",
        target="snap_target",
        summary=summary,
        steps=[step],
    )


# ---------------------------------------------------------------------------
# serialize_plan / deserialize_plan round-trip
# ---------------------------------------------------------------------------


class TestSerializePlan:
    def test_round_trip(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)
        assert restored.plan_id == plan.plan_id
        assert restored.base == plan.base
        assert restored.target == plan.target
        assert len(restored.steps) == len(plan.steps)
        assert restored.steps[0].model == plan.steps[0].model
        assert restored.steps[0].run_type == plan.steps[0].run_type

    def test_json_is_valid(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)
        assert "plan_id" in parsed
        assert "steps" in parsed

    def test_keys_are_sorted(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        parsed = json.loads(json_str)
        keys = list(parsed.keys())
        assert keys == sorted(keys)

    def test_deterministic_output(self):
        plan = _make_plan()
        json1 = serialize_plan(plan)
        json2 = serialize_plan(plan)
        assert json1 == json2

    def test_two_space_indent(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        # Check that indentation uses 2 spaces (first nested key).
        lines = json_str.split("\n")
        # Find the first indented line.
        indented = [line for line in lines if line.startswith("  ") and not line.startswith("    ")]
        assert len(indented) > 0


# ---------------------------------------------------------------------------
# deserialize_plan
# ---------------------------------------------------------------------------


class TestDeserializePlan:
    def test_valid_json(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        restored = deserialize_plan(json_str)
        assert isinstance(restored, Plan)

    def test_invalid_json_raises_value_error(self):
        with pytest.raises((ValueError, ValidationError)):
            deserialize_plan("not valid json")

    def test_missing_required_field_raises_validation_error(self):
        # plan_id is required.
        bad_json = json.dumps({"base": "snap1", "target": "snap2"})
        with pytest.raises(ValidationError):
            deserialize_plan(bad_json)


# ---------------------------------------------------------------------------
# validate_plan_schema
# ---------------------------------------------------------------------------


class TestValidatePlanSchema:
    def test_valid_plan_no_errors(self):
        plan = _make_plan()
        json_str = serialize_plan(plan)
        errors = validate_plan_schema(json_str)
        assert errors == []

    def test_missing_fields_produces_errors(self):
        bad_json = json.dumps({"base": "snap1"})
        errors = validate_plan_schema(bad_json)
        assert len(errors) > 0

    def test_invalid_json_produces_errors(self):
        errors = validate_plan_schema("not json at all {{{")
        assert len(errors) > 0
        assert any("Invalid JSON" in e or "JSON" in e for e in errors)

    def test_empty_object_produces_errors(self):
        errors = validate_plan_schema("{}")
        assert len(errors) > 0

    def test_valid_minimal_plan(self):
        minimal = {
            "plan_id": "x" * 64,
            "base": "snap1",
            "target": "snap2",
            "summary": {
                "total_steps": 0,
            },
            "steps": [],
        }
        errors = validate_plan_schema(json.dumps(minimal))
        assert errors == []
