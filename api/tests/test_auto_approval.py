"""Tests for api/api/services/auto_approval.py

Covers:
- Auto-approve low-risk plan (all rules pass)
- Reject high-risk plan (score too high)
- Reject expensive plan (cost too high)
- Reject breaking change
- Reject SLA-tagged model affected
- Reject dashboard-dependency model affected
- Force manual review override
- Audit trail recorded (decision serialization)
- Disabled engine always rejects
"""

from __future__ import annotations

from typing import Any

import pytest

from api.services.auto_approval import (
    ApprovalDecision,
    ApprovalRule,
    AutoApprovalConfig,
    AutoApprovalEngine,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _low_risk_plan() -> dict[str, Any]:
    """Return a plan that should pass all auto-approval rules."""
    return {
        "plan_id": "safe-plan",
        "summary": {
            "total_steps": 1,
            "estimated_cost_usd": 5.0,
            "models_changed": ["staging.orders"],
        },
        "steps": [
            {
                "step_id": "s1",
                "model": "staging.orders",
                "run_type": "FULL_REFRESH",
            },
        ],
    }


def _low_risk_advisory() -> dict[str, Any]:
    """Return advisory data with low risk scores for all models."""
    return {
        "staging.orders": {
            "semantic_classification": {
                "change_type": "non_breaking",
                "confidence": 0.95,
            },
            "cost_prediction": {
                "estimated_cost_usd": 5.0,
            },
            "risk_score": {
                "risk_score": 1.0,
                "risk_level": "low",
            },
        },
    }


def _enabled_config(**kwargs: Any) -> AutoApprovalConfig:
    """Return an enabled auto-approval config with optional overrides."""
    defaults = {"enabled": True}
    defaults.update(kwargs)
    return AutoApprovalConfig(**defaults)


# ---------------------------------------------------------------------------
# Auto-approve low-risk plan (all rules pass)
# ---------------------------------------------------------------------------


def test_auto_approve_low_risk_plan() -> None:
    """A plan with low risk, low cost, and non-breaking changes is auto-approved."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    advisory = _low_risk_advisory()
    model_tags: dict[str, list[str]] = {"staging.orders": ["staging"]}

    decision = engine.evaluate(plan, advisory, model_tags)

    assert decision.auto_approved is True
    assert "All auto-approval rules passed" in decision.decision_reason
    assert all(rule.passed for rule in decision.rules_evaluated)
    assert len(decision.rules_evaluated) == 6


def test_auto_approve_no_advisory() -> None:
    """With no advisory data, all rules pass (risk defaults to 0, change type unknown is allowed)."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()

    decision = engine.evaluate(plan, advisory_data=None, model_tags=None)

    assert decision.auto_approved is True
    # Risk score 0.0 < 3.0 threshold -> passes
    risk_rule = next(r for r in decision.rules_evaluated if r.rule_name == "risk_score")
    assert risk_rule.passed is True


# ---------------------------------------------------------------------------
# Reject high-risk plan (score too high)
# ---------------------------------------------------------------------------


def test_reject_high_risk_score() -> None:
    """A plan with high risk score is rejected."""
    config = _enabled_config(max_risk_score=3.0)
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    advisory = {
        "staging.orders": {
            "semantic_classification": {"change_type": "non_breaking"},
            "risk_score": {"risk_score": 7.5, "risk_level": "high"},
        },
    }

    decision = engine.evaluate(plan, advisory)

    assert decision.auto_approved is False
    risk_rule = next(r for r in decision.rules_evaluated if r.rule_name == "risk_score")
    assert risk_rule.passed is False
    assert "7.5" in risk_rule.reason
    assert "3.0" in risk_rule.reason


def test_reject_risk_score_exactly_at_threshold() -> None:
    """Risk score equal to the threshold is rejected (must be strictly less than)."""
    config = _enabled_config(max_risk_score=5.0)
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    advisory = {
        "staging.orders": {
            "risk_score": {"risk_score": 5.0},
            "semantic_classification": {"change_type": "non_breaking"},
        },
    }

    decision = engine.evaluate(plan, advisory)

    assert decision.auto_approved is False
    risk_rule = next(r for r in decision.rules_evaluated if r.rule_name == "risk_score")
    assert risk_rule.passed is False


def test_max_risk_across_multiple_models() -> None:
    """The highest risk score across all models determines the risk check."""
    config = _enabled_config(max_risk_score=3.0)
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {
            "total_steps": 2,
            "estimated_cost_usd": 5.0,
            "models_changed": ["model_a", "model_b"],
        },
        "steps": [
            {"step_id": "s1", "model": "model_a"},
            {"step_id": "s2", "model": "model_b"},
        ],
    }
    advisory = {
        "model_a": {"risk_score": {"risk_score": 1.0}, "semantic_classification": {"change_type": "non_breaking"}},
        "model_b": {"risk_score": {"risk_score": 4.0}, "semantic_classification": {"change_type": "non_breaking"}},
    }

    decision = engine.evaluate(plan, advisory)

    assert decision.auto_approved is False
    risk_rule = next(r for r in decision.rules_evaluated if r.rule_name == "risk_score")
    assert risk_rule.passed is False
    assert "4.0" in risk_rule.reason


# ---------------------------------------------------------------------------
# Reject expensive plan (cost too high)
# ---------------------------------------------------------------------------


def test_reject_expensive_plan() -> None:
    """A plan exceeding the cost threshold is rejected."""
    config = _enabled_config(max_cost_usd=50.0)
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {
            "total_steps": 1,
            "estimated_cost_usd": 75.0,
            "models_changed": ["staging.orders"],
        },
        "steps": [],
    }

    decision = engine.evaluate(plan, _low_risk_advisory())

    assert decision.auto_approved is False
    cost_rule = next(r for r in decision.rules_evaluated if r.rule_name == "cost_threshold")
    assert cost_rule.passed is False
    assert "$75.00" in cost_rule.reason
    assert "$50.00" in cost_rule.reason


def test_reject_cost_exactly_at_threshold() -> None:
    """Cost equal to the threshold is rejected (must be strictly less than)."""
    config = _enabled_config(max_cost_usd=50.0)
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {"total_steps": 1, "estimated_cost_usd": 50.0, "models_changed": ["m1"]},
        "steps": [],
    }

    decision = engine.evaluate(plan, _low_risk_advisory())

    assert decision.auto_approved is False
    cost_rule = next(r for r in decision.rules_evaluated if r.rule_name == "cost_threshold")
    assert cost_rule.passed is False


def test_approve_cost_just_below_threshold() -> None:
    """Cost just below the threshold passes."""
    config = _enabled_config(max_cost_usd=50.0)
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {"total_steps": 1, "estimated_cost_usd": 49.99, "models_changed": ["staging.orders"]},
        "steps": [],
    }

    decision = engine.evaluate(plan, _low_risk_advisory())

    cost_rule = next(r for r in decision.rules_evaluated if r.rule_name == "cost_threshold")
    assert cost_rule.passed is True


# ---------------------------------------------------------------------------
# Reject breaking change
# ---------------------------------------------------------------------------


def test_reject_breaking_change() -> None:
    """A plan with a breaking change type is rejected."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    advisory = {
        "staging.orders": {
            "semantic_classification": {
                "change_type": "breaking",
                "confidence": 0.9,
            },
            "risk_score": {"risk_score": 1.0},
        },
    }

    decision = engine.evaluate(plan, advisory)

    assert decision.auto_approved is False
    change_rule = next(r for r in decision.rules_evaluated if r.rule_name == "change_type")
    assert change_rule.passed is False
    assert "staging.orders=breaking" in change_rule.reason


def test_approve_cosmetic_only_change() -> None:
    """COSMETIC_ONLY is in the default allowed set and passes."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    advisory = {
        "staging.orders": {
            "semantic_classification": {"change_type": "COSMETIC_ONLY"},
            "risk_score": {"risk_score": 0.5},
        },
    }

    decision = engine.evaluate(plan, advisory)

    change_rule = next(r for r in decision.rules_evaluated if r.rule_name == "change_type")
    assert change_rule.passed is True


def test_unknown_change_type_is_tolerated() -> None:
    """An 'unknown' change type (e.g., no advisory) does not disqualify."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    # Advisory exists but classification has change_type = "unknown"
    advisory = {
        "staging.orders": {
            "semantic_classification": {"change_type": "unknown"},
            "risk_score": {"risk_score": 0.5},
        },
    }

    decision = engine.evaluate(plan, advisory)

    change_rule = next(r for r in decision.rules_evaluated if r.rule_name == "change_type")
    assert change_rule.passed is True


# ---------------------------------------------------------------------------
# Reject SLA-tagged model affected
# ---------------------------------------------------------------------------


def test_reject_sla_tagged_model() -> None:
    """A plan affecting an SLA-tagged model is rejected."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["sla", "core"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    assert decision.auto_approved is False
    sla_rule = next(r for r in decision.rules_evaluated if r.rule_name == "sla_models")
    assert sla_rule.passed is False
    assert "staging.orders" in sla_rule.reason


def test_reject_sla_critical_tag() -> None:
    """sla-critical tag also blocks auto-approval."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["sla-critical"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    sla_rule = next(r for r in decision.rules_evaluated if r.rule_name == "sla_models")
    assert sla_rule.passed is False


def test_approve_non_sla_model() -> None:
    """A model without SLA tags passes the SLA check."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["staging", "core"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    sla_rule = next(r for r in decision.rules_evaluated if r.rule_name == "sla_models")
    assert sla_rule.passed is True


def test_sla_check_case_insensitive() -> None:
    """SLA tag matching is case-insensitive."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["SLA"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    sla_rule = next(r for r in decision.rules_evaluated if r.rule_name == "sla_models")
    assert sla_rule.passed is False


# ---------------------------------------------------------------------------
# Reject dashboard-dependency model affected
# ---------------------------------------------------------------------------


def test_reject_dashboard_dependency() -> None:
    """A plan affecting a dashboard-dependent model is rejected."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["dashboard"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    assert decision.auto_approved is False
    dash_rule = next(r for r in decision.rules_evaluated if r.rule_name == "dashboard_dependencies")
    assert dash_rule.passed is False
    assert "staging.orders" in dash_rule.reason


def test_reject_executive_dashboard_tag() -> None:
    """executive-dashboard tag also blocks auto-approval."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()
    model_tags = {"staging.orders": ["executive-dashboard"]}

    decision = engine.evaluate(plan, _low_risk_advisory(), model_tags)

    dash_rule = next(r for r in decision.rules_evaluated if r.rule_name == "dashboard_dependencies")
    assert dash_rule.passed is False


# ---------------------------------------------------------------------------
# Force manual review override
# ---------------------------------------------------------------------------


def test_reject_force_manual_review_model() -> None:
    """A model explicitly flagged for manual review blocks auto-approval."""
    config = _enabled_config(force_manual_models=["staging.orders"])
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()

    decision = engine.evaluate(plan, _low_risk_advisory())

    assert decision.auto_approved is False
    manual_rule = next(r for r in decision.rules_evaluated if r.rule_name == "force_manual_review")
    assert manual_rule.passed is False
    assert "staging.orders" in manual_rule.reason


def test_approve_non_force_manual_model() -> None:
    """Models not in the force-manual list pass the check."""
    config = _enabled_config(force_manual_models=["some.other.model"])
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()

    decision = engine.evaluate(plan, _low_risk_advisory())

    manual_rule = next(r for r in decision.rules_evaluated if r.rule_name == "force_manual_review")
    assert manual_rule.passed is True


# ---------------------------------------------------------------------------
# Audit trail recorded
# ---------------------------------------------------------------------------


def test_decision_audit_trail_serialization() -> None:
    """ApprovalDecision.to_dict() contains full audit trail."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = _low_risk_plan()

    decision = engine.evaluate(plan, _low_risk_advisory(), {"staging.orders": ["staging"]})

    audit = decision.to_dict()

    assert isinstance(audit, dict)
    assert "auto_approved" in audit
    assert audit["auto_approved"] is True
    assert "rules" in audit
    assert isinstance(audit["rules"], list)
    assert len(audit["rules"]) == 6

    for rule_entry in audit["rules"]:
        assert "rule" in rule_entry
        assert "passed" in rule_entry
        assert "reason" in rule_entry
        assert isinstance(rule_entry["rule"], str)
        assert isinstance(rule_entry["passed"], bool)
        assert isinstance(rule_entry["reason"], str)

    assert "decision_reason" in audit
    assert "decided_at" in audit
    assert isinstance(audit["decided_at"], str)


def test_decision_audit_trail_rejected() -> None:
    """Rejected decision to_dict includes failing rule names in reason."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {"total_steps": 1, "estimated_cost_usd": 100.0, "models_changed": ["staging.orders"]},
        "steps": [],
    }
    advisory = {
        "staging.orders": {
            "semantic_classification": {"change_type": "breaking"},
            "risk_score": {"risk_score": 8.0},
        },
    }

    decision = engine.evaluate(plan, advisory, {"staging.orders": ["sla"]})
    audit = decision.to_dict()

    assert audit["auto_approved"] is False
    assert "Manual approval required" in audit["decision_reason"]

    # Multiple failing rules should be listed
    failing_rules = [r for r in audit["rules"] if not r["passed"]]
    assert len(failing_rules) >= 3  # risk_score, cost_threshold, change_type, sla_models


def test_audit_trail_decided_at_is_iso_format() -> None:
    """The decided_at field is a valid ISO 8601 timestamp."""
    from datetime import datetime

    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    decision = engine.evaluate(_low_risk_plan(), _low_risk_advisory())
    audit = decision.to_dict()

    # Should parse without error
    parsed = datetime.fromisoformat(audit["decided_at"])
    assert parsed.tzinfo is not None  # timezone-aware


# ---------------------------------------------------------------------------
# Disabled engine always rejects
# ---------------------------------------------------------------------------


def test_disabled_engine_always_rejects() -> None:
    """When auto-approval is disabled, every plan is rejected."""
    config = AutoApprovalConfig(enabled=False)
    engine = AutoApprovalEngine(config)

    decision = engine.evaluate(_low_risk_plan(), _low_risk_advisory())

    assert decision.auto_approved is False
    assert len(decision.rules_evaluated) == 1
    assert decision.rules_evaluated[0].rule_name == "feature_enabled"
    assert decision.rules_evaluated[0].passed is False
    assert "not enabled" in decision.decision_reason.lower() or "disabled" in decision.decision_reason.lower()


def test_default_config_disabled() -> None:
    """Default AutoApprovalConfig has enabled=False."""
    config = AutoApprovalConfig()
    assert config.enabled is False

    engine = AutoApprovalEngine()
    assert engine.enabled is False


# ---------------------------------------------------------------------------
# Enabled property
# ---------------------------------------------------------------------------


def test_engine_enabled_property() -> None:
    """The enabled property reflects config.enabled."""
    engine_on = AutoApprovalEngine(_enabled_config())
    assert engine_on.enabled is True

    engine_off = AutoApprovalEngine(AutoApprovalConfig(enabled=False))
    assert engine_off.enabled is False


# ---------------------------------------------------------------------------
# Multiple failing rules
# ---------------------------------------------------------------------------


def test_multiple_failing_rules_all_reported() -> None:
    """When multiple rules fail, all are reported in the decision."""
    config = _enabled_config(
        max_risk_score=3.0,
        max_cost_usd=10.0,
        force_manual_models=["staging.orders"],
    )
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {
            "total_steps": 1,
            "estimated_cost_usd": 100.0,
            "models_changed": ["staging.orders"],
        },
        "steps": [],
    }
    advisory = {
        "staging.orders": {
            "semantic_classification": {"change_type": "breaking"},
            "risk_score": {"risk_score": 9.0},
        },
    }
    model_tags = {"staging.orders": ["sla", "dashboard"]}

    decision = engine.evaluate(plan, advisory, model_tags)

    assert decision.auto_approved is False

    failing = {r.rule_name for r in decision.rules_evaluated if not r.passed}
    assert "risk_score" in failing
    assert "cost_threshold" in failing
    assert "change_type" in failing
    assert "sla_models" in failing
    assert "dashboard_dependencies" in failing
    assert "force_manual_review" in failing

    # All 6 rules fail
    assert len(failing) == 6


# ---------------------------------------------------------------------------
# Edge case: empty plan
# ---------------------------------------------------------------------------


def test_empty_plan_auto_approved() -> None:
    """A plan with zero steps and zero cost is auto-approved."""
    config = _enabled_config()
    engine = AutoApprovalEngine(config)

    plan = {
        "summary": {"total_steps": 0, "estimated_cost_usd": 0.0, "models_changed": []},
        "steps": [],
    }

    decision = engine.evaluate(plan)

    assert decision.auto_approved is True
    assert all(r.passed for r in decision.rules_evaluated)
