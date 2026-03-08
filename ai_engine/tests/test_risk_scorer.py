"""Tests for ai_engine.engines.risk_scorer.RiskScorer.

Covers zero-risk baseline, downstream depth scoring (+1.5 per level, capped
at 6.0), SLA tags (+3), dashboard dependencies (+2), historical failure rate
(+1 if >5%), critical model tags (+0.5 each), score clamping to [0, 10],
business_critical flag, approval_required flag, risk_factors list contents,
and custom threshold configuration.
"""

from __future__ import annotations

import pytest

from ai_engine.engines.risk_scorer import RiskScorer
from ai_engine.models.requests import RiskScoreRequest
from ai_engine.models.responses import RiskScoreResponse

# ================================================================== #
# Helpers
# ================================================================== #


def _req(
    downstream_depth: int = 0,
    sla_tags: list[str] | None = None,
    dashboard_dependencies: list[str] | None = None,
    model_tags: list[str] | None = None,
    historical_failure_rate: float = 0.0,
    model_name: str = "catalog.schema.model_a",
) -> RiskScoreRequest:
    return RiskScoreRequest(
        model_name=model_name,
        downstream_depth=downstream_depth,
        sla_tags=sla_tags or [],
        dashboard_dependencies=dashboard_dependencies or [],
        model_tags=model_tags or [],
        historical_failure_rate=historical_failure_rate,
    )


# ================================================================== #
# Zero risk (no contributing factors)
# ================================================================== #


class TestZeroRisk:
    """A request with all zeroes / empty fields should produce 0.0 risk."""

    def test_baseline_zero_risk(self):
        scorer = RiskScorer()
        result = scorer.score(_req())
        assert result.risk_score == 0.0
        assert result.business_critical is False
        assert result.approval_required is False
        assert result.risk_factors == []

    def test_zero_depth_no_tags(self):
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=0,
                sla_tags=[],
                dashboard_dependencies=[],
                model_tags=[],
                historical_failure_rate=0.0,
            )
        )
        assert result.risk_score == 0.0
        assert len(result.risk_factors) == 0


# ================================================================== #
# Downstream depth scoring
# ================================================================== #


class TestDownstreamDepth:
    """Downstream depth contributes +1.5 per level, capped at 6.0."""

    @pytest.mark.parametrize(
        "depth,expected_score",
        [
            (1, 1.5),
            (2, 3.0),
            (3, 4.5),
            (4, 6.0),  # cap: 4 * 1.5 = 6.0
            (5, 6.0),  # cap: min(7.5, 6.0) = 6.0
            (10, 6.0),  # cap holds
        ],
        ids=["depth-1", "depth-2", "depth-3", "depth-4-cap", "depth-5-cap", "depth-10-cap"],
    )
    def test_depth_scoring(self, depth, expected_score):
        scorer = RiskScorer()
        result = scorer.score(_req(downstream_depth=depth))
        assert result.risk_score == expected_score

    def test_depth_factor_in_risk_factors(self):
        scorer = RiskScorer()
        result = scorer.score(_req(downstream_depth=2))
        assert any("Downstream depth" in f for f in result.risk_factors)
        assert any("+3.0" in f for f in result.risk_factors)


# ================================================================== #
# SLA tags (+3.0)
# ================================================================== #


class TestSLATags:
    """Presence of any SLA tags adds +3.0."""

    def test_single_sla_tag(self):
        scorer = RiskScorer()
        result = scorer.score(_req(sla_tags=["gold"]))
        assert result.risk_score == 3.0
        assert any("SLA" in f for f in result.risk_factors)

    def test_multiple_sla_tags(self):
        """Multiple SLA tags still add only +3.0 total."""
        scorer = RiskScorer()
        result = scorer.score(_req(sla_tags=["gold", "p1", "urgent"]))
        assert result.risk_score == 3.0

    def test_sla_factor_text(self):
        scorer = RiskScorer()
        result = scorer.score(_req(sla_tags=["p1"]))
        assert any("p1" in f for f in result.risk_factors)


# ================================================================== #
# Dashboard dependencies (+2.0)
# ================================================================== #


class TestDashboardDependencies:
    """Presence of dashboard dependencies adds +2.0."""

    def test_single_dashboard(self):
        scorer = RiskScorer()
        result = scorer.score(_req(dashboard_dependencies=["exec_dashboard"]))
        assert result.risk_score == 2.0
        assert any("Dashboard" in f for f in result.risk_factors)

    def test_multiple_dashboards(self):
        """Multiple dashboards still add only +2.0."""
        scorer = RiskScorer()
        result = scorer.score(_req(dashboard_dependencies=["dash_1", "dash_2", "dash_3"]))
        assert result.risk_score == 2.0


# ================================================================== #
# Historical failure rate (+1.0 if > 5%)
# ================================================================== #


class TestHistoricalFailureRate:
    """Failure rate > 5% adds +1.0."""

    def test_failure_rate_above_threshold(self):
        scorer = RiskScorer()
        result = scorer.score(_req(historical_failure_rate=0.10))
        assert result.risk_score == 1.0
        assert any("failure rate" in f.lower() for f in result.risk_factors)

    def test_failure_rate_at_threshold(self):
        """Exactly 5% does NOT trigger the penalty (> 0.05, not >=)."""
        scorer = RiskScorer()
        result = scorer.score(_req(historical_failure_rate=0.05))
        assert result.risk_score == 0.0

    def test_failure_rate_below_threshold(self):
        scorer = RiskScorer()
        result = scorer.score(_req(historical_failure_rate=0.04))
        assert result.risk_score == 0.0

    def test_failure_rate_just_above_threshold(self):
        scorer = RiskScorer()
        result = scorer.score(_req(historical_failure_rate=0.051))
        assert result.risk_score == 1.0


# ================================================================== #
# Critical model tags (+0.5 each for "critical", "production", "revenue")
# ================================================================== #


class TestCriticalTags:
    """Tags matching _CRITICAL_TAGS add +0.5 each."""

    @pytest.mark.parametrize(
        "tag",
        ["critical", "production", "revenue"],
        ids=["critical", "production", "revenue"],
    )
    def test_single_critical_tag(self, tag):
        scorer = RiskScorer()
        result = scorer.score(_req(model_tags=[tag]))
        assert result.risk_score == 0.5
        assert any(tag in f.lower() for f in result.risk_factors)

    def test_all_critical_tags(self):
        scorer = RiskScorer()
        result = scorer.score(_req(model_tags=["critical", "production", "revenue"]))
        assert result.risk_score == 1.5  # 3 * 0.5

    def test_non_critical_tags_ignored(self):
        scorer = RiskScorer()
        result = scorer.score(_req(model_tags=["dev", "test", "staging"]))
        assert result.risk_score == 0.0

    def test_case_insensitive_matching(self):
        """Tags are lowered before comparison."""
        scorer = RiskScorer()
        result = scorer.score(_req(model_tags=["Critical", "PRODUCTION"]))
        assert result.risk_score == 1.0  # 2 * 0.5

    def test_mixed_critical_and_non_critical(self):
        scorer = RiskScorer()
        result = scorer.score(_req(model_tags=["critical", "test", "revenue"]))
        assert result.risk_score == 1.0  # 2 * 0.5


# ================================================================== #
# Score clamping to [0, 10]
# ================================================================== #


class TestScoreClamping:
    """The final score must be in [0.0, 10.0]."""

    def test_maximum_score_clamped(self):
        """All factors maxed out should be clamped to 10.0."""
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=10,  # +6.0 (capped)
                sla_tags=["gold"],  # +3.0
                dashboard_dependencies=["d"],  # +2.0
                historical_failure_rate=0.5,  # +1.0
                model_tags=["critical", "production", "revenue"],  # +1.5
            )
        )
        # Total unclamped: 6.0 + 3.0 + 2.0 + 1.0 + 1.5 = 13.5 -> clamped to 10.0
        assert result.risk_score == 10.0

    def test_score_never_negative(self):
        scorer = RiskScorer()
        result = scorer.score(_req())
        assert result.risk_score >= 0.0


# ================================================================== #
# business_critical flag
# ================================================================== #


class TestBusinessCritical:
    """business_critical = True when score >= manual_review_threshold (default 7.0)."""

    def test_not_critical_below_threshold(self):
        scorer = RiskScorer()
        # Score 6.0 (depth=4)
        result = scorer.score(_req(downstream_depth=4))
        assert result.risk_score == 6.0
        assert result.business_critical is False

    def test_critical_at_threshold(self):
        scorer = RiskScorer()
        # Score = 6.0 + 1.0 = 7.0 (depth=4 + failure rate)
        result = scorer.score(_req(downstream_depth=4, historical_failure_rate=0.10))
        assert result.risk_score == 7.0
        assert result.business_critical is True

    def test_critical_above_threshold(self):
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=4,
                sla_tags=["gold"],
                historical_failure_rate=0.10,
            )
        )
        assert result.risk_score == 10.0
        assert result.business_critical is True


# ================================================================== #
# approval_required flag
# ================================================================== #


class TestApprovalRequired:
    """approval_required = True when score >= auto_approve_threshold (default 3.0)."""

    def test_no_approval_below_threshold(self):
        scorer = RiskScorer()
        # Score 2.0 (dashboard only)
        result = scorer.score(_req(dashboard_dependencies=["d"]))
        assert result.risk_score == 2.0
        assert result.approval_required is False

    def test_approval_at_threshold(self):
        scorer = RiskScorer()
        # Score 3.0 (sla tags)
        result = scorer.score(_req(sla_tags=["gold"]))
        assert result.risk_score == 3.0
        assert result.approval_required is True

    def test_approval_above_threshold(self):
        scorer = RiskScorer()
        result = scorer.score(_req(downstream_depth=3))
        assert result.risk_score == 4.5
        assert result.approval_required is True


# ================================================================== #
# risk_factors list content
# ================================================================== #


class TestRiskFactorsContent:
    """Verify the risk_factors list is populated correctly."""

    def test_all_factors_present(self):
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=2,
                sla_tags=["gold"],
                dashboard_dependencies=["exec"],
                historical_failure_rate=0.10,
                model_tags=["critical"],
            )
        )
        factor_text = " ".join(result.risk_factors)
        assert "Downstream depth" in factor_text
        assert "SLA" in factor_text
        assert "Dashboard" in factor_text
        assert "failure rate" in factor_text.lower()
        assert "critical" in factor_text.lower()

    def test_no_factors_when_zero_risk(self):
        scorer = RiskScorer()
        result = scorer.score(_req())
        assert result.risk_factors == []

    def test_factor_count_matches_contributions(self):
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=1,
                sla_tags=["p1"],
                model_tags=["revenue"],
            )
        )
        # 3 factors: depth, SLA, critical tag
        assert len(result.risk_factors) == 3


# ================================================================== #
# Custom thresholds
# ================================================================== #


class TestCustomThresholds:
    """Verify custom auto_approve_threshold and manual_review_threshold."""

    def test_custom_auto_approve_threshold(self):
        scorer = RiskScorer(auto_approve_threshold=1.0)
        result = scorer.score(_req(historical_failure_rate=0.10))
        assert result.risk_score == 1.0
        assert result.approval_required is True

    def test_custom_manual_review_threshold(self):
        scorer = RiskScorer(manual_review_threshold=2.0)
        result = scorer.score(_req(dashboard_dependencies=["d"]))
        assert result.risk_score == 2.0
        assert result.business_critical is True

    def test_high_thresholds_loosen_requirements(self):
        scorer = RiskScorer(
            auto_approve_threshold=9.0,
            manual_review_threshold=9.5,
        )
        result = scorer.score(_req(downstream_depth=4, sla_tags=["gold"]))
        # Score = 6.0 + 3.0 = 9.0
        assert result.risk_score == 9.0
        assert result.approval_required is True
        assert result.business_critical is False  # 9.0 < 9.5


# ================================================================== #
# Composite scenarios
# ================================================================== #


class TestCompositeScenarios:
    """Multi-factor scenarios verifying additive scoring."""

    @pytest.mark.parametrize(
        "depth,sla,dash,failure,tags,expected_score",
        [
            (0, [], [], 0.0, [], 0.0),
            (1, [], [], 0.0, [], 1.5),
            (2, ["gold"], [], 0.0, [], 6.0),  # 3.0 + 3.0
            (0, [], ["d"], 0.10, [], 3.0),  # 2.0 + 1.0
            (1, ["p1"], ["d"], 0.10, ["critical"], 8.0),  # 1.5+3+2+1+0.5=8.0
        ],
        ids=[
            "no-factors",
            "depth-only",
            "depth-plus-sla",
            "dash-plus-failure",
            "all-factors",
        ],
    )
    def test_combined_scoring(self, depth, sla, dash, failure, tags, expected_score):
        scorer = RiskScorer()
        result = scorer.score(
            _req(
                downstream_depth=depth,
                sla_tags=sla,
                dashboard_dependencies=dash,
                historical_failure_rate=failure,
                model_tags=tags,
            )
        )
        assert result.risk_score == expected_score

    def test_response_is_pydantic_model(self):
        scorer = RiskScorer()
        result = scorer.score(_req(downstream_depth=1))
        assert isinstance(result, RiskScoreResponse)
        data = result.model_dump()
        assert "risk_score" in data
        assert "business_critical" in data
        assert "approval_required" in data
        assert "risk_factors" in data
