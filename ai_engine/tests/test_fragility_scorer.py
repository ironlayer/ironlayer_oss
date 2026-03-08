"""Tests for the dependency fragility scorer.

Covers linear chains, diamond patterns, wide fanout, isolated nodes,
critical path detection, batch scoring, and edge cases.  All tests
verify determinism — identical inputs must produce identical outputs.
"""

from __future__ import annotations

import pytest

from ai_engine.engines.fragility_scorer import FragilityScore, FragilityScorer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def scorer() -> FragilityScorer:
    """Return a scorer with default weights (0.4 / 0.3 / 0.3)."""
    return FragilityScorer()


# ---------------------------------------------------------------------------
# Linear chain: A → B → C → D
# ---------------------------------------------------------------------------


class TestLinearChain:
    """A→B→C→D: each model depends on the previous."""

    @pytest.fixture
    def dag(self) -> dict[str, list[str]]:
        return {
            "A": [],
            "B": ["A"],
            "C": ["B"],
            "D": ["C"],
        }

    def test_root_has_no_upstream_risk(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.5, "B": 0.3, "C": 0.2, "D": 0.1}
        result = scorer.compute_fragility("A", dag, preds)
        assert result.upstream_risk == 0.0
        assert result.own_risk == 0.5

    def test_leaf_has_upstream_propagation(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.8, "B": 0.5, "C": 0.3, "D": 0.1}
        result = scorer.compute_fragility("D", dag, preds)
        # Upstream risk from C at depth 1: 0.3 * 0.8 = 0.24
        # Upstream risk from B at depth 2: 0.5 * 0.64 = 0.32
        # Upstream risk from A at depth 3: 0.8 * 0.512 = 0.4096
        assert result.upstream_risk == pytest.approx(0.4096, abs=0.001)

    def test_root_has_maximum_cascade_risk(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.5, "B": 0.3, "C": 0.2, "D": 0.1}
        result = scorer.compute_fragility("A", dag, preds)
        # A's downstream: B, C, D = 3 models.
        # cascade = 3 * 0.5 = 1.5
        assert result.cascade_risk == pytest.approx(1.5, abs=0.001)

    def test_leaf_has_zero_cascade_risk(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.5, "B": 0.3, "C": 0.2, "D": 0.1}
        result = scorer.compute_fragility("D", dag, preds)
        assert result.cascade_risk == 0.0

    def test_fragility_score_bounded(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0}
        for name in dag:
            result = scorer.compute_fragility(name, dag, preds)
            assert 0.0 <= result.fragility_score <= 10.0


# ---------------------------------------------------------------------------
# Diamond pattern: A → B, A → C, B → D, C → D
# ---------------------------------------------------------------------------


class TestDiamondPattern:
    """Diamond DAG: converging/diverging."""

    @pytest.fixture
    def dag(self) -> dict[str, list[str]]:
        return {
            "A": [],
            "B": ["A"],
            "C": ["A"],
            "D": ["B", "C"],
        }

    def test_d_sees_upstream_from_both_paths(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.6, "B": 0.4, "C": 0.5, "D": 0.1}
        result = scorer.compute_fragility("D", dag, preds)
        # Direct parents B (0.4*0.8=0.32) and C (0.5*0.8=0.40)
        # Grandparent A via either: 0.6*0.64=0.384
        # Max upstream = 0.40 (from C at depth 1)
        assert result.upstream_risk == pytest.approx(0.40, abs=0.01)

    def test_a_has_two_downstream_children(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.5, "B": 0.2, "C": 0.2, "D": 0.1}
        result = scorer.compute_fragility("A", dag, preds)
        # A's downstream: B, C, D = 3 models.
        assert result.cascade_risk == pytest.approx(3 * 0.5, abs=0.001)

    def test_diamond_determinism(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"A": 0.5, "B": 0.4, "C": 0.3, "D": 0.2}
        r1 = scorer.compute_fragility("D", dag, preds)
        r2 = scorer.compute_fragility("D", dag, preds)
        assert r1.fragility_score == r2.fragility_score
        assert r1.upstream_risk == r2.upstream_risk


# ---------------------------------------------------------------------------
# Wide fanout: root with many children
# ---------------------------------------------------------------------------


class TestWideFanout:
    """Root node with 10 direct children — worst case for breadth."""

    @pytest.fixture
    def dag(self) -> dict[str, list[str]]:
        dag: dict[str, list[str]] = {"root": []}
        for i in range(10):
            dag[f"child_{i:02d}"] = ["root"]
        return dag

    def test_root_has_high_cascade_risk(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {name: 0.5 for name in dag}
        result = scorer.compute_fragility("root", dag, preds)
        # 10 children * 0.5 = 5.0
        assert result.cascade_risk == pytest.approx(5.0, abs=0.001)

    def test_children_have_no_cascade(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {name: 0.3 for name in dag}
        result = scorer.compute_fragility("child_00", dag, preds)
        assert result.cascade_risk == 0.0

    def test_children_have_upstream_from_root(self, scorer: FragilityScorer, dag: dict) -> None:
        preds = {"root": 0.9}
        preds.update({f"child_{i:02d}": 0.1 for i in range(10)})
        result = scorer.compute_fragility("child_05", dag, preds)
        # root at depth 1: 0.9 * 0.8 = 0.72
        assert result.upstream_risk == pytest.approx(0.72, abs=0.001)


# ---------------------------------------------------------------------------
# Isolated node (no deps, no dependents)
# ---------------------------------------------------------------------------


class TestIsolatedNode:
    """A model with no upstream or downstream dependencies."""

    def test_isolated_node_fragility(self, scorer: FragilityScorer) -> None:
        dag = {"lonely": []}
        preds = {"lonely": 0.4}
        result = scorer.compute_fragility("lonely", dag, preds)
        assert result.upstream_risk == 0.0
        assert result.cascade_risk == 0.0
        assert result.own_risk == 0.4
        # Score = 0.4 * 0.4 * 10 = 1.6
        assert result.fragility_score == pytest.approx(1.6, abs=0.01)

    def test_isolated_node_zero_failure(self, scorer: FragilityScorer) -> None:
        dag = {"healthy": []}
        preds = {"healthy": 0.0}
        result = scorer.compute_fragility("healthy", dag, preds)
        assert result.fragility_score == 0.0
        assert result.risk_factors == []


# ---------------------------------------------------------------------------
# Missing predictions
# ---------------------------------------------------------------------------


class TestMissingPredictions:
    """Models not in the failure_predictions map default to 0.0."""

    def test_missing_model_defaults_to_zero(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"]}
        preds: dict[str, float] = {}  # no predictions at all
        result = scorer.compute_fragility("B", dag, preds)
        assert result.own_risk == 0.0
        assert result.upstream_risk == 0.0
        assert result.cascade_risk == 0.0
        assert result.fragility_score == 0.0

    def test_partial_predictions(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        preds = {"A": 0.5}  # B and C unknown
        result = scorer.compute_fragility("C", dag, preds)
        # Own risk = 0.0 (C not in preds)
        # Upstream from B at depth 1: 0.0 * 0.8 = 0.0
        # Upstream from A at depth 2: 0.5 * 0.64 = 0.32
        assert result.upstream_risk == pytest.approx(0.32, abs=0.001)
        assert result.own_risk == 0.0


# ---------------------------------------------------------------------------
# Critical path detection
# ---------------------------------------------------------------------------


class TestCriticalPath:
    """Tests for _is_on_critical_path (all ancestors > 0.3)."""

    def test_all_above_threshold_is_critical(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        preds = {"A": 0.5, "B": 0.4, "C": 0.6}
        result = scorer.compute_fragility("C", dag, preds)
        assert result.critical_path is True

    def test_one_ancestor_below_threshold_not_critical(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        preds = {"A": 0.1, "B": 0.4, "C": 0.6}  # A < 0.3
        result = scorer.compute_fragility("C", dag, preds)
        assert result.critical_path is False

    def test_own_below_threshold_not_critical(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.5, "B": 0.2}  # B's own < 0.3
        result = scorer.compute_fragility("B", dag, preds)
        assert result.critical_path is False

    def test_root_with_high_own_is_critical(self, scorer: FragilityScorer) -> None:
        dag = {"A": []}
        preds = {"A": 0.5}
        result = scorer.compute_fragility("A", dag, preds)
        # No ancestors to check, own > 0.3 → critical.
        assert result.critical_path is True

    def test_root_with_low_own_not_critical(self, scorer: FragilityScorer) -> None:
        dag = {"A": []}
        preds = {"A": 0.2}
        result = scorer.compute_fragility("A", dag, preds)
        assert result.critical_path is False

    def test_critical_path_appears_in_risk_factors(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.5, "B": 0.5}
        result = scorer.compute_fragility("B", dag, preds)
        assert result.critical_path is True
        assert any("critical path" in f.lower() for f in result.risk_factors)


# ---------------------------------------------------------------------------
# Batch scoring
# ---------------------------------------------------------------------------


class TestBatchScoring:
    """Tests for compute_batch()."""

    def test_batch_returns_all_models(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        preds = {"A": 0.5, "B": 0.3, "C": 0.1}
        results = scorer.compute_batch(dag, preds)
        assert len(results) == 3
        names = {r.model_name for r in results}
        assert names == {"A", "B", "C"}

    def test_batch_sorted_descending(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        preds = {"A": 0.9, "B": 0.1, "C": 0.01}
        results = scorer.compute_batch(dag, preds)
        scores = [r.fragility_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_batch_determinism(self, scorer: FragilityScorer) -> None:
        dag = {"X": [], "Y": ["X"], "Z": ["Y"]}
        preds = {"X": 0.4, "Y": 0.5, "Z": 0.3}
        r1 = scorer.compute_batch(dag, preds)
        r2 = scorer.compute_batch(dag, preds)
        for a, b in zip(r1, r2):
            assert a.model_name == b.model_name
            assert a.fragility_score == b.fragility_score

    def test_batch_single_model(self, scorer: FragilityScorer) -> None:
        dag = {"only": []}
        preds = {"only": 0.5}
        results = scorer.compute_batch(dag, preds)
        assert len(results) == 1
        assert results[0].model_name == "only"

    def test_batch_empty_dag(self, scorer: FragilityScorer) -> None:
        results = scorer.compute_batch({}, {})
        assert results == []


# ---------------------------------------------------------------------------
# Risk factors
# ---------------------------------------------------------------------------


class TestRiskFactors:
    """Tests that risk_factors list contains meaningful descriptions."""

    def test_own_risk_in_factors(self, scorer: FragilityScorer) -> None:
        dag = {"A": []}
        preds = {"A": 0.7}
        result = scorer.compute_fragility("A", dag, preds)
        assert any("own failure" in f.lower() for f in result.risk_factors)

    def test_upstream_risk_in_factors(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.6, "B": 0.1}
        result = scorer.compute_fragility("B", dag, preds)
        assert any("upstream" in f.lower() for f in result.risk_factors)

    def test_cascade_risk_in_factors(self, scorer: FragilityScorer) -> None:
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.5, "B": 0.1}
        result = scorer.compute_fragility("A", dag, preds)
        assert any("cascade" in f.lower() for f in result.risk_factors)

    def test_zero_risk_has_no_factors(self, scorer: FragilityScorer) -> None:
        dag = {"A": []}
        preds = {"A": 0.0}
        result = scorer.compute_fragility("A", dag, preds)
        assert result.risk_factors == []


# ---------------------------------------------------------------------------
# Custom weights
# ---------------------------------------------------------------------------


class TestCustomWeights:
    """Tests that custom weights alter the composite score."""

    def test_own_weight_dominates(self) -> None:
        scorer = FragilityScorer(own_weight=1.0, upstream_weight=0.0, cascade_weight=0.0)
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.8, "B": 0.5}
        result = scorer.compute_fragility("B", dag, preds)
        # Score = 1.0 * 0.5 * 10 = 5.0
        assert result.fragility_score == pytest.approx(5.0, abs=0.01)

    def test_upstream_weight_dominates(self) -> None:
        scorer = FragilityScorer(own_weight=0.0, upstream_weight=1.0, cascade_weight=0.0)
        dag = {"A": [], "B": ["A"]}
        preds = {"A": 0.8, "B": 0.0}
        result = scorer.compute_fragility("B", dag, preds)
        # Upstream from A: 0.8 * 0.8 = 0.64, capped at 1.0.
        # Score = 1.0 * 0.64 * 10 = 6.4
        assert result.fragility_score == pytest.approx(6.4, abs=0.01)


# ---------------------------------------------------------------------------
# FragilityScore model validation
# ---------------------------------------------------------------------------


class TestFragilityScoreModel:
    """Tests for the FragilityScore Pydantic model."""

    def test_defaults(self) -> None:
        score = FragilityScore(model_name="test")
        assert score.own_risk == 0.0
        assert score.upstream_risk == 0.0
        assert score.cascade_risk == 0.0
        assert score.fragility_score == 0.0
        assert score.critical_path is False
        assert score.risk_factors == []

    def test_model_dump(self) -> None:
        score = FragilityScore(
            model_name="m1",
            own_risk=0.5,
            upstream_risk=0.3,
            cascade_risk=1.2,
            fragility_score=4.5,
            critical_path=True,
            risk_factors=["High own risk"],
        )
        data = score.model_dump()
        assert data["model_name"] == "m1"
        assert data["fragility_score"] == 4.5
        assert data["critical_path"] is True
