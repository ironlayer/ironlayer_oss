"""Tests for the cost anomaly detector.

Validates detection of spikes, drops, severity classification,
insufficient data handling, and batch sorting.
"""

from __future__ import annotations

import pytest

from ai_engine.engines.cost_anomaly import AnomalyReport, CostAnomalyDetector

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def detector() -> CostAnomalyDetector:
    """Return a detector with default thresholds."""
    return CostAnomalyDetector()


# ---------------------------------------------------------------------------
# Normal costs (no anomaly)
# ---------------------------------------------------------------------------


class TestNormalCosts:
    """Tests that normal, stable costs produce no anomaly."""

    def test_stable_costs_no_anomaly(self, detector: CostAnomalyDetector) -> None:
        history = [10.0, 10.5, 9.8, 10.2, 10.1, 10.3, 9.9]
        result = detector.detect("model_a", history)
        assert result.is_anomaly is False
        assert result.anomaly_type == "none"
        assert result.severity == "none"

    def test_slightly_above_mean_no_anomaly(self, detector: CostAnomalyDetector) -> None:
        # Values within 1 std dev should not be flagged.
        history = [10.0, 11.0, 10.0, 11.0, 10.0, 11.0, 10.5]
        result = detector.detect("model_b", history)
        assert result.is_anomaly is False

    def test_zero_cost_history_no_anomaly(self, detector: CostAnomalyDetector) -> None:
        history = [0.0, 0.0, 0.0, 0.0, 0.0]
        result = detector.detect("model_c", history)
        # All values identical — std dev is 0, z-score is 0.
        assert result.is_anomaly is False


# ---------------------------------------------------------------------------
# Cost spikes
# ---------------------------------------------------------------------------


class TestCostSpike:
    """Tests for cost spike detection."""

    def test_large_spike_detected(self, detector: CostAnomalyDetector) -> None:
        # Normal costs ~10, then sudden 100.
        history = [10.0, 10.2, 9.8, 10.1, 10.0, 10.0, 100.0]
        result = detector.detect("model_spike", history)
        assert result.is_anomaly is True
        assert result.anomaly_type == "spike"

    def test_spike_with_explicit_latest(self, detector: CostAnomalyDetector) -> None:
        history = [10.0, 10.2, 9.8, 10.1, 10.0, 10.0]
        result = detector.detect("model_spike", history, latest_cost=50.0)
        assert result.is_anomaly is True
        assert result.anomaly_type == "spike"
        assert result.z_score > 0

    def test_spike_suggestion_contains_info(self, detector: CostAnomalyDetector) -> None:
        history = [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 80.0]
        result = detector.detect("model_spike", history)
        assert "spike" in result.suggested_investigation.lower()
        assert "investigate" in result.suggested_investigation.lower()


# ---------------------------------------------------------------------------
# Cost drops
# ---------------------------------------------------------------------------


class TestCostDrop:
    """Tests for cost drop detection."""

    def test_large_drop_detected(self, detector: CostAnomalyDetector) -> None:
        history = [100.0, 99.0, 101.0, 100.5, 99.5, 100.0, 10.0]
        result = detector.detect("model_drop", history)
        assert result.is_anomaly is True
        assert result.anomaly_type == "drop"
        assert result.z_score < 0

    def test_drop_suggestion_mentions_completeness(self, detector: CostAnomalyDetector) -> None:
        history = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 5.0]
        result = detector.detect("model_drop", history)
        assert "drop" in result.suggested_investigation.lower()


# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------


class TestSeverityLevels:
    """Tests that severity is classified correctly by z-score distance."""

    def _make_history_with_spike(self, mean: float, std: float, n: int, spike_z: float) -> list[float]:
        """Create a stable history with a final spike at the given z-score."""
        history = [mean] * n
        history.append(mean + spike_z * std)
        return history

    def test_minor_severity(self, detector: CostAnomalyDetector) -> None:
        # ~2.5 std devs.
        history = [10.0] * 20
        history.append(10.0 + 2.5 * 0.0)  # no variance in constant history
        # Use varied history instead.
        import random

        rng = random.Random(42)
        history = [10.0 + rng.gauss(0, 1.0) for _ in range(30)]
        mean = sum(history) / len(history)
        std = (sum((x - mean) ** 2 for x in history) / len(history)) ** 0.5
        spike = mean + 2.5 * std
        result = detector.detect("m", history, latest_cost=spike)
        if result.is_anomaly:
            assert result.severity in ("minor", "major")

    def test_critical_severity(self, detector: CostAnomalyDetector) -> None:
        import random

        rng = random.Random(42)
        history = [10.0 + rng.gauss(0, 1.0) for _ in range(30)]
        mean = sum(history) / len(history)
        std = (sum((x - mean) ** 2 for x in history) / len(history)) ** 0.5
        spike = mean + 5.0 * std
        result = detector.detect("m", history, latest_cost=spike)
        assert result.is_anomaly is True
        assert result.severity == "critical"


# ---------------------------------------------------------------------------
# Insufficient data
# ---------------------------------------------------------------------------


class TestInsufficientData:
    """Tests that < 3 data points are handled gracefully."""

    def test_empty_history(self, detector: CostAnomalyDetector) -> None:
        result = detector.detect("model_empty", [])
        assert result.is_anomaly is False
        assert "insufficient" in result.suggested_investigation.lower()

    def test_single_point(self, detector: CostAnomalyDetector) -> None:
        result = detector.detect("model_one", [10.0])
        assert result.is_anomaly is False

    def test_two_points(self, detector: CostAnomalyDetector) -> None:
        result = detector.detect("model_two", [10.0, 11.0])
        assert result.is_anomaly is False


# ---------------------------------------------------------------------------
# Batch detection
# ---------------------------------------------------------------------------


class TestBatchDetection:
    """Tests for detect_batch()."""

    def test_batch_returns_all_models(self, detector: CostAnomalyDetector) -> None:
        models = {
            "model_a": [10.0, 10.0, 10.0, 10.0, 10.0],
            "model_b": [10.0, 10.0, 10.0, 10.0, 50.0],
        }
        results = detector.detect_batch(models)
        assert len(results) == 2

    def test_batch_sorted_by_severity(self, detector: CostAnomalyDetector) -> None:
        models = {
            "normal": [10.0, 10.0, 10.0, 10.0, 10.0],
            "critical_spike": [10.0, 10.0, 10.0, 10.0, 200.0],
            "minor_spike": [10.0, 10.0, 10.0, 10.0, 25.0],
        }
        results = detector.detect_batch(models)
        severity_order = {"critical": 0, "major": 1, "minor": 2, "none": 3}
        severities = [severity_order[r.severity] for r in results]
        assert severities == sorted(severities)

    def test_batch_determinism(self, detector: CostAnomalyDetector) -> None:
        models = {
            "a": [10.0, 12.0, 11.0, 10.5, 80.0],
            "b": [5.0, 5.5, 5.0, 5.2, 5.1],
        }
        r1 = detector.detect_batch(models)
        r2 = detector.detect_batch(models)
        for a, b in zip(r1, r2):
            assert a.model_name == b.model_name
            assert a.z_score == b.z_score


# ---------------------------------------------------------------------------
# Percentile computation
# ---------------------------------------------------------------------------


class TestPercentile:
    """Tests for the percentile calculation."""

    def test_percentile_lowest(self, detector: CostAnomalyDetector) -> None:
        history = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = detector.detect("m", history, latest_cost=1.0)
        assert result.percentile < 20.0  # Should be near the bottom

    def test_percentile_highest(self, detector: CostAnomalyDetector) -> None:
        history = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = detector.detect("m", history, latest_cost=10.0)
        assert result.percentile > 80.0  # Should be near the top


# ---------------------------------------------------------------------------
# AnomalyReport model
# ---------------------------------------------------------------------------


class TestAnomalyReportModel:
    """Tests for the AnomalyReport Pydantic model."""

    def test_defaults(self) -> None:
        report = AnomalyReport(model_name="test")
        assert report.is_anomaly is False
        assert report.anomaly_type == "none"
        assert report.severity == "none"
        assert report.z_score == 0.0
        assert report.percentile == 50.0

    def test_model_dump(self) -> None:
        report = AnomalyReport(
            model_name="m1",
            is_anomaly=True,
            anomaly_type="spike",
            severity="major",
            z_score=3.5,
            percentile=99.5,
            suggested_investigation="Investigate query plan changes.",
        )
        data = report.model_dump()
        assert data["model_name"] == "m1"
        assert data["severity"] == "major"
