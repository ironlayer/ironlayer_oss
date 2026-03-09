"""Unit tests for the core_engine.telemetry package.

Covers:
- collector.capture_run_telemetry
- emitter.MetricsEmitter
- privacy.scrub_pii, TelemetryScrubber
- kpi.KPIThreshold, KPIEvaluator
- retention.RetentionPolicy
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from core_engine.models.telemetry import RunTelemetry
from core_engine.telemetry.collector import capture_run_telemetry
from core_engine.telemetry.emitter import MetricsEmitter
from core_engine.telemetry.kpi import (
    ALL_KPIS,
    KPIEvaluator,
    KPIResult,
    KPIStatus,
    KPIThreshold,
)
from core_engine.telemetry.privacy import (
    TelemetryConsent,
    TelemetryScrubber,
    anonymize_identifier,
    check_consent,
    scrub_dict,
    scrub_pii,
)
from core_engine.telemetry.retention import RetentionPolicy

# ===========================================================================
# capture_run_telemetry
# ===========================================================================


class TestCaptureRunTelemetry:
    def test_creates_run_telemetry_from_metadata(self):
        metadata = {
            "runtime_seconds": 42.5,
            "shuffle_bytes": 1024,
            "input_rows": 10000,
            "output_rows": 500,
            "partition_count": 4,
            "cluster_id": "cluster-abc",
        }
        telemetry = capture_run_telemetry("run-1", "analytics.orders", metadata)

        assert isinstance(telemetry, RunTelemetry)
        assert telemetry.run_id == "run-1"
        assert telemetry.model_name == "analytics.orders"
        assert telemetry.runtime_seconds == 42.5
        assert telemetry.shuffle_bytes == 1024
        assert telemetry.input_rows == 10000
        assert telemetry.output_rows == 500
        assert telemetry.partition_count == 4
        assert telemetry.cluster_id == "cluster-abc"

    def test_defaults_for_missing_keys(self):
        telemetry = capture_run_telemetry("run-1", "my_model", {})
        assert telemetry.runtime_seconds == 0.0
        assert telemetry.shuffle_bytes == 0
        assert telemetry.input_rows == 0
        assert telemetry.output_rows == 0
        assert telemetry.partition_count == 0
        assert telemetry.cluster_id is None

    def test_non_numeric_values_fall_back_to_defaults(self):
        metadata = {
            "runtime_seconds": "not a number",
            "shuffle_bytes": "bad",
            "input_rows": None,
        }
        telemetry = capture_run_telemetry("run-1", "my_model", metadata)
        assert telemetry.runtime_seconds == 0.0
        assert telemetry.shuffle_bytes == 0
        assert telemetry.input_rows == 0

    def test_empty_string_cluster_id_becomes_none(self):
        metadata = {"cluster_id": ""}
        telemetry = capture_run_telemetry("run-1", "my_model", metadata)
        assert telemetry.cluster_id is None

    def test_negative_values_clamped_to_zero(self):
        metadata = {"runtime_seconds": -5.0, "input_rows": -100}
        telemetry = capture_run_telemetry("run-1", "my_model", metadata)
        assert telemetry.runtime_seconds == 0.0
        assert telemetry.input_rows == 0


# ===========================================================================
# MetricsEmitter
# ===========================================================================


class TestMetricsEmitter:
    def test_emit_creates_event(self, tmp_path: Path):
        metrics_file = tmp_path / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=metrics_file)

        emitter.emit("test.event", {"key": "value"})

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        event_data = json.loads(lines[0])
        assert event_data["event"] == "test.event"
        assert event_data["data"]["key"] == "value"
        assert "timestamp" in event_data

    def test_emit_multiple_events(self, tmp_path: Path):
        metrics_file = tmp_path / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=metrics_file)

        emitter.emit("event.one", {"n": 1})
        emitter.emit("event.two", {"n": 2})

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_plan_generated(self, tmp_path: Path):
        metrics_file = tmp_path / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=metrics_file)

        emitter.plan_generated(
            plan_id="plan123",
            models_changed=["model_a", "model_b"],
            duration_ms=150.0,
        )

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        event_data = json.loads(lines[0])
        assert event_data["event"] == "plan.generated"
        assert event_data["data"]["plan_id"] == "plan123"
        assert event_data["data"]["models_changed_count"] == 2

    def test_run_started(self, tmp_path: Path):
        metrics_file = tmp_path / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=metrics_file)

        emitter.run_started(run_id="run-abc", model="analytics.orders")

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        event_data = json.loads(lines[0])
        assert event_data["event"] == "run.started"
        assert event_data["data"]["run_id"] == "run-abc"
        assert event_data["data"]["model"] == "analytics.orders"

    def test_run_finished(self, tmp_path: Path):
        metrics_file = tmp_path / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=metrics_file)

        emitter.run_finished(
            run_id="run-abc",
            model="analytics.orders",
            status="SUCCESS",
            duration_ms=5000.0,
        )

        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        event_data = json.loads(lines[0])
        assert event_data["event"] == "run.finished"
        assert event_data["data"]["status"] == "SUCCESS"

    def test_no_metrics_file(self):
        # Should not raise even without a file.
        emitter = MetricsEmitter(metrics_file=None)
        emitter.emit("test.event", {"key": "value"})

    def test_structured_output(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        emitter = MetricsEmitter(metrics_file=None, structured=True)
        emitter.emit("test.event", {"key": "value"})

        captured = capsys.readouterr()
        assert "test.event" in captured.out

    def test_creates_parent_directories(self, tmp_path: Path):
        nested = tmp_path / "sub" / "dir" / "metrics.jsonl"
        emitter = MetricsEmitter(metrics_file=nested)
        emitter.emit("test.event", {})
        assert nested.exists()


# ===========================================================================
# scrub_pii
# ===========================================================================


class TestScrubPii:
    def test_removes_emails(self):
        text = "Contact user@example.com for details"
        scrubbed = scrub_pii(text)
        assert "user@example.com" not in scrubbed
        assert "[REDACTED_EMAIL]" in scrubbed

    def test_removes_phone_numbers(self):
        text = "Call me at 555-123-4567"
        scrubbed = scrub_pii(text)
        assert "555-123-4567" not in scrubbed
        assert "[REDACTED_PHONE]" in scrubbed

    def test_removes_ip_addresses(self):
        text = "Server at 192.168.1.100"
        scrubbed = scrub_pii(text)
        assert "192.168.1.100" not in scrubbed
        assert "[REDACTED_IP]" in scrubbed

    def test_removes_databricks_tokens(self):
        # Pattern matches dapi + exactly 32 hex chars (see privacy._PII_PATTERNS).
        # Token is built dynamically to avoid secret-scanning false positives.
        _fake_tok = "dapi" + "0123456789abcdef" * 2  # 32 hex chars, not a real token
        text = f"Token: {_fake_tok}"
        scrubbed = scrub_pii(text)
        assert _fake_tok not in scrubbed
        assert "[REDACTED_TOKEN]" in scrubbed

    def test_removes_generic_secrets(self):
        text = "password=my_secret_pw"
        scrubbed = scrub_pii(text)
        assert "my_secret_pw" not in scrubbed
        assert "[REDACTED_SECRET]" in scrubbed

    def test_no_pii_unchanged(self):
        text = "SELECT id, name FROM users WHERE active = true"
        scrubbed = scrub_pii(text)
        assert scrubbed == text

    def test_multiple_pii_types(self):
        text = "Email: user@test.com, IP: 10.0.0.1"
        scrubbed = scrub_pii(text)
        assert "user@test.com" not in scrubbed
        assert "10.0.0.1" not in scrubbed


# ===========================================================================
# TelemetryScrubber
# ===========================================================================


class TestTelemetryScrubber:
    def test_none_consent_returns_none(self):
        scrubber = TelemetryScrubber(consent=TelemetryConsent.NONE)
        result = scrubber.process_telemetry({"key": "value"})
        assert result is None

    def test_local_only_scrubs_pii(self):
        scrubber = TelemetryScrubber(consent=TelemetryConsent.LOCAL_ONLY)
        data = {"message": "Contact admin@example.com"}
        result = scrubber.process_telemetry(data)
        assert result is not None
        assert "admin@example.com" not in result["message"]
        assert "[REDACTED_EMAIL]" in result["message"]

    def test_anonymized_hashes_identifiers(self):
        scrubber = TelemetryScrubber(
            consent=TelemetryConsent.ANONYMIZED,
            anonymization_salt="test_salt",
        )
        data = {
            "model_name": "analytics.orders",
            "tenant_id": "tenant-123",
            "run_id": "run-abc",
        }
        result = scrubber.process_telemetry(data)
        assert result is not None
        # Model name should be anonymized (hashed).
        assert result["model_name"] != "analytics.orders"
        assert len(result["model_name"]) == 16  # Truncated hash.
        assert result["tenant_id"] != "tenant-123"
        assert result["run_id"] != "run-abc"

    def test_full_consent_scrubs_pii_but_no_anonymization(self):
        scrubber = TelemetryScrubber(consent=TelemetryConsent.FULL)
        data = {
            "model_name": "analytics.orders",
            "message": "user@test.com",
        }
        result = scrubber.process_telemetry(data)
        assert result is not None
        # Model name stays intact.
        assert result["model_name"] == "analytics.orders"
        # PII is still scrubbed.
        assert "user@test.com" not in result["message"]

    def test_should_collect(self):
        assert TelemetryScrubber(consent=TelemetryConsent.NONE).should_collect() is False
        assert TelemetryScrubber(consent=TelemetryConsent.LOCAL_ONLY).should_collect() is True
        assert TelemetryScrubber(consent=TelemetryConsent.ANONYMIZED).should_collect() is True
        assert TelemetryScrubber(consent=TelemetryConsent.FULL).should_collect() is True

    def test_should_share(self):
        assert TelemetryScrubber(consent=TelemetryConsent.NONE).should_share() is False
        assert TelemetryScrubber(consent=TelemetryConsent.LOCAL_ONLY).should_share() is False
        assert TelemetryScrubber(consent=TelemetryConsent.ANONYMIZED).should_share() is False
        assert TelemetryScrubber(consent=TelemetryConsent.FULL).should_share() is True

    def test_consent_property(self):
        scrubber = TelemetryScrubber(consent=TelemetryConsent.FULL)
        assert scrubber.consent == TelemetryConsent.FULL

    def test_process_sql_for_logging(self):
        scrubber = TelemetryScrubber(consent=TelemetryConsent.FULL)
        sql = "SELECT * FROM users WHERE email = 'admin@test.com'"
        result = scrubber.process_sql_for_logging(sql)
        assert "admin@test.com" not in result


# ===========================================================================
# check_consent
# ===========================================================================


class TestCheckConsent:
    @pytest.mark.parametrize(
        "consent,action,expected",
        [
            (TelemetryConsent.NONE, "collect", False),
            (TelemetryConsent.NONE, "store", False),
            (TelemetryConsent.NONE, "share", False),
            (TelemetryConsent.LOCAL_ONLY, "collect", True),
            (TelemetryConsent.LOCAL_ONLY, "store", True),
            (TelemetryConsent.LOCAL_ONLY, "share", False),
            (TelemetryConsent.LOCAL_ONLY, "aggregate", False),
            (TelemetryConsent.ANONYMIZED, "collect", True),
            (TelemetryConsent.ANONYMIZED, "aggregate", True),
            (TelemetryConsent.ANONYMIZED, "share", False),
            (TelemetryConsent.FULL, "collect", True),
            (TelemetryConsent.FULL, "store", True),
            (TelemetryConsent.FULL, "share", True),
            (TelemetryConsent.FULL, "aggregate", True),
        ],
    )
    def test_consent_permissions(self, consent: TelemetryConsent, action: str, expected: bool):
        assert check_consent(consent, action) is expected


# ===========================================================================
# anonymize_identifier
# ===========================================================================


class TestAnonymizeIdentifier:
    def test_deterministic(self):
        h1 = anonymize_identifier("test_id", salt="salt")
        h2 = anonymize_identifier("test_id", salt="salt")
        assert h1 == h2

    def test_different_input_different_hash(self):
        h1 = anonymize_identifier("id_a", salt="salt")
        h2 = anonymize_identifier("id_b", salt="salt")
        assert h1 != h2

    def test_different_salt_different_hash(self):
        h1 = anonymize_identifier("test_id", salt="salt_a")
        h2 = anonymize_identifier("test_id", salt="salt_b")
        assert h1 != h2

    def test_truncated_to_16_chars(self):
        result = anonymize_identifier("test_id")
        assert len(result) == 16

    def test_hex_output(self):
        result = anonymize_identifier("test_id")
        assert all(c in "0123456789abcdef" for c in result)


# ===========================================================================
# scrub_dict
# ===========================================================================


class TestScrubDict:
    def test_scrubs_string_values(self):
        data = {"email": "user@example.com", "count": 42}
        result = scrub_dict(data)
        assert "user@example.com" not in result["email"]
        assert result["count"] == 42

    def test_deep_recursion(self):
        data = {
            "outer": {
                "inner": "Contact: admin@example.com",
            }
        }
        result = scrub_dict(data, deep=True)
        assert "admin@example.com" not in result["outer"]["inner"]

    def test_list_values(self):
        data = {
            "items": ["user@test.com", "safe text"],
        }
        result = scrub_dict(data, deep=True)
        assert "user@test.com" not in result["items"][0]
        assert result["items"][1] == "safe text"

    def test_shallow_mode(self):
        data = {
            "nested": {"email": "user@test.com"},
        }
        result = scrub_dict(data, deep=False)
        # Nested dict should not be scrubbed in shallow mode.
        assert result["nested"]["email"] == "user@test.com"


# ===========================================================================
# KPIThreshold
# ===========================================================================


class TestKPIThreshold:
    def test_evaluate_passing_lower_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="seconds",
            target_value=30.0,
            warning_value=45.0,
            direction="lower_is_better",
        )
        assert kpi.evaluate(20.0) == KPIStatus.PASSING

    def test_evaluate_warning_lower_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="seconds",
            target_value=30.0,
            warning_value=45.0,
            direction="lower_is_better",
        )
        assert kpi.evaluate(35.0) == KPIStatus.WARNING

    def test_evaluate_failing_lower_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="seconds",
            target_value=30.0,
            warning_value=45.0,
            direction="lower_is_better",
        )
        assert kpi.evaluate(60.0) == KPIStatus.FAILING

    def test_evaluate_passing_higher_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="percent",
            target_value=95.0,
            warning_value=90.0,
            direction="higher_is_better",
        )
        assert kpi.evaluate(98.0) == KPIStatus.PASSING

    def test_evaluate_warning_higher_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="percent",
            target_value=95.0,
            warning_value=90.0,
            direction="higher_is_better",
        )
        assert kpi.evaluate(92.0) == KPIStatus.WARNING

    def test_evaluate_failing_higher_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="percent",
            target_value=95.0,
            warning_value=90.0,
            direction="higher_is_better",
        )
        assert kpi.evaluate(80.0) == KPIStatus.FAILING

    def test_evaluate_none_returns_insufficient_data(self):
        kpi = KPIThreshold(
            name="test",
            description="Test KPI",
            unit="seconds",
            target_value=30.0,
            warning_value=45.0,
            direction="lower_is_better",
        )
        assert kpi.evaluate(None) == KPIStatus.INSUFFICIENT_DATA

    def test_evaluate_at_boundary_lower_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test",
            unit="s",
            target_value=30.0,
            warning_value=45.0,
            direction="lower_is_better",
        )
        # Exactly at target -> passing.
        assert kpi.evaluate(30.0) == KPIStatus.PASSING
        # Exactly at warning -> warning.
        assert kpi.evaluate(45.0) == KPIStatus.WARNING

    def test_evaluate_at_boundary_higher_is_better(self):
        kpi = KPIThreshold(
            name="test",
            description="Test",
            unit="%",
            target_value=95.0,
            warning_value=90.0,
            direction="higher_is_better",
        )
        assert kpi.evaluate(95.0) == KPIStatus.PASSING
        assert kpi.evaluate(90.0) == KPIStatus.WARNING


# ===========================================================================
# KPIEvaluator
# ===========================================================================


class TestKPIEvaluator:
    def test_evaluate_all_no_data(self):
        evaluator = KPIEvaluator()
        results = evaluator.evaluate_all()
        assert len(results) == len(ALL_KPIS)
        for r in results:
            assert r.status == KPIStatus.INSUFFICIENT_DATA
            assert r.sample_size == 0

    def test_evaluate_all_with_data(self):
        data = {
            "plan_generation_time_seconds": [10.0, 15.0, 20.0],
            "plan_accuracy_percent": [96.0, 97.0, 98.0],
        }
        evaluator = KPIEvaluator(metrics_data=data)
        results = evaluator.evaluate_all()
        assert len(results) == len(ALL_KPIS)

        # plan_generation_time should be evaluated (not INSUFFICIENT_DATA).
        gen_time = next(r for r in results if r.kpi.name == "plan_generation_time_seconds")
        assert gen_time.status != KPIStatus.INSUFFICIENT_DATA
        assert gen_time.sample_size == 3

    def test_evaluate_all_returns_kpi_results(self):
        evaluator = KPIEvaluator()
        results = evaluator.evaluate_all()
        for r in results:
            assert isinstance(r, KPIResult)
            assert isinstance(r.kpi, KPIThreshold)
            assert isinstance(r.status, KPIStatus)

    def test_evaluate_single_found(self):
        data = {"plan_generation_time_seconds": [5.0, 10.0]}
        evaluator = KPIEvaluator(metrics_data=data)
        result = evaluator.evaluate_single("plan_generation_time_seconds")
        assert result is not None
        assert result.kpi.name == "plan_generation_time_seconds"
        assert result.sample_size == 2

    def test_evaluate_single_not_found(self):
        evaluator = KPIEvaluator()
        result = evaluator.evaluate_single("nonexistent_kpi")
        assert result is None

    def test_generate_report(self):
        evaluator = KPIEvaluator()
        report = evaluator.generate_report()
        assert "summary" in report
        assert "kpis" in report
        assert report["summary"]["total_kpis"] == len(ALL_KPIS)
        assert "health" in report["summary"]

    def test_passing_kpi_with_data(self):
        # All generation times well within target (30s).
        data = {"plan_generation_time_seconds": [5.0, 10.0, 15.0, 20.0]}
        evaluator = KPIEvaluator(metrics_data=data)
        result = evaluator.evaluate_single("plan_generation_time_seconds")
        assert result is not None
        assert result.status == KPIStatus.PASSING


# ===========================================================================
# RetentionPolicy
# ===========================================================================


class TestRetentionPolicy:
    def test_default_values(self):
        policy = RetentionPolicy()
        assert policy.raw_retention_days == 30
        assert policy.hourly_retention_days == 365
        assert policy.daily_retention_days == 0

    def test_custom_values(self):
        policy = RetentionPolicy(
            raw_retention_days=7,
            hourly_retention_days=90,
            daily_retention_days=730,
        )
        assert policy.raw_retention_days == 7
        assert policy.hourly_retention_days == 90
        assert policy.daily_retention_days == 730

    def test_frozen(self):
        policy = RetentionPolicy()
        with pytest.raises(AttributeError):
            policy.raw_retention_days = 10  # type: ignore[misc]


# ===========================================================================
# ALL_KPIS constant
# ===========================================================================


class TestAllKpis:
    def test_contains_expected_kpis(self):
        names = {kpi.name for kpi in ALL_KPIS}
        assert "plan_generation_time_seconds" in names
        assert "plan_accuracy_percent" in names
        assert "cost_savings_percent" in names
        assert "semantic_diff_false_positive_rate" in names
        assert "planner_determinism_rate" in names

    def test_all_have_required_fields(self):
        for kpi in ALL_KPIS:
            assert kpi.name
            assert kpi.description
            assert kpi.unit
            assert kpi.direction in ("lower_is_better", "higher_is_better")
            assert kpi.target_value >= 0
            assert kpi.warning_value >= 0


# ===========================================================================
# TelemetryConsent enum
# ===========================================================================


class TestTelemetryConsent:
    def test_values(self):
        assert TelemetryConsent.NONE.value == "none"
        assert TelemetryConsent.LOCAL_ONLY.value == "local_only"
        assert TelemetryConsent.ANONYMIZED.value == "anonymized"
        assert TelemetryConsent.FULL.value == "full"
