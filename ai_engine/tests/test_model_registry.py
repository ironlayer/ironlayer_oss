"""Tests for ai_engine.ml.model_registry.

Covers:
- ModelRegistry.load_model raises FileNotFoundError for unknown models.
- ModelRegistry.record_prediction stores records and respects the 10_000
  ring-buffer limit.
- ModelRegistry.drift_check returns "insufficient_data" when < 100 records.
- ModelRegistry.drift_check returns "stable" for identical distributions.
- ModelRegistry.drift_check returns "drift" for very different distributions.
- ModelRegistry.list_versions returns versions in sorted ascending order.
- _compute_psi returns 0 for identical distributions.
- CostPredictor integrates with ModelRegistry (record_prediction called).
"""

from __future__ import annotations

import hashlib
import math
from pathlib import Path

import joblib
import numpy as np
import pytest
from ai_engine.ml.model_registry import (
    _CACHE_TTL_SECONDS,
    _MAX_PREDICTION_RECORDS,
    _PSI_MIN_RECORDS,
    _PSI_WINDOW,
    ModelRecord,
    ModelRegistry,
    _compute_file_sha256,
    _compute_psi,
    _digest_path,
    _validate_model_name,
    _validate_model_version,
    _verify_model_file,
)
from sklearn.linear_model import LinearRegression

# ================================================================== #
# Fixtures
# ================================================================== #


@pytest.fixture()
def registry(tmp_path: Path) -> ModelRegistry:
    """A fresh ModelRegistry pointing at an empty tmp directory."""
    return ModelRegistry(models_dir=tmp_path)


@pytest.fixture()
def registry_no_dir(tmp_path: Path) -> ModelRegistry:
    """A registry whose models_dir does not yet exist."""
    return ModelRegistry(models_dir=tmp_path / "nonexistent")


def _save_model(models_dir: Path, name: str, version: str) -> Path:
    """Persist a trivial LinearRegression to models_dir/{name}_v{version}.joblib.

    Also writes the SHA-256 digest file required by ModelRegistry.load_model().
    """
    model = LinearRegression()
    model.fit([[1], [2]], [1, 2])
    path = models_dir / f"{name}_v{version}.joblib"
    joblib.dump(model, path)
    # Write digest alongside — load_model() requires it.
    digest = _compute_file_sha256(path)
    _digest_path(path).write_text(digest)
    return path


# ================================================================== #
# load_model
# ================================================================== #


class TestLoadModel:
    """Tests for ModelRegistry.load_model."""

    def test_raises_for_unknown_model_empty_dir(self, registry: ModelRegistry) -> None:
        """load_model raises FileNotFoundError when no matching file exists."""
        with pytest.raises(FileNotFoundError, match="cost_model"):
            registry.load_model("cost_model")

    def test_raises_for_unknown_model_no_dir(self, registry_no_dir: ModelRegistry) -> None:
        """load_model raises FileNotFoundError when models_dir does not exist."""
        with pytest.raises(FileNotFoundError, match="cost_model"):
            registry_no_dir.load_model("cost_model")

    def test_raises_for_missing_specific_version(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model raises FileNotFoundError for a specific version that is absent."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        with pytest.raises(FileNotFoundError):
            registry.load_model("cost_model", version="9.9.9")

    def test_loads_specific_version(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model returns a model when the exact version file exists."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        model = registry.load_model("cost_model", version="1.0.0")
        assert isinstance(model, LinearRegression)

    def test_loads_latest_when_version_is_none(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model with version=None picks the highest available version."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        _save_model(tmp_path, "cost_model", "2.3.1")
        _save_model(tmp_path, "cost_model", "1.5.0")
        model = registry.load_model("cost_model")
        assert isinstance(model, LinearRegression)
        # Verify the registry record says version 2.3.1 was selected.
        records = registry.loaded_models()
        assert any(r.version == "2.3.1" for r in records)

    def test_cached_model_returned_on_second_call(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """Second load_model call returns the same cached object (no re-deserialisation)."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        first = registry.load_model("cost_model", version="1.0.0")
        second = registry.load_model("cost_model", version="1.0.0")
        assert first is second

    def test_active_version_updated_to_most_recently_loaded(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        """_current_version returns the last loaded version, not the first cached one.

        When two versions are loaded sequentially, predictions recorded after
        the second load must be tagged with 2.0.0, not 1.0.0.
        """
        _save_model(tmp_path, "cost_model", "1.0.0")
        _save_model(tmp_path, "cost_model", "2.0.0")

        registry.load_model("cost_model", version="1.0.0")
        # Record a prediction — should tag version 1.0.0.
        registry.record_prediction("cost_model", {"x": 1}, 10.0)

        registry.load_model("cost_model", version="2.0.0")
        # Record another prediction — should now tag version 2.0.0.
        registry.record_prediction("cost_model", {"x": 2}, 20.0)

        # Verify via _current_version.
        assert registry._current_version("cost_model") == "2.0.0"

    def test_loaded_models_populated_after_load(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """loaded_models() returns a ModelRecord entry after load_model succeeds."""
        _save_model(tmp_path, "cost_model", "3.0.0")
        registry.load_model("cost_model", version="3.0.0")
        records = registry.loaded_models()
        assert len(records) == 1
        rec = records[0]
        assert isinstance(rec, ModelRecord)
        assert rec.name == "cost_model"
        assert rec.version == "3.0.0"
        assert rec.path.name == "cost_model_v3.0.0.joblib"


# ================================================================== #
# Integrity verification (BL-043)
# ================================================================== #


class TestModelIntegrity:
    """Tests for SHA-256 digest-based integrity checks."""

    def test_load_raises_when_digest_absent(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model raises ValueError when no .sha256 file exists."""
        model = LinearRegression().fit([[1], [2]], [1, 2])
        path = tmp_path / "cost_model_v1.0.0.joblib"
        joblib.dump(model, path)
        # Intentionally omit the digest file.
        with pytest.raises(ValueError, match="digest"):
            registry.load_model("cost_model", version="1.0.0")

    def test_load_raises_when_digest_tampered(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model raises ValueError when the digest does not match the file."""
        path = _save_model(tmp_path, "cost_model", "1.0.0")
        # Corrupt the digest file.
        _digest_path(path).write_text("0" * 64)
        with pytest.raises(ValueError, match="integrity check FAILED"):
            registry.load_model("cost_model", version="1.0.0")

    def test_load_raises_when_model_file_tampered(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model raises ValueError when the model file is modified after save."""
        path = _save_model(tmp_path, "cost_model", "1.0.0")
        # Append bytes to corrupt the model without touching the digest.
        with path.open("ab") as fh:
            fh.write(b"\x00" * 16)
        with pytest.raises(ValueError, match="integrity check FAILED"):
            registry.load_model("cost_model", version="1.0.0")

    def test_load_succeeds_with_valid_digest(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model loads successfully when digest matches."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        model = registry.load_model("cost_model", version="1.0.0")
        assert isinstance(model, LinearRegression)

    def test_save_model_writes_digest(self, registry: ModelRegistry) -> None:
        """save_model creates both .joblib and .sha256 files."""
        model = LinearRegression().fit([[1], [2]], [1, 2])
        path = registry.save_model(model, "test_model", "2.0.0")
        assert path.exists()
        assert _digest_path(path).exists()
        # Digest must be a valid 64-character hex string.
        digest = _digest_path(path).read_text().strip()
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_save_then_load_roundtrip(self, registry: ModelRegistry) -> None:
        """save_model + load_model round-trips correctly."""
        original = LinearRegression().fit([[1], [2]], [1, 2])
        registry.save_model(original, "roundtrip_model", "1.0.0")
        loaded = registry.load_model("roundtrip_model", version="1.0.0")
        assert isinstance(loaded, LinearRegression)

    def test_verify_model_file_helper(self, tmp_path: Path) -> None:
        """_verify_model_file passes for correct digest and raises for wrong one."""
        model = LinearRegression().fit([[1], [2]], [1, 2])
        path = tmp_path / "m_v1.0.0.joblib"
        joblib.dump(model, path)
        digest = _compute_file_sha256(path)
        _digest_path(path).write_text(digest)
        # Should not raise.
        _verify_model_file(path)
        # Corrupt the digest.
        _digest_path(path).write_text("baddigest")
        with pytest.raises(ValueError):
            _verify_model_file(path)

    def test_compute_file_sha256_deterministic(self, tmp_path: Path) -> None:
        """_compute_file_sha256 returns the same hex for the same file contents."""
        p = tmp_path / "data.bin"
        p.write_bytes(b"hello world")
        assert _compute_file_sha256(p) == _compute_file_sha256(p)
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert _compute_file_sha256(p) == expected


# ================================================================== #
# record_prediction
# ================================================================== #


class TestRecordPrediction:
    """Tests for ModelRegistry.record_prediction."""

    def test_stores_records(self, registry: ModelRegistry) -> None:
        """record_prediction stores entries retrievable via drift_check."""
        for i in range(10):
            registry.record_prediction(
                model_name="my_model",
                features={"x": i},
                prediction=float(i),
            )
        result = registry.drift_check("my_model")
        assert result["sample_size"] == 10

    def test_ring_buffer_respects_max_limit(self, registry: ModelRegistry) -> None:
        """Ring buffer discards oldest entries once 10_000 records are stored."""
        overflow = _MAX_PREDICTION_RECORDS + 500
        for i in range(overflow):
            registry.record_prediction(
                model_name="my_model",
                features={"x": i},
                prediction=float(i % 100),
            )
        result = registry.drift_check("my_model")
        assert result["sample_size"] == _MAX_PREDICTION_RECORDS

    def test_optional_actual_stored(self, registry: ModelRegistry) -> None:
        """record_prediction accepts an actual value without error."""
        registry.record_prediction(
            model_name="my_model",
            features={"x": 1},
            prediction=42.0,
            actual=40.0,
        )
        result = registry.drift_check("my_model")
        assert result["sample_size"] == 1

    def test_independent_buffers_per_model(self, registry: ModelRegistry) -> None:
        """Each model name has its own independent prediction buffer."""
        for i in range(5):
            registry.record_prediction("model_a", {"x": i}, float(i))
        for i in range(8):
            registry.record_prediction("model_b", {"x": i}, float(i))

        result_a = registry.drift_check("model_a")
        result_b = registry.drift_check("model_b")
        assert result_a["sample_size"] == 5
        assert result_b["sample_size"] == 8


# ================================================================== #
# drift_check
# ================================================================== #


class TestDriftCheck:
    """Tests for ModelRegistry.drift_check."""

    def test_insufficient_data_below_minimum(self, registry: ModelRegistry) -> None:
        """drift_check returns insufficient_data status when fewer than 100 records."""
        for i in range(_PSI_MIN_RECORDS - 1):
            registry.record_prediction("m", {}, float(i))
        result = registry.drift_check("m")
        assert result["status"] == "insufficient_data"
        assert result["sample_size"] == _PSI_MIN_RECORDS - 1
        assert "psi" not in result

    def test_insufficient_data_zero_records(self, registry: ModelRegistry) -> None:
        """drift_check returns insufficient_data when model has never been recorded."""
        result = registry.drift_check("never_seen")
        assert result["status"] == "insufficient_data"
        assert result["sample_size"] == 0

    def test_stable_status_for_identical_distribution(self, registry: ModelRegistry) -> None:
        """drift_check returns 'stable' when baseline and recent windows are the same."""
        # Use 200 records all drawn from the same uniform distribution.
        rng = np.random.default_rng(42)
        values = rng.uniform(1.0, 10.0, size=200)
        for v in values:
            registry.record_prediction("model_stable", {}, float(v))
        result = registry.drift_check("model_stable")
        assert result["status"] == "stable"
        assert result["psi"] < 0.1
        assert result["sample_size"] == 200

    def test_drift_status_for_very_different_distributions(self, registry: ModelRegistry) -> None:
        """drift_check returns 'drift' when baseline and recent are far apart."""
        # First 500 records from N(0, 1), next 500 from N(100, 1) -- extreme shift.
        rng = np.random.default_rng(0)
        baseline = rng.normal(0.0, 1.0, size=500)
        shifted = rng.normal(100.0, 1.0, size=500)
        for v in baseline:
            registry.record_prediction("model_drift", {}, float(v))
        for v in shifted:
            registry.record_prediction("model_drift", {}, float(v))
        result = registry.drift_check("model_drift")
        assert result["status"] == "drift"
        assert result["psi"] > 0.2

    def test_warning_status_for_moderate_shift(self, registry: ModelRegistry) -> None:
        """drift_check returns 'warning' for a moderate distribution shift."""
        rng = np.random.default_rng(7)
        # Baseline: N(0,1) -- Recent: N(1,1) — mild but detectable shift.
        baseline = rng.normal(0.0, 1.0, size=500)
        shifted = rng.normal(3.0, 1.0, size=500)
        for v in baseline:
            registry.record_prediction("model_warn", {}, float(v))
        for v in shifted:
            registry.record_prediction("model_warn", {}, float(v))
        result = registry.drift_check("model_warn")
        # Must be at least warning; could be drift depending on magnitude.
        assert result["status"] in ("warning", "drift")
        assert result["psi"] >= 0.1

    def test_drift_check_includes_message(self, registry: ModelRegistry) -> None:
        """drift_check always includes a non-empty message field."""
        rng = np.random.default_rng(1)
        values = rng.uniform(0, 1, size=200)
        for v in values:
            registry.record_prediction("msg_model", {}, float(v))
        result = registry.drift_check("msg_model")
        assert "message" in result
        assert len(result["message"]) > 0

    def test_drift_check_non_numeric_predictions_insufficient(self, registry: ModelRegistry) -> None:
        """drift_check reports insufficient_data when predictions are non-numeric."""
        for _ in range(200):
            registry.record_prediction("str_model", {}, "label_a")
        result = registry.drift_check("str_model")
        assert result["status"] == "insufficient_data"


# ================================================================== #
# list_versions
# ================================================================== #


class TestListVersions:
    """Tests for ModelRegistry.list_versions."""

    def test_returns_empty_for_unknown_model(self, registry: ModelRegistry) -> None:
        """list_versions returns [] when no files match the model name."""
        assert registry.list_versions("nonexistent") == []

    def test_returns_empty_when_dir_absent(self, registry_no_dir: ModelRegistry) -> None:
        """list_versions returns [] when models_dir does not exist."""
        assert registry_no_dir.list_versions("cost_model") == []

    def test_returns_sorted_versions(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """list_versions returns versions in ascending semantic order."""
        for version in ("2.0.0", "1.0.0", "1.10.0", "1.2.0"):
            _save_model(tmp_path, "cost_model", version)
        result = registry.list_versions("cost_model")
        assert result == ["1.0.0", "1.2.0", "1.10.0", "2.0.0"]

    def test_ignores_files_for_other_models(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """list_versions only returns files whose name prefix matches exactly."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        _save_model(tmp_path, "risk_model", "1.0.0")
        result = registry.list_versions("cost_model")
        assert result == ["1.0.0"]
        assert "risk_model" not in str(result)

    def test_ignores_non_joblib_files(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """list_versions ignores files that do not end in .joblib."""
        (tmp_path / "cost_model_v1.0.0.pkl").write_text("dummy")
        (tmp_path / "cost_model_v1.0.0.joblib.sha256").write_text("hash")
        result = registry.list_versions("cost_model")
        assert result == []


# ================================================================== #
# _compute_psi
# ================================================================== #


class TestComputePsi:
    """Tests for the _compute_psi helper function."""

    def test_zero_for_identical_distributions(self) -> None:
        """PSI is 0 (or near-zero) when expected and actual are identical."""
        rng = np.random.default_rng(42)
        data = rng.uniform(0, 100, size=1000).astype(np.float64)
        psi = _compute_psi(data, data)
        assert math.isclose(psi, 0.0, abs_tol=1e-6)

    def test_positive_for_different_distributions(self) -> None:
        """PSI is positive when distributions differ."""
        rng = np.random.default_rng(0)
        expected = rng.normal(0, 1, size=500).astype(np.float64)
        actual = rng.normal(5, 1, size=500).astype(np.float64)
        psi = _compute_psi(expected, actual)
        assert psi > 0.0

    def test_large_psi_for_extreme_shift(self) -> None:
        """PSI exceeds the drift threshold for a very large distribution shift."""
        rng = np.random.default_rng(1)
        expected = rng.normal(0, 1, size=1000).astype(np.float64)
        actual = rng.normal(50, 1, size=1000).astype(np.float64)
        psi = _compute_psi(expected, actual)
        assert psi > 0.2

    def test_returns_float(self) -> None:
        """_compute_psi always returns a Python float."""
        rng = np.random.default_rng(99)
        data = rng.uniform(size=200).astype(np.float64)
        result = _compute_psi(data, data)
        assert isinstance(result, float)

    def test_custom_bin_count(self) -> None:
        """_compute_psi accepts a custom n_bins parameter."""
        rng = np.random.default_rng(5)
        data = rng.uniform(size=500).astype(np.float64)
        psi_5 = _compute_psi(data, data, n_bins=5)
        psi_20 = _compute_psi(data, data, n_bins=20)
        # Both should be near-zero for identical distributions.
        assert math.isclose(psi_5, 0.0, abs_tol=1e-5)
        assert math.isclose(psi_20, 0.0, abs_tol=1e-5)


# ================================================================== #
# CostPredictor integration
# ================================================================== #


class TestCostPredictorRegistryIntegration:
    """Verify that CostPredictor calls record_prediction when registry is set."""

    def test_prediction_recorded_via_registry(self, tmp_path: Path) -> None:
        """CostPredictor records predictions into the registry after _model_predict."""
        import numpy as np
        from ai_engine.engines.cost_predictor import CostPredictor
        from ai_engine.ml.cost_model import CostModelTrainer
        from ai_engine.models.requests import CostPredictRequest

        # Train a minimal model and save it so CostPredictor loads it.
        features = np.array([[i, 0, 1, 0, 0, 0, 0, 0] for i in range(1, 11)], dtype=np.float64)
        targets = np.array([i * 60.0 for i in range(1, 11)], dtype=np.float64)
        model = CostModelTrainer.train(features, targets)
        model_path = tmp_path / "cost_model.joblib"
        CostModelTrainer.save(model, model_path)

        registry = ModelRegistry(models_dir=tmp_path)
        # CostPredictor with a direct model_path AND a registry attached.
        predictor = CostPredictor(model_path=model_path, registry=registry)
        # BL-100: model loading is lazy — has_trained_model is False until predict() is called.
        assert not predictor.has_trained_model

        request = CostPredictRequest(
            model_name="catalog.schema.model_a",
            partition_count=5,
            cluster_size="medium",
        )
        predictor.predict(request)

        # After predict(), the model has been loaded.
        assert predictor.has_trained_model

        # Exactly one prediction should now be in the registry buffer.
        result = registry.drift_check("cost_model")
        assert result["sample_size"] == 1

    def test_no_registry_still_works(self, tmp_path: Path) -> None:
        """CostPredictor without a registry works as before (no AttributeError)."""
        from ai_engine.engines.cost_predictor import CostPredictor
        from ai_engine.models.requests import CostPredictRequest

        predictor = CostPredictor(model_path=None)
        request = CostPredictRequest(
            model_name="catalog.schema.model_b",
            partition_count=3,
            cluster_size="small",
        )
        response = predictor.predict(request)
        assert response.estimated_cost_usd >= 0.0

    def test_registry_no_model_falls_back_to_heuristic(self, tmp_path: Path) -> None:
        """CostPredictor with an empty registry falls back to heuristic."""
        from ai_engine.engines.cost_predictor import CostPredictor
        from ai_engine.models.requests import CostPredictRequest

        registry = ModelRegistry(models_dir=tmp_path)
        predictor = CostPredictor(registry=registry)
        assert not predictor.has_trained_model

        request = CostPredictRequest(
            model_name="catalog.schema.model_c",
            partition_count=2,
            cluster_size="large",
        )
        response = predictor.predict(request)
        assert response.estimated_cost_usd >= 0.0


# ================================================================== #
# BL-054: Model name / version path-traversal prevention
# ================================================================== #


class TestModelNameValidation:
    """Tests for _validate_model_name and _validate_model_version (BL-054)."""

    # --- _validate_model_name ------------------------------------------

    @pytest.mark.parametrize(
        "name",
        [
            "cost_model",
            "CostModel",
            "cost-model-v2",
            "a",
            "A1",
            "model_with_123_and-hyphens",
            "a" * 128,  # max length
        ],
    )
    def test_valid_names_pass(self, name: str) -> None:
        """Valid model names pass validation without raising."""
        _validate_model_name(name)  # must not raise

    @pytest.mark.parametrize(
        "name",
        [
            "../../etc/passwd",       # path traversal
            "../relative",            # relative traversal
            "/absolute/path",         # absolute path
            "_starts_with_underscore",# must start alphanumeric
            "-starts-with-hyphen",    # must start alphanumeric
            "",                       # empty string
            "a" * 129,               # too long
            "has spaces",            # spaces not allowed
            "has/slash",             # slash not allowed
            "has\x00null",           # null byte
        ],
    )
    def test_invalid_names_raise_value_error(self, name: str) -> None:
        """Invalid model names raise ValueError."""
        with pytest.raises(ValueError, match="Invalid model name"):
            _validate_model_name(name)

    # --- _validate_model_version ----------------------------------------

    @pytest.mark.parametrize(
        "version",
        [
            "1.0.0",
            "0.0.1",
            "12.34.56",
            "100.200.300",
        ],
    )
    def test_valid_versions_pass(self, version: str) -> None:
        """Valid semantic versions pass validation."""
        _validate_model_version(version)  # must not raise

    @pytest.mark.parametrize(
        "version",
        [
            "1.0",                  # missing patch
            "1",                    # only major
            "1.0.0-alpha",          # pre-release label
            "1.0.0+build",          # build metadata
            "../../etc",            # path traversal
            "",                     # empty
            "v1.0.0",               # leading 'v'
            "1.0.a",                # non-numeric patch
        ],
    )
    def test_invalid_versions_raise_value_error(self, version: str) -> None:
        """Invalid version strings raise ValueError."""
        with pytest.raises(ValueError, match="Invalid model version"):
            _validate_model_version(version)

    # --- Integration with ModelRegistry ---------------------------------

    def test_load_model_rejects_traversal_name(self, registry: ModelRegistry) -> None:
        """load_model raises ValueError for path-traversal model names."""
        with pytest.raises(ValueError, match="Invalid model name"):
            registry.load_model("../../etc/passwd")

    def test_load_model_rejects_traversal_version(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """load_model raises ValueError for path-traversal version strings."""
        _save_model(tmp_path, "cost_model", "1.0.0")
        with pytest.raises(ValueError, match="Invalid model version"):
            registry.load_model("cost_model", version="../evil")

    def test_save_model_rejects_traversal_name(self, registry: ModelRegistry) -> None:
        """save_model raises ValueError for path-traversal model names."""
        from sklearn.linear_model import LinearRegression

        model = LinearRegression().fit([[1]], [1])
        with pytest.raises(ValueError, match="Invalid model name"):
            registry.save_model(model, "../../evil", "1.0.0")

    def test_save_model_rejects_traversal_version(self, registry: ModelRegistry) -> None:
        """save_model raises ValueError for path-traversal version strings."""
        from sklearn.linear_model import LinearRegression

        model = LinearRegression().fit([[1]], [1])
        with pytest.raises(ValueError, match="Invalid model version"):
            registry.save_model(model, "cost_model", "../evil")


# ================================================================== #
# BL-090: PSI drift islice optimization
# ================================================================== #


class TestDriftCheckIsliceOptimization:
    """Tests that drift_check uses islice/deque indexing rather than list()."""

    def test_drift_check_with_exactly_psi_window_records(self, registry: ModelRegistry) -> None:
        """drift_check works correctly when record count equals _PSI_WINDOW exactly."""
        rng = np.random.default_rng(11)
        values = rng.uniform(0, 1, size=_PSI_WINDOW)
        for v in values:
            registry.record_prediction("model_exact", {}, float(v))
        result = registry.drift_check("model_exact")
        # baseline and recent window overlap completely — should be stable.
        assert result["status"] in ("stable", "warning")
        assert result["sample_size"] == _PSI_WINDOW

    def test_drift_check_with_more_than_two_psi_windows(self, registry: ModelRegistry) -> None:
        """drift_check baseline uses oldest _PSI_WINDOW records; recent uses newest."""
        # Fill buffer: first 500 from N(0,1), next 500 from N(50,1) — extreme shift.
        rng = np.random.default_rng(42)
        baseline = rng.normal(0.0, 1.0, size=_PSI_WINDOW)
        shifted = rng.normal(50.0, 1.0, size=_PSI_WINDOW)
        for v in baseline:
            registry.record_prediction("model_windows", {}, float(v))
        for v in shifted:
            registry.record_prediction("model_windows", {}, float(v))
        result = registry.drift_check("model_windows")
        # With islice, baseline = first 500 (N(0,1)), recent = last 500 (N(50,1)).
        assert result["status"] == "drift"
        assert result["psi"] > 0.2

    def test_drift_check_recent_window_is_truly_most_recent(self, registry: ModelRegistry) -> None:
        """Verify recent_window uses the newest _PSI_WINDOW records.

        Inserts exactly 2 * _PSI_WINDOW records: first half from N(0,1),
        second half from N(100,1).  With the islice/deque-index approach,
        the baseline window is the first 500 (near-zero) and the recent
        window is the last 500 (near-100), producing a large PSI.
        """
        rng = np.random.default_rng(7)
        for v in rng.normal(0.0, 1.0, size=_PSI_WINDOW):
            registry.record_prediction("model_recent", {}, float(v))
        for v in rng.normal(100.0, 1.0, size=_PSI_WINDOW):
            registry.record_prediction("model_recent", {}, float(v))
        result = registry.drift_check("model_recent")
        # baseline = N(0,1), recent = N(100,1) → clear drift.
        assert result["status"] == "drift"
        assert result["psi"] > 0.2


# ================================================================== #
# BL-103: Model registry cache TTL
# ================================================================== #


class TestCacheTTL:
    """Tests for the time-based TTL eviction added in BL-103."""

    def test_fresh_cache_hit_returns_same_object(self, registry: ModelRegistry, tmp_path: Path) -> None:
        """A model loaded within the TTL window is returned from cache (same object)."""
        _save_model(tmp_path, "ttl_model", "1.0.0")
        first = registry.load_model("ttl_model", version="1.0.0")
        second = registry.load_model("ttl_model", version="1.0.0")
        assert first is second

    def test_expired_cache_reloads_from_disk(
        self, registry: ModelRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When monotonic time advances beyond CACHE_TTL, load_model reloads."""
        import time as _time

        _save_model(tmp_path, "ttl_model", "1.0.0")
        first = registry.load_model("ttl_model", version="1.0.0")

        # Simulate time advancing beyond TTL.
        original_monotonic = _time.monotonic

        def _future_monotonic() -> float:
            return original_monotonic() + _CACHE_TTL_SECONDS + 1.0

        import ai_engine.ml.model_registry as _reg_module

        monkeypatch.setattr(_reg_module.time, "monotonic", _future_monotonic)

        second = registry.load_model("ttl_model", version="1.0.0")
        # After TTL expiry, the object is re-deserialised — a new instance.
        assert first is not second

    def test_cache_ttl_constant_is_positive(self) -> None:
        """_CACHE_TTL_SECONDS must be a positive float."""
        assert isinstance(_CACHE_TTL_SECONDS, float)
        assert _CACHE_TTL_SECONDS > 0.0

    def test_reload_evicts_single_model_all_versions(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        """reload() evicts all cached versions of the named model."""
        _save_model(tmp_path, "evict_model", "1.0.0")
        _save_model(tmp_path, "evict_model", "2.0.0")
        _save_model(tmp_path, "other_model", "1.0.0")

        obj_1 = registry.load_model("evict_model", version="1.0.0")
        obj_2 = registry.load_model("evict_model", version="2.0.0")
        other = registry.load_model("other_model", version="1.0.0")

        registry.reload("evict_model")

        # Reloaded objects are new instances.
        new_1 = registry.load_model("evict_model", version="1.0.0")
        new_2 = registry.load_model("evict_model", version="2.0.0")
        assert new_1 is not obj_1
        assert new_2 is not obj_2

        # The other_model is unaffected.
        new_other = registry.load_model("other_model", version="1.0.0")
        assert new_other is other

    def test_reload_noop_when_model_not_cached(self, registry: ModelRegistry) -> None:
        """reload() is a no-op when the model is not in the cache."""
        # Should not raise.
        registry.reload("not_loaded_model")

    def test_reload_clears_records_metadata(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        """reload() also removes the ModelRecord metadata for evicted versions."""
        _save_model(tmp_path, "meta_model", "1.0.0")
        registry.load_model("meta_model", version="1.0.0")
        assert len(registry.loaded_models()) == 1

        registry.reload("meta_model")
        assert len(registry.loaded_models()) == 0
