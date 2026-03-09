"""Central registry for versioned ML models with drift detection.

Provides a ``ModelRegistry`` that:
- Scans a configurable ``models_dir`` for joblib files following the
  naming convention ``{name}_v{version}.joblib`` (e.g. ``cost_model_v1.2.0.joblib``).
- Caches loaded models in memory keyed by ``(name, version)``.
- Records predictions per model (ring buffer, 10 000 entries max) for
  retrospective drift analysis.
- Implements Population Stability Index (PSI) drift detection comparing
  recent predictions against the earliest baseline window.

Thread-safety: uses ``collections.deque`` with ``maxlen`` for the ring
buffer; all operations are synchronous and GIL-safe.  No additional
locking is required for asyncio-only callers.
"""

from __future__ import annotations

import hashlib
import hmac as _hmac
import itertools
import logging
import re
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, TypedDict

import joblib
import numpy as np

logger = logging.getLogger(__name__)

# Regex matching the versioned model filename convention.
_MODEL_FILENAME_RE = re.compile(
    r"^(?P<name>.+?)_v(?P<version>\d+\.\d+\.\d+)\.joblib$"
)

# Allowlist regexes for caller-supplied model names and version strings (BL-054).
# Names: alphanumeric start, then alphanumeric / underscore / hyphen, max 128 chars.
# Versions: strict semantic version (major.minor.patch).
# These prevent path-traversal attacks (e.g. name="../../etc/passwd") since
# _model_path() constructs a filesystem path directly from the caller's input.
_MODEL_NAME_ALLOWLIST_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")
_MODEL_VERSION_ALLOWLIST_RE = re.compile(r"^\d+\.\d+\.\d+$")

# Ring-buffer capacity per model.
_MAX_PREDICTION_RECORDS = 10_000

# PSI thresholds.
_PSI_WARN_THRESHOLD = 0.1
_PSI_DRIFT_THRESHOLD = 0.2

# Minimum records for drift check.
_PSI_MIN_RECORDS = 100

# Window size for baseline / recent comparison.
_PSI_WINDOW = 500

# Cache TTL: evict model objects from _cache after this many seconds (BL-103).
_CACHE_TTL_SECONDS: float = 3600.0


# ---------------------------------------------------------------------------
# Input validation helpers (path-traversal prevention)
# ---------------------------------------------------------------------------


def _validate_model_name(name: str) -> None:
    """Raise ``ValueError`` if *name* is not a safe model identifier.

    Allows alphanumeric characters, underscores, and hyphens.  The first
    character must be alphanumeric.  Maximum length is 128 characters.

    This prevents path-traversal attacks (e.g. ``"../../etc/passwd"``) since
    :meth:`ModelRegistry._model_path` constructs filesystem paths directly
    from caller-supplied names (BL-054).

    Raises
    ------
    ValueError
        If the name contains disallowed characters, starts with a non-
        alphanumeric character, or exceeds the length limit.
    """
    if not _MODEL_NAME_ALLOWLIST_RE.match(name):
        raise ValueError(
            f"Invalid model name {name!r}.  Model names must start with a "
            "letter or digit and may only contain letters, digits, underscores, "
            "and hyphens (max 128 characters)."
        )


def _validate_model_version(version: str) -> None:
    """Raise ``ValueError`` if *version* is not a strict semantic version.

    Accepts ``major.minor.patch`` (e.g. ``"1.2.0"``) only.  Pre-release
    labels (``1.0.0-alpha``) and build metadata (``1.0.0+build``) are
    rejected to keep the filesystem naming convention unambiguous and safe.

    Raises
    ------
    ValueError
        If *version* does not match ``\\d+.\\d+.\\d+``.
    """
    if not _MODEL_VERSION_ALLOWLIST_RE.match(version):
        raise ValueError(
            f"Invalid model version {version!r}.  Versions must be in "
            "strict semantic version format: major.minor.patch (e.g. '1.2.0')."
        )


# ---------------------------------------------------------------------------
# Model integrity helpers (SHA-256 digest files)
# ---------------------------------------------------------------------------


def _digest_path(model_path: Path) -> Path:
    """Return the path to the SHA-256 digest file for *model_path*.

    The digest file is stored alongside the model with the ``.sha256``
    extension replacing ``.joblib``, e.g.::

        cost_model_v1.2.0.joblib  →  cost_model_v1.2.0.sha256
    """
    return model_path.with_suffix(".sha256")


def _compute_file_sha256(path: Path) -> str:
    """Return the lowercase hex SHA-256 digest of the file at *path*."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _verify_model_file(model_path: Path) -> None:
    """Verify the SHA-256 integrity digest of *model_path* before loading.

    Reads the expected hex digest from ``{model_path}.sha256`` (created by
    :meth:`ModelRegistry.save_model`) and compares it against the computed
    digest of the model file using :func:`hmac.compare_digest` to prevent
    timing-oracle attacks.

    Raises
    ------
    ValueError
        If the digest file is absent or the digests do not match.  The
        error message does not reveal the stored or computed hash.
    """
    digest_file = _digest_path(model_path)
    if not digest_file.exists():
        raise ValueError(
            f"Model integrity digest not found at {digest_file}.  "
            "All model files must be saved via ModelRegistry.save_model() "
            "so that a verified digest is generated alongside the model.  "
            "Refusing to deserialise an unverified joblib file."
        )
    expected_hex = digest_file.read_text().strip().lower()
    actual_hex = _compute_file_sha256(model_path)
    if not _hmac.compare_digest(expected_hex, actual_hex):
        raise ValueError(
            f"Model file integrity check FAILED for {model_path}.  "
            "The file contents do not match the stored digest — it may "
            "have been modified or tampered with.  Re-save the model via "
            "ModelRegistry.save_model() to regenerate a valid digest."
        )


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


class ModelRecord(NamedTuple):
    """Metadata for a model that has been loaded into memory."""

    name: str
    version: str
    path: Path
    loaded_at: datetime


class PredictionRecord(TypedDict):
    """One recorded prediction, stored for drift analysis."""

    model_name: str
    version: str
    features: dict[str, Any]
    prediction: Any
    actual: Any | None  # ground-truth value if already known at record time
    recorded_at: str  # ISO-8601 timestamp


# ---------------------------------------------------------------------------
# PSI calculation (module-private)
# ---------------------------------------------------------------------------


def _compute_psi(expected: np.ndarray, actual: np.ndarray, n_bins: int = 10) -> float:
    """Compute Population Stability Index between two distributions.

    Parameters
    ----------
    expected:
        1-D array representing the baseline (reference) distribution.
    actual:
        1-D array representing the recent (current) distribution.
    n_bins:
        Number of quantile-based bins.  Defaults to 10.

    Returns
    -------
    float
        PSI value.  0 indicates identical distributions; values above
        0.2 signal significant distribution shift.
    """
    bins = np.quantile(expected, np.linspace(0, 1, n_bins + 1))
    # Ensure all values fall within a bin by nudging the edges.
    bins[0] -= 1e-8
    bins[-1] += 1e-8

    expected_counts = np.histogram(expected, bins=bins)[0]
    actual_counts = np.histogram(actual, bins=bins)[0]

    # Normalise to proportions; add tiny epsilon to avoid div-by-zero.
    expected_pct = (expected_counts + 1e-6) / (len(expected) + 1e-6 * n_bins)
    actual_pct = (actual_counts + 1e-6) / (len(actual) + 1e-6 * n_bins)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


# ---------------------------------------------------------------------------
# ModelRegistry
# ---------------------------------------------------------------------------


class ModelRegistry:
    """Central registry for versioned ML models.

    Supports:
    - Semantic versioning with ``None`` meaning "latest".
    - Prediction logging for drift detection.
    - Population Stability Index (PSI) drift check.

    Model files must follow the naming convention::

        {name}_v{major}.{minor}.{patch}.joblib

    For example ``cost_model_v1.2.0.joblib``.
    """

    def __init__(self, models_dir: Path | str = "models") -> None:
        """Initialise the registry pointing at *models_dir*.

        The directory is created lazily — missing directories produce no
        error at construction time; a ``FileNotFoundError`` is raised
        only when a ``load_model`` call cannot resolve the requested
        model file.
        """
        self._models_dir = Path(models_dir)
        # (name, version) -> (model_object, loaded_at: float) — BL-103: TTL eviction
        self._cache: dict[tuple[str, str], tuple[Any, float]] = {}
        # (name, version) -> ModelRecord (metadata)
        self._records: dict[tuple[str, str], ModelRecord] = {}
        # model_name -> most-recently-loaded version string
        # Updated on every successful load_model call; used by
        # record_prediction to tag predictions with the correct version.
        self._active_version: dict[str, str] = {}
        # model_name -> deque[PredictionRecord] (ring buffer)
        self._predictions: dict[str, deque[PredictionRecord]] = {}
        # Track which models have already emitted a buffer-full warning so we
        # log it once, not on every single record_prediction() call.
        self._warned_buffer_full: set[str] = set()

        logger.info("ModelRegistry initialised (models_dir=%s)", self._models_dir)

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------

    def load_model(self, name: str, version: str | None = None) -> Any:
        """Load a model by *name* and optional *version*.

        Parameters
        ----------
        name:
            Logical model name (e.g. ``"cost_model"``).
        version:
            Semantic version string ``"major.minor.patch"`` or ``None``
            to select the highest available version automatically
            (``"latest"`` alias).

        Returns
        -------
        Any
            The deserialised model object (typically a scikit-learn
            estimator loaded via ``joblib``).

        Raises
        ------
        FileNotFoundError
            If no matching model file can be found in ``models_dir``.
        """
        # Validate caller-supplied name and version before constructing any
        # filesystem path (BL-054: prevent path-traversal via crafted names).
        _validate_model_name(name)
        if version is not None:
            _validate_model_version(version)

        resolved_version = version if version is not None else self._resolve_latest(name)
        cache_key = (name, resolved_version)

        # BL-103: check cache with TTL eviction.
        if cache_key in self._cache:
            cached_model, loaded_at = self._cache[cache_key]
            if time.monotonic() - loaded_at <= _CACHE_TTL_SECONDS:
                logger.debug("Cache hit for model %s v%s", name, resolved_version)
                return cached_model
            # TTL expired — evict and reload.
            logger.debug(
                "Cache TTL expired for model %s v%s — reloading from disk",
                name,
                resolved_version,
            )
            del self._cache[cache_key]

        model_path = self._model_path(name, resolved_version)
        if not model_path.exists():
            raise FileNotFoundError(
                f"Model '{name}' version '{resolved_version}' not found at {model_path}"
            )

        # Verify SHA-256 digest before deserialising.  joblib uses pickle
        # internally, so loading a crafted file can execute arbitrary code.
        # The digest check ensures only files written by save_model() are loaded.
        _verify_model_file(model_path)

        logger.info("Loading model %s v%s from %s", name, resolved_version, model_path)
        model = joblib.load(model_path)

        self._cache[cache_key] = (model, time.monotonic())
        self._records[cache_key] = ModelRecord(
            name=name,
            version=resolved_version,
            path=model_path,
            loaded_at=datetime.now(tz=UTC),
        )
        # Track the most-recently-loaded version so record_prediction can
        # tag predictions correctly even when multiple versions are cached.
        self._active_version[name] = resolved_version
        logger.info("Model %s v%s loaded and cached", name, resolved_version)
        return model

    # ------------------------------------------------------------------
    # Prediction recording
    # ------------------------------------------------------------------

    def record_prediction(
        self,
        model_name: str,
        features: dict[str, Any],
        prediction: Any,
        actual: Any | None = None,
    ) -> None:
        """Record a prediction for drift tracking.

        Parameters
        ----------
        model_name:
            The logical model name (e.g. ``"cost_model"``).
        features:
            Feature dict used to produce the prediction.
        prediction:
            The model's output value.
        actual:
            Ground-truth value if already known; can be filled in later
            for offline accuracy tracking.

        Keeps the last ``10_000`` records per model (ring buffer).
        This method is synchronous and GIL-safe — safe to call from
        asyncio code without extra locking.
        """
        if model_name not in self._predictions:
            self._predictions[model_name] = deque(maxlen=_MAX_PREDICTION_RECORDS)

        # Resolve which version is currently cached for this model name.
        version = self._current_version(model_name)

        record: PredictionRecord = {
            "model_name": model_name,
            "version": version,
            "features": features,
            "prediction": prediction,
            "actual": actual,
            "recorded_at": datetime.now(tz=UTC).isoformat(),
        }
        # BL-091: Warn *once* when the ring buffer first fills up (not on every append).
        buf = self._predictions[model_name]
        if len(buf) == buf.maxlen and model_name not in self._warned_buffer_full:
            self._warned_buffer_full.add(model_name)
            logger.warning(
                "Prediction ring buffer full for model %s — oldest entries will be "
                "dropped on subsequent appends. Consider increasing "
                "_MAX_PREDICTION_RECORDS.",
                model_name,
            )
        buf.append(record)

    # ------------------------------------------------------------------
    # Drift detection
    # ------------------------------------------------------------------

    def drift_check(self, model_name: str) -> dict[str, Any]:
        """Run a Population Stability Index (PSI) drift check.

        Compares the distribution of the most recent 500 predictions
        against the earliest 500 recorded predictions (baseline window).

        PSI thresholds
        --------------
        - ``< 0.1``   → ``"stable"``
        - ``0.1–0.2`` → ``"warning"``
        - ``> 0.2``   → ``"drift"``

        Parameters
        ----------
        model_name:
            Logical model name to check.

        Returns
        -------
        dict[str, Any]
            A dict with keys:
            - ``"model"`` – model name
            - ``"status"`` – ``"stable"`` | ``"warning"`` | ``"drift"`` |
              ``"insufficient_data"``
            - ``"psi"`` – computed PSI (omitted when ``insufficient_data``)
            - ``"sample_size"`` – total recorded predictions
            - ``"message"`` – human-readable description
        """
        records = self._predictions.get(model_name)
        total = len(records) if records else 0

        if total < _PSI_MIN_RECORDS:
            return {
                "model": model_name,
                "status": "insufficient_data",
                "sample_size": total,
                "message": (
                    f"Need at least {_PSI_MIN_RECORDS} predictions for drift analysis; "
                    f"have {total}."
                ),
            }

        # BL-090: avoid materialising the full 10 000-entry deque into a list.
        # Baseline: first _PSI_WINDOW entries — use islice to read only what we need.
        baseline_window = list(itertools.islice(records, _PSI_WINDOW))
        # Recent: last _PSI_WINDOW entries — direct deque index access, then reverse.
        recent_size = min(_PSI_WINDOW, len(records))
        recent_window = [records[-(i + 1)] for i in range(recent_size)][::-1]

        baseline_values = self._extract_numeric_predictions(baseline_window)
        recent_values = self._extract_numeric_predictions(recent_window)

        if len(baseline_values) == 0 or len(recent_values) == 0:
            return {
                "model": model_name,
                "status": "insufficient_data",
                "sample_size": total,
                "message": "Predictions are non-numeric; PSI cannot be computed.",
            }

        psi = _compute_psi(
            np.array(baseline_values, dtype=np.float64),
            np.array(recent_values, dtype=np.float64),
        )

        if psi < _PSI_WARN_THRESHOLD:
            status = "stable"
            message = f"Distribution is stable (PSI={psi:.4f})."
        elif psi < _PSI_DRIFT_THRESHOLD:
            status = "warning"
            message = f"Moderate distribution shift detected (PSI={psi:.4f}); monitor closely."
        else:
            status = "drift"
            message = f"Significant distribution drift detected (PSI={psi:.4f}); consider retraining."

        return {
            "model": model_name,
            "status": status,
            "psi": psi,
            "sample_size": total,
            "message": message,
        }

    # ------------------------------------------------------------------
    # Version listing
    # ------------------------------------------------------------------

    def list_versions(self, name: str) -> list[str]:
        """Return all available versions for *name*, sorted ascending.

        Scans ``models_dir`` for files matching the naming convention.

        Parameters
        ----------
        name:
            Logical model name.

        Returns
        -------
        list[str]
            Version strings (``"major.minor.patch"``) in ascending order.
        """
        return self._scan_versions(name)

    # ------------------------------------------------------------------
    # Loaded models
    # ------------------------------------------------------------------

    def loaded_models(self) -> list[ModelRecord]:
        """Return metadata for all currently cached models."""
        return list(self._records.values())

    def reload(self, name: str) -> None:
        """Evict all cached versions of *name* so the next call to
        :meth:`load_model` re-deserialises from disk.

        Useful when a model file has been updated in-place without a
        version bump (e.g. during canary retraining).  Does nothing if
        the model is not currently cached.

        Parameters
        ----------
        name:
            Logical model name (e.g. ``"cost_model"``).
        """
        keys_to_drop = [k for k in self._cache if k[0] == name]
        for key in keys_to_drop:
            del self._cache[key]
            self._records.pop(key, None)
        # Clear tracked active version so record_prediction() does not tag
        # new predictions with the stale pre-reload version string.
        self._active_version.pop(name, None)
        # Allow the buffer-full warning to fire again after a reload.
        self._warned_buffer_full.discard(name)
        if keys_to_drop:
            logger.info(
                "Evicted %d cached version(s) of model '%s' (reload requested)",
                len(keys_to_drop),
                name,
            )

    # ------------------------------------------------------------------
    # Saving models (always use this — writes the digest alongside)
    # ------------------------------------------------------------------

    def save_model(self, model: Any, name: str, version: str) -> Path:
        """Persist *model* to disk and write an SHA-256 integrity digest.

        Uses the ``{name}_v{version}.joblib`` naming convention.  Both the
        model file and its companion ``.sha256`` digest file are written to
        ``models_dir``.  The digest is written *after* the model file so that
        a partial write leaves no valid digest, making the inconsistency
        detectable by :meth:`load_model`.

        Parameters
        ----------
        model:
            Any joblib-serialisable object (e.g. a scikit-learn estimator).
        name:
            Logical model name (e.g. ``"cost_model"``).
        version:
            Semantic version string ``"major.minor.patch"`` (e.g. ``"1.2.0"``).

        Returns
        -------
        Path
            Absolute path to the saved ``.joblib`` file.
        """
        # Validate caller-supplied name and version before constructing any
        # filesystem path (BL-054: prevent path-traversal via crafted names).
        _validate_model_name(name)
        _validate_model_version(version)

        self._models_dir.mkdir(parents=True, exist_ok=True)
        model_path = self._model_path(name, version)
        joblib.dump(model, model_path)
        digest = _compute_file_sha256(model_path)
        _digest_path(model_path).write_text(digest)
        logger.info(
            "Saved model %s v%s to %s (SHA-256 digest written)",
            name,
            version,
            model_path,
        )
        return model_path

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _model_path(self, name: str, version: str) -> Path:
        """Build the expected filesystem path for a model file."""
        return self._models_dir / f"{name}_v{version}.joblib"

    def _scan_versions(self, name: str) -> list[str]:
        """Scan ``models_dir`` and return sorted version strings for *name*."""
        if not self._models_dir.exists():
            return []

        versions: list[tuple[int, int, int]] = []
        for entry in self._models_dir.iterdir():
            if not entry.is_file():
                continue
            match = _MODEL_FILENAME_RE.match(entry.name)
            if match and match.group("name") == name:
                raw = match.group("version")
                try:
                    parts = tuple(int(x) for x in raw.split("."))
                    if len(parts) == 3:
                        versions.append(parts)  # type: ignore[arg-type]
                except ValueError:
                    continue

        versions.sort()
        return [f"{major}.{minor}.{patch}" for major, minor, patch in versions]

    def _resolve_latest(self, name: str) -> str:
        """Return the latest (highest) version string for *name*.

        Raises
        ------
        FileNotFoundError
            If no versions are found in ``models_dir``.
        """
        available = self._scan_versions(name)
        if not available:
            raise FileNotFoundError(
                f"No versions of model '{name}' found in {self._models_dir}"
            )
        return available[-1]

    def _current_version(self, model_name: str) -> str:
        """Return the most-recently-loaded version for *model_name*, or ``'unknown'``.

        Uses the ``_active_version`` map which is updated on every successful
        ``load_model`` call, ensuring the correct version is returned even when
        multiple versions of the same model are in the cache.
        """
        return self._active_version.get(model_name, "unknown")

    @staticmethod
    def _extract_numeric_predictions(records: list[PredictionRecord]) -> list[float]:
        """Extract numeric prediction values from a list of records."""
        values: list[float] = []
        for rec in records:
            pred = rec["prediction"]
            try:
                values.append(float(pred))
            except (TypeError, ValueError):
                pass
        return values
