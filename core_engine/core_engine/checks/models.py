"""Data models for the IronLayer Check Engine.

Defines the core enums and Pydantic models used across all check types:
check categories, result statuses, severity levels, and summary aggregations.
"""

from __future__ import annotations

import time
from enum import Enum

from pydantic import BaseModel, Field

from core_engine.models.model_definition import ModelDefinition


class CheckType(str, Enum):
    """Category of quality check."""

    MODEL_TEST = "MODEL_TEST"
    SCHEMA_CONTRACT = "SCHEMA_CONTRACT"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    RECONCILIATION = "RECONCILIATION"
    DATA_FRESHNESS = "DATA_FRESHNESS"
    CROSS_MODEL = "CROSS_MODEL"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    CUSTOM = "CUSTOM"


class CheckStatus(str, Enum):
    """Outcome of a single check execution."""

    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    ERROR = "ERROR"
    SKIP = "SKIP"


class CheckSeverity(str, Enum):
    """How critical a check failure is."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class CheckResult(BaseModel):
    """The outcome of a single check execution."""

    check_type: CheckType = Field(..., description="Category of check that produced this result.")
    model_name: str = Field(..., description="Canonical model name the check was run against.")
    status: CheckStatus = Field(..., description="Outcome of the check execution.")
    severity: CheckSeverity = Field(
        default=CheckSeverity.MEDIUM,
        description="How critical this result is when status is FAIL or WARN.",
    )
    message: str = Field(default="", description="Human-readable description of the result.")
    detail: str = Field(default="", description="Additional context (e.g. column name, SQL, violation info).")
    duration_ms: int = Field(default=0, description="Execution time in milliseconds.")


class CheckSummary(BaseModel):
    """Aggregated results across multiple check executions."""

    total: int = Field(default=0, description="Total number of checks executed.")
    passed: int = Field(default=0, description="Number of checks that passed.")
    failed: int = Field(default=0, description="Number of checks that failed.")
    warned: int = Field(default=0, description="Number of checks that produced warnings.")
    errored: int = Field(default=0, description="Number of checks that encountered errors.")
    skipped: int = Field(default=0, description="Number of checks that were skipped.")
    blocking_failures: int = Field(
        default=0,
        description="Number of FAIL results with severity CRITICAL or HIGH.",
    )
    results: list[CheckResult] = Field(
        default_factory=list,
        description="All individual check results, sorted deterministically.",
    )
    duration_ms: int = Field(default=0, description="Total execution time in milliseconds.")

    @property
    def has_blocking_failures(self) -> bool:
        """Return True if any results would block a plan apply."""
        return self.blocking_failures > 0

    @staticmethod
    def from_results(results: list[CheckResult], duration_ms: int = 0) -> CheckSummary:
        """Build a summary from a list of check results.

        Results are sorted deterministically by (model_name, check_type, status).
        """
        sorted_results = sorted(
            results,
            key=lambda r: (r.model_name, r.check_type.value, r.status.value),
        )

        passed = sum(1 for r in sorted_results if r.status == CheckStatus.PASS)
        failed = sum(1 for r in sorted_results if r.status == CheckStatus.FAIL)
        warned = sum(1 for r in sorted_results if r.status == CheckStatus.WARN)
        errored = sum(1 for r in sorted_results if r.status == CheckStatus.ERROR)
        skipped = sum(1 for r in sorted_results if r.status == CheckStatus.SKIP)
        blocking = sum(
            1
            for r in sorted_results
            if r.status == CheckStatus.FAIL and r.severity in (CheckSeverity.CRITICAL, CheckSeverity.HIGH)
        )

        return CheckSummary(
            total=len(sorted_results),
            passed=passed,
            failed=failed,
            warned=warned,
            errored=errored,
            skipped=skipped,
            blocking_failures=blocking,
            results=sorted_results,
            duration_ms=duration_ms,
        )


class CheckContext(BaseModel):
    """Context passed to check implementations during execution.

    Contains the models to check and optional configuration.
    """

    models: list[ModelDefinition] = Field(
        default_factory=list,
        description="Model definitions to run checks against.",
    )
    plan_id: str | None = Field(
        default=None,
        description="Optional plan ID to associate check results with.",
    )
    check_types: list[CheckType] | None = Field(
        default=None,
        description="When set, only run checks of these types. None means run all.",
    )
    model_names: list[str] | None = Field(
        default=None,
        description="When set, only run checks for these models. None means run all.",
    )


class Timer:
    """Simple monotonic timer for measuring check execution duration."""

    def __init__(self) -> None:
        self._start: float = 0.0

    def start(self) -> None:
        self._start = time.monotonic()

    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self._start) * 1000)
