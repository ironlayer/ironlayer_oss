"""Built-in check that wraps the existing schema contract validator.

Validates model output columns against declared schema contracts to
detect COLUMN_REMOVED, TYPE_CHANGED, NULLABLE_TIGHTENED, and
COLUMN_ADDED violations.
"""

from __future__ import annotations

import logging

from core_engine.checks.base import BaseCheck
from core_engine.checks.models import (
    CheckContext,
    CheckResult,
    CheckSeverity,
    CheckStatus,
    CheckType,
    Timer,
)
from core_engine.contracts.schema_validator import (
    ViolationSeverity,
    validate_schema_contract,
)
from core_engine.models.model_definition import SchemaContractMode

logger = logging.getLogger(__name__)

# Map contract violation severity to check severity.
_VIOLATION_SEVERITY_MAP: dict[ViolationSeverity, CheckSeverity] = {
    ViolationSeverity.BREAKING: CheckSeverity.CRITICAL,
    ViolationSeverity.WARNING: CheckSeverity.MEDIUM,
    ViolationSeverity.INFO: CheckSeverity.LOW,
}


class SchemaContractCheck(BaseCheck):
    """Validate model output columns against declared schema contracts.

    Wraps :func:`validate_schema_contract` to produce :class:`CheckResult`
    objects that integrate with the unified check engine.
    """

    @property
    def check_type(self) -> CheckType:
        return CheckType.SCHEMA_CONTRACT

    async def execute(self, context: CheckContext) -> list[CheckResult]:
        """Run schema contract validation for all models in the context.

        Only models with contracts enabled (WARN or STRICT mode) are
        checked.  Models with DISABLED contracts produce a SKIP result.
        """
        results: list[CheckResult] = []

        models = context.models
        if context.model_names is not None:
            model_name_set = set(context.model_names)
            models = [m for m in models if m.name in model_name_set]

        for model in sorted(models, key=lambda m: m.name):
            if model.contract_mode == SchemaContractMode.DISABLED:
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.SKIP,
                        severity=CheckSeverity.LOW,
                        message="Schema contracts disabled for this model.",
                    )
                )
                continue

            if not model.contract_columns:
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.SKIP,
                        severity=CheckSeverity.LOW,
                        message="No contract columns defined for this model.",
                    )
                )
                continue

            timer = Timer()
            timer.start()

            try:
                validation = validate_schema_contract(model)
            except Exception as exc:
                elapsed = timer.elapsed_ms()
                logger.warning(
                    "Schema contract validation failed for %s: %s",
                    model.name,
                    exc,
                )
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.ERROR,
                        severity=CheckSeverity.HIGH,
                        message=f"Contract validation error: {exc}",
                        duration_ms=elapsed,
                    )
                )
                continue

            elapsed = timer.elapsed_ms()

            if not validation.violations:
                results.append(
                    CheckResult(
                        check_type=self.check_type,
                        model_name=model.name,
                        status=CheckStatus.PASS,
                        severity=CheckSeverity.LOW,
                        message=f"Schema contract validated successfully ({len(model.contract_columns)} columns).",
                        duration_ms=elapsed,
                    )
                )
            else:
                for violation in validation.violations:
                    severity = _VIOLATION_SEVERITY_MAP.get(violation.severity, CheckSeverity.MEDIUM)

                    # In WARN mode, downgrade CRITICAL to MEDIUM (non-blocking).
                    if model.contract_mode == SchemaContractMode.WARN and severity == CheckSeverity.CRITICAL:
                        status = CheckStatus.WARN
                        severity = CheckSeverity.MEDIUM
                    elif violation.severity == ViolationSeverity.INFO:
                        status = CheckStatus.WARN
                    else:
                        status = CheckStatus.FAIL

                    results.append(
                        CheckResult(
                            check_type=self.check_type,
                            model_name=model.name,
                            status=status,
                            severity=severity,
                            message=violation.message,
                            detail=f"{violation.violation_type}: {violation.column_name}",
                            duration_ms=elapsed,
                        )
                    )

        # Sort deterministically.
        results.sort(key=lambda r: (r.model_name, r.detail, r.status.value))
        return results
