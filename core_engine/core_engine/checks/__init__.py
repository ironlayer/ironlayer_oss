"""IronLayer Check Engine -- unified quality and validation framework.

Provides a single interface for running all data quality checks:
model tests, schema contracts, schema drift, reconciliation, and
custom validations.

Quick start::

    from core_engine.checks import create_default_engine, CheckContext

    engine = create_default_engine()
    context = CheckContext(models=my_models)
    summary = await engine.run(context)
    print(summary.total, summary.passed, summary.failed)
"""

from core_engine.checks.base import BaseCheck
from core_engine.checks.engine import CheckEngine, create_default_engine
from core_engine.checks.models import (
    CheckContext,
    CheckResult,
    CheckSeverity,
    CheckStatus,
    CheckSummary,
    CheckType,
)
from core_engine.checks.registry import CheckRegistry

__all__ = [
    "BaseCheck",
    "CheckContext",
    "CheckEngine",
    "CheckRegistry",
    "CheckResult",
    "CheckSeverity",
    "CheckStatus",
    "CheckSummary",
    "CheckType",
    "create_default_engine",
]
