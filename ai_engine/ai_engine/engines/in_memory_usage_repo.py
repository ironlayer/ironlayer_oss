"""In-process LLM usage repository for the AI Advisory Engine.

The AI engine is a stateless advisory service with no direct database
access.  This module provides a lightweight in-process usage repository
that satisfies the :class:`~ai_engine.engines.budget_guard.BudgetGuard`
interface without requiring a database connection.

Characteristics
---------------
* Per-process only: usage totals reset on service restart.  This is
  acceptable for a platform-level advisory service -- budget enforcement
  is intended as a runtime safeguard against runaway costs, not a
  durable accounting system (which belongs in the API service with DB).
* Thread/coroutine safe: asyncio is single-threaded, so plain list/dict
  operations are safe without additional locking.
* Memory bounded: records older than ``max_age_days`` are pruned on each
  write to prevent unbounded growth over long-running processes.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_MAX_AGE_DAYS = 31  # keep one full month of records


class InMemoryLLMUsageRepo:
    """Lightweight in-process usage store compatible with BudgetGuard.

    Each record is a plain dict:
    ``{"ts": datetime, "cost": float, "call_type": str, "model_id": str,
       "input_tokens": int, "output_tokens": int, "latency_ms": int,
       "success": bool, "error_type": str | None}``

    Parameters
    ----------
    max_age_days:
        Records older than this many days are pruned on each write.
        Defaults to 31 to cover a full monthly budget window.
    """

    def __init__(self, max_age_days: int = _DEFAULT_MAX_AGE_DAYS) -> None:
        self._max_age_days = max_age_days
        self._records: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # BudgetGuard interface
    # ------------------------------------------------------------------

    async def get_period_cost(self, *, since: datetime) -> float:
        """Return total estimated cost (USD) for records at or after *since*."""
        return sum(r["cost"] for r in self._records if r["ts"] >= since)

    async def record_usage(
        self,
        *,
        call_type: str,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
        estimated_cost_usd: float,
        latency_ms: int,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """Append a usage record and prune stale entries."""
        self._records.append(
            {
                "ts": datetime.now(UTC),
                "cost": estimated_cost_usd,
                "call_type": call_type,
                "model_id": model_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "latency_ms": latency_ms,
                "success": success,
                "error_type": error_type,
            }
        )
        self._prune()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def total_records(self) -> int:
        """Return the number of records currently in memory."""
        return len(self._records)

    def snapshot(self) -> list[dict[str, Any]]:
        """Return a shallow copy of all records (for testing / inspection)."""
        return list(self._records)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _prune(self) -> None:
        """Remove records older than *max_age_days*."""
        cutoff = datetime.now(UTC) - timedelta(days=self._max_age_days)
        before = len(self._records)
        self._records = [r for r in self._records if r["ts"] >= cutoff]
        pruned = before - len(self._records)
        if pruned:
            logger.debug("InMemoryLLMUsageRepo pruned %d stale records", pruned)
