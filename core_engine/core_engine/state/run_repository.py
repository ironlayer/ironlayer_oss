"""RunRepository — CRUD for the runs table."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core_engine.state.tables import RunTable


class RunRepository:
    """CRUD operations for the ``runs`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def create_run(self, run_record: dict[str, Any]) -> RunTable:
        """Create a run row from a dictionary matching ``RunRecord`` fields."""
        row = RunTable(
            run_id=run_record["run_id"],
            tenant_id=self._tenant_id,
            plan_id=run_record["plan_id"],
            step_id=run_record["step_id"],
            model_name=run_record["model_name"],
            status=run_record["status"],
            started_at=run_record.get("started_at"),
            finished_at=run_record.get("finished_at"),
            input_range_start=run_record.get("input_range_start"),
            input_range_end=run_record.get("input_range_end"),
            error_message=run_record.get("error_message"),
            logs_uri=run_record.get("logs_uri"),
            cluster_used=run_record.get("cluster_used"),
            executor_version=run_record["executor_version"],
            retry_count=run_record.get("retry_count", 0),
            cost_usd=run_record.get("cost_usd"),
            external_run_id=run_record.get("external_run_id"),
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def update_status(
        self,
        run_id: str,
        status: str,
        finished_at: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Transition a run to a new status, optionally recording completion details."""
        values: dict[str, Any] = {"status": status}
        if finished_at is not None:
            values["finished_at"] = finished_at
        if error_message is not None:
            values["error_message"] = error_message
        stmt = (
            update(RunTable)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.run_id == run_id,
            )
            .values(**values)
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def get_by_plan(self, plan_id: str) -> list[RunTable]:
        """Return all runs belonging to a plan, ordered by start time."""
        stmt = (
            select(RunTable)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.plan_id == plan_id,
            )
            .order_by(RunTable.started_at.asc().nulls_last())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, run_id: str) -> RunTable | None:
        """Fetch a single run by its identifier."""
        stmt = select(RunTable).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.run_id == run_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_historical_stats(self, model_name: str) -> dict[str, Any]:
        """Compute aggregate statistics from the last 30 completed runs of a model."""
        sub = (
            select(RunTable)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name == model_name,
                RunTable.status == "COMPLETED",
                RunTable.finished_at.is_not(None),
                RunTable.started_at.is_not(None),
            )
            .order_by(RunTable.finished_at.desc())
            .limit(30)
            .subquery()
        )

        count_stmt = select(func.count()).select_from(sub)
        count_result = await self._session.execute(count_stmt)
        run_count: int = count_result.scalar_one()

        if run_count == 0:
            return {
                "avg_runtime_seconds": None,
                "avg_cost_usd": None,
                "run_count": 0,
            }

        avg_stmt = select(
            func.avg(func.extract("epoch", sub.c.finished_at) - func.extract("epoch", sub.c.started_at)).label(
                "avg_runtime"
            ),
            func.avg(sub.c.cost_usd).label("avg_cost"),
        ).select_from(sub)
        avg_result = await self._session.execute(avg_stmt)
        avg_row = avg_result.one()

        return {
            "avg_runtime_seconds": float(avg_row.avg_runtime) if avg_row.avg_runtime is not None else None,
            "avg_cost_usd": float(avg_row.avg_cost) if avg_row.avg_cost is not None else None,
            "run_count": run_count,
        }

    async def update_cost(self, run_id: str, cost_usd: float) -> None:
        """Set the computed cost for a completed run."""
        stmt = (
            update(RunTable)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.run_id == run_id,
            )
            .values(cost_usd=cost_usd)
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def get_recent_runs(
        self,
        model_name: str,
        limit: int = 50,
    ) -> list[RunTable]:
        """Return the most recent runs for a model, newest first."""
        stmt = (
            select(RunTable)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name == model_name,
            )
            .order_by(RunTable.started_at.desc().nulls_last())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_model_run_summary(
        self,
        model_name: str,
    ) -> dict[str, Any]:
        """Compute run summary statistics for failure prediction."""
        now = datetime.now(UTC)
        seven_days_ago = now - timedelta(days=7)

        total_stmt = select(
            func.count().label("total"),
            func.count().filter(RunTable.status == "FAILED").label("failed"),
        ).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
        )
        total_result = await self._session.execute(total_stmt)
        total_row = total_result.one()
        total_runs = total_row.total or 0
        failed_runs = total_row.failed or 0

        recent_stmt = select(
            func.count().label("total"),
            func.count().filter(RunTable.status == "FAILED").label("failed"),
        ).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
            RunTable.started_at >= seven_days_ago,
        )
        recent_result = await self._session.execute(recent_stmt)
        recent_row = recent_result.one()
        recent_runs = recent_row.total or 0
        recent_failures = recent_row.failed or 0

        recent_ordered = await self.get_recent_runs(model_name, limit=20)
        consecutive = 0
        for run in recent_ordered:
            if run.status == "FAILED":
                consecutive += 1
            else:
                break

        avg_stmt = select(
            func.avg(func.extract("epoch", RunTable.finished_at) - func.extract("epoch", RunTable.started_at))
        ).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
            RunTable.status == "COMPLETED",
            RunTable.started_at.is_not(None),
            RunTable.finished_at.is_not(None),
        )
        avg_result = await self._session.execute(avg_stmt)
        avg_runtime = avg_result.scalar_one() or 0.0

        recent_avg_stmt = select(
            func.avg(func.extract("epoch", RunTable.finished_at) - func.extract("epoch", RunTable.started_at))
        ).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
            RunTable.status == "COMPLETED",
            RunTable.started_at >= seven_days_ago,
            RunTable.started_at.is_not(None),
            RunTable.finished_at.is_not(None),
        )
        recent_avg_result = await self._session.execute(recent_avg_stmt)
        recent_avg_runtime = recent_avg_result.scalar_one() or 0.0

        runtime_trend = 0.0
        if avg_runtime > 0:
            runtime_trend = (recent_avg_runtime - avg_runtime) / avg_runtime

        last_success_stmt = (
            select(RunTable.finished_at)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name == model_name,
                RunTable.status == "COMPLETED",
                RunTable.finished_at.is_not(None),
            )
            .order_by(RunTable.finished_at.desc())
            .limit(1)
        )
        last_success_result = await self._session.execute(last_success_stmt)
        last_success = last_success_result.scalar_one_or_none()
        hours_since = 0.0
        if last_success:
            hours_since = (now - last_success).total_seconds() / 3600

        last_error = None
        if recent_ordered and recent_ordered[0].status == "FAILED":
            last_error = recent_ordered[0].error_message

        cost_stmt = (
            select(RunTable.cost_usd)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name == model_name,
                RunTable.cost_usd.is_not(None),
            )
            .order_by(RunTable.started_at.desc())
            .limit(50)
        )
        cost_result = await self._session.execute(cost_stmt)
        all_costs = [r[0] for r in cost_result.all()]

        recent_cost_stmt = (
            select(RunTable.cost_usd)
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name == model_name,
                RunTable.cost_usd.is_not(None),
                RunTable.started_at >= seven_days_ago,
            )
            .order_by(RunTable.started_at.desc())
        )
        recent_cost_result = await self._session.execute(recent_cost_stmt)
        recent_costs = [r[0] for r in recent_cost_result.all()]

        return {
            "total_runs": total_runs,
            "failed_runs": failed_runs,
            "recent_runs": recent_runs,
            "recent_failures": recent_failures,
            "consecutive_failures": consecutive,
            "avg_runtime_seconds": float(avg_runtime),
            "recent_avg_runtime_seconds": float(recent_avg_runtime),
            "runtime_trend": runtime_trend,
            "hours_since_last_success": hours_since,
            "last_error_type": last_error,
            "recent_costs": recent_costs,
            "historical_costs": all_costs,
        }

    async def count_for_model(self, model_name: str) -> int:
        """Return the total number of runs for a model."""
        stmt = select(func.count()).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one() or 0

    async def count_by_status(self, model_name: str, status: str) -> int:
        """Return the number of runs for a model with a specific status."""
        stmt = select(func.count()).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name == model_name,
            RunTable.status == status,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one() or 0

    async def get_historical_stats_batch(self, model_names: list[str]) -> dict[str, Any]:
        """Return historical stats for multiple models in two queries instead of 2N.

        Uses ``WHERE model_name IN (...)`` aggregated with ``GROUP BY model_name``.
        Returns a mapping of ``{model_name: {avg_runtime_seconds, avg_cost_usd, run_count}}``.
        Models with no completed runs are absent from the result.
        """
        if not model_names:
            return {}

        # Subquery: last 30 completed runs per model, ranked newest-first.
        ranked = (
            select(
                RunTable.model_name,
                RunTable.started_at,
                RunTable.finished_at,
                RunTable.cost_usd,
                func.row_number()
                .over(
                    partition_by=RunTable.model_name,
                    order_by=RunTable.finished_at.desc(),
                )
                .label("rn"),
            )
            .where(
                RunTable.tenant_id == self._tenant_id,
                RunTable.model_name.in_(model_names),
                RunTable.status == "COMPLETED",
                RunTable.finished_at.is_not(None),
                RunTable.started_at.is_not(None),
            )
            .subquery()
        )

        # Only take the most recent 30 runs per model.
        filtered = select(
            ranked.c.model_name,
            ranked.c.started_at,
            ranked.c.finished_at,
            ranked.c.cost_usd,
        ).where(ranked.c.rn <= 30).subquery()

        agg_stmt = select(
            filtered.c.model_name,
            func.count().label("run_count"),
            func.avg(
                func.extract("epoch", filtered.c.finished_at)
                - func.extract("epoch", filtered.c.started_at)
            ).label("avg_runtime"),
            func.avg(filtered.c.cost_usd).label("avg_cost"),
        ).group_by(filtered.c.model_name)

        agg_result = await self._session.execute(agg_stmt)
        output: dict[str, Any] = {}
        for row in agg_result.all():
            output[row.model_name] = {
                "avg_runtime_seconds": float(row.avg_runtime) if row.avg_runtime is not None else None,
                "avg_cost_usd": float(row.avg_cost) if row.avg_cost is not None else None,
                "run_count": row.run_count,
            }
        return output

    async def get_failure_rates_batch(self, model_names: list[str]) -> dict[str, float]:
        """Return the failure rate for each model in a single aggregated query.

        Uses ``WHERE model_name IN (...)`` with ``GROUP BY model_name``.
        Returns a mapping of ``{model_name: failure_rate}`` where ``failure_rate``
        is ``failed_count / total_count``.  Models with zero runs get a rate of 0.0.
        """
        if not model_names:
            return {}

        stmt = select(
            RunTable.model_name,
            func.count().label("total"),
            func.count().filter(RunTable.status == "FAILED").label("failed"),
        ).where(
            RunTable.tenant_id == self._tenant_id,
            RunTable.model_name.in_(model_names),
        ).group_by(RunTable.model_name)

        result = await self._session.execute(stmt)
        rates: dict[str, float] = {}
        for row in result.all():
            total = row.total or 0
            failed = row.failed or 0
            rates[row.model_name] = 0.0 if total == 0 else failed / total
        # Ensure all requested names have an entry (even if no runs exist).
        for name in model_names:
            if name not in rates:
                rates[name] = 0.0
        return rates
