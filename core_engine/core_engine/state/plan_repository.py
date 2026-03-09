"""PlanRepository — CRUD for the plans table."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import cast, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core_engine.state.tables import PlanTable


class PlanRepository:
    """CRUD operations for the ``plans`` table."""

    def __init__(self, session: AsyncSession, tenant_id: str = "default") -> None:
        self._session = session
        self._tenant_id = tenant_id

    async def save_plan(
        self,
        plan_id: str,
        base_sha: str,
        target_sha: str,
        plan_json: str,
    ) -> PlanTable:
        """Persist a new execution plan."""
        row = PlanTable(
            plan_id=plan_id,
            tenant_id=self._tenant_id,
            base_sha=base_sha,
            target_sha=target_sha,
            plan_json=plan_json,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get_plan(self, plan_id: str) -> PlanTable | None:
        """Fetch a plan by its identifier."""
        stmt = select(PlanTable).where(
            PlanTable.tenant_id == self._tenant_id,
            PlanTable.plan_id == plan_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def add_approval(self, plan_id: str, user: str, comment: str) -> None:
        """Append an approval entry to the plan's ``approvals_json`` array."""
        now = datetime.now(UTC)
        new_entry = json.dumps({"user": user, "at": now.isoformat(), "comment": comment})

        bind = self._session.get_bind()
        dialect_name = getattr(getattr(bind, "dialect", None), "name", "")

        if "postgresql" in str(dialect_name):
            from sqlalchemy.dialects.postgresql import JSONB

            stmt = (
                update(PlanTable)
                .where(
                    PlanTable.tenant_id == self._tenant_id,
                    PlanTable.plan_id == plan_id,
                )
                .values(
                    approvals_json=func.coalesce(
                        cast(PlanTable.approvals_json, JSONB),
                        cast("[]", JSONB),
                    ).concat(cast(f"[{new_entry}]", JSONB))
                )
            )
            result = await self._session.execute(stmt)
            if result.rowcount == 0:  # type: ignore[attr-defined]
                raise ValueError(f"Plan {plan_id} not found")
            await self._session.flush()
        else:
            async with self._session.begin_nested():
                plan = await self.get_plan(plan_id)
                if plan is None:
                    raise ValueError(f"Plan {plan_id} not found")

                approvals: list[dict[str, str]] = (
                    json.loads(plan.approvals_json) if plan.approvals_json else []  # type: ignore[arg-type]
                )
                approvals.append(
                    {
                        "user": user,
                        "comment": comment,
                        "at": now.isoformat(),
                    }
                )

                stmt = (
                    update(PlanTable)
                    .where(
                        PlanTable.tenant_id == self._tenant_id,
                        PlanTable.plan_id == plan_id,
                    )
                    .values(approvals_json=json.dumps(approvals))
                )
                await self._session.execute(stmt)
            await self._session.flush()

    async def list_recent(self, limit: int = 20, offset: int = 0) -> list[PlanTable]:
        """Return the most recently created plans with SQL-level pagination.

        Parameters
        ----------
        limit:
            Maximum number of rows to return.
        offset:
            Number of rows to skip before returning results (applied at SQL
            level to avoid O(offset) memory overhead).
        """
        stmt = (
            select(PlanTable)
            .where(PlanTable.tenant_id == self._tenant_id)
            .order_by(PlanTable.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_after_cursor(
        self,
        cursor_ts: str,
        cursor_id: str,
        limit: int = 20,
    ) -> list[PlanTable]:
        """BL-120: Keyset-paginated list using a (created_at, plan_id) cursor.

        Returns rows where ``created_at < cursor_ts``, or where
        ``created_at == cursor_ts AND plan_id < cursor_id`` (tie-breaking),
        ordered by ``(created_at DESC, plan_id DESC)``.

        This predicate is O(1) when the index covers ``(tenant_id, created_at,
        plan_id)`` — unlike offset queries which are O(offset).

        Parameters
        ----------
        cursor_ts:
            ISO 8601 ``created_at`` of the last row seen (decoded from cursor).
        cursor_id:
            ``plan_id`` of the last row seen (decoded from cursor, for tie-breaking).
        limit:
            Maximum number of rows to return.
        """
        stmt = (
            select(PlanTable)
            .where(PlanTable.tenant_id == self._tenant_id)
            .where(
                or_(
                    PlanTable.created_at < cursor_ts,
                    (PlanTable.created_at == cursor_ts) & (PlanTable.plan_id < cursor_id),
                )
            )
            .order_by(PlanTable.created_at.desc(), PlanTable.plan_id.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
