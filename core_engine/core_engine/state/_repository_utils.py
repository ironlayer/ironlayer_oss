"""Shared helpers for repository modules (escape, dialect-aware upsert)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession


def _escape_like(value: str) -> str:
    """Escape SQL LIKE metacharacters so they are treated as literal characters.

    Handles the backslash escape character itself first, then the ``%`` and
    ``_`` wildcards.  The escaped string is safe to interpolate into a LIKE
    pattern that uses ``\\`` as its escape character (the SQL default).
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def _dialect_upsert(
    session: AsyncSession,
    table: Any,
    values: dict[str, Any],
    index_elements: list[str],
    update_columns: list[str],
) -> Any:
    """Dialect-aware upsert: PostgreSQL ``ON CONFLICT DO UPDATE`` or SQLite equivalent."""
    bind = session.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", "")

    stmt: Any
    if "postgresql" in str(dialect_name):
        from sqlalchemy.dialects.postgresql import insert as _pg_insert

        stmt = _pg_insert(table).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )
    else:
        from sqlalchemy.dialects.sqlite import insert as _sqlite_insert

        stmt = _sqlite_insert(table).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={col: values[col] for col in update_columns},
        )
    return await session.execute(stmt)


async def _dialect_upsert_nothing(
    session: AsyncSession,
    table: Any,
    values: dict[str, Any],
    index_elements: list[str] | None = None,
    constraint: str | None = None,
) -> Any:
    """Dialect-aware insert with ``ON CONFLICT DO NOTHING``."""
    bind = session.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", "")

    conflict_kwargs: dict[str, Any] = {}
    if constraint is not None:
        conflict_kwargs["constraint"] = constraint
    elif index_elements is not None:
        conflict_kwargs["index_elements"] = index_elements

    stmt: Any
    if "postgresql" in str(dialect_name):
        from sqlalchemy.dialects.postgresql import insert as _pg_insert

        stmt = _pg_insert(table).values(**values)
        stmt = stmt.on_conflict_do_nothing(**conflict_kwargs)
    else:
        from sqlalchemy.dialects.sqlite import insert as _sqlite_insert

        stmt = _sqlite_insert(table).values(**values)
        sqlite_kwargs: dict[str, Any] = {}
        if index_elements is not None:
            sqlite_kwargs["index_elements"] = index_elements
        stmt = stmt.on_conflict_do_nothing(**sqlite_kwargs)
    return await session.execute(stmt)
