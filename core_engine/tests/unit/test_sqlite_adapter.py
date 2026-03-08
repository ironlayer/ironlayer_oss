"""Tests for the SQLite adapter used in local dev mode."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from core_engine.state.sqlite_adapter import (
    create_local_tables,
    get_local_engine,
    get_local_session,
    set_local_tenant_context,
)

# ---------------------------------------------------------------------------
# Engine creation
# ---------------------------------------------------------------------------


class TestGetLocalEngine:
    """Verify SQLite engine creation."""

    def test_creates_in_memory_engine(self) -> None:
        engine = get_local_engine(":memory:")
        assert engine is not None
        assert "sqlite" in str(engine.url)

    def test_creates_file_engine(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        engine = get_local_engine(db_path)
        assert engine is not None
        assert "sqlite" in str(engine.url)

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        db_path = tmp_path / "nested" / "deep" / "test.db"
        engine = get_local_engine(db_path)
        assert engine is not None
        assert db_path.parent.exists()

    def test_engine_url_contains_path(self, tmp_path: Path) -> None:
        db_path = tmp_path / "my.db"
        engine = get_local_engine(db_path)
        assert "my.db" in str(engine.url)


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


class TestCreateLocalTables:
    """Verify that ORM tables can be created in SQLite."""

    @pytest.mark.asyncio
    async def test_creates_tables_in_memory(self) -> None:
        engine = get_local_engine(":memory:")
        await create_local_tables(engine)
        # Should not raise -- tables created successfully.
        await engine.dispose()

    @pytest.mark.asyncio
    async def test_creates_tables_on_disk(self, tmp_path: Path) -> None:
        db_path = tmp_path / "tables.db"
        engine = get_local_engine(db_path)
        await create_local_tables(engine)
        assert db_path.exists()
        await engine.dispose()

    @pytest.mark.asyncio
    async def test_idempotent_creation(self, tmp_path: Path) -> None:
        """Calling create_local_tables twice should be safe."""
        db_path = tmp_path / "idem.db"
        engine = get_local_engine(db_path)
        await create_local_tables(engine)
        await create_local_tables(engine)  # Should not raise.
        await engine.dispose()


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


class TestGetLocalSession:
    """Verify session lifecycle with SQLite."""

    @pytest.mark.asyncio
    async def test_session_commit_on_success(self) -> None:
        engine = get_local_engine(":memory:")
        await create_local_tables(engine)

        async with get_local_session(engine) as session:
            # Session should be usable.
            from sqlalchemy import text

            result = await session.execute(text("SELECT 1"))
            assert result.scalar() == 1

        await engine.dispose()

    @pytest.mark.asyncio
    async def test_session_rollback_on_error(self) -> None:
        engine = get_local_engine(":memory:")
        await create_local_tables(engine)

        with pytest.raises(ValueError, match="test error"):
            async with get_local_session(engine) as session:
                raise ValueError("test error")

        await engine.dispose()


# ---------------------------------------------------------------------------
# Tenant context (no-op for SQLite)
# ---------------------------------------------------------------------------


class TestSetLocalTenantContext:
    """Verify tenant context is a no-op for SQLite."""

    @pytest.mark.asyncio
    async def test_noop_does_not_raise(self) -> None:
        engine = get_local_engine(":memory:")
        await create_local_tables(engine)

        async with get_local_session(engine) as session:
            # Should not raise -- it's a no-op.
            await set_local_tenant_context(session, "tenant-123")

        await engine.dispose()

    @pytest.mark.asyncio
    async def test_noop_with_empty_tenant(self) -> None:
        engine = get_local_engine(":memory:")
        await create_local_tables(engine)

        async with get_local_session(engine) as session:
            await set_local_tenant_context(session, "")

        await engine.dispose()


# ---------------------------------------------------------------------------
# Database.get_engine dispatch
# ---------------------------------------------------------------------------


class TestDatabaseDispatch:
    """Verify that database.get_engine dispatches correctly."""

    def test_sqlite_url_dispatches_to_local_engine(self) -> None:
        from core_engine.state.database import get_engine

        engine = get_engine("sqlite+aiosqlite:///:memory:")
        assert "sqlite" in str(engine.url)

    def test_sqlite_file_url_dispatches(self, tmp_path: Path) -> None:
        from core_engine.state.database import get_engine

        db_path = tmp_path / "dispatch.db"
        engine = get_engine(f"sqlite+aiosqlite:///{db_path}")
        assert "sqlite" in str(engine.url)


# ---------------------------------------------------------------------------
# Tenant context dispatch in database.py
# ---------------------------------------------------------------------------


class TestTenantContextDispatch:
    """Verify set_tenant_context is a no-op for SQLite sessions."""

    @pytest.mark.asyncio
    async def test_sqlite_tenant_context_noop(self) -> None:
        from core_engine.state.database import get_engine, set_tenant_context
        from sqlalchemy.ext.asyncio import async_sessionmaker

        engine = get_engine("sqlite+aiosqlite:///:memory:")
        await create_local_tables(engine)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        session = factory()
        try:
            # Should not raise on SQLite.
            await set_tenant_context(session, "test-tenant")
        finally:
            await session.close()

        await engine.dispose()
