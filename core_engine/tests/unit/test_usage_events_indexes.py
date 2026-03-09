"""Tests for BL-101: redundant index removal on usage_events table.

Verifies that:
- UsageEventTable.__table_args__ contains exactly one index.
- The surviving index covers (tenant_id, event_type, created_at).
- The duplicate index ix_usage_events_tenant_type_month is absent.
- The subset index ix_usage_events_tenant_created is absent.
- Migration 029 exists with correct revision metadata.
"""

from __future__ import annotations

import importlib

import pytest
from core_engine.state.tables import UsageEventTable
from sqlalchemy import Index


class TestUsageEventTableIndexes:
    """Structural tests for UsageEventTable index definitions."""

    @staticmethod
    def _get_table_indexes() -> list[Index]:
        """Extract Index instances from UsageEventTable.__table_args__."""
        args = UsageEventTable.__table_args__
        return [arg for arg in args if isinstance(arg, Index)]

    def test_exactly_one_index(self) -> None:
        """UsageEventTable must have exactly one index after BL-101 cleanup."""
        indexes = self._get_table_indexes()
        assert len(indexes) == 1, (
            f"Expected 1 index on usage_events, found {len(indexes)}: "
            f"{[idx.name for idx in indexes]}"
        )

    def test_surviving_index_name(self) -> None:
        """The surviving index is ix_usage_events_tenant_type_created."""
        indexes = self._get_table_indexes()
        names = {idx.name for idx in indexes}
        assert "ix_usage_events_tenant_type_created" in names

    def test_duplicate_index_removed(self) -> None:
        """ix_usage_events_tenant_type_month (duplicate) is absent."""
        indexes = self._get_table_indexes()
        names = {idx.name for idx in indexes}
        assert "ix_usage_events_tenant_type_month" not in names, (
            "Duplicate index ix_usage_events_tenant_type_month should have been removed by BL-101"
        )

    def test_subset_index_removed(self) -> None:
        """ix_usage_events_tenant_created (subset) is absent."""
        indexes = self._get_table_indexes()
        names = {idx.name for idx in indexes}
        assert "ix_usage_events_tenant_created" not in names, (
            "Subset index ix_usage_events_tenant_created should have been removed by BL-101"
        )

    def test_surviving_index_covers_correct_columns(self) -> None:
        """The surviving index covers (tenant_id, event_type, created_at)."""
        indexes = self._get_table_indexes()
        surviving = next(
            idx for idx in indexes if idx.name == "ix_usage_events_tenant_type_created"
        )
        # SQLAlchemy stores column expressions — extract column names.
        col_names = [col.key for col in surviving.columns]
        assert col_names == ["tenant_id", "event_type", "created_at"], (
            f"Unexpected columns on composite index: {col_names}"
        )


class TestMigration029:
    """Verify that migration 029 exists and has correct metadata."""

    @pytest.fixture()
    def migration_module(self):
        """Import the migration 029 module."""
        return importlib.import_module(
            "core_engine.state.migrations.versions.029_drop_redundant_usage_events_indexes"
        )

    def test_migration_revision(self, migration_module) -> None:
        """Migration 029 has revision ID '029'."""
        assert migration_module.revision == "029"

    def test_migration_down_revision(self, migration_module) -> None:
        """Migration 029 revises migration '028'."""
        assert migration_module.down_revision == "028"

    def test_migration_has_upgrade(self, migration_module) -> None:
        """Migration 029 defines an upgrade() function."""
        assert callable(migration_module.upgrade)

    def test_migration_has_downgrade(self, migration_module) -> None:
        """Migration 029 defines a downgrade() function."""
        assert callable(migration_module.downgrade)
