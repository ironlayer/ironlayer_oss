"""Drop redundant indexes on the usage_events table (BL-101).

The ``usage_events`` table was created in migration 010 with two indexes:

* ``ix_usage_events_tenant_type_created`` — ``(tenant_id, event_type, created_at)``
* ``ix_usage_events_tenant_created``       — ``(tenant_id, created_at)``

Migration 022 then added a *third* index:

* ``ix_usage_events_tenant_type_month``    — ``(tenant_id, event_type, created_at)``

This third index is byte-for-byte identical to the first — same columns,
same order — offering zero additional query coverage at double the write
and storage cost.

The second index, ``ix_usage_events_tenant_created``, covers only
``(tenant_id, created_at)``.  PostgreSQL can satisfy any query that
filters on ``(tenant_id, created_at)`` by using a partial scan of the
leading columns of the composite index, making the smaller index
superfluous.

This migration drops the two redundant indexes.  The surviving composite
index ``ix_usage_events_tenant_type_created`` continues to serve all
queries efficiently.

Revision ID: 029
Revises: 028
Create Date: 2026-03-07 00:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "029"
down_revision: str | None = "028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop the duplicate index (identical columns to ix_usage_events_tenant_type_created).
    op.drop_index("ix_usage_events_tenant_type_month", table_name="usage_events")

    # Drop the subset index (leading-column subset of the composite index above).
    op.drop_index("ix_usage_events_tenant_created", table_name="usage_events")


def downgrade() -> None:
    # Restore the subset index.
    op.create_index(
        "ix_usage_events_tenant_created",
        "usage_events",
        ["tenant_id", "created_at"],
    )

    # Restore the duplicate index.
    op.create_index(
        "ix_usage_events_tenant_type_month",
        "usage_events",
        ["tenant_id", "event_type", "created_at"],
    )
