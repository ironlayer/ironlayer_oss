"""Add retention_days column to tenant_config.

Stores the per-tenant audit log retention window in days.  Defaults to
365 days (one year).  A scheduled cleanup job uses this value to purge
audit_log entries older than ``retention_days`` for the tenant.  The
GDPR right-to-erasure anonymization feature also uses this table to
scope per-tenant data lifetime.

Revision ID: 025
Revises: 024
Create Date: 2026-03-07 00:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "025"
down_revision: str | None = "024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "tenant_config",
        sa.Column(
            "retention_days",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("365"),
        ),
    )


def downgrade() -> None:
    op.drop_column("tenant_config", "retention_days")
