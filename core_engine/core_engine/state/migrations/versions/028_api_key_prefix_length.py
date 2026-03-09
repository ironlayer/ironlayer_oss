"""Widen api_keys.key_prefix column from VARCHAR(8) to VARCHAR(16).

The previous 8-character prefix provided insufficient entropy for
display/identification purposes.  16 characters gives users a longer,
more recognisable fragment when managing multiple keys in the UI while
still revealing nothing about the full secret.

All existing prefix values are already at most 8 characters so the
ALTER is safe without a data migration.

Revision ID: 028
Revises: 027
Create Date: 2026-03-07 00:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "028"
down_revision: str | None = "027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "api_keys",
        "key_prefix",
        existing_type=sa.String(8),
        type_=sa.String(16),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "api_keys",
        "key_prefix",
        existing_type=sa.String(16),
        type_=sa.String(8),
        existing_nullable=False,
    )
