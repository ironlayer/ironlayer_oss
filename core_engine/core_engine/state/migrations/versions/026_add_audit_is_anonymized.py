"""Add is_anonymized flag to audit_log.

Marks rows that have undergone GDPR right-to-erasure processing so that
hash-chain verification (``verify_chain``) can skip the hash recomputation
for these entries — the stored ``entry_hash`` (computed from the original
data before erasure) is used as-is to advance the chain link, keeping all
surrounding non-anonymized entries verifiable.

Without this flag, ``verify_chain`` would permanently return ``False`` for
any tenant that had GDPR anonymization applied, making it impossible to
distinguish deliberate tampering from legitimate erasure.

Revision ID: 026
Revises: 025
Create Date: 2026-03-07 00:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "026"
down_revision: str | None = "025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "audit_log",
        sa.Column(
            "is_anonymized",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("audit_log", "is_anonymized")
