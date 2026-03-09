"""Add single-column index on audit_log.tenant_id.

Queries that filter only on ``tenant_id`` — such as GDPR anonymization
(``anonymize_user_entries``) and retention cleanup (``cleanup_old_entries``)
— previously had to use the leftmost prefix of a composite index
(``ix_audit_tenant_created`` or ``ix_audit_tenant_action``), forcing the
planner to evaluate both index columns even when only ``tenant_id`` was
needed.  A dedicated single-column index lets the planner choose a
covering scan for these high-frequency maintenance queries, avoiding a
full sequential scan of the table after a long downtime.

Revision ID: 027
Revises: 026
Create Date: 2026-03-07 00:00:00.000000+00:00

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "027"
down_revision: str | None = "026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("ix_audit_log_tenant_id", "audit_log", ["tenant_id"])


def downgrade() -> None:
    op.drop_index("ix_audit_log_tenant_id", table_name="audit_log")
