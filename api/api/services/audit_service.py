"""Centralized audit logging service.

Wraps :class:`AuditRepository` with predefined action constants and a
simplified interface for use by API routers and services.  Every
security-relevant operation in the platform should be funnelled through
this service so that the audit trail is consistent and complete.
"""

from __future__ import annotations

import logging

from core_engine.state.repository import AuditRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Action constants
# ---------------------------------------------------------------------------


class AuditAction:
    """Well-known audit action identifiers.

    Using string constants (rather than an enum) keeps the interface simple
    and allows ad-hoc extension without a migration for actions that are
    only relevant to logs, not queries.
    """

    PLAN_CREATED = "PLAN_CREATED"
    PLAN_APPROVED = "PLAN_APPROVED"
    PLAN_REJECTED = "PLAN_REJECTED"
    PLAN_AUTO_APPROVED = "PLAN_AUTO_APPROVED"
    PLAN_APPLIED = "PLAN_APPLIED"
    RUN_STARTED = "RUN_STARTED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    BACKFILL_REQUESTED = "BACKFILL_REQUESTED"
    CREDENTIAL_STORED = "CREDENTIAL_STORED"
    CREDENTIAL_DELETED = "CREDENTIAL_DELETED"
    MODEL_REGISTERED = "MODEL_REGISTERED"
    AUTH_SUCCESS = "AUTH_SUCCESS"
    AUTH_FAILURE = "AUTH_FAILURE"
    SQL_GUARD_VIOLATION = "SQL_GUARD_VIOLATION"
    TOKEN_REVOKED = "TOKEN_REVOKED"
    TENANT_PROVISIONED = "TENANT_PROVISIONED"
    TENANT_DEACTIVATED = "TENANT_DEACTIVATED"
    TENANT_CONFIG_UPDATED = "TENANT_CONFIG_UPDATED"
    AI_FEEDBACK_SUBMITTED = "AI_FEEDBACK_SUBMITTED"
    LLM_BUDGET_EXCEEDED = "LLM_BUDGET_EXCEEDED"
    LLM_BUDGET_UPDATED = "LLM_BUDGET_UPDATED"
    SETTINGS_UPDATED = "SETTINGS_UPDATED"


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class AuditService:
    """Thin wrapper around :class:`AuditRepository` for router-level use.

    Parameters
    ----------
    session:
        The async database session for the current request scope.
    tenant_id:
        Tenant identifier extracted from the authenticated request.
    actor:
        Identity of the user or system principal performing the action.
        Defaults to ``"system"`` for internal / automated operations.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        tenant_id: str,
        actor: str = "system",
    ) -> None:
        self._repo = AuditRepository(session, tenant_id=tenant_id)
        self._actor = actor

    async def log(
        self,
        action: str,
        entity_type: str | None = None,
        entity_id: str | None = None,
        **kwargs: object,
    ) -> str:
        """Record an audit event.

        Any extra keyword arguments are stored in the ``metadata_json``
        column for additional context (e.g. ``comment``, ``reason``,
        ``start_date``, ``end_date``).

        Returns the generated audit entry ID.
        """
        metadata: dict | None = dict(kwargs) if kwargs else None  # type: ignore[arg-type]
        return await self._repo.log(
            actor=self._actor,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            metadata=metadata,
        )

    async def cleanup_old_logs(self, retention_days: int) -> int:
        """Purge audit log entries older than retention_days for this tenant."""
        deleted = await self._repo.cleanup_old_entries(retention_days)
        await self._repo._session.commit()
        return deleted

    async def anonymize_user_data(self, user_id: str) -> int:
        """GDPR right-to-erasure: anonymize all audit entries for a specific user."""
        count = await self._repo.anonymize_user_entries(user_id)
        await self._repo._session.commit()
        return count
