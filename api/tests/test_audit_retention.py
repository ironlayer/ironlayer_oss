"""Tests for audit log retention and GDPR anonymization (BL-031, BL-042).

Covers:
- AuditService.cleanup_old_logs: deletes old entries, returns correct count
- AuditService.anonymize_user_data: delegates to repo, commits via self._session
- AuditRepository.anonymize_user_entries: sets is_anonymized=True alongside redaction
- AuditRepository.verify_chain: skips hash check for is_anonymized entries
- DELETE /api/v1/audit/users/{user_id}: requires ADMIN role, returns 200
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.usefixtures("bypass_feature_gate")


# ---------------------------------------------------------------------------
# AuditService.cleanup_old_logs
# ---------------------------------------------------------------------------


class TestCleanupOldLogs:
    """AuditService.cleanup_old_logs delegates to AuditRepository and commits."""

    async def test_cleanup_returns_delete_count(self) -> None:
        """cleanup_old_logs returns the number of rows deleted."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=5)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1", actor="system")
            result = await svc.cleanup_old_logs(retention_days=30)

        assert result == 5
        repo_instance.cleanup_old_entries.assert_awaited_once_with(30)
        # Commit must be called via self._session, not self._repo._session.
        session.commit.assert_awaited_once()

    async def test_cleanup_returns_zero_when_nothing_to_delete(self) -> None:
        """cleanup_old_logs returns 0 when no entries are old enough."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=0)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            result = await svc.cleanup_old_logs(retention_days=365)

        assert result == 0

    async def test_cleanup_passes_retention_days(self) -> None:
        """cleanup_old_logs forwards the retention_days argument correctly."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=12)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            await svc.cleanup_old_logs(retention_days=90)

        repo_instance.cleanup_old_entries.assert_awaited_once_with(90)


# ---------------------------------------------------------------------------
# AuditService.anonymize_user_data
# ---------------------------------------------------------------------------


class TestAnonymizeUserData:
    """AuditService.anonymize_user_data delegates to AuditRepository and commits."""

    async def test_anonymize_returns_affected_count(self) -> None:
        """anonymize_user_data returns the number of rows updated."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=3)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            result = await svc.anonymize_user_data("alice@example.com")

        assert result == 3
        repo_instance.anonymize_user_entries.assert_awaited_once_with("alice@example.com")
        # Commit must go through self._session — not self._repo._session.
        session.commit.assert_awaited_once()

    async def test_anonymize_returns_zero_when_user_not_found(self) -> None:
        """anonymize_user_data returns 0 when user has no audit entries."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=0)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            result = await svc.anonymize_user_data("nobody@example.com")

        assert result == 0

    async def test_anonymize_passes_user_id(self) -> None:
        """anonymize_user_data forwards the user_id argument correctly."""
        session = AsyncMock()

        with patch("api.services.audit_service.AuditRepository") as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=7)

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            await svc.anonymize_user_data("bob@example.com")

        repo_instance.anonymize_user_entries.assert_awaited_once_with("bob@example.com")


# ---------------------------------------------------------------------------
# AuditRepository.anonymize_user_entries — is_anonymized flag
# ---------------------------------------------------------------------------


class TestAnonymizeUserEntriesFlag:
    """verify that anonymize_user_entries sets is_anonymized=True."""

    async def test_update_sets_is_anonymized_true(self) -> None:
        """anonymize_user_entries passes is_anonymized=True in the UPDATE values."""
        session = AsyncMock()
        execute_result = MagicMock()
        execute_result.rowcount = 2
        session.execute = AsyncMock(return_value=execute_result)

        from core_engine.state.repository import AuditRepository

        repo = AuditRepository(session, tenant_id="tenant-x")
        count = await repo.anonymize_user_entries("alice@example.com")

        assert count == 2
        # Verify execute was called and the compiled statement includes
        # is_anonymized in the VALUES clause.
        session.execute.assert_awaited_once()
        call_args = session.execute.call_args[0][0]
        # The UPDATE statement should include is_anonymized in its values.
        compiled = str(call_args.compile(compile_kwargs={"literal_binds": True}))
        assert "is_anonymized" in compiled


# ---------------------------------------------------------------------------
# AuditRepository.verify_chain — anonymized entry handling
# ---------------------------------------------------------------------------


class TestVerifyChainAnonymizedEntries:
    """verify_chain skips hash check for is_anonymized entries."""

    async def test_chain_valid_when_all_entries_normal(self) -> None:
        """verify_chain returns True when no anomalies (basic sanity check)."""
        session = AsyncMock()
        # Empty table — should return (True, 0).
        execute_result = MagicMock()
        execute_result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=execute_result)

        from core_engine.state.repository import AuditRepository

        repo = AuditRepository(session, tenant_id="tenant-y")
        is_valid, checked = await repo.verify_chain(limit=100)

        assert is_valid is True
        assert checked == 0

    async def test_chain_valid_skipping_anonymized_entry(self) -> None:
        """verify_chain returns True when an anonymized entry is in the chain.

        An anonymized entry's hash cannot be recomputed from its current
        field values; verify_chain must advance the chain link using the
        stored entry_hash without failing the verification.
        """
        import hashlib
        import json
        from datetime import datetime, timezone

        def make_hash(**kwargs: object) -> str:
            payload = json.dumps(kwargs, sort_keys=True, default=str)
            return hashlib.sha256(payload.encode()).hexdigest()

        now = datetime.now(tz=timezone.utc)

        # Build a two-entry chain where the second entry is anonymized.
        h0 = make_hash(
            tenant_id="t",
            actor="alice",
            action="LOGIN",
            entity_type=None,
            entity_id=None,
            metadata=None,
            previous_hash=None,
            created_at=str(now),
        )

        entry0 = MagicMock()
        entry0.id = "e0"
        entry0.tenant_id = "t"
        entry0.actor = "alice"
        entry0.action = "LOGIN"
        entry0.entity_type = None
        entry0.entity_id = None
        entry0.metadata_json = None
        entry0.previous_hash = None
        entry0.entry_hash = h0
        entry0.created_at = now
        entry0.is_anonymized = False

        # The anonymized entry: actor/metadata already redacted, is_anonymized=True.
        entry1 = MagicMock()
        entry1.id = "e1"
        entry1.tenant_id = "t"
        entry1.actor = "[REDACTED]"
        entry1.action = "PLAN_CREATED"
        entry1.entity_type = "plan"
        entry1.entity_id = "p1"
        entry1.metadata_json = None
        entry1.previous_hash = h0  # correct chain link
        entry1.entry_hash = "original-hash-before-erasure"  # can't be recomputed
        entry1.created_at = now
        entry1.is_anonymized = True

        session = AsyncMock()
        execute_result = MagicMock()
        execute_result.scalars.return_value.all.return_value = [entry0, entry1]
        session.execute = AsyncMock(return_value=execute_result)

        from core_engine.state.repository import AuditRepository

        repo = AuditRepository(session, tenant_id="t")
        # We need _compute_hash to work; patch it to use our make_hash.
        repo._compute_hash = lambda **kw: make_hash(**kw)

        is_valid, checked = await repo.verify_chain(limit=100)

        assert is_valid is True
        # entry0 was verified, entry1 was skipped (is_anonymized).
        assert checked == 1


# ---------------------------------------------------------------------------
# DELETE /api/v1/audit/users/{user_id}
# ---------------------------------------------------------------------------


class TestAuditAnonymizeEndpoint:
    """DELETE /api/v1/audit/users/{user_id} endpoint tests."""

    async def test_anonymize_endpoint_returns_200(
        self, client: AsyncClient, mock_session: AsyncMock
    ) -> None:
        """An ADMIN-role request to the anonymize endpoint returns 200."""
        with patch("api.routers.audit.AuditService") as MockSvc:
            svc_instance = MockSvc.return_value
            svc_instance.anonymize_user_data = AsyncMock(return_value=4)

            resp = await client.delete("/api/v1/audit/users/alice%40example.com")

        assert resp.status_code == 200
        body = resp.json()
        assert body["entries_anonymized"] == 4
        assert "alice@example.com" in body["user_id"]

    async def test_anonymize_endpoint_zero_entries(
        self, client: AsyncClient, mock_session: AsyncMock
    ) -> None:
        """Endpoint returns 200 with count=0 when user has no audit entries."""
        with patch("api.routers.audit.AuditService") as MockSvc:
            svc_instance = MockSvc.return_value
            svc_instance.anonymize_user_data = AsyncMock(return_value=0)

            resp = await client.delete("/api/v1/audit/users/ghost")

        assert resp.status_code == 200
        body = resp.json()
        assert body["entries_anonymized"] == 0
        assert body["user_id"] == "ghost"

    async def test_anonymize_endpoint_calls_service_with_user_id(
        self, client: AsyncClient, mock_session: AsyncMock
    ) -> None:
        """The endpoint passes the path-parameter user_id to AuditService."""
        with patch("api.routers.audit.AuditService") as MockSvc:
            svc_instance = MockSvc.return_value
            svc_instance.anonymize_user_data = AsyncMock(return_value=2)

            await client.delete("/api/v1/audit/users/carol")

        svc_instance.anonymize_user_data.assert_awaited_once_with("carol")
