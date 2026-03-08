"""Tests for audit log retention and GDPR anonymization (BL-031).

Covers:
- AuditService.cleanup_old_logs: deletes old entries, returns correct count
- AuditService.anonymize_user_data: replaces actor with [REDACTED], returns count
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
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=5)
            # _session is accessed via self._repo._session
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1", actor="system")
            # Patch _repo directly since AuditService already constructed it
            svc._repo = repo_instance

            result = await svc.cleanup_old_logs(retention_days=30)

        assert result == 5
        repo_instance.cleanup_old_entries.assert_awaited_once_with(30)
        session.commit.assert_awaited_once()

    async def test_cleanup_returns_zero_when_nothing_to_delete(self) -> None:
        """cleanup_old_logs returns 0 when no entries are old enough."""
        session = AsyncMock()
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=0)
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            svc._repo = repo_instance

            result = await svc.cleanup_old_logs(retention_days=365)

        assert result == 0

    async def test_cleanup_passes_retention_days(self) -> None:
        """cleanup_old_logs forwards the retention_days argument correctly."""
        session = AsyncMock()
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.cleanup_old_entries = AsyncMock(return_value=12)
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            svc._repo = repo_instance

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
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=3)
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            svc._repo = repo_instance

            result = await svc.anonymize_user_data("alice@example.com")

        assert result == 3
        repo_instance.anonymize_user_entries.assert_awaited_once_with("alice@example.com")
        session.commit.assert_awaited_once()

    async def test_anonymize_returns_zero_when_user_not_found(self) -> None:
        """anonymize_user_data returns 0 when user has no audit entries."""
        session = AsyncMock()
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=0)
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            svc._repo = repo_instance

            result = await svc.anonymize_user_data("nobody@example.com")

        assert result == 0

    async def test_anonymize_passes_user_id(self) -> None:
        """anonymize_user_data forwards the user_id argument correctly."""
        session = AsyncMock()
        session.commit = AsyncMock()

        with patch(
            "api.services.audit_service.AuditRepository"
        ) as MockRepo:
            repo_instance = MockRepo.return_value
            repo_instance.anonymize_user_entries = AsyncMock(return_value=7)
            repo_instance._session = session

            from api.services.audit_service import AuditService

            svc = AuditService(session, tenant_id="t1")
            svc._repo = repo_instance

            await svc.anonymize_user_data("bob@example.com")

        repo_instance.anonymize_user_entries.assert_awaited_once_with("bob@example.com")


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
