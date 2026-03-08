"""Tests for api/api/services/github_webhook_service.py

Covers:
- HMAC-SHA256 signature validation (valid, invalid, malformed)
- Push event handling (matching config, no config, incomplete payload)
- Config CRUD (create with bcrypt, list, delete)
- Auto-plan / auto-apply toggle behaviour
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.services.github_webhook_service import GitHubWebhookService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Mock async database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


def _make_service(session: AsyncMock, tenant_id: str = "test-tenant") -> GitHubWebhookService:
    """Create a GitHubWebhookService."""
    return GitHubWebhookService(session, tenant_id=tenant_id)


def _compute_signature(payload: bytes, secret: str) -> str:
    """Compute a valid GitHub webhook HMAC-SHA256 signature."""
    sig = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={sig}"


# ---------------------------------------------------------------------------
# Signature validation
# ---------------------------------------------------------------------------


class TestSignatureValidation:
    """Verify HMAC-SHA256 signature validation."""

    def test_valid_signature(self) -> None:
        """A correctly computed HMAC-SHA256 signature passes validation."""
        payload = b'{"ref":"refs/heads/main"}'
        secret = "my-webhook-secret"
        signature = _compute_signature(payload, secret)

        assert GitHubWebhookService.validate_signature(payload, signature, secret) is True

    def test_invalid_signature(self) -> None:
        """An incorrect HMAC-SHA256 signature fails validation."""
        payload = b'{"ref":"refs/heads/main"}'
        secret = "my-webhook-secret"

        assert (
            GitHubWebhookService.validate_signature(
                payload, "sha256=0000000000000000000000000000000000000000000000000000000000000000", secret
            )
            is False
        )

    def test_missing_sha256_prefix(self) -> None:
        """A signature without the 'sha256=' prefix fails."""
        payload = b'{"ref":"refs/heads/main"}'
        secret = "my-webhook-secret"
        sig = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

        assert GitHubWebhookService.validate_signature(payload, sig, secret) is False

    def test_empty_signature(self) -> None:
        """An empty signature string fails."""
        assert GitHubWebhookService.validate_signature(b"test", "", "secret") is False

    def test_different_payload_fails(self) -> None:
        """Signature computed for different payload fails on original."""
        secret = "my-webhook-secret"
        sig = _compute_signature(b"different", secret)

        assert GitHubWebhookService.validate_signature(b"original", sig, secret) is False

    def test_different_secret_fails(self) -> None:
        """Signature computed with different secret fails."""
        payload = b'{"ref":"refs/heads/main"}'
        sig = _compute_signature(payload, "secret-A")

        assert GitHubWebhookService.validate_signature(payload, sig, "secret-B") is False


# ---------------------------------------------------------------------------
# Push event handling
# ---------------------------------------------------------------------------


class TestPushEventHandling:
    """Verify handle_push_event processes push payloads correctly."""

    @pytest.mark.asyncio
    async def test_matching_config_auto_plan(self, mock_session: AsyncMock) -> None:
        """Push to a configured repo+branch with auto_plan triggers a plan."""
        config_row = MagicMock()
        config_row.auto_plan = True
        config_row.auto_apply = False

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {"clone_url": "https://github.com/org/repo.git"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "plan_triggered"
        assert result["base_sha"] == "aaa111"
        assert result["target_sha"] == "bbb222"
        assert result["branch"] == "main"
        assert result["auto_apply"] is False

    @pytest.mark.asyncio
    async def test_matching_config_auto_plan_disabled(self, mock_session: AsyncMock) -> None:
        """Push to a configured repo+branch with auto_plan=False returns acknowledged."""
        config_row = MagicMock()
        config_row.auto_plan = False
        config_row.auto_apply = False

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {"clone_url": "https://github.com/org/repo.git"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "acknowledged"
        assert result["auto_plan"] is False

    @pytest.mark.asyncio
    async def test_no_matching_config(self, mock_session: AsyncMock) -> None:
        """Push with no matching config returns ignored."""
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {"clone_url": "https://github.com/org/unknown-repo.git"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "ignored"
        assert result["reason"] == "no_matching_config"

    @pytest.mark.asyncio
    async def test_incomplete_payload_missing_repo(self, mock_session: AsyncMock) -> None:
        """Push event missing repository data returns ignored."""
        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "ignored"
        assert result["reason"] == "incomplete_payload"

    @pytest.mark.asyncio
    async def test_incomplete_payload_missing_shas(self, mock_session: AsyncMock) -> None:
        """Push event missing SHAs returns ignored."""
        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "",
            "after": "",
            "repository": {"clone_url": "https://github.com/org/repo.git"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "ignored"
        assert result["reason"] == "incomplete_payload"

    @pytest.mark.asyncio
    async def test_branch_extraction_from_ref(self, mock_session: AsyncMock) -> None:
        """Branch name is correctly extracted from refs/heads/<branch>."""
        config_row = MagicMock()
        config_row.auto_plan = True
        config_row.auto_apply = True

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/feature/my-branch",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {"clone_url": "https://github.com/org/repo.git"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "plan_triggered"
        assert result["branch"] == "feature/my-branch"
        assert result["auto_apply"] is True

    @pytest.mark.asyncio
    async def test_html_url_fallback(self, mock_session: AsyncMock) -> None:
        """Uses html_url when clone_url is not present."""
        config_row = MagicMock()
        config_row.auto_plan = True
        config_row.auto_apply = False

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        event_data = {
            "ref": "refs/heads/main",
            "before": "aaa111",
            "after": "bbb222",
            "repository": {"html_url": "https://github.com/org/repo"},
        }

        result = await service.handle_push_event(event_data)

        assert result["status"] == "plan_triggered"
        assert result["repo_url"] == "https://github.com/org/repo"


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------


class TestConfigCRUD:
    """Verify webhook configuration CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_config_hashes_secret(self, mock_session: AsyncMock) -> None:
        """create_config stores a bcrypt hash, not the plaintext secret."""
        import bcrypt as bcrypt_mod

        service = _make_service(mock_session)
        result = await service.create_config(
            repo_url="https://github.com/org/repo.git",
            branch="main",
            secret="my-webhook-secret-12345",
        )

        assert result["provider"] == "github"
        assert result["repo_url"] == "https://github.com/org/repo.git"
        assert result["branch"] == "main"
        assert result["auto_plan"] is True
        assert result["auto_apply"] is False

        # Verify the secret hash was passed to the table row.
        mock_session.add.assert_called_once()
        added_row = mock_session.add.call_args[0][0]
        assert added_row.secret_hash is not None
        # Verify the hash is valid bcrypt
        assert bcrypt_mod.checkpw(
            b"my-webhook-secret-12345",
            added_row.secret_hash.encode("utf-8"),
        )

    @pytest.mark.asyncio
    async def test_create_config_stores_encrypted_secret(self, mock_session: AsyncMock) -> None:
        """create_config stores an encrypted secret when vault is provided."""
        from api.security import CredentialVault

        vault = CredentialVault("test-encryption-key-for-unit-tests")
        service = GitHubWebhookService(
            mock_session,
            tenant_id="test-tenant",
            credential_vault=vault,
        )
        await service.create_config(
            repo_url="https://github.com/org/repo.git",
            branch="main",
            secret="my-webhook-secret-12345",
        )

        mock_session.add.assert_called_once()
        added_row = mock_session.add.call_args[0][0]
        # secret_encrypted should be a non-empty Fernet ciphertext.
        assert added_row.secret_encrypted is not None
        assert len(added_row.secret_encrypted) > 0
        # Decrypting it should yield the original secret.
        decrypted = vault.decrypt(added_row.secret_encrypted)
        assert decrypted == "my-webhook-secret-12345"

    @pytest.mark.asyncio
    async def test_create_config_no_vault_skips_encryption(self, mock_session: AsyncMock) -> None:
        """create_config sets secret_encrypted=None when no vault is provided."""
        service = _make_service(mock_session)
        await service.create_config(
            repo_url="https://github.com/org/repo.git",
            branch="main",
            secret="my-webhook-secret-12345",
        )

        mock_session.add.assert_called_once()
        added_row = mock_session.add.call_args[0][0]
        assert added_row.secret_encrypted is None

    @pytest.mark.asyncio
    async def test_create_config_custom_flags(self, mock_session: AsyncMock) -> None:
        """create_config respects auto_plan and auto_apply flags."""
        service = _make_service(mock_session)
        result = await service.create_config(
            repo_url="https://github.com/org/repo.git",
            branch="develop",
            secret="secret-12345678",
            auto_plan=False,
            auto_apply=True,
        )

        assert result["auto_plan"] is False
        assert result["auto_apply"] is True
        assert result["branch"] == "develop"

    @pytest.mark.asyncio
    async def test_list_configs_returns_entries(self, mock_session: AsyncMock) -> None:
        """list_configs returns all configs for the tenant."""
        row1 = MagicMock()
        row1.id = 1
        row1.provider = "github"
        row1.repo_url = "https://github.com/org/repo1.git"
        row1.branch = "main"
        row1.auto_plan = True
        row1.auto_apply = False
        row1.created_at = datetime(2024, 6, 1, tzinfo=timezone.utc)
        row1.updated_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

        row2 = MagicMock()
        row2.id = 2
        row2.provider = "github"
        row2.repo_url = "https://github.com/org/repo2.git"
        row2.branch = "develop"
        row2.auto_plan = True
        row2.auto_apply = True
        row2.created_at = datetime(2024, 6, 2, tzinfo=timezone.utc)
        row2.updated_at = datetime(2024, 6, 2, tzinfo=timezone.utc)

        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [row1, row2]
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        configs = await service.list_configs()

        assert len(configs) == 2
        assert configs[0]["repo_url"] == "https://github.com/org/repo1.git"
        assert configs[1]["branch"] == "develop"
        assert configs[1]["auto_apply"] is True
        # Secrets are never exposed in list output
        for c in configs:
            assert "secret" not in c
            assert "secret_hash" not in c

    @pytest.mark.asyncio
    async def test_list_configs_empty(self, mock_session: AsyncMock) -> None:
        """list_configs returns empty list when no configs exist."""
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        configs = await service.list_configs()

        assert configs == []

    @pytest.mark.asyncio
    async def test_delete_config_success(self, mock_session: AsyncMock) -> None:
        """delete_config returns True when a row is deleted."""
        result_mock = MagicMock()
        result_mock.rowcount = 1
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        deleted = await service.delete_config(config_id=42)

        assert deleted is True

    @pytest.mark.asyncio
    async def test_delete_config_not_found(self, mock_session: AsyncMock) -> None:
        """delete_config returns False when no row matches."""
        result_mock = MagicMock()
        result_mock.rowcount = 0
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        deleted = await service.delete_config(config_id=999)

        assert deleted is False


# ---------------------------------------------------------------------------
# Verify secret
# ---------------------------------------------------------------------------


class TestVerifySecret:
    """Verify the verify_secret method."""

    @pytest.mark.asyncio
    async def test_verify_secret_config_exists(self, mock_session: AsyncMock) -> None:
        """verify_secret returns True if config exists."""
        config_row = MagicMock()
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        result = await service.verify_secret(config_id=1, payload=b"test", signature="sha256=abc")

        assert result is True

    @pytest.mark.asyncio
    async def test_verify_secret_config_not_found(self, mock_session: AsyncMock) -> None:
        """verify_secret returns False if config doesn't exist."""
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session)
        result = await service.verify_secret(config_id=999, payload=b"test", signature="sha256=abc")

        assert result is False
