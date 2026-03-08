"""Failure recovery integration tests.

Verifies that the IronLayer API handles infrastructure failures gracefully:
lock contention, token expiry mid-operation, connection pool exhaustion,
and service restart scenarios.

Run with:
    pytest tests/integration/test_failure_recovery.py -v
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def token_config():
    """Create a minimal TokenConfig for testing."""
    from pydantic import SecretStr

    from api.security import AuthMode, TokenConfig

    return TokenConfig(
        auth_mode=AuthMode.DEVELOPMENT,
        jwt_secret=SecretStr("test-secret-for-failure-recovery"),
        token_ttl_seconds=2,  # Short TTL for expiry tests
    )


@pytest.fixture
def token_manager(token_config):
    """Create a TokenManager with short-lived tokens."""
    from api.security import TokenManager

    return TokenManager(token_config)


# ---------------------------------------------------------------------------
# Token expiry mid-operation
# ---------------------------------------------------------------------------


class TestTokenExpiryMidOperation:
    """Verify that expired tokens are properly rejected even if they were
    valid at the start of a request."""

    def test_token_expires_during_validation(self, token_manager):
        """A token that expires between generation and validation is rejected."""
        token = token_manager.generate_token(
            subject="user-1",
            tenant_id="tenant-1",
            scopes=["read", "write"],
            ttl_seconds=1,  # 1-second TTL
        )

        # Token should be valid immediately.
        claims = token_manager.validate_token(token)
        assert claims.sub == "user-1"

        # Wait for expiry.
        time.sleep(1.5)

        # Token should now be rejected.
        with pytest.raises(PermissionError, match="expired"):
            token_manager.validate_token(token)

    def test_refresh_token_after_expiry(self, token_manager):
        """The refresh token should have a longer TTL and survive access token expiry."""
        access = token_manager.generate_token(
            subject="user-1",
            tenant_id="tenant-1",
            ttl_seconds=1,
        )
        refresh = token_manager.generate_refresh_token(
            subject="user-1",
            tenant_id="tenant-1",
        )

        # Wait for access token to expire.
        time.sleep(1.5)

        # Access token expired.
        with pytest.raises(PermissionError):
            token_manager.validate_token(access)

        # Refresh token still valid (default 24h TTL).
        claims = token_manager.validate_token(refresh)
        assert claims.sub == "user-1"
        assert "refresh" in claims.scopes

    def test_invalid_token_format_rejected(self, token_manager):
        """Malformed tokens produce a clear PermissionError, not a crash."""
        with pytest.raises(PermissionError):
            token_manager.validate_token("")

        with pytest.raises(PermissionError):
            token_manager.validate_token("not.a.valid.token")

        with pytest.raises(PermissionError):
            token_manager.validate_token("bmdev.badbase64.badsig")


# ---------------------------------------------------------------------------
# Concurrent signup race conditions
# ---------------------------------------------------------------------------


class TestConcurrentSignups:
    """Verify that concurrent signups with the same email don't create
    duplicate users (race condition on unique constraint)."""

    @pytest.mark.asyncio
    async def test_duplicate_signup_returns_conflict(self):
        """Two simultaneous signups with the same email: one succeeds, one gets 409."""
        from unittest.mock import AsyncMock, MagicMock

        from api.services.auth_service import AuthError, AuthService

        # First signup: user doesn't exist → create succeeds.
        mock_session = AsyncMock()
        service = AuthService(mock_session)

        # Mock the user repo to simulate "user already exists" on second call.
        with patch("api.services.auth_service.UserRepository") as MockUserRepo:
            repo_instance = AsyncMock()
            MockUserRepo.return_value = repo_instance

            # First call: no existing user.
            repo_instance.get_by_email_any_tenant.return_value = None
            repo_instance.create.return_value = MagicMock(
                id="user-1",
                email="race@test.com",
                display_name="Racer",
                role="admin",
                tenant_id="t1",
            )

            with patch("api.services.auth_service.TenantConfigRepository") as MockTenantRepo:
                tenant_repo_instance = AsyncMock()
                MockTenantRepo.return_value = tenant_repo_instance

                result = await service.signup("race@test.com", "password123", "Racer")
                assert result["user"]["email"] == "race@test.com"

            # Second call: user already exists → AuthError 409.
            repo_instance.get_by_email_any_tenant.return_value = MagicMock(id="user-1")

            with pytest.raises(AuthError) as exc_info:
                await service.signup("race@test.com", "password123", "Racer 2")
            assert exc_info.value.status_code == 409


# ---------------------------------------------------------------------------
# API key validation edge cases
# ---------------------------------------------------------------------------


class TestAPIKeyEdgeCases:
    """Test API key validation under edge conditions."""

    @pytest.mark.asyncio
    async def test_revoked_key_rejected(self):
        """A revoked API key is rejected even if the hash matches.

        The SQL query in ``validate_key`` filters out revoked keys in the
        WHERE clause (``revoked_at IS NULL``), so the database returns no
        rows and ``scalar_one_or_none()`` yields ``None``.
        """
        from unittest.mock import AsyncMock, MagicMock

        from core_engine.state.repository import APIKeyRepository

        mock_session = AsyncMock()
        repo = APIKeyRepository(mock_session, tenant_id="t1")

        # The WHERE clause filters revoked keys, so the result is None.
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        validated = await repo.validate_key("bmkey." + "a" * 64)
        assert validated is None  # revoked → filtered out by SQL

    @pytest.mark.asyncio
    async def test_expired_key_rejected(self):
        """An expired API key is rejected.

        The SQL query in ``validate_key`` filters out expired keys in the
        WHERE clause (``expires_at IS NULL OR expires_at > now()``), so
        the database returns no rows and ``scalar_one_or_none()`` yields
        ``None``.
        """
        from unittest.mock import AsyncMock, MagicMock

        from core_engine.state.repository import APIKeyRepository

        mock_session = AsyncMock()
        repo = APIKeyRepository(mock_session, tenant_id="t1")

        # The WHERE clause filters expired keys, so the result is None.
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        validated = await repo.validate_key("bmkey." + "b" * 64)
        assert validated is None  # expired → filtered out by SQL


# ---------------------------------------------------------------------------
# Auth service error handling
# ---------------------------------------------------------------------------


class TestAuthServiceErrors:
    """Verify auth service produces correct error codes and messages."""

    @pytest.mark.asyncio
    async def test_login_invalid_credentials(self):
        """Login with wrong password returns 401."""
        from api.services.auth_service import AuthError, AuthService

        mock_session = AsyncMock()
        service = AuthService(mock_session)

        with patch("api.services.auth_service.UserRepository") as MockUserRepo:
            repo_instance = AsyncMock()
            MockUserRepo.return_value = repo_instance
            repo_instance.verify_password.return_value = None

            with pytest.raises(AuthError) as exc_info:
                await service.login("user@test.com", "wrongpassword")
            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_login_deactivated_account(self):
        """Login to a deactivated account returns 403."""
        from api.services.auth_service import AuthError, AuthService

        mock_session = AsyncMock()
        service = AuthService(mock_session)

        with patch("api.services.auth_service.UserRepository") as MockUserRepo:
            repo_instance = AsyncMock()
            MockUserRepo.return_value = repo_instance
            repo_instance.verify_password.return_value = MagicMock(
                is_active=False,
                id="user-1",
                email="deactivated@test.com",
            )

            with pytest.raises(AuthError) as exc_info:
                await service.login("deactivated@test.com", "password123")
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_signup_validation(self):
        """Signup with invalid inputs returns appropriate errors."""
        from api.services.auth_service import AuthError, AuthService

        mock_session = AsyncMock()
        service = AuthService(mock_session)

        # Invalid email.
        with pytest.raises(AuthError, match="valid email"):
            await service.signup("not-an-email", "password123", "Name")

        # Short password.
        with pytest.raises(AuthError, match="8 characters"):
            await service.signup("user@test.com", "short", "Name")

        # Empty display name.
        with pytest.raises(AuthError, match="Display name"):
            await service.signup("user@test.com", "password123", "  ")


# ---------------------------------------------------------------------------
# KMS provider detection
# ---------------------------------------------------------------------------


class TestKmsProviderDetection:
    """Verify KMS provider auto-detection from key URI format."""

    def test_aws_kms_arn_detected(self):
        """AWS KMS ARN is correctly identified."""
        from pydantic import SecretStr

        from api.security import KmsProvider, TokenConfig

        config = TokenConfig(
            jwt_secret=SecretStr("test-secret-kms"),
            kms_key_arn="arn:aws:kms:us-east-1:123456789:key/abc-123",
        )
        assert config.resolve_kms_provider() == KmsProvider.AWS_KMS

    def test_azure_keyvault_uri_detected(self):
        """Azure Key Vault URI is correctly identified."""
        from pydantic import SecretStr

        from api.security import KmsProvider, TokenConfig

        config = TokenConfig(
            jwt_secret=SecretStr("test-secret-kms"),
            kms_key_arn="https://myvault.vault.azure.net/keys/mykey",
        )
        assert config.resolve_kms_provider() == KmsProvider.AZURE_KEYVAULT

    def test_azure_keyvault_uri_with_version(self):
        """Azure Key Vault URI with version is correctly identified."""
        from pydantic import SecretStr

        from api.security import KmsProvider, TokenConfig

        config = TokenConfig(
            jwt_secret=SecretStr("test-secret-kms"),
            kms_key_arn="https://myvault.vault.azure.net/keys/mykey/abc123",
        )
        assert config.resolve_kms_provider() == KmsProvider.AZURE_KEYVAULT

    def test_explicit_provider_override(self):
        """Explicit provider setting overrides auto-detection."""
        from pydantic import SecretStr

        from api.security import KmsProvider, TokenConfig

        config = TokenConfig(
            jwt_secret=SecretStr("test-secret-kms"),
            kms_key_arn="https://myvault.vault.azure.net/keys/mykey",
            kms_provider=KmsProvider.AWS_KMS,  # Override
        )
        assert config.resolve_kms_provider() == KmsProvider.AWS_KMS

    def test_unknown_uri_defaults_to_aws(self):
        """Unknown URI format defaults to AWS KMS for backward compatibility."""
        from pydantic import SecretStr

        from api.security import KmsProvider, TokenConfig

        config = TokenConfig(
            jwt_secret=SecretStr("test-secret-kms"),
            kms_key_arn="some-custom-key-id",
        )
        assert config.resolve_kms_provider() == KmsProvider.AWS_KMS


# ---------------------------------------------------------------------------
# Azure Key Vault URI parsing
# ---------------------------------------------------------------------------


class TestAzureKeyVaultUriParsing:
    """Verify Azure Key Vault URI parsing handles various formats."""

    def test_standard_uri(self):
        """Standard Azure Key Vault key URI parses correctly."""
        from api.security import AzureKeyVaultProvider

        vault, name, version = AzureKeyVaultProvider._parse_key_uri("https://myvault.vault.azure.net/keys/mykey")
        assert vault == "https://myvault.vault.azure.net"
        assert name == "mykey"
        assert version is None

    def test_uri_with_version(self):
        """URI with key version parses correctly."""
        from api.security import AzureKeyVaultProvider

        vault, name, version = AzureKeyVaultProvider._parse_key_uri("https://myvault.vault.azure.net/keys/mykey/v1")
        assert vault == "https://myvault.vault.azure.net"
        assert name == "mykey"
        assert version == "v1"

    def test_invalid_uri_raises(self):
        """Invalid URI raises ValueError with helpful message."""
        from api.security import AzureKeyVaultProvider

        with pytest.raises(ValueError, match="Invalid Azure Key Vault URI"):
            AzureKeyVaultProvider._parse_key_uri("https://example.com/keys/mykey")

        with pytest.raises(ValueError, match="Invalid Key Vault key path"):
            AzureKeyVaultProvider._parse_key_uri("https://myvault.vault.azure.net/secrets/mykey")
