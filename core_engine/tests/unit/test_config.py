"""Unit tests for core_engine.config."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from core_engine.config import (
    ClusterSize,
    PlatformEnv,
    Settings,
    StateStoreType,
    load_settings,
)

# ---------------------------------------------------------------------------
# Settings - default values
# ---------------------------------------------------------------------------


class TestSettingsDefaults:
    def test_default_env(self):
        settings = Settings()
        assert settings.env == PlatformEnv.DEV

    def test_default_debug(self):
        settings = Settings()
        assert settings.debug is False

    def test_default_database_url(self):
        settings = Settings()
        assert "ironlayer" in settings.database_url

    def test_default_database_pool_size(self):
        settings = Settings()
        assert settings.database_pool_size == 10

    def test_default_databricks_none(self):
        settings = Settings()
        assert settings.databricks_host is None
        assert settings.databricks_token is None
        assert settings.databricks_warehouse_id is None

    def test_default_state_store_type(self):
        settings = Settings()
        assert settings.state_store_type == StateStoreType.LOCAL

    def test_default_cluster_size(self):
        settings = Settings()
        assert settings.default_cluster_size == ClusterSize.SMALL

    def test_default_max_retries(self):
        settings = Settings()
        assert settings.max_retries == 3

    def test_default_lookback_days(self):
        settings = Settings()
        assert settings.default_lookback_days == 30

    def test_default_lock_ttl(self):
        settings = Settings()
        assert settings.lock_ttl_seconds == 3600


# ---------------------------------------------------------------------------
# Environment variable overrides
# ---------------------------------------------------------------------------


class TestSettingsEnvOverrides:
    def test_env_var_overrides_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_ENV", "prod")
        settings = Settings()
        assert settings.env == PlatformEnv.PROD

    def test_env_var_overrides_debug(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DEBUG", "true")
        settings = Settings()
        assert settings.debug is True

    def test_env_var_overrides_max_retries(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_MAX_RETRIES", "5")
        settings = Settings()
        assert settings.max_retries == 5

    def test_env_var_overrides_default_cluster_size(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DEFAULT_CLUSTER_SIZE", "large")
        settings = Settings()
        assert settings.default_cluster_size == ClusterSize.LARGE

    def test_env_var_databricks_host(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_HOST", "https://my-workspace.databricks.com")
        settings = Settings()
        assert settings.databricks_host == "https://my-workspace.databricks.com"

    def test_env_var_databricks_token(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_TOKEN", "dapiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        settings = Settings()
        assert isinstance(settings.databricks_token, SecretStr)
        assert settings.databricks_token.get_secret_value() == "dapiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


# ---------------------------------------------------------------------------
# is_databricks_configured
# ---------------------------------------------------------------------------


class TestIsDatabricksConfigured:
    def test_not_configured_by_default(self):
        settings = Settings()
        assert settings.is_databricks_configured() is False

    def test_configured_with_host_and_token(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_HOST", "https://workspace.databricks.com")
        monkeypatch.setenv("PLATFORM_DATABRICKS_TOKEN", "dapi123")
        settings = Settings()
        assert settings.is_databricks_configured() is True

    def test_not_configured_with_only_host(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_HOST", "https://workspace.databricks.com")
        settings = Settings()
        assert settings.is_databricks_configured() is False

    def test_not_configured_with_only_token(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_TOKEN", "dapi123")
        settings = Settings()
        assert settings.is_databricks_configured() is False


# ---------------------------------------------------------------------------
# load_settings
# ---------------------------------------------------------------------------


class TestLoadSettings:
    def test_load_defaults(self):
        settings = load_settings()
        assert isinstance(settings, Settings)
        assert settings.env == PlatformEnv.DEV

    def test_load_with_overrides(self):
        settings = load_settings(debug=True, max_retries=10)
        assert settings.debug is True
        assert settings.max_retries == 10

    def test_load_with_env_override(self):
        settings = load_settings(env="staging")
        assert settings.env == PlatformEnv.STAGING


# ---------------------------------------------------------------------------
# Enum values
# ---------------------------------------------------------------------------


class TestEnums:
    def test_platform_env_values(self):
        assert PlatformEnv.DEV.value == "dev"
        assert PlatformEnv.STAGING.value == "staging"
        assert PlatformEnv.PROD.value == "prod"

    def test_cluster_size_values(self):
        assert ClusterSize.SMALL.value == "small"
        assert ClusterSize.MEDIUM.value == "medium"
        assert ClusterSize.LARGE.value == "large"

    def test_state_store_type_values(self):
        assert StateStoreType.POSTGRES.value == "postgres"
        assert StateStoreType.LOCAL.value == "local"


# ---------------------------------------------------------------------------
# Token masking validator
# ---------------------------------------------------------------------------


class TestTokenMasking:
    def test_none_token_stays_none(self):
        settings = Settings(databricks_token=None)
        assert settings.databricks_token is None

    def test_string_token_becomes_secret(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PLATFORM_DATABRICKS_TOKEN", "my_secret_token")
        settings = Settings()
        assert isinstance(settings.databricks_token, SecretStr)
        assert settings.databricks_token.get_secret_value() == "my_secret_token"
