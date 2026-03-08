"""Core engine configuration loaded from environment variables."""

from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class PlatformEnv(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class ClusterSize(str, Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class StateStoreType(str, Enum):
    POSTGRES = "postgres"
    LOCAL = "local"


class Settings(BaseSettings):
    """Application settings loaded from environment variables with PLATFORM_ prefix."""

    model_config = SettingsConfigDict(
        env_prefix="PLATFORM_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    env: PlatformEnv = PlatformEnv.DEV
    debug: bool = False

    # Database
    database_url: str = "postgresql+asyncpg://ironlayer:ironlayer_dev@localhost:5432/ironlayer"
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # Databricks
    databricks_host: str | None = None
    databricks_token: SecretStr | None = None
    databricks_warehouse_id: str | None = None

    # State store
    state_store_type: StateStoreType = StateStoreType.LOCAL

    # Delta catalog (optional, for telemetry offload)
    delta_catalog: str = "platform_metadata"

    # Execution
    default_cluster_size: ClusterSize = ClusterSize.SMALL
    max_retries: int = 3
    retry_backoff_base: float = 2.0
    retry_max_delay: float = 60.0
    job_poll_interval: float = 10.0
    job_timeout_seconds: int = 3600

    # Local execution
    local_db_path: Path = Path(".ironlayer/local.duckdb")

    # Planner
    default_lookback_days: int = 30

    # Telemetry
    metrics_file: Path | None = None
    structured_logging: bool = False

    # Lock TTL
    lock_ttl_seconds: int = 3600

    # AI engine
    ai_engine_url: str = "http://localhost:8001"
    ai_engine_timeout: float = 10.0

    @field_validator("databricks_token", mode="before")
    @classmethod
    def mask_token_in_repr(cls, v: str | None) -> SecretStr | None:
        if v is None:
            return None
        if isinstance(v, SecretStr):
            return v
        return SecretStr(v)

    def is_databricks_configured(self) -> bool:
        return self.databricks_host is not None and self.databricks_token is not None


def load_settings(**overrides: object) -> Settings:
    """Load settings from environment, with optional overrides for testing."""
    settings = Settings(**overrides)  # type: ignore[arg-type]

    if settings.debug:
        logger.info("Loaded settings for environment: %s", settings.env.value)

    return settings
