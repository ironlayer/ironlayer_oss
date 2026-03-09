"""API-layer configuration loaded from environment variables."""

from __future__ import annotations

from typing import Self

from core_engine.config import PlatformEnv
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    """FastAPI application settings.

    All values can be overridden via environment variables prefixed with
    ``API_`` (e.g. ``API_HOST=0.0.0.0``) or through a ``.env`` file in the
    working directory.
    """

    model_config = SettingsConfigDict(
        env_prefix="API_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # root .env may contain vars for other services
    )

    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False

    # PostgreSQL connection string (asyncpg driver).
    database_url: str = "postgresql+asyncpg://ironlayer:ironlayer_dev@localhost:5432/ironlayer"

    # AI advisory engine URL and request timeout.
    ai_engine_url: str = "http://localhost:8001"
    ai_engine_timeout: float = 10.0

    # Environment label used for approval gate logic.
    platform_env: PlatformEnv = PlatformEnv.DEV

    @field_validator("platform_env", mode="before")
    @classmethod
    def _normalize_platform_env(cls, v: str | PlatformEnv) -> PlatformEnv | str:
        """Accept \"production\" from API_PLATFORM_ENV and normalize to PROD."""
        if isinstance(v, str) and v.lower() == "production":
            return PlatformEnv.PROD
        return v

    # Origins permitted by the CORS middleware.
    cors_origins: list[str] = ["http://localhost:3000"]

    # Whether CORS responses include credentials (cookies, auth headers).
    cors_allow_credentials: bool = True

    @model_validator(mode="after")
    def _validate_cors_origins(self) -> Self:
        """Reject unsafe wildcard CORS configurations.

        Two distinct checks:
        1. Credentials + wildcard: browsers silently reject this combination;
           fail fast at startup to catch misconfiguration early.
        2. BL-078: Wildcard in non-dev environments is a hard error.
           A misconfigured production deployment could silently allow all CORS
           origins, enabling cross-site credential theft.  Dev environments
           may use ``*`` (with a warning) because localhost lacks strict-origin.
        """
        _dev_envs = (PlatformEnv.DEV,)
        wildcard_present = "*" in self.cors_origins

        if self.cors_allow_credentials and wildcard_present:
            raise ValueError(
                "Cannot use wildcard origins with credentials. "
                "Specify explicit origins instead of '*' when "
                "cors_allow_credentials=True."
            )
        # BL-078: hard-fail on wildcard in non-dev environments.
        if wildcard_present and self.platform_env not in _dev_envs:
            raise ValueError(
                f"Wildcard CORS origins ('*') are not permitted in "
                f"{self.platform_env} environment. "
                "Specify explicit allowed origins instead."
            )
        return self

    # Request body size limit (bytes). Rejects requests with larger Content-Length.
    max_request_body_size: int = 1_048_576  # 1 MiB

    # Redis (shared state for rate limiting and token revocation across replicas).
    # When unset, in-process fallbacks are used automatically.
    # Generate: redis://:<password>@<host>:6379/0
    redis_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("redis_url", "REDIS_URL"),
    )

    # Rate limiting.
    rate_limit_enabled: bool = True
    rate_limit_requests_per_minute: int = 60
    rate_limit_burst_multiplier: float = 1.5
    rate_limit_auth_endpoints_per_minute: int = 20

    # Token revocation.
    token_revocation_enabled: bool = True

    # JWT signing secret (env: JWT_SECRET, no API_ prefix). Required in staging/prod.
    jwt_secret: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("jwt_secret", "JWT_SECRET"),
    )

    # Previous JWT secret for zero-downtime rotation (env: JWT_SECRET_PREVIOUS).
    # Set to the old secret while deploying a new JWT_SECRET; tokens signed with
    # the old secret remain valid until this is removed (after all tokens expire).
    jwt_secret_previous: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("jwt_secret_previous", "JWT_SECRET_PREVIOUS"),
    )

    # Allowed base directory for repo_path validation (path traversal prevention).
    allowed_repo_base: str = "/workspace"

    # Structured JSON logging for SIEM integration.
    structured_logging: bool = False

    # Encryption key for CredentialVault (LLM keys, secrets at rest).
    # Separate from JWT_SECRET: a JWT compromise must NOT also expose credentials.
    # Readable as CREDENTIAL_ENCRYPTION_KEY or API_CREDENTIAL_ENCRYPTION_KEY.
    # REQUIRED when AUTH_MODE != development; startup will refuse to start with the default.
    # Generate: python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
    credential_encryption_key: SecretStr = Field(
        default=SecretStr("ironlayer-dev-secret-change-in-production"),
        validation_alias=AliasChoices("api_credential_encryption_key", "CREDENTIAL_ENCRYPTION_KEY"),
    )

    # Stripe billing integration.
    billing_enabled: bool = False
    stripe_secret_key: SecretStr = SecretStr("")
    stripe_webhook_secret: SecretStr = SecretStr("")
    stripe_price_id_community: str = ""
    stripe_price_id_team: str = ""
    stripe_price_id_enterprise: str = ""
    stripe_metered_price_id: str = ""
    stripe_price_id_per_seat: str = ""

    # Database connection pool settings (BL-096).
    # pool_pre_ping=True is always enabled (connection health checks before use).
    db_pool_size: int = Field(default=20, validation_alias='DB_POOL_SIZE')
    db_max_overflow: int = Field(default=10, validation_alias='DB_MAX_OVERFLOW')
    db_pool_timeout: float = Field(default=30.0, validation_alias='DB_POOL_TIMEOUT')

    # Invoice PDF storage path.
    invoice_storage_path: str = "/var/lib/ironlayer/invoices"


def load_api_settings() -> APISettings:
    """Construct settings from the environment / ``.env`` file."""
    return APISettings()
