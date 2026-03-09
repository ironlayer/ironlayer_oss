"""Tests for APISettings config validation (BL-078, BL-082).

Covers:
- BL-078: Wildcard CORS origins rejected in non-dev environments (hard fail)
- BL-078: Wildcard CORS with credentials always rejected
- BL-078: Dev environment allows wildcard (but still rejects credentials + wildcard)
- Existing CORS credential validation unchanged
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from core_engine.config import PlatformEnv
from api.config import APISettings


class TestCORSWildcardValidation:
    """BL-078: Wildcard CORS hard-rejected in non-dev environments."""

    def test_wildcard_cors_rejected_in_prod(self) -> None:
        """Wildcard CORS must raise ValidationError in production."""
        with pytest.raises(ValidationError, match="Wildcard CORS"):
            APISettings(
                platform_env=PlatformEnv.PROD,
                cors_origins=["*"],
                cors_allow_credentials=False,
            )

    def test_wildcard_cors_rejected_in_staging(self) -> None:
        """Wildcard CORS must raise ValidationError in staging."""
        with pytest.raises(ValidationError, match="Wildcard CORS"):
            APISettings(
                platform_env=PlatformEnv.STAGING,
                cors_origins=["*"],
                cors_allow_credentials=False,
            )

    def test_wildcard_cors_allowed_in_dev(self) -> None:
        """Wildcard CORS must NOT raise in development (for local convenience)."""
        # No credentials + wildcard + dev: should succeed
        settings = APISettings(
            platform_env=PlatformEnv.DEV,
            cors_origins=["*"],
            cors_allow_credentials=False,
        )
        assert "*" in settings.cors_origins

    def test_wildcard_with_credentials_always_rejected(self) -> None:
        """Credentials + wildcard is always rejected regardless of environment."""
        with pytest.raises(ValidationError, match="wildcard origins with credentials"):
            APISettings(
                platform_env=PlatformEnv.DEV,
                cors_origins=["*"],
                cors_allow_credentials=True,
            )

    def test_explicit_origins_allowed_in_prod(self) -> None:
        """Explicit origins with no wildcard must pass in production."""
        settings = APISettings(
            platform_env=PlatformEnv.PROD,
            cors_origins=["https://app.ironlayer.io"],
            cors_allow_credentials=True,
        )
        assert settings.cors_origins == ["https://app.ironlayer.io"]

    def test_wildcard_mixed_with_explicit_origin_rejected_in_prod(self) -> None:
        """Having '*' in a mixed list still triggers the hard-fail in prod."""
        with pytest.raises(ValidationError, match="Wildcard CORS"):
            APISettings(
                platform_env=PlatformEnv.PROD,
                cors_origins=["https://app.ironlayer.io", "*"],
                cors_allow_credentials=False,
            )

    def test_default_settings_are_valid(self) -> None:
        """Default APISettings must pass validation without errors."""
        # Default platform_env=DEV, cors_origins=["http://localhost:3000"]
        settings = APISettings()
        assert settings.cors_origins == ["http://localhost:3000"]
