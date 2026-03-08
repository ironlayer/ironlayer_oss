"""Configuration for the AI Advisory Engine."""

from __future__ import annotations

from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AISettings(BaseSettings):
    """All configuration for the AI Advisory Engine.

    Values are loaded from environment variables prefixed with ``AI_ENGINE_``
    or from a ``.env`` file in the working directory.
    """

    model_config = SettingsConfigDict(env_prefix="AI_ENGINE_", env_file=".env")

    # --- server ---
    port: int = 8001
    host: str = "0.0.0.0"
    debug: bool = False

    # --- LLM integration (feature-flagged) ---
    llm_enabled: bool = False
    llm_provider: str = "anthropic"
    llm_api_key: SecretStr | None = None
    llm_model: str = "claude-sonnet-4-5-20250929"
    llm_timeout: float = 10.0
    llm_max_tokens: int = 1024

    # --- cost model ---
    cost_model_path: Path = Path("models/cost_model.joblib")

    # --- risk thresholds ---
    risk_auto_approve_threshold: float = 3.0
    risk_manual_review_threshold: float = 7.0

    # --- semantic classifier ---
    semantic_confidence_threshold: float = 0.7  # below this, try LLM

    # --- response cache ---
    cache_enabled: bool = True
    cache_max_entries: int = 10_000


def load_ai_settings() -> AISettings:
    """Factory helper that creates *AISettings* from the environment."""
    return AISettings()
