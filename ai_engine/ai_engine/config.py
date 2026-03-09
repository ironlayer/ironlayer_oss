"""Configuration for the AI Advisory Engine.

Platform environment: The AI engine reads ``PLATFORM_ENV`` (e.g. from the
orchestrator) and uses string checks (e.g. "development", "dev", "local"
for dev-like behavior). Valid values should match the rest of the platform:
use ``dev``, ``staging``, or ``prod`` (or ``production`` where the API
normalizes it). The AI engine does not depend on core_engine; it remains
string-based for this setting.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# BL-077: Explicit allowlist of LLM model names accepted at startup.
# Prevents accidental use of unexpectedly expensive or unknown models.
ALLOWED_LLM_MODELS: frozenset[str] = frozenset(
    {
        "claude-sonnet-4-5-20250929",
        "claude-3-5-haiku-20241022",
        "claude-opus-4-5",
        "claude-3-haiku-20240307",
        "claude-3-sonnet-20240229",
        "claude-3-opus-20240229",
        # Future models can be added here when approved for budget-controlled use.
    }
)


class AISettings(BaseSettings):
    """All configuration for the AI Advisory Engine.

    Values are loaded from environment variables prefixed with ``AI_ENGINE_``
    or from a ``.env`` file in the working directory.
    """

    model_config = SettingsConfigDict(env_prefix="AI_ENGINE_", env_file=".env", extra="ignore")

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

    # --- LLM budget (platform-level; None = unlimited) ---
    llm_daily_budget_usd: float | None = None
    llm_monthly_budget_usd: float | None = None

    @field_validator("llm_model")
    @classmethod
    def _validate_llm_model(cls, v: str) -> str:
        """BL-077: Reject unknown LLM model names at startup.

        Only explicitly allowlisted models are accepted.  This prevents
        accidental use of expensive or unvetted models via misconfigured
        AI_ENGINE_LLM_MODEL environment variables.
        """
        if v not in ALLOWED_LLM_MODELS:
            raise ValueError(
                f"Unknown LLM model '{v}'. "
                f"Allowed models: {sorted(ALLOWED_LLM_MODELS)}"
            )
        logger.info("AI engine LLM model: %s", v)
        return v


def load_ai_settings() -> AISettings:
    """Factory helper that creates *AISettings* from the environment."""
    return AISettings()
