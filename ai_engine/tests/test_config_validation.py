"""Tests for AISettings config validation (BL-077).

Covers:
- Known LLM models are accepted at startup
- Unknown model names raise ValidationError
- ALLOWED_LLM_MODELS constant contains the expected entries
"""

from __future__ import annotations

import pytest
from ai_engine.config import ALLOWED_LLM_MODELS, AISettings
from pydantic import ValidationError


class TestLLMModelAllowlist:
    """BL-077: LLM model name validated against allowlist at startup."""

    def test_default_model_is_allowed(self) -> None:
        """The default model (claude-sonnet) must pass validation."""
        settings = AISettings()
        assert settings.llm_model == "claude-sonnet-4-5-20250929"

    @pytest.mark.parametrize("model", sorted(ALLOWED_LLM_MODELS))
    def test_all_allowlisted_models_accepted(self, model: str) -> None:
        """Every model in ALLOWED_LLM_MODELS should pass the validator."""
        settings = AISettings(llm_model=model)
        assert settings.llm_model == model

    def test_unknown_model_raises_validation_error(self) -> None:
        """An unknown model name must raise ValidationError at init time."""
        with pytest.raises(ValidationError, match="Unknown LLM model"):
            AISettings(llm_model="gpt-4o-unknown-model")

    def test_empty_model_name_raises_validation_error(self) -> None:
        """An empty model name is not in the allowlist."""
        with pytest.raises(ValidationError, match="Unknown LLM model"):
            AISettings(llm_model="")

    def test_partial_model_name_raises_validation_error(self) -> None:
        """Prefix of a valid name is still rejected."""
        with pytest.raises(ValidationError, match="Unknown LLM model"):
            AISettings(llm_model="claude-sonnet")

    def test_allowlist_error_includes_valid_options(self) -> None:
        """Error message must list the valid alternatives."""
        with pytest.raises(ValidationError) as exc_info:
            AISettings(llm_model="gpt-4-turbo")
        assert "Allowed models" in str(exc_info.value)

    def test_allowed_llm_models_is_frozenset(self) -> None:
        """ALLOWED_LLM_MODELS must be immutable to prevent accidental mutation."""
        assert isinstance(ALLOWED_LLM_MODELS, frozenset)

    def test_allowed_llm_models_non_empty(self) -> None:
        """At least one model must be in the allowlist."""
        assert len(ALLOWED_LLM_MODELS) > 0

    def test_default_model_in_allowlist(self) -> None:
        """The hardcoded default model must be in the allowlist."""
        assert "claude-sonnet-4-5-20250929" in ALLOWED_LLM_MODELS
