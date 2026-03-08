"""Tests for the versioned prompt template registry (F2)."""

from __future__ import annotations

import pytest

from ai_engine.engines.prompts import (
    PROMPT_REGISTRY,
    PromptTemplate,
    get_prompt,
)


class TestPromptTemplate:
    """Verify the PromptTemplate dataclass."""

    def test_frozen(self) -> None:
        template = PromptTemplate(key="test", version="v1", content="hello", description="test")
        with pytest.raises(AttributeError):
            template.version = "v2"  # type: ignore[misc]

    def test_fields(self) -> None:
        template = PromptTemplate(
            key="test_key",
            version="v42",
            content="system prompt text",
            description="A test template",
        )
        assert template.key == "test_key"
        assert template.version == "v42"
        assert template.content == "system prompt text"
        assert template.description == "A test template"


class TestPromptRegistry:
    """Verify the registry contains the expected prompts."""

    def test_classify_change_system_registered(self) -> None:
        assert "classify_change_system" in PROMPT_REGISTRY

    def test_suggest_optimization_system_registered(self) -> None:
        assert "suggest_optimization_system" in PROMPT_REGISTRY

    def test_classify_change_system_version(self) -> None:
        template = PROMPT_REGISTRY["classify_change_system"]
        assert template.version == "v1"

    def test_suggest_optimization_system_version(self) -> None:
        template = PROMPT_REGISTRY["suggest_optimization_system"]
        assert template.version == "v1"

    def test_classify_change_content_non_empty(self) -> None:
        template = PROMPT_REGISTRY["classify_change_system"]
        assert len(template.content) > 50  # Substantial prompt text

    def test_suggest_optimization_content_non_empty(self) -> None:
        template = PROMPT_REGISTRY["suggest_optimization_system"]
        assert len(template.content) > 50

    def test_classify_change_content_mentions_json(self) -> None:
        """The classifier prompt should instruct JSON output."""
        template = PROMPT_REGISTRY["classify_change_system"]
        assert "JSON" in template.content

    def test_suggest_optimization_content_mentions_json(self) -> None:
        """The optimizer prompt should instruct JSON output."""
        template = PROMPT_REGISTRY["suggest_optimization_system"]
        assert "JSON" in template.content


class TestGetPrompt:
    """Verify the get_prompt() lookup function."""

    def test_get_existing_prompt(self) -> None:
        template = get_prompt("classify_change_system")
        assert isinstance(template, PromptTemplate)
        assert template.key == "classify_change_system"

    def test_get_nonexistent_prompt_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown prompt key"):
            get_prompt("nonexistent_prompt")

    def test_get_prompt_returns_same_object(self) -> None:
        t1 = get_prompt("classify_change_system")
        t2 = get_prompt("classify_change_system")
        assert t1 is t2  # Same frozen object
