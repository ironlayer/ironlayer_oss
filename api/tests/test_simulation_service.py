"""Tests for SimulationService._load_models safety cap (BL-093).

Verifies that:
- _load_models always passes limit=500 to ModelRepository.list_all().
- A warning is logged when the returned list reaches the cap.
- No warning is logged when the list is below the cap.
- The returned ModelDefinitions are correctly built from the repo rows.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.simulation_service import SimulationService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model_row(name: str) -> MagicMock:
    """Return a minimal mock ModelTable row."""
    row = MagicMock()
    row.model_name = name
    row.kind = "FULL_REFRESH"
    row.materialization = "TABLE"
    row.repo_path = f"models/{name}.sql"
    row.time_column = None
    row.unique_key = None
    row.owner = None
    row.tags = None
    row.metadata_json = None
    return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSimulationServiceLoadModelsCap:
    """Verify _load_models respects the 500-row safety cap."""

    @pytest.mark.asyncio
    async def test_list_all_called_with_limit_500(self) -> None:
        """_load_models calls repo.list_all(limit=500) rather than the default."""
        session = AsyncMock()
        service = SimulationService(session=session, tenant_id="tenant-1")

        repo_mock = AsyncMock()
        repo_mock.list_all = AsyncMock(return_value=[_make_model_row("model_a")])

        with patch("api.services.simulation_service.ModelRepository", return_value=repo_mock):
            definitions = await service._load_models()

        repo_mock.list_all.assert_called_once_with(limit=SimulationService._MODEL_LOAD_LIMIT)
        assert len(definitions) == 1
        assert definitions[0].name == "model_a"

    @pytest.mark.asyncio
    async def test_no_warning_when_below_cap(self, caplog: pytest.LogCaptureFixture) -> None:
        """No warning is emitted when fewer than 500 models are returned."""
        session = AsyncMock()
        service = SimulationService(session=session, tenant_id="tenant-2")

        rows = [_make_model_row(f"model_{i}") for i in range(100)]
        repo_mock = AsyncMock()
        repo_mock.list_all = AsyncMock(return_value=rows)

        import logging

        with patch("api.services.simulation_service.ModelRepository", return_value=repo_mock):
            with caplog.at_level(logging.WARNING, logger="api.services.simulation_service"):
                await service._load_models()

        assert not any("cap" in msg.lower() or "incomplete" in msg.lower() for msg in caplog.messages)

    @pytest.mark.asyncio
    async def test_warning_logged_when_at_cap(self, caplog: pytest.LogCaptureFixture) -> None:
        """A warning is logged when exactly _MODEL_LOAD_LIMIT models are returned."""
        session = AsyncMock()
        service = SimulationService(session=session, tenant_id="tenant-3")

        cap = SimulationService._MODEL_LOAD_LIMIT
        rows = [_make_model_row(f"model_{i}") for i in range(cap)]
        repo_mock = AsyncMock()
        repo_mock.list_all = AsyncMock(return_value=rows)

        import logging

        with patch("api.services.simulation_service.ModelRepository", return_value=repo_mock):
            with caplog.at_level(logging.WARNING, logger="api.services.simulation_service"):
                result = await service._load_models()

        assert len(result) == cap
        warning_messages = [m for m in caplog.messages if "incomplete" in m.lower() or "cap" in m.lower()]
        assert warning_messages, "Expected a warning about the graph being incomplete at the cap"

    @pytest.mark.asyncio
    async def test_model_load_limit_constant(self) -> None:
        """_MODEL_LOAD_LIMIT is 500."""
        assert SimulationService._MODEL_LOAD_LIMIT == 500

    @pytest.mark.asyncio
    async def test_empty_repo_returns_empty_list(self) -> None:
        """_load_models returns [] when repo returns no rows."""
        session = AsyncMock()
        service = SimulationService(session=session, tenant_id="tenant-4")

        repo_mock = AsyncMock()
        repo_mock.list_all = AsyncMock(return_value=[])

        with patch("api.services.simulation_service.ModelRepository", return_value=repo_mock):
            result = await service._load_models()

        assert result == []
