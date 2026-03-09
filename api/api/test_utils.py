"""Test utilities for API tests (e.g. app.state setup when lifespan does not run)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from api.config import APISettings


def set_app_state_for_test(
    app: Any,
    *,
    settings: APISettings,
    session: Any,
    ai_client: Any,
    metering: Any,
) -> None:
    """Populate app.state so middleware and request-scoped deps work when lifespan does not run."""
    app.state.settings = settings
    app.state.ai_client = ai_client
    app.state.metering = metering
    app.state.engine = None

    class _MockSessionCM:
        def __init__(self, s: Any) -> None:
            self._session = s

        async def __aenter__(self) -> Any:
            return self._session

        async def __aexit__(self, *args: Any) -> None:
            pass

    _mock_sf = MagicMock()
    _mock_sf.return_value = _MockSessionCM(session)
    app.state.session_factory = _mock_sf
