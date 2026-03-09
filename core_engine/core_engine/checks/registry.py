"""Check registry for discovering and managing check implementations.

Provides a central registry where check types are registered and
looked up by :class:`CheckType` enum value.
"""

from __future__ import annotations

import logging

from core_engine.checks.base import BaseCheck
from core_engine.checks.models import CheckType

logger = logging.getLogger(__name__)


class CheckRegistry:
    """Registry for check implementations.

    Maintains a mapping of :class:`CheckType` to :class:`BaseCheck`
    instances.  The :class:`CheckEngine` uses this to discover which
    checks are available for execution.
    """

    def __init__(self) -> None:
        self._checks: dict[CheckType, BaseCheck] = {}

    def register(self, check: BaseCheck) -> None:
        """Register a check implementation.

        Parameters
        ----------
        check:
            The check instance to register.  Its ``check_type`` property
            determines the key under which it is stored.

        Raises
        ------
        ValueError
            If a check with the same ``check_type`` is already registered.
        """
        if check.check_type in self._checks:
            raise ValueError(
                f"Check type {check.check_type.value} is already registered. "
                f"Unregister the existing check first."
            )
        self._checks[check.check_type] = check
        logger.debug("Registered check type: %s", check.check_type.value)

    def unregister(self, check_type: CheckType) -> None:
        """Remove a check implementation from the registry.

        Parameters
        ----------
        check_type:
            The check type to remove.

        Raises
        ------
        KeyError
            If the check type is not registered.
        """
        if check_type not in self._checks:
            raise KeyError(f"Check type {check_type.value} is not registered.")
        del self._checks[check_type]
        logger.debug("Unregistered check type: %s", check_type.value)

    def get(self, check_type: CheckType) -> BaseCheck | None:
        """Look up a check implementation by type.

        Returns ``None`` if the type is not registered.
        """
        return self._checks.get(check_type)

    def get_all(self) -> list[BaseCheck]:
        """Return all registered checks, sorted by check type."""
        return [self._checks[ct] for ct in sorted(self._checks, key=lambda ct: ct.value)]

    def get_types(self) -> list[CheckType]:
        """Return all registered check types, sorted."""
        return sorted(self._checks.keys(), key=lambda ct: ct.value)

    def __len__(self) -> int:
        return len(self._checks)

    def __contains__(self, check_type: CheckType) -> bool:
        return check_type in self._checks
