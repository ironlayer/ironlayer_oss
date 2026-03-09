"""Abstract base class for check implementations.

All check types in the check engine must subclass :class:`BaseCheck`
and implement the :meth:`execute` method.
"""

from __future__ import annotations

import abc

from core_engine.checks.models import CheckContext, CheckResult, CheckType


class BaseCheck(abc.ABC):
    """Abstract base for all check implementations.

    Subclasses must implement :attr:`check_type` and :meth:`execute`.
    Check implementations should be stateless; all required data is
    passed via the :class:`CheckContext`.
    """

    @property
    @abc.abstractmethod
    def check_type(self) -> CheckType:
        """The category of check this implementation provides."""

    @abc.abstractmethod
    async def execute(self, context: CheckContext) -> list[CheckResult]:
        """Run the check and return results.

        Parameters
        ----------
        context:
            The execution context containing models and configuration.

        Returns
        -------
        list[CheckResult]
            Results sorted deterministically by (model_name, check_type).
        """
