"""SQL toolkit factory.

Provides :func:`get_sql_toolkit` — the single entry point for consumer code.
Thread-safe singleton with configurable implementation backend.
"""

from __future__ import annotations

import threading
from typing import Callable

from ._protocols import SqlToolkit

_lock = threading.Lock()
_instance: SqlToolkit | None = None
_factory_fn: Callable[[], SqlToolkit] | None = None


def register_implementation(factory_fn: Callable[[], SqlToolkit]) -> None:
    """Register a factory function for creating :class:`SqlToolkit` instances.

    Called once at application startup.  If not called, the default SQLGlot
    implementation is used.
    """
    global _factory_fn, _instance
    with _lock:
        _factory_fn = factory_fn
        _instance = None  # force re-creation on next access


def get_sql_toolkit() -> SqlToolkit:
    """Return the active :class:`SqlToolkit` singleton.

    Thread-safe.  Lazily instantiated on first call.  Defaults to the
    SQLGlot-backed implementation if no custom factory has been registered.
    """
    global _instance
    if _instance is not None:
        return _instance

    with _lock:
        # Double-checked locking
        if _instance is not None:
            return _instance

        if _factory_fn is not None:
            _instance = _factory_fn()
        else:
            from .impl.sqlglot_impl import SqlGlotToolkit

            _instance = SqlGlotToolkit()

        return _instance


def reset_toolkit() -> None:
    """Reset the singleton.  **For testing only.**"""
    global _instance, _factory_fn
    with _lock:
        _instance = None
        _factory_fn = None
