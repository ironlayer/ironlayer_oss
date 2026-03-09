"""Global CLI option state set by the Typer callback and read by commands.

Commands must not mutate this state; only the app callback sets it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

# Set by app callback; read by commands and helpers.
_json_output: bool = False
_metrics_file: Path | None = None
_env: str = "dev"


def set_global_options(
    json_output: bool = False,
    metrics_file: Path | None = None,
    env: str = "dev",
) -> None:
    """Called by the app callback to store global option values."""
    global _json_output, _metrics_file, _env  # noqa: PLW0603
    _json_output = json_output
    _metrics_file = metrics_file
    _env = env


def get_json_output() -> bool:
    return _json_output


def get_metrics_file() -> Path | None:
    return _metrics_file


def get_env() -> str:
    return _env


def emit_metrics(event: str, data: dict) -> None:
    """Append a timestamped metrics event to the metrics file, if configured.

    Failures are swallowed so metrics never break command execution.
    """
    if _metrics_file is None:
        return
    record = {
        "event": event,
        "timestamp": datetime.now(UTC).isoformat(),
        "data": data,
    }
    try:
        with _metrics_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    except OSError:
        pass
