"""Cloud authentication and configuration management.

Stores API credentials in ``~/.ironlayer/config.toml`` for connecting
the local CLI to IronLayer Cloud.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover – Python < 3.11 fallback
    import tomli as tomllib  # type: ignore[no-redef]

_CONFIG_DIR = Path.home() / ".ironlayer"
_CONFIG_FILE = _CONFIG_DIR / "config.toml"


def load_cloud_config() -> dict[str, Any]:
    """Load cloud configuration from ``~/.ironlayer/config.toml``.

    Returns an empty dict if the file does not exist or cannot be parsed.
    """
    if not _CONFIG_FILE.exists():
        return {}
    try:
        with open(_CONFIG_FILE, "rb") as fh:
            return tomllib.load(fh)
    except Exception:
        return {}


def load_stored_token() -> str | None:
    """Return the stored API token, or ``None`` if not authenticated."""
    config = load_cloud_config()
    return config.get("cloud", {}).get("api_token")


def load_api_url() -> str:
    """Return the configured API URL, defaulting to production."""
    config = load_cloud_config()
    return config.get("cloud", {}).get("api_url", "https://api.ironlayer.app")


def save_cloud_config(api_url: str, api_token: str) -> None:
    """Save cloud credentials to ``~/.ironlayer/config.toml`` with secure permissions.

    The file is written with ``0o600`` (owner read/write only) to prevent
    other users on the system from reading the stored API token.
    """
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    content = f'[cloud]\napi_url = "{api_url}"\napi_token = "{api_token}"\n'
    _CONFIG_FILE.write_text(content, encoding="utf-8")

    # Restrict permissions to owner only (0o600).
    os.chmod(_CONFIG_FILE, stat.S_IRUSR | stat.S_IWUSR)


def clear_cloud_config() -> None:
    """Remove stored cloud credentials."""
    if _CONFIG_FILE.exists():
        _CONFIG_FILE.unlink()
