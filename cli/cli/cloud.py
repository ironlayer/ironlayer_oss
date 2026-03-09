"""Cloud authentication and configuration management.

Stores API URL in ``~/.ironlayer/config.toml`` and API token in the OS
keychain (macOS Keychain, GNOME Keyring, Windows Credential Locker) when
the ``keyring`` package is installed.  Falls back to the TOML file for
token storage when keyring is unavailable (e.g. headless CI runners).
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
_KEYRING_SERVICE = "ironlayer-cli"
_KEYRING_USERNAME = "api_token"  # Legacy username kept for keyring namespace consistency
_KEYRING_ACCESS_USERNAME = "access_token"
_KEYRING_REFRESH_USERNAME = "refresh_token"

# Legacy credentials file written by helpers.py (pre-BL-105).
# Kept so migrate_legacy_credentials() can detect and upgrade it.
_LEGACY_CREDENTIALS_FILE = Path.home() / ".ironlayer" / "credentials.json"


def _keyring_get() -> str | None:
    """Try to read the token from the OS keychain.  Returns ``None`` on any failure."""
    try:
        import keyring  # type: ignore[import-untyped]

        return keyring.get_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
    except Exception:
        return None


def _keyring_set(token: str) -> bool:
    """Try to store the token in the OS keychain.  Returns ``True`` on success."""
    try:
        import keyring  # type: ignore[import-untyped]

        keyring.set_password(_KEYRING_SERVICE, _KEYRING_USERNAME, token)
        return True
    except Exception:
        return False


def _keyring_delete() -> None:
    """Try to remove the token from the OS keychain.  Silently ignores errors."""
    try:
        import keyring  # type: ignore[import-untyped]

        keyring.delete_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
    except Exception:
        pass


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
    """Return the stored API token, or ``None`` if not authenticated.

    Checks the OS keychain first; falls back to the TOML file.
    """
    token = _keyring_get()
    if token is not None:
        return token
    config = load_cloud_config()
    return config.get("cloud", {}).get("api_token")


def load_api_url() -> str:
    """Return the configured API URL, defaulting to production."""
    config = load_cloud_config()
    return config.get("cloud", {}).get("api_url", "https://api.ironlayer.app")


def save_cloud_config(api_url: str, api_token: str) -> None:
    """Save cloud credentials with secure storage.

    The API token is stored in the OS keychain when ``keyring`` is
    available.  The TOML file always stores the API URL and is used as a
    fallback for the token when keychain is not available.

    The file is written with ``0o600`` (owner read/write only) to prevent
    other users on the system from reading any stored credentials.
    """
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    used_keyring = _keyring_set(api_token)

    # Always persist api_url in the TOML file.  Persist the token there
    # too when keyring is unavailable so the CLI still works headless.
    if used_keyring:
        content = f'[cloud]\napi_url = "{api_url}"\n'
    else:
        content = f'[cloud]\napi_url = "{api_url}"\napi_token = "{api_token}"\n'
    _CONFIG_FILE.write_text(content, encoding="utf-8")

    # Restrict permissions to owner only (0o600).
    os.chmod(_CONFIG_FILE, stat.S_IRUSR | stat.S_IWUSR)


def clear_cloud_config() -> None:
    """Remove stored cloud credentials from both keychain and config file."""
    _keyring_delete()
    if _CONFIG_FILE.exists():
        _CONFIG_FILE.unlink()


# ---------------------------------------------------------------------------
# Full credential set (BL-105) — access token + refresh token + email + URL
# ---------------------------------------------------------------------------


def save_full_credentials(
    api_url: str,
    access_token: str,
    refresh_token: str,
    email: str,
) -> None:
    """Persist the full post-login credential set securely.

    - ``access_token`` and ``refresh_token`` are stored in the OS keychain
      when the ``keyring`` package is available; otherwise they fall back to
      the TOML config file (written with ``0o600`` permissions).
    - ``api_url`` and ``email`` are always written to the TOML file (they
      are not secret but are needed for display and reconnection).

    This replaces the legacy plaintext JSON approach used by helpers.py
    (pre-BL-105).  On first call, any legacy ``credentials.json`` is
    automatically migrated and removed.
    """
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Try OS keychain for sensitive tokens.
    access_in_keyring = False
    refresh_in_keyring = False
    try:
        import keyring  # type: ignore[import-untyped]

        keyring.set_password(_KEYRING_SERVICE, _KEYRING_ACCESS_USERNAME, access_token)
        access_in_keyring = True
        keyring.set_password(_KEYRING_SERVICE, _KEYRING_REFRESH_USERNAME, refresh_token)
        refresh_in_keyring = True
    except Exception:
        pass  # Fall back to TOML file storage below.

    # Build TOML content — always include non-sensitive fields.
    lines = ["[cloud]", f'api_url = "{api_url}"', f'email = "{email}"']
    if not access_in_keyring:
        lines.append(f'access_token = "{access_token}"')
    if not refresh_in_keyring:
        lines.append(f'refresh_token = "{refresh_token}"')
    content = "\n".join(lines) + "\n"

    _CONFIG_FILE.write_text(content, encoding="utf-8")
    os.chmod(_CONFIG_FILE, stat.S_IRUSR | stat.S_IWUSR)


def load_full_credentials() -> dict[str, str] | None:
    """Return the stored credential set, or ``None`` if not authenticated.

    Checks the OS keychain for tokens first; falls back to the TOML file.
    Also transparently migrates any legacy ``credentials.json`` on first call.

    Returns a dict with keys: ``api_url``, ``access_token``, ``refresh_token``,
    ``email``.  Returns ``None`` if no credentials are stored.
    """
    # Transparent migration from the legacy plaintext JSON (helpers.py).
    migrate_legacy_credentials()

    config = load_cloud_config()
    cloud = config.get("cloud", {})

    api_url = cloud.get("api_url", "")
    email = cloud.get("email", "")

    # Access token: keychain takes priority, then TOML fallback.
    access_token: str | None = None
    try:
        import keyring  # type: ignore[import-untyped]

        access_token = keyring.get_password(_KEYRING_SERVICE, _KEYRING_ACCESS_USERNAME)
    except Exception:
        pass
    if not access_token:
        access_token = cloud.get("access_token", "")

    # Refresh token: same pattern.
    refresh_token: str | None = None
    try:
        import keyring  # type: ignore[import-untyped]

        refresh_token = keyring.get_password(_KEYRING_SERVICE, _KEYRING_REFRESH_USERNAME)
    except Exception:
        pass
    if not refresh_token:
        refresh_token = cloud.get("refresh_token", "")

    if not api_url or not access_token:
        return None

    return {
        "api_url": api_url,
        "access_token": access_token,
        "refresh_token": refresh_token or "",
        "email": email,
    }


def delete_full_credentials() -> None:
    """Remove all stored credentials from the OS keychain and the config file."""
    try:
        import keyring  # type: ignore[import-untyped]

        for username in (_KEYRING_ACCESS_USERNAME, _KEYRING_REFRESH_USERNAME, _KEYRING_USERNAME):
            try:
                keyring.delete_password(_KEYRING_SERVICE, username)
            except Exception:
                pass
    except Exception:
        pass

    if _CONFIG_FILE.exists():
        _CONFIG_FILE.unlink()

    # Also remove legacy file if it somehow still exists.
    if _LEGACY_CREDENTIALS_FILE.exists():
        _LEGACY_CREDENTIALS_FILE.unlink()


def migrate_legacy_credentials() -> None:
    """One-time migration from legacy plaintext ``credentials.json`` to keyring/TOML.

    If ``~/.ironlayer/credentials.json`` exists, its contents are read,
    stored via :func:`save_full_credentials`, and then the JSON file is
    deleted.  Subsequent calls are no-ops.
    """
    if not _LEGACY_CREDENTIALS_FILE.exists():
        return

    try:
        import json

        data = json.loads(_LEGACY_CREDENTIALS_FILE.read_text(encoding="utf-8"))
        api_url = data.get("api_url", "")
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", "")
        email = data.get("email", "")

        if api_url and access_token:
            save_full_credentials(api_url, access_token, refresh_token, email)

        _LEGACY_CREDENTIALS_FILE.unlink()
    except Exception:
        # Never crash the CLI due to a migration failure; the user can re-login.
        pass
