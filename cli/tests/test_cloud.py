"""Comprehensive tests for cli/cli/cloud.py -- cloud authentication and configuration.

Covers all functions: load_cloud_config, load_stored_token, load_api_url,
save_cloud_config, clear_cloud_config.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# load_cloud_config
# ---------------------------------------------------------------------------


class TestLoadCloudConfig:
    """Tests for load_cloud_config()."""

    def test_returns_empty_dict_when_file_missing(self, tmp_path: Path) -> None:
        """When config file doesn't exist, return empty dict."""
        with patch("cli.cloud._CONFIG_FILE", tmp_path / "nonexistent" / "config.toml"):
            from cli.cloud import load_cloud_config

            result = load_cloud_config()
            assert result == {}

    def test_loads_valid_toml(self, tmp_path: Path) -> None:
        """A valid TOML file should be parsed correctly."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_url = "https://api.example.com"\napi_token = "tok-123"\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_cloud_config

            result = load_cloud_config()
            assert result["cloud"]["api_url"] == "https://api.example.com"
            assert result["cloud"]["api_token"] == "tok-123"

    def test_returns_empty_dict_on_parse_error(self, tmp_path: Path) -> None:
        """Malformed TOML should return empty dict, not raise."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("this is not valid toml [[[", encoding="utf-8")
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_cloud_config

            result = load_cloud_config()
            assert result == {}

    def test_returns_empty_dict_on_binary_content(self, tmp_path: Path) -> None:
        """Binary file content should return empty dict, not raise."""
        config_file = tmp_path / "config.toml"
        config_file.write_bytes(b"\x00\x01\x02\xff\xfe")
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_cloud_config

            result = load_cloud_config()
            assert result == {}

    def test_loads_nested_sections(self, tmp_path: Path) -> None:
        """TOML with multiple sections should be parsed correctly."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_url = "https://api.ironlayer.app"\napi_token = "abc"\n\n[settings]\nverbose = true\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_cloud_config

            result = load_cloud_config()
            assert result["cloud"]["api_url"] == "https://api.ironlayer.app"
            assert result["settings"]["verbose"] is True


# ---------------------------------------------------------------------------
# load_stored_token
# ---------------------------------------------------------------------------


class TestLoadStoredToken:
    """Tests for load_stored_token()."""

    def test_returns_token_when_present(self, tmp_path: Path) -> None:
        """Should return the api_token from [cloud] section."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_token = "my-secret-token"\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_stored_token

            assert load_stored_token() == "my-secret-token"

    def test_returns_none_when_no_file(self, tmp_path: Path) -> None:
        """Should return None when config file doesn't exist."""
        with patch("cli.cloud._CONFIG_FILE", tmp_path / "missing.toml"):
            from cli.cloud import load_stored_token

            assert load_stored_token() is None

    def test_returns_none_when_no_cloud_section(self, tmp_path: Path) -> None:
        """Should return None when [cloud] section is missing."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[settings]\nverbose = true\n", encoding="utf-8")
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_stored_token

            assert load_stored_token() is None

    def test_returns_none_when_no_token_key(self, tmp_path: Path) -> None:
        """Should return None when api_token key is missing from [cloud]."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_url = "https://api.ironlayer.app"\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_stored_token

            assert load_stored_token() is None


# ---------------------------------------------------------------------------
# load_api_url
# ---------------------------------------------------------------------------


class TestLoadApiUrl:
    """Tests for load_api_url()."""

    def test_returns_configured_url(self, tmp_path: Path) -> None:
        """Should return the api_url from [cloud] section."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_url = "https://custom-api.example.com"\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_api_url

            assert load_api_url() == "https://custom-api.example.com"

    def test_returns_default_when_no_file(self, tmp_path: Path) -> None:
        """Should return production default when config file missing."""
        with patch("cli.cloud._CONFIG_FILE", tmp_path / "missing.toml"):
            from cli.cloud import load_api_url

            assert load_api_url() == "https://api.ironlayer.app"

    def test_returns_default_when_no_url_key(self, tmp_path: Path) -> None:
        """Should return default when api_url key is missing."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[cloud]\napi_token = "tok"\n',
            encoding="utf-8",
        )
        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import load_api_url

            assert load_api_url() == "https://api.ironlayer.app"


# ---------------------------------------------------------------------------
# save_cloud_config
# ---------------------------------------------------------------------------


class TestSaveCloudConfig:
    """Tests for save_cloud_config()."""

    def test_creates_config_file(self, tmp_path: Path) -> None:
        """Should create the config file with cloud credentials."""
        config_dir = tmp_path / ".ironlayer"
        config_file = config_dir / "config.toml"
        with (
            patch("cli.cloud._CONFIG_DIR", config_dir),
            patch("cli.cloud._CONFIG_FILE", config_file),
        ):
            from cli.cloud import save_cloud_config

            save_cloud_config("https://api.ironlayer.app", "my-token-123")

            assert config_file.exists()
            content = config_file.read_text(encoding="utf-8")
            assert "[cloud]" in content
            assert 'api_url = "https://api.ironlayer.app"' in content
            assert 'api_token = "my-token-123"' in content

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Should create the .ironlayer directory if it doesn't exist."""
        config_dir = tmp_path / "deep" / "nested" / ".ironlayer"
        config_file = config_dir / "config.toml"
        with (
            patch("cli.cloud._CONFIG_DIR", config_dir),
            patch("cli.cloud._CONFIG_FILE", config_file),
        ):
            from cli.cloud import save_cloud_config

            save_cloud_config("https://api.example.com", "tok")

            assert config_dir.exists()
            assert config_file.exists()

    def test_sets_secure_permissions(self, tmp_path: Path) -> None:
        """Config file should have 0600 permissions (owner read/write only)."""
        config_dir = tmp_path / ".ironlayer"
        config_file = config_dir / "config.toml"
        with (
            patch("cli.cloud._CONFIG_DIR", config_dir),
            patch("cli.cloud._CONFIG_FILE", config_file),
        ):
            from cli.cloud import save_cloud_config

            save_cloud_config("https://api.ironlayer.app", "secret")

            file_stat = config_file.stat()
            mode = stat.S_IMODE(file_stat.st_mode)
            assert mode == 0o600

    def test_overwrites_existing_config(self, tmp_path: Path) -> None:
        """Should overwrite an existing config file."""
        config_dir = tmp_path / ".ironlayer"
        config_file = config_dir / "config.toml"
        config_dir.mkdir(parents=True)
        config_file.write_text('[cloud]\napi_token = "old-token"\n', encoding="utf-8")

        with (
            patch("cli.cloud._CONFIG_DIR", config_dir),
            patch("cli.cloud._CONFIG_FILE", config_file),
        ):
            from cli.cloud import save_cloud_config

            save_cloud_config("https://api.ironlayer.app", "new-token")

            content = config_file.read_text(encoding="utf-8")
            assert "new-token" in content
            assert "old-token" not in content


# ---------------------------------------------------------------------------
# clear_cloud_config
# ---------------------------------------------------------------------------


class TestClearCloudConfig:
    """Tests for clear_cloud_config()."""

    def test_removes_config_file(self, tmp_path: Path) -> None:
        """Should remove the config file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[cloud]\napi_token = 'secret'\n", encoding="utf-8")

        with patch("cli.cloud._CONFIG_FILE", config_file):
            from cli.cloud import clear_cloud_config

            clear_cloud_config()

            assert not config_file.exists()

    def test_noop_when_file_missing(self, tmp_path: Path) -> None:
        """Should silently succeed if config file doesn't exist."""
        with patch("cli.cloud._CONFIG_FILE", tmp_path / "missing.toml"):
            from cli.cloud import clear_cloud_config

            # Should not raise.
            clear_cloud_config()
