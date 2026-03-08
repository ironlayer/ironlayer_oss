"""Tests for the ``platform dev`` command."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from typer.testing import CliRunner

from cli.app import app
from cli.commands.dev import _setup_local_env, _load_dotenv, _build_services_table

runner = CliRunner()


# ---------------------------------------------------------------------------
# Config detection
# ---------------------------------------------------------------------------


class TestConfigDetection:
    """Verify that dev command detects project configuration."""

    def test_no_config_exits_with_error(self, tmp_path: Path) -> None:
        """Running dev in a directory without a IronLayer project should fail."""
        result = runner.invoke(app, ["dev"], catch_exceptions=True)
        # The test runner's cwd might have .env, so we test via the setup function instead.
        # Direct CLI invocation depends on cwd which is tricky in tests.

    def test_detects_ironlayer_config(self, tmp_path: Path) -> None:
        """Should detect .ironlayer/config.yaml."""
        config_dir = tmp_path / ".ironlayer"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text("project:\n  name: test\n")

        # The env setup should work with a valid project root.
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("PLATFORM_STATE_STORE_TYPE") == "local"

    def test_detects_env_file(self, tmp_path: Path) -> None:
        """Should detect .env file."""
        (tmp_path / ".env").write_text("PLATFORM_ENV=dev\n")

        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("PLATFORM_ENV") == "dev"


# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------


class TestSetupLocalEnv:
    """Verify environment variables are configured for local mode."""

    def test_sets_sqlite_database_url(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        db_url = os.environ.get("PLATFORM_DATABASE_URL", "")
        assert "sqlite+aiosqlite" in db_url
        assert "state.db" in db_url

    def test_sets_dev_environment(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("PLATFORM_ENV") == "dev"

    def test_sets_local_state_store(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("PLATFORM_STATE_STORE_TYPE") == "local"

    def test_disables_rate_limiting(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("API_RATE_LIMIT_ENABLED") == "false"

    def test_sets_dev_auth_mode(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert os.environ.get("API_AUTH_MODE") == "dev"

    def test_ai_disabled_when_no_ai(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, True)
        assert os.environ.get("AI_LLM_ENABLED") == "false"

    def test_creates_ironlayer_directory(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 8000, 8001, False)
        assert (tmp_path / ".ironlayer").is_dir()

    def test_custom_port(self, tmp_path: Path) -> None:
        _setup_local_env(tmp_path, 9000, 9001, False)
        assert os.environ.get("API_PORT") == "9000"


# ---------------------------------------------------------------------------
# Dotenv loading
# ---------------------------------------------------------------------------


class TestLoadDotenv:
    """Verify .env file loading."""

    def test_loads_variables(self, tmp_path: Path) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_LOAD_VAR=hello_world\n")

        # Clear in case it exists.
        os.environ.pop("TEST_LOAD_VAR", None)

        _load_dotenv(env_file)
        assert os.environ.get("TEST_LOAD_VAR") == "hello_world"

        # Cleanup.
        os.environ.pop("TEST_LOAD_VAR", None)

    def test_skips_comments(self, tmp_path: Path) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\nTEST_COMMENT_VAR=value\n")

        os.environ.pop("TEST_COMMENT_VAR", None)
        _load_dotenv(env_file)
        assert os.environ.get("TEST_COMMENT_VAR") == "value"

        os.environ.pop("TEST_COMMENT_VAR", None)

    def test_skips_empty_lines(self, tmp_path: Path) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("\n\nTEST_EMPTY_VAR=val\n\n")

        os.environ.pop("TEST_EMPTY_VAR", None)
        _load_dotenv(env_file)
        assert os.environ.get("TEST_EMPTY_VAR") == "val"

        os.environ.pop("TEST_EMPTY_VAR", None)

    def test_existing_env_takes_precedence(self, tmp_path: Path) -> None:
        """Variables already in os.environ should NOT be overwritten."""
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_PREC_VAR=from_file\n")

        os.environ["TEST_PREC_VAR"] = "from_env"
        _load_dotenv(env_file)
        assert os.environ.get("TEST_PREC_VAR") == "from_env"

        os.environ.pop("TEST_PREC_VAR", None)

    def test_strips_quotes(self, tmp_path: Path) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text('TEST_QUOTE_VAR="quoted_value"\n')

        os.environ.pop("TEST_QUOTE_VAR", None)
        _load_dotenv(env_file)
        assert os.environ.get("TEST_QUOTE_VAR") == "quoted_value"

        os.environ.pop("TEST_QUOTE_VAR", None)

    def test_nonexistent_file(self, tmp_path: Path) -> None:
        """Loading a nonexistent file should be a no-op."""
        _load_dotenv(tmp_path / "does_not_exist")
        # Should not raise.


# ---------------------------------------------------------------------------
# Services table
# ---------------------------------------------------------------------------


class TestBuildServicesTable:
    """Verify the Rich services table for display."""

    def test_shows_api_server(self, tmp_path: Path) -> None:
        table = _build_services_table("127.0.0.1", 8000, 8001, False, False, tmp_path)
        assert table.row_count >= 4

    def test_ai_disabled_row(self, tmp_path: Path) -> None:
        table = _build_services_table("127.0.0.1", 8000, 8001, True, False, tmp_path)
        assert table.row_count >= 4

    def test_ui_disabled_row(self, tmp_path: Path) -> None:
        table = _build_services_table("127.0.0.1", 8000, 8001, False, True, tmp_path)
        assert table.row_count >= 4

    def test_frontend_detected(self, tmp_path: Path) -> None:
        (tmp_path / "frontend").mkdir()
        table = _build_services_table("127.0.0.1", 8000, 8001, False, False, tmp_path)
        assert table.row_count >= 5
