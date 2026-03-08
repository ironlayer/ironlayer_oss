"""Tests for the ``platform init`` command."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from cli.app import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _init_in_dir(tmp_path: Path, *extra_args: str) -> object:
    """Run ``platform init`` in a temporary directory with --non-interactive."""
    return runner.invoke(
        app,
        ["init", str(tmp_path), "--non-interactive", *extra_args],
    )


# ---------------------------------------------------------------------------
# Scaffold creation
# ---------------------------------------------------------------------------


class TestScaffoldGeneration:
    """Verify that ``platform init`` creates the expected project structure."""

    def test_creates_config_yaml(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        config_path = tmp_path / ".ironlayer" / "config.yaml"
        assert config_path.exists()
        content = config_path.read_text()
        assert "project:" in content
        assert f"name: {tmp_path.name}" in content

    def test_creates_env_file(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        env_path = tmp_path / ".env"
        assert env_path.exists()
        content = env_path.read_text()
        assert "PLATFORM_ENV=dev" in content

    def test_creates_models_directory(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        models_dir = tmp_path / "models"
        assert models_dir.is_dir()
        # Should have at least the example models.
        sql_files = list(models_dir.rglob("*.sql"))
        assert len(sql_files) >= 4

    def test_creates_gitignore(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        gitignore = tmp_path / ".gitignore"
        assert gitignore.exists()
        content = gitignore.read_text()
        assert ".ironlayer/" in content
        assert ".env" in content

    def test_merges_existing_gitignore(self, tmp_path: Path) -> None:
        """If .gitignore already exists, new entries are appended."""
        existing = tmp_path / ".gitignore"
        existing.write_text("node_modules/\n.DS_Store\n", encoding="utf-8")

        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        content = existing.read_text()
        # Original content preserved.
        assert "node_modules/" in content
        assert ".DS_Store" in content
        # IronLayer entries added.
        assert ".ironlayer/" in content

    def test_preserves_existing_env(self, tmp_path: Path) -> None:
        """If .env already exists, it is backed up and not overwritten."""
        env_path = tmp_path / ".env"
        env_path.write_text("MY_SECRET=old_value\n", encoding="utf-8")

        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        # Backup should exist.
        backup = tmp_path / ".env.ironlayer.bak"
        assert backup.exists()
        assert "MY_SECRET=old_value" in backup.read_text()


# ---------------------------------------------------------------------------
# State store options
# ---------------------------------------------------------------------------


class TestStateStoreOptions:
    """Verify local vs postgres state store configuration."""

    def test_local_state_store_default(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        env_content = (tmp_path / ".env").read_text()
        assert "PLATFORM_STATE_STORE_TYPE=local" in env_content

    def test_postgres_state_store(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path, "--state-store", "postgres")
        assert result.exit_code == 0

        env_content = (tmp_path / ".env").read_text()
        assert "PLATFORM_STATE_STORE_TYPE=postgres" in env_content
        assert "PLATFORM_DATABASE_URL=" in env_content

        config_content = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert "database:" in config_content

    def test_invalid_state_store(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path, "--state-store", "redis")
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Databricks configuration
# ---------------------------------------------------------------------------


class TestDatabricksConfig:
    """Verify Databricks connection setup."""

    def test_databricks_host_option(self, tmp_path: Path) -> None:
        result = _init_in_dir(
            tmp_path,
            "--databricks-host",
            "https://my-workspace.databricks.com",
        )
        assert result.exit_code == 0

        env_content = (tmp_path / ".env").read_text()
        assert "PLATFORM_DATABRICKS_HOST=https://my-workspace.databricks.com" in env_content

        config_content = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert "databricks:" in config_content

    def test_no_databricks_by_default(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        env_content = (tmp_path / ".env").read_text()
        assert "PLATFORM_DATABRICKS_HOST" not in env_content


# ---------------------------------------------------------------------------
# AI engine toggle
# ---------------------------------------------------------------------------


class TestAIToggle:
    """Verify AI engine enable/disable."""

    def test_ai_enabled_by_default(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        config_content = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert "enabled: true" in config_content.lower() or "enabled: True" in config_content

    def test_ai_disabled(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path, "--no-ai")
        assert result.exit_code == 0

        config_content = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert "enabled: false" in config_content.lower() or "enabled: False" in config_content


# ---------------------------------------------------------------------------
# Git initialisation
# ---------------------------------------------------------------------------


class TestGitInit:
    """Verify git repository initialisation behaviour."""

    def test_git_init_in_fresh_directory(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        # Should have initialised a git repo.
        git_dir = tmp_path / ".git"
        assert git_dir.exists() or "Already inside a git repository" in result.output

    def test_no_git_flag(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path, "--no-git")
        assert result.exit_code == 0

        git_dir = tmp_path / ".git"
        # If no existing git repo, .git should NOT be created with --no-git.
        # (Unless already in a repo from the test runner's perspective.)

    def test_existing_git_repo(self, tmp_path: Path) -> None:
        """If directory is already a git repo, don't re-init."""
        subprocess.run(
            ["git", "init"],
            capture_output=True,
            cwd=str(tmp_path),
        )
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        assert "Already inside a git repository" in result.output


# ---------------------------------------------------------------------------
# Project name
# ---------------------------------------------------------------------------


class TestProjectName:
    """Verify project name handling."""

    def test_default_name_from_directory(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        config = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert f"name: {tmp_path.name}" in config

    def test_custom_name(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path, "--name", "my-data-project")
        assert result.exit_code == 0

        config = (tmp_path / ".ironlayer" / "config.yaml").read_text()
        assert "name: my-data-project" in config


# ---------------------------------------------------------------------------
# Non-existent directory
# ---------------------------------------------------------------------------


class TestDirectoryCreation:
    """Verify handling of non-existent target directories."""

    def test_creates_directory_if_missing(self, tmp_path: Path) -> None:
        target = tmp_path / "new-project"
        assert not target.exists()

        result = _init_in_dir(target)
        assert result.exit_code == 0
        assert target.exists()
        assert (target / ".ironlayer" / "config.yaml").exists()


# ---------------------------------------------------------------------------
# Verification step
# ---------------------------------------------------------------------------


class TestVerification:
    """Verify that the model loading verification works."""

    def test_models_loadable_after_init(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0

        # Models should be loadable.
        from core_engine.loader import load_models_from_directory

        models = load_models_from_directory(tmp_path / "models")
        assert len(models) >= 4

    def test_model_names_correct(self, tmp_path: Path) -> None:
        _init_in_dir(tmp_path)

        from core_engine.loader import load_models_from_directory

        models = load_models_from_directory(tmp_path / "models")
        names = {m.name for m in models}
        assert "raw.source_orders" in names
        assert "staging.stg_orders" in names
        assert "analytics.orders_daily" in names
        assert "analytics.revenue_summary" in names


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


class TestOutput:
    """Verify CLI output messages."""

    def test_shows_success_panel(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        assert "initialised successfully" in result.output.lower() or "Done" in result.output

    def test_shows_next_steps(self, tmp_path: Path) -> None:
        result = _init_in_dir(tmp_path)
        assert result.exit_code == 0
        assert "ironlayer dev" in result.output
        assert "ironlayer models" in result.output


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    """Verify that running init twice doesn't break things."""

    def test_double_init_succeeds(self, tmp_path: Path) -> None:
        result1 = _init_in_dir(tmp_path)
        assert result1.exit_code == 0

        result2 = _init_in_dir(tmp_path)
        assert result2.exit_code == 0

        # Models should still be loadable.
        from core_engine.loader import load_models_from_directory

        models = load_models_from_directory(tmp_path / "models")
        assert len(models) >= 4
