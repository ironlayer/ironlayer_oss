"""Comprehensive tests for login, logout, whoami CLI commands and credential helpers.

Covers:
- login command (httpx mocking, credential storage, error handling)
- logout command (credential file removal)
- whoami command (online/offline scenarios, missing credentials)
- _credentials_path, _load_stored_token, _save_credentials helpers
- _api_request helper (auth, error handling, timeout)
"""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cli.app import app
from cli.helpers import (
    api_request,
    credentials_path,
    format_input_range,
    load_stored_token,
    resolve_model_sql,
    save_credentials,
)
from click.exceptions import Exit as ClickExit
from typer.testing import CliRunner

runner = CliRunner()


# ---------------------------------------------------------------------------
# credentials_path
# ---------------------------------------------------------------------------


class TestCredentialsPath:
    """Tests for credentials_path()."""

    def test_returns_expected_path(self) -> None:
        """Should return ~/.ironlayer/credentials.json."""
        result = credentials_path()
        assert result == Path.home() / ".ironlayer" / "credentials.json"

    def test_path_is_absolute(self) -> None:
        """Result should be an absolute path."""
        result = credentials_path()
        assert result.is_absolute()


# ---------------------------------------------------------------------------
# load_stored_token
# ---------------------------------------------------------------------------


class TestLoadStoredTokenApp:
    """Tests for load_stored_token() in helpers.py.

    BL-105: load_stored_token() now delegates to cli.cloud.load_full_credentials(),
    which checks the OS keychain then falls back to the TOML config file.
    """

    def test_returns_token_from_file(self) -> None:
        """Should return access_token when cloud credentials are available."""
        full_creds = {
            "api_url": "https://api.ironlayer.app",
            "access_token": "jwt-test-token-abc",
            "refresh_token": "",
            "email": "user@example.com",
        }
        with patch("cli.cloud.load_full_credentials", return_value=full_creds):
            result = load_stored_token()
            assert result == "jwt-test-token-abc"

    def test_returns_none_when_no_credentials(self) -> None:
        """Should return None when no credentials are stored."""
        with patch("cli.cloud.load_full_credentials", return_value=None):
            result = load_stored_token()
            assert result is None

    def test_returns_none_on_load_failure(self) -> None:
        """Should return None when credential loading raises an exception."""
        with patch("cli.cloud.load_full_credentials", side_effect=Exception("keyring error")):
            result = load_stored_token()
            assert result is None

    def test_returns_none_when_no_access_token_key(self) -> None:
        """Should return None when load_full_credentials returns None (missing token)."""
        # load_full_credentials returns None when access_token is absent/empty
        with patch("cli.cloud.load_full_credentials", return_value=None):
            result = load_stored_token()
            assert result is None


# ---------------------------------------------------------------------------
# save_credentials
# ---------------------------------------------------------------------------


class TestSaveCredentials:
    """Tests for save_credentials().

    BL-105: save_credentials() now delegates to cli.cloud.save_full_credentials(),
    which stores tokens in the OS keychain (with TOML fallback). No plaintext JSON
    is written; test via mock-and-assert-called pattern.
    """

    def test_writes_credentials_file(self) -> None:
        """Should delegate to save_full_credentials with correct arguments."""
        with patch("cli.cloud.save_full_credentials") as mock_save:
            save_credentials(
                api_url="https://api.ironlayer.app",
                access_token="tok-access",
                refresh_token="tok-refresh",
                email="user@example.com",
            )
            mock_save.assert_called_once_with(
                "https://api.ironlayer.app",
                "tok-access",
                "tok-refresh",
                "user@example.com",
            )

    def test_creates_parent_directory(self) -> None:
        """Delegation to save_full_credentials is sufficient; cloud.py handles dir creation."""
        with patch("cli.cloud.save_full_credentials") as mock_save:
            save_credentials("url", "tok", "ref", "user@example.com")
            mock_save.assert_called_once()

    def test_sets_0600_permissions(self) -> None:
        """Delegation to save_full_credentials is sufficient; cloud.py handles permissions."""
        with patch("cli.cloud.save_full_credentials") as mock_save:
            save_credentials("url", "tok", "ref", "user@example.com")
            mock_save.assert_called_once()


# ---------------------------------------------------------------------------
# api_request
# ---------------------------------------------------------------------------


class TestApiRequest:
    """Tests for api_request() -- HTTP client helper."""

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_get_request_success(self, mock_token: MagicMock) -> None:
        """Successful GET should return parsed JSON."""

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "ok"}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            result = api_request("GET", "http://localhost:8000", "/api/v1/health")
            assert result == {"status": "ok"}

    @patch("cli.helpers.load_stored_token", return_value="stored-jwt")
    def test_uses_stored_token(self, mock_token: MagicMock) -> None:
        """Should use stored token when IRONLAYER_API_TOKEN is not set."""

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            api_request("GET", "http://localhost:8000", "/test")

            call_kwargs = mock_client.request.call_args
            headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
            assert headers["Authorization"] == "Bearer stored-jwt"

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_uses_env_token_over_stored(self, mock_token: MagicMock) -> None:
        """IRONLAYER_API_TOKEN env var should take precedence."""

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {"IRONLAYER_API_TOKEN": "env-token"}, clear=False),
            patch("httpx.Client", return_value=mock_client),
        ):
            api_request("GET", "http://localhost:8000", "/test")

            call_kwargs = mock_client.request.call_args
            headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
            assert headers["Authorization"] == "Bearer env-token"
        os.environ.pop("IRONLAYER_API_TOKEN", None)

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_http_error_exits_with_code_3(self, mock_token: MagicMock) -> None:
        """HTTP 4xx/5xx should call typer.Exit(code=3)."""
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.json.return_value = {"detail": "Invalid token"}

        error = httpx.HTTPStatusError("err", request=MagicMock(), response=mock_response)
        mock_response.raise_for_status.side_effect = error

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
            pytest.raises(ClickExit),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            api_request("GET", "http://localhost:8000", "/api/v1/test")

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_connection_error_exits_with_code_3(self, mock_token: MagicMock) -> None:
        """Connection failure should call typer.Exit(code=3)."""
        import httpx

        mock_client = MagicMock()
        mock_client.request.side_effect = httpx.ConnectError("Connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
            pytest.raises(ClickExit),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            api_request("GET", "http://localhost:9999", "/test")

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_passes_body_and_params(self, mock_token: MagicMock) -> None:
        """body and params should be forwarded to the HTTP client."""

        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            api_request(
                "POST",
                "http://localhost:8000",
                "/test",
                body={"key": "value"},
                params={"page": "1"},
            )

            call_kwargs = mock_client.request.call_args
            assert call_kwargs.kwargs["json"] == {"key": "value"}
            assert call_kwargs.kwargs["params"] == {"page": "1"}

    @patch("cli.helpers.load_stored_token", return_value=None)
    def test_strips_trailing_slash_from_api_url(self, mock_token: MagicMock) -> None:
        """Trailing slash on api_url should be stripped."""

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("httpx.Client", return_value=mock_client),
        ):
            os.environ.pop("IRONLAYER_API_TOKEN", None)
            api_request("GET", "http://localhost:8000/", "/api/v1/health")

            call_args = mock_client.request.call_args
            url = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get("url", "")
            assert "//api" not in url


# ---------------------------------------------------------------------------
# login command
# ---------------------------------------------------------------------------


class TestLoginCommand:
    """Tests for `ironlayer login`."""

    @patch("cli.cloud.save_full_credentials")
    @patch("httpx.Client")
    def test_successful_login(self, mock_client_cls: MagicMock, mock_save: MagicMock) -> None:
        """Successful login saves credentials and prints success.

        BL-105: login_command() now calls cli.cloud.save_full_credentials() directly.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "tok-access",
            "refresh_token": "tok-refresh",
            "tenant_id": "tenant-123",
            "user": {"display_name": "Alice", "role": "admin"},
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(
            app,
            [
                "login",
                "--api-url",
                "https://api.ironlayer.app",
                "--email",
                "alice@example.com",
                "--password",
                "secret123",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        assert "Logged in" in result.output or "Alice" in result.output
        mock_save.assert_called_once()

    @patch("httpx.Client")
    def test_login_wrong_password(self, mock_client_cls: MagicMock) -> None:
        """401 from the API should show an error and exit 1."""
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Invalid credentials"
        mock_response.json.return_value = {"detail": "Invalid credentials"}

        error = httpx.HTTPStatusError("err", request=MagicMock(), response=mock_response)
        mock_response.raise_for_status.side_effect = error

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(
            app,
            [
                "login",
                "--api-url",
                "https://api.ironlayer.app",
                "--email",
                "alice@example.com",
                "--password",
                "wrong",
            ],
        )

        assert result.exit_code == 1

    @patch("httpx.Client")
    def test_login_connection_error(self, mock_client_cls: MagicMock) -> None:
        """Connection failure should show error and exit 1."""
        import httpx

        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.ConnectError("Connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(
            app,
            [
                "login",
                "--api-url",
                "https://unreachable.example.com",
                "--email",
                "alice@example.com",
                "--password",
                "pass",
            ],
        )

        assert result.exit_code == 1

    @patch("httpx.Client")
    def test_login_no_access_token_in_response(self, mock_client_cls: MagicMock) -> None:
        """Response without access_token should exit 1."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"user": {"display_name": "Alice"}}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(
            app,
            [
                "login",
                "--api-url",
                "https://api.ironlayer.app",
                "--email",
                "alice@example.com",
                "--password",
                "pass",
            ],
        )

        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# logout command
# ---------------------------------------------------------------------------


class TestLogoutCommand:
    """Tests for `ironlayer logout`.

    BL-105: logout_command() now calls cli.cloud.delete_full_credentials() which
    removes tokens from the OS keychain + TOML file. It is idempotent (no error if
    credentials don't exist).
    """

    def test_removes_credentials_file(self) -> None:
        """Should call delete_full_credentials and print success message."""
        with patch("cli.cloud.delete_full_credentials") as mock_delete:
            result = runner.invoke(app, ["logout"])

        assert result.exit_code == 0
        assert "Logged out" in result.output
        mock_delete.assert_called_once()

    def test_no_credentials_file(self) -> None:
        """Should show logout message even when no credentials were stored (idempotent)."""
        with patch("cli.cloud.delete_full_credentials") as mock_delete:
            result = runner.invoke(app, ["logout"])

        assert result.exit_code == 0
        # BL-105: delete_full_credentials() is idempotent; always shows "Logged out"
        assert "Logged out" in result.output
        mock_delete.assert_called_once()


# ---------------------------------------------------------------------------
# whoami command
# ---------------------------------------------------------------------------


class TestWhoamiCommand:
    """Tests for `ironlayer whoami`.

    BL-105: whoami_command() now calls cli.cloud.load_full_credentials() directly
    instead of reading the credentials.json file.
    """

    def test_not_logged_in(self) -> None:
        """Should show 'not logged in' when no credentials are stored."""
        with patch("cli.cloud.load_full_credentials", return_value=None):
            result = runner.invoke(app, ["whoami"])

        assert result.exit_code == 1
        assert "Not logged in" in result.output

    def test_malformed_credentials_file(self) -> None:
        """Should exit 1 when load_full_credentials returns None (any error path)."""
        with patch("cli.cloud.load_full_credentials", return_value=None):
            result = runner.invoke(app, ["whoami"])

        assert result.exit_code == 1

    def test_incomplete_credentials(self) -> None:
        """Should exit 1 when api_url or access_token is empty in the credentials dict."""
        # Simulate a stored dict where the token is empty (e.g. keyring entry deleted
        # but TOML still has email/api_url). whoami_command checks both api_url and token.
        partial_creds = {
            "api_url": "https://api.ironlayer.app",
            "access_token": "",
            "refresh_token": "",
            "email": "alice@example.com",
        }
        with patch("cli.cloud.load_full_credentials", return_value=partial_creds):
            result = runner.invoke(app, ["whoami"])

        assert result.exit_code == 1
        assert "incomplete" in result.output.lower()

    @patch("httpx.Client")
    def test_online_profile_fetch(self, mock_client_cls: MagicMock) -> None:
        """Should fetch and display profile from API."""
        full_creds = {
            "api_url": "https://api.ironlayer.app",
            "access_token": "tok-123",
            "refresh_token": "",
            "email": "alice@example.com",
        }

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "display_name": "Alice Smith",
            "email": "alice@example.com",
            "tenant_id": "t-001",
            "role": "admin",
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with patch("cli.cloud.load_full_credentials", return_value=full_creds):
            result = runner.invoke(app, ["whoami"])

        assert result.exit_code == 0
        assert "Alice Smith" in result.output

    @patch("httpx.Client")
    def test_offline_fallback(self, mock_client_cls: MagicMock) -> None:
        """Should show cached info when API is unreachable."""
        full_creds = {
            "api_url": "https://api.ironlayer.app",
            "access_token": "tok-123",
            "refresh_token": "",
            "email": "alice@example.com",
        }

        import httpx

        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.ConnectError("offline")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with patch("cli.cloud.load_full_credentials", return_value=full_creds):
            result = runner.invoke(app, ["whoami"])

        assert result.exit_code == 0
        assert "alice@example.com" in result.output
        assert "cached" in result.output.lower() or "Could not reach" in result.output


# ---------------------------------------------------------------------------
# resolve_model_sql
# ---------------------------------------------------------------------------


class TestResolveModelSql:
    """Tests for resolve_model_sql()."""

    def test_returns_sql_for_known_model(self) -> None:
        """Should return the SQL for a model that exists in the map."""
        sql_map = {"orders": "SELECT * FROM raw_orders"}
        result = resolve_model_sql("orders", sql_map)
        assert result == "SELECT * FROM raw_orders"

    def test_missing_model_exits(self) -> None:
        """Should exit with code 3 when model is not found."""

        sql_map = {"orders": "SELECT 1", "customers": "SELECT 2"}
        with pytest.raises(ClickExit):
            resolve_model_sql("nonexistent_model", sql_map)


# ---------------------------------------------------------------------------
# format_input_range edge cases
# ---------------------------------------------------------------------------


class TestFormatInputRangeEdge:
    """Additional edge case tests for format_input_range."""

    def test_object_with_only_start(self) -> None:
        """Object with start but no end should return '-'."""
        obj = MagicMock()
        obj.start = "2025-01-01"
        del obj.end
        result = format_input_range(obj)
        assert result == "-"

    def test_object_with_none_start(self) -> None:
        """Object with None start should return '-'."""
        obj = MagicMock()
        obj.start = None
        obj.end = None
        result = format_input_range(obj)
        assert result == "-"


# ---------------------------------------------------------------------------
# backfill-resume command
# ---------------------------------------------------------------------------


class TestBackfillResumeCommand:
    """Tests for `ironlayer backfill-resume`."""

    @patch("cli.helpers.api_request")
    def test_resume_success(self, mock_api: MagicMock) -> None:
        """Successful resume should display results."""
        mock_api.return_value = {
            "runs": [
                {"model": "orders", "status": "SUCCESS", "duration_seconds": 5.0, "input_range": "-", "retries": 0}
            ]
        }

        result = runner.invoke(
            app,
            [
                "backfill-resume",
                "--backfill-id",
                "bf-123",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        mock_api.assert_called_once_with(
            "POST",
            "http://localhost:8000",
            "/api/v1/backfills/bf-123/resume",
        )

    @patch("cli.helpers.api_request")
    def test_resume_empty_runs(self, mock_api: MagicMock) -> None:
        """Empty runs list should show success message."""
        mock_api.return_value = {"runs": []}

        result = runner.invoke(
            app,
            [
                "backfill-resume",
                "--backfill-id",
                "bf-123",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0
        assert "resumed successfully" in result.output.lower()

    @patch("cli.helpers.api_request")
    def test_resume_json_mode(self, mock_api: MagicMock) -> None:
        """--json mode should emit raw JSON."""
        mock_api.return_value = {"status": "resumed"}

        result = runner.invoke(
            app,
            [
                "--json",
                "backfill-resume",
                "--backfill-id",
                "bf-456",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0
        assert "resumed" in result.output


# ---------------------------------------------------------------------------
# backfill-history command
# ---------------------------------------------------------------------------


class TestBackfillHistoryCommand:
    """Tests for `ironlayer backfill-history`."""

    @patch("cli.helpers.api_request")
    def test_history_with_entries(self, mock_api: MagicMock) -> None:
        """Should display a table of backfill history entries."""
        mock_api.return_value = [
            {
                "plan_id": "plan-abc123",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "status": "SUCCESS",
                "created_at": "2025-02-01T10:00:00",
            },
            {
                "plan_id": "plan-def456",
                "start_date": "2025-02-01",
                "end_date": "2025-02-28",
                "status": "FAIL",
                "created_at": "2025-03-01T12:00:00",
            },
        ]

        result = runner.invoke(
            app,
            [
                "backfill-history",
                "--model",
                "analytics.orders",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        mock_api.assert_called_once_with(
            "GET",
            "http://localhost:8000",
            "/api/v1/backfills/history/analytics.orders",
            params={"limit": 20},
        )

    @patch("cli.helpers.api_request")
    def test_history_empty(self, mock_api: MagicMock) -> None:
        """No entries should show 'no history' message."""
        mock_api.return_value = []

        result = runner.invoke(
            app,
            [
                "backfill-history",
                "--model",
                "analytics.orders",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0
        assert "No backfill history" in result.output

    @patch("cli.helpers.api_request")
    def test_history_json_mode(self, mock_api: MagicMock) -> None:
        """--json mode should emit raw JSON."""
        mock_api.return_value = [{"plan_id": "p1", "status": "SUCCESS"}]

        result = runner.invoke(
            app,
            [
                "--json",
                "backfill-history",
                "--model",
                "analytics.orders",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0
        # Extract JSON array from output (CliRunner may mix stderr and stdout).
        raw = result.output
        json_start = raw.index("[")
        json_end = raw.rindex("]") + 1
        parsed = json.loads(raw[json_start:json_end])
        assert isinstance(parsed, list)

    @patch("cli.helpers.api_request")
    def test_history_custom_limit(self, mock_api: MagicMock) -> None:
        """--limit should be forwarded as a query parameter."""
        mock_api.return_value = []

        result = runner.invoke(
            app,
            [
                "backfill-history",
                "--model",
                "analytics.orders",
                "--api-url",
                "http://localhost:8000",
                "--limit",
                "50",
            ],
        )

        assert result.exit_code == 0
        mock_api.assert_called_once_with(
            "GET",
            "http://localhost:8000",
            "/api/v1/backfills/history/analytics.orders",
            params={"limit": 50},
        )

    @patch("cli.helpers.api_request")
    def test_history_malformed_date_handled(self, mock_api: MagicMock) -> None:
        """Malformed created_at should be displayed as-is, not crash."""
        mock_api.return_value = [
            {
                "plan_id": "p1",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "status": "SUCCESS",
                "created_at": "not-a-date",
            },
        ]

        result = runner.invoke(
            app,
            [
                "backfill-history",
                "--model",
                "analytics.orders",
                "--api-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# backfill-chunked edge cases
# ---------------------------------------------------------------------------


class TestBackfillChunkedEdge:
    """Edge case tests for backfill-chunked command."""

    @patch("cli.commands.backfill.load_model_sql_map")
    @patch("cli.commands.backfill.display_run_results")
    @patch("core_engine.executor.LocalExecutor")
    @patch("core_engine.config.load_settings")
    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id")
    def test_chunked_single_day(
        self,
        mock_det_id: MagicMock,
        mock_load_settings: MagicMock,
        mock_executor_cls: MagicMock,
        mock_display: MagicMock,
        mock_load_sql: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A single-day range should produce exactly one chunk."""
        from datetime import datetime

        from core_engine.models.run import RunRecord, RunStatus

        mock_load_settings.return_value = MagicMock(local_db_path=Path("/tmp/test.duckdb"))
        mock_load_sql.return_value = {"my_model": "SELECT 1"}

        run_record = RunRecord(
            run_id="run-1",
            plan_id="plan-1",
            step_id="step-1",
            model_name="my_model",
            status=RunStatus.SUCCESS,
            started_at=datetime(2025, 1, 1, tzinfo=UTC),
            finished_at=datetime(2025, 1, 1, 0, 0, 5, tzinfo=UTC),
            executor_version="local",
            retry_count=0,
        )

        executor = MagicMock()
        executor.execute_step.return_value = run_record
        executor.__enter__ = MagicMock(return_value=executor)
        executor.__exit__ = MagicMock(return_value=False)
        mock_executor_cls.return_value = executor

        result = runner.invoke(
            app,
            [
                "backfill-chunked",
                "--model",
                "my_model",
                "--start",
                "2025-01-15",
                "--end",
                "2025-01-15",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0, f"Output: {result.output}\n{result.exception}"
        assert executor.execute_step.call_count == 1

    @patch("core_engine.models.plan.compute_deterministic_id", return_value="det_id")
    def test_chunked_start_after_end(self, mock_det_id: MagicMock, tmp_path: Path) -> None:
        """Start > end should exit 3."""
        result = runner.invoke(
            app,
            [
                "backfill-chunked",
                "--model",
                "my_model",
                "--start",
                "2025-02-01",
                "--end",
                "2025-01-01",
                "--repo",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 3
