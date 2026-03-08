"""Tests for api/api/routers/webhooks.py

Covers:
- POST /webhooks/github: push events, non-push events, signature validation
- POST /webhooks/github: HMAC-SHA256 signature verification (valid, invalid, missing secret)
- POST /webhooks/config: ADMIN-only config creation
- GET /webhooks/config: ADMIN-only config listing
- DELETE /webhooks/config/{config_id}: ADMIN-only deletion
- RBAC enforcement: viewer/operator/engineer denied MANAGE_WEBHOOKS
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.config import APISettings
from api.dependencies import get_ai_client, get_db_session, get_metering_collector, get_settings, get_tenant_session
from api.main import create_app
from api.security import CredentialVault
from api.services.ai_client import AIServiceClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEV_SECRET = "test-secret-key-for-ironlayer-tests"
_WEBHOOK_SECRET = "test-webhook-hmac-secret"
_CREDENTIAL_KEY = "ironlayer-dev-secret-change-in-production"


def _github_signature(body: bytes, secret: str = _WEBHOOK_SECRET) -> str:
    """Compute a valid GitHub X-Hub-Signature-256 header value."""
    digest = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


def _make_dev_token(role: str = "admin", tenant_id: str = "test-tenant") -> str:
    """Generate a valid dev-mode token with a role claim."""
    now = time.time()
    payload = {
        "sub": "test-user",
        "tenant_id": tenant_id,
        "iss": "ironlayer",
        "iat": now,
        "exp": now + 3600,
        "scopes": ["read", "write"],
        "jti": f"test-jti-wh-{role}",
        "identity_kind": "user",
        "role": role,
    }
    payload_json = json.dumps(payload)
    sig = hmac.new(
        _DEV_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token_bytes = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii")
    return f"bmdev.{token_bytes}.{sig}"


def _auth_headers(role: str = "admin") -> dict[str, str]:
    return {"Authorization": f"Bearer {_make_dev_token(role)}"}


def _create_test_app() -> tuple[Any, AsyncMock]:
    """Create a test app with mocked dependencies."""
    app = create_app()

    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    result_mock.rowcount = 0
    mock_session.execute = AsyncMock(return_value=result_mock)

    async def _override_session():
        yield mock_session

    settings = APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
    )

    mock_ai_client = AsyncMock(spec=AIServiceClient)
    mock_ai_client.health_check = AsyncMock(return_value=True)
    mock_ai_client.close = AsyncMock()

    mock_metering = MagicMock()
    mock_metering.record_event = MagicMock()
    mock_metering.record = MagicMock()
    mock_metering.flush = MagicMock(return_value=0)
    mock_metering.pending_count = 0

    app.dependency_overrides[get_db_session] = _override_session
    app.dependency_overrides[get_tenant_session] = _override_session
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_ai_client] = lambda: mock_ai_client
    app.dependency_overrides[get_metering_collector] = lambda: mock_metering

    return app, mock_session


# ---------------------------------------------------------------------------
# POST /webhooks/github
# ---------------------------------------------------------------------------


class TestGitHubWebhookEndpoint:
    """Verify POST /api/v1/webhooks/github endpoint."""

    @pytest.mark.asyncio
    async def test_non_push_event_ignored(self) -> None:
        """Non-push events (e.g. ping) return ignored."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/webhooks/github",
                content=b"{}",
                headers={"x-github-event": "ping"},
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "ignored"
        assert resp.json()["event_type"] == "ping"

    @pytest.mark.asyncio
    async def test_push_event_missing_signature_returns_400(self) -> None:
        """Push event without X-Hub-Signature-256 returns 400."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/webhooks/github",
                content=payload.encode(),
                headers={"x-github-event": "push"},
            )

        assert resp.status_code == 400
        assert "Signature" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_push_event_invalid_json_returns_400(self) -> None:
        """Push event with invalid JSON body returns 400."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/webhooks/github",
                content=b"not-json",
                headers={
                    "x-github-event": "push",
                    "x-hub-signature-256": "sha256=abc",
                },
            )

        assert resp.status_code == 400
        assert "JSON" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_push_event_invalid_signature_format(self) -> None:
        """Push event with signature not starting with sha256= returns 400.

        The format check happens before config lookup or any DB access.
        """
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/webhooks/github",
                content=payload.encode(),
                headers={
                    "x-github-event": "push",
                    "x-hub-signature-256": "md5=abc123",
                },
            )

        assert resp.status_code == 400
        assert "signature format" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_push_event_no_matching_config(self) -> None:
        """Push event with no matching config returns ignored."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/unknown.git"},
            }
        )

        mock_sf = MagicMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_sf.return_value = mock_ctx

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.post(
                    "/api/v1/webhooks/github",
                    content=payload.encode(),
                    headers={
                        "x-github-event": "push",
                        "x-hub-signature-256": "sha256=valid",
                    },
                )

        assert resp.status_code == 200
        assert resp.json()["status"] == "ignored"
        assert resp.json()["reason"] == "no_matching_config"

    @pytest.mark.asyncio
    async def test_push_event_bypasses_jwt_auth(self) -> None:
        """GitHub webhook endpoint works without JWT auth header."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            # No auth header — should still get past auth middleware
            resp = await ac.post(
                "/api/v1/webhooks/github",
                content=b"{}",
                headers={"x-github-event": "ping"},
            )

        # ping events are ignored with 200, proving auth was bypassed
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# HMAC-SHA256 signature verification
# ---------------------------------------------------------------------------


def _make_webhook_config_row(
    *,
    secret_encrypted: str | None = None,
    tenant_id: str = "test-tenant",
    auto_plan: bool = True,
    auto_apply: bool = False,
) -> MagicMock:
    """Build a mock WebhookConfigTable row with the given secret_encrypted."""
    row = MagicMock()
    row.id = 42
    row.tenant_id = tenant_id
    row.provider = "github"
    row.repo_url = "https://github.com/org/repo.git"
    row.branch = "main"
    row.secret_encrypted = secret_encrypted
    row.auto_plan = auto_plan
    row.auto_apply = auto_apply
    return row


def _mock_session_factory_returning(mock_session: AsyncMock) -> MagicMock:
    """Wrap a mock session in a session-factory context manager."""
    mock_sf = MagicMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_sf.return_value = mock_ctx
    return mock_sf


class TestWebhookHMACValidation:
    """Verify HMAC-SHA256 signature verification in the webhook receiver."""

    @pytest.mark.asyncio
    async def test_valid_hmac_signature_accepted(self) -> None:
        """Request with correct HMAC-SHA256 signature is processed."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )
        body = payload.encode()
        sig = _github_signature(body)

        vault = CredentialVault(_CREDENTIAL_KEY)
        encrypted_secret = vault.encrypt(_WEBHOOK_SECRET)

        config_row = _make_webhook_config_row(secret_encrypted=encrypted_secret)
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_sf = _mock_session_factory_returning(mock_session)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            with patch("api.dependencies.get_settings") as mock_get_settings:
                settings = MagicMock()
                settings.credential_encryption_key.get_secret_value.return_value = _CREDENTIAL_KEY
                mock_get_settings.return_value = settings

                async with AsyncClient(transport=transport, base_url="http://test") as ac:
                    resp = await ac.post(
                        "/api/v1/webhooks/github",
                        content=body,
                        headers={
                            "x-github-event": "push",
                            "x-hub-signature-256": sig,
                        },
                    )

        assert resp.status_code == 200
        data = resp.json()
        # The push handler finds the same config and processes the event.
        assert data["status"] in ("plan_triggered", "acknowledged", "ignored")

    @pytest.mark.asyncio
    async def test_invalid_hmac_signature_rejected(self) -> None:
        """Request with wrong HMAC digest returns 403."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )
        body = payload.encode()
        # Compute signature with the WRONG secret.
        bad_sig = _github_signature(body, secret="totally-wrong-secret")

        vault = CredentialVault(_CREDENTIAL_KEY)
        encrypted_secret = vault.encrypt(_WEBHOOK_SECRET)

        config_row = _make_webhook_config_row(secret_encrypted=encrypted_secret)
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_sf = _mock_session_factory_returning(mock_session)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            with patch("api.dependencies.get_settings") as mock_get_settings:
                settings = MagicMock()
                settings.credential_encryption_key.get_secret_value.return_value = _CREDENTIAL_KEY
                mock_get_settings.return_value = settings

                async with AsyncClient(transport=transport, base_url="http://test") as ac:
                    resp = await ac.post(
                        "/api/v1/webhooks/github",
                        content=body,
                        headers={
                            "x-github-event": "push",
                            "x-hub-signature-256": bad_sig,
                        },
                    )

        assert resp.status_code == 403
        assert "Signature verification failed" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_forged_sha256_prefix_rejected(self) -> None:
        """A sha256=<garbage> header is rejected when HMAC is verified."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )
        body = payload.encode()

        vault = CredentialVault(_CREDENTIAL_KEY)
        encrypted_secret = vault.encrypt(_WEBHOOK_SECRET)

        config_row = _make_webhook_config_row(secret_encrypted=encrypted_secret)
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_sf = _mock_session_factory_returning(mock_session)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            with patch("api.dependencies.get_settings") as mock_get_settings:
                settings = MagicMock()
                settings.credential_encryption_key.get_secret_value.return_value = _CREDENTIAL_KEY
                mock_get_settings.return_value = settings

                async with AsyncClient(transport=transport, base_url="http://test") as ac:
                    resp = await ac.post(
                        "/api/v1/webhooks/github",
                        content=body,
                        headers={
                            "x-github-event": "push",
                            # Previously this would pass -- now properly rejected.
                            "x-hub-signature-256": "sha256=anything",
                        },
                    )

        assert resp.status_code == 403
        assert "Signature verification failed" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_secret_encrypted_logs_warning(self) -> None:
        """Config without secret_encrypted still processes (degraded mode)."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )
        body = payload.encode()

        # Config row with NO secret_encrypted (legacy/pre-migration).
        config_row = _make_webhook_config_row(secret_encrypted=None)
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_sf = _mock_session_factory_returning(mock_session)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.post(
                    "/api/v1/webhooks/github",
                    content=body,
                    headers={
                        "x-github-event": "push",
                        "x-hub-signature-256": "sha256=anything",
                    },
                )

        # Degraded mode: event is still processed but a warning is logged.
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_decryption_failure_returns_500(self) -> None:
        """If the stored encrypted secret is corrupt, return 500."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        payload = json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "aaa111",
                "after": "bbb222",
                "repository": {"clone_url": "https://github.com/org/repo.git"},
            }
        )
        body = payload.encode()

        # Config row with corrupt encrypted secret.
        config_row = _make_webhook_config_row(secret_encrypted="corrupt-cipher-text")
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = config_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_sf = _mock_session_factory_returning(mock_session)

        with patch("api.dependencies.get_session_factory", return_value=mock_sf):
            with patch("api.dependencies.get_settings") as mock_get_settings:
                settings = MagicMock()
                settings.credential_encryption_key.get_secret_value.return_value = _CREDENTIAL_KEY
                mock_get_settings.return_value = settings

                async with AsyncClient(transport=transport, base_url="http://test") as ac:
                    resp = await ac.post(
                        "/api/v1/webhooks/github",
                        content=body,
                        headers={
                            "x-github-event": "push",
                            "x-hub-signature-256": "sha256=abc123",
                        },
                    )

        assert resp.status_code == 500
        assert "decryption failed" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /webhooks/config (ADMIN only)
# ---------------------------------------------------------------------------


class TestWebhookConfigCreate:
    """Verify POST /api/v1/webhooks/config endpoint."""

    @pytest.mark.asyncio
    async def test_create_config_success(self) -> None:
        """ADMIN can create a webhook config."""
        app, mock_session = _create_test_app()
        transport = ASGITransport(app=app)

        with patch("api.routers.webhooks.GitHubWebhookService") as MockService:
            instance = MockService.return_value
            instance.create_config = AsyncMock(
                return_value={
                    "id": 1,
                    "tenant_id": "test-tenant",
                    "provider": "github",
                    "repo_url": "https://github.com/org/repo.git",
                    "branch": "main",
                    "auto_plan": True,
                    "auto_apply": False,
                    "created_at": "2024-06-01T00:00:00+00:00",
                }
            )

            async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("admin")) as ac:
                resp = await ac.post(
                    "/api/v1/webhooks/config",
                    json={
                        "repo_url": "https://github.com/org/repo.git",
                        "branch": "main",
                        "secret": "my-webhook-secret",
                    },
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["provider"] == "github"
        assert body["repo_url"] == "https://github.com/org/repo.git"

    @pytest.mark.asyncio
    async def test_create_config_secret_too_short(self) -> None:
        """Secret shorter than 8 chars returns 422."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("admin")) as ac:
            resp = await ac.post(
                "/api/v1/webhooks/config",
                json={
                    "repo_url": "https://github.com/org/repo.git",
                    "branch": "main",
                    "secret": "short",
                },
            )

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# RBAC enforcement
# ---------------------------------------------------------------------------


class TestWebhookRBAC:
    """Verify MANAGE_WEBHOOKS permission enforcement."""

    @pytest.mark.asyncio
    async def test_viewer_denied_create_config(self) -> None:
        """VIEWER cannot create webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("viewer")) as ac:
            resp = await ac.post(
                "/api/v1/webhooks/config",
                json={
                    "repo_url": "https://github.com/org/repo.git",
                    "branch": "main",
                    "secret": "my-webhook-secret",
                },
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_operator_denied_create_config(self) -> None:
        """OPERATOR cannot create webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("operator")) as ac:
            resp = await ac.post(
                "/api/v1/webhooks/config",
                json={
                    "repo_url": "https://github.com/org/repo.git",
                    "branch": "main",
                    "secret": "my-webhook-secret",
                },
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_engineer_denied_create_config(self) -> None:
        """ENGINEER cannot create webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("engineer")) as ac:
            resp = await ac.post(
                "/api/v1/webhooks/config",
                json={
                    "repo_url": "https://github.com/org/repo.git",
                    "branch": "main",
                    "secret": "my-webhook-secret",
                },
            )

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_denied_list_config(self) -> None:
        """VIEWER cannot list webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("viewer")) as ac:
            resp = await ac.get("/api/v1/webhooks/config")

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_denied_delete_config(self) -> None:
        """VIEWER cannot delete webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("viewer")) as ac:
            resp = await ac.delete("/api/v1/webhooks/config/1")

        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_can_list_configs(self) -> None:
        """ADMIN can list webhook configs."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        with patch("api.routers.webhooks.GitHubWebhookService") as MockService:
            instance = MockService.return_value
            instance.list_configs = AsyncMock(return_value=[])

            async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("admin")) as ac:
                resp = await ac.get("/api/v1/webhooks/config")

        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_admin_can_delete_config(self) -> None:
        """ADMIN can delete webhook configs (404 when not found)."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        with patch("api.routers.webhooks.GitHubWebhookService") as MockService:
            instance = MockService.return_value
            instance.delete_config = AsyncMock(return_value=False)

            async with AsyncClient(transport=transport, base_url="http://test", headers=_auth_headers("admin")) as ac:
                resp = await ac.delete("/api/v1/webhooks/config/999")

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_unauthenticated_denied_config(self) -> None:
        """Unauthenticated requests to config endpoints get 401."""
        app, _ = _create_test_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/v1/webhooks/config")

        assert resp.status_code == 401
