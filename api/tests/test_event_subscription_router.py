"""Tests for the event subscription CRUD router.

Validates endpoint behaviour, RBAC enforcement, and request validation
using the standard IronLayer test patterns (mock session, dev tokens).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from conftest import _make_dev_token

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_subscription_row(
    *,
    sub_id: int = 1,
    tenant_id: str = "default",
    name: str = "CI/CD Hook",
    url: str = "https://ci.example.com/hooks/ironlayer",
    event_types: list[str] | None = None,
    active: bool = True,
    description: str | None = "CI pipeline trigger",
) -> MagicMock:
    """Create a mock EventSubscriptionTable row."""
    row = MagicMock()
    row.id = sub_id
    row.tenant_id = tenant_id
    row.name = name
    row.url = url
    row.event_types = event_types
    row.active = active
    row.description = description
    row.secret_hash = None
    row.created_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    row.updated_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    return row


# ---------------------------------------------------------------------------
# Create subscription
# ---------------------------------------------------------------------------


class TestCreateSubscription:
    """POST /api/v1/event-subscriptions"""

    @pytest.mark.asyncio
    async def test_create_subscription(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        row = _make_subscription_row()

        # Mock the repository's create method via session.add + flush.
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        # We need to patch the repository so create() returns our mock row.
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.create = AsyncMock(return_value=row)
            MockRepo.return_value = repo_instance

            resp = await client.post(
                "/api/v1/event-subscriptions",
                json={
                    "name": "CI/CD Hook",
                    "url": "https://ci.example.com/hooks/ironlayer",
                    "event_types": ["plan.generated", "plan.apply_completed"],
                    "description": "CI pipeline trigger",
                },
            )

        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "CI/CD Hook"
        assert data["url"] == "https://ci.example.com/hooks/ironlayer"
        assert data["active"] is True

    @pytest.mark.asyncio
    async def test_create_subscription_with_secret(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        row = _make_subscription_row()

        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.create = AsyncMock(return_value=row)
            MockRepo.return_value = repo_instance

            resp = await client.post(
                "/api/v1/event-subscriptions",
                json={
                    "name": "Secure Hook",
                    "url": "https://secure.example.com/hook",
                    "secret": "my-super-secret-key-123",
                },
            )

        assert resp.status_code == 201
        # Secret should NOT be in the response.
        data = resp.json()
        assert "secret" not in data
        assert "secret_hash" not in data

    @pytest.mark.asyncio
    async def test_create_subscription_validation_error(self, client: AsyncClient) -> None:
        # Missing required 'url' field.
        resp = await client.post(
            "/api/v1/event-subscriptions",
            json={"name": "No URL"},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_create_subscription_short_secret_rejected(self, client: AsyncClient) -> None:
        resp = await client.post(
            "/api/v1/event-subscriptions",
            json={
                "name": "Bad Secret",
                "url": "https://example.com/hook",
                "secret": "short",  # < 8 chars
            },
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# List subscriptions
# ---------------------------------------------------------------------------


class TestListSubscriptions:
    """GET /api/v1/event-subscriptions"""

    @pytest.mark.asyncio
    async def test_list_subscriptions(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        rows = [
            _make_subscription_row(sub_id=1, name="Hook A"),
            _make_subscription_row(sub_id=2, name="Hook B"),
        ]

        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_all = AsyncMock(return_value=rows)
            MockRepo.return_value = repo_instance

            resp = await client.get("/api/v1/event-subscriptions")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["name"] == "Hook A"
        assert data[1]["name"] == "Hook B"

    @pytest.mark.asyncio
    async def test_list_subscriptions_empty(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.list_all = AsyncMock(return_value=[])
            MockRepo.return_value = repo_instance

            resp = await client.get("/api/v1/event-subscriptions")

        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Get subscription
# ---------------------------------------------------------------------------


class TestGetSubscription:
    """GET /api/v1/event-subscriptions/{id}"""

    @pytest.mark.asyncio
    async def test_get_subscription(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        row = _make_subscription_row(sub_id=42, name="My Hook")

        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get = AsyncMock(return_value=row)
            MockRepo.return_value = repo_instance

            resp = await client.get("/api/v1/event-subscriptions/42")

        assert resp.status_code == 200
        assert resp.json()["id"] == 42
        assert resp.json()["name"] == "My Hook"

    @pytest.mark.asyncio
    async def test_get_subscription_not_found(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.get = AsyncMock(return_value=None)
            MockRepo.return_value = repo_instance

            resp = await client.get("/api/v1/event-subscriptions/999")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Update subscription
# ---------------------------------------------------------------------------


class TestUpdateSubscription:
    """PUT /api/v1/event-subscriptions/{id}"""

    @pytest.mark.asyncio
    async def test_update_subscription(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        updated_row = _make_subscription_row(sub_id=1, name="Updated Hook")

        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.update = AsyncMock(return_value=updated_row)
            MockRepo.return_value = repo_instance

            resp = await client.put(
                "/api/v1/event-subscriptions/1",
                json={"name": "Updated Hook"},
            )

        assert resp.status_code == 200
        assert resp.json()["name"] == "Updated Hook"

    @pytest.mark.asyncio
    async def test_update_subscription_not_found(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.update = AsyncMock(return_value=None)
            MockRepo.return_value = repo_instance

            resp = await client.put(
                "/api/v1/event-subscriptions/999",
                json={"name": "No Such Hook"},
            )

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_update_subscription_deactivate(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        deactivated = _make_subscription_row(sub_id=1, active=False)

        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.update = AsyncMock(return_value=deactivated)
            MockRepo.return_value = repo_instance

            resp = await client.put(
                "/api/v1/event-subscriptions/1",
                json={"active": False},
            )

        assert resp.status_code == 200
        assert resp.json()["active"] is False


# ---------------------------------------------------------------------------
# Delete subscription
# ---------------------------------------------------------------------------


class TestDeleteSubscription:
    """DELETE /api/v1/event-subscriptions/{id}"""

    @pytest.mark.asyncio
    async def test_delete_subscription(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.delete = AsyncMock(return_value=True)
            MockRepo.return_value = repo_instance

            resp = await client.delete("/api/v1/event-subscriptions/1")

        assert resp.status_code == 200
        assert resp.json()["deleted"] is True
        assert resp.json()["subscription_id"] == 1

    @pytest.mark.asyncio
    async def test_delete_subscription_not_found(self, client: AsyncClient, mock_session: AsyncMock) -> None:
        with patch("api.routers.event_subscriptions.EventSubscriptionRepository") as MockRepo:
            repo_instance = AsyncMock()
            repo_instance.delete = AsyncMock(return_value=False)
            MockRepo.return_value = repo_instance

            resp = await client.delete("/api/v1/event-subscriptions/999")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# RBAC enforcement
# ---------------------------------------------------------------------------


class TestRBAC:
    """Tests that MANAGE_WEBHOOKS permission is enforced."""

    @pytest.mark.asyncio
    async def test_viewer_cannot_create_subscription(self, app: Any) -> None:
        viewer_token = _make_dev_token(role="viewer")
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ) as viewer_client:
            resp = await viewer_client.post(
                "/api/v1/event-subscriptions",
                json={
                    "name": "Test",
                    "url": "https://example.com/hook",
                },
            )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_cannot_list_subscriptions(self, app: Any) -> None:
        viewer_token = _make_dev_token(role="viewer")
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ) as viewer_client:
            resp = await viewer_client.get("/api/v1/event-subscriptions")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_viewer_cannot_delete_subscription(self, app: Any) -> None:
        viewer_token = _make_dev_token(role="viewer")
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ) as viewer_client:
            resp = await viewer_client.delete("/api/v1/event-subscriptions/1")
        assert resp.status_code == 403
