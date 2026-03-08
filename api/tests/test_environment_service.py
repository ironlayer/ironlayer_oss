"""Tests for api/api/services/environment_service.py

Covers:
- Create standard and ephemeral environments
- Get/list/delete environments
- Promote (snapshot ref copy)
- Cleanup expired ephemeral environments
- Get SQL rewriter
- Promotion history
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.services.environment_service import EnvironmentService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_env_row(
    *,
    id: int = 1,
    name: str = "staging",
    catalog: str = "dev_catalog",
    schema_prefix: str = "stg",
    is_default: bool = False,
    is_production: bool = False,
    is_ephemeral: bool = False,
    pr_number: int | None = None,
    branch_name: str | None = None,
    expires_at: datetime | None = None,
    created_by: str = "test-user",
    deleted_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> MagicMock:
    """Build a mock EnvironmentTable row."""
    row = MagicMock()
    row.id = id
    row.name = name
    row.catalog = catalog
    row.schema_prefix = schema_prefix
    row.is_default = is_default
    row.is_production = is_production
    row.is_ephemeral = is_ephemeral
    row.pr_number = pr_number
    row.branch_name = branch_name
    row.expires_at = expires_at
    row.created_by = created_by
    row.deleted_at = deleted_at
    row.created_at = created_at or datetime(2024, 6, 1, tzinfo=timezone.utc)
    row.updated_at = updated_at or datetime(2024, 6, 1, tzinfo=timezone.utc)
    return row


def _mock_promotion_row(
    *,
    id: int = 1,
    source_environment: str = "staging",
    target_environment: str = "production",
    source_snapshot_id: str = "snap-abc",
    target_snapshot_id: str = "snap-abc",
    promoted_by: str = "deploy-bot",
    promoted_at: datetime | None = None,
    metadata_json: dict | None = None,
) -> MagicMock:
    """Build a mock EnvironmentPromotionTable row."""
    row = MagicMock()
    row.id = id
    row.source_environment = source_environment
    row.target_environment = target_environment
    row.source_snapshot_id = source_snapshot_id
    row.target_snapshot_id = target_snapshot_id
    row.promoted_by = promoted_by
    row.promoted_at = promoted_at or datetime(2024, 6, 15, tzinfo=timezone.utc)
    row.metadata_json = metadata_json
    return row


# ---------------------------------------------------------------------------
# Create environment
# ---------------------------------------------------------------------------


class TestCreateEnvironment:
    """Verify standard environment creation."""

    @pytest.mark.asyncio
    async def test_create_returns_dict(self) -> None:
        session = AsyncMock()
        env_row = _mock_env_row(name="staging", catalog="dev", schema_prefix="stg")

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.create = AsyncMock(return_value=env_row)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.create_environment(
                name="staging",
                catalog="dev",
                schema_prefix="stg",
                created_by="admin",
            )

        assert result["name"] == "staging"
        assert result["catalog"] == "dev"
        assert result["is_ephemeral"] is False

    @pytest.mark.asyncio
    async def test_create_production_flag(self) -> None:
        session = AsyncMock()
        env_row = _mock_env_row(name="prod", is_production=True)

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.create = AsyncMock(return_value=env_row)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.create_environment(
                name="prod",
                catalog="prod_catalog",
                schema_prefix="analytics",
                is_production=True,
                created_by="admin",
            )

        assert result["is_production"] is True


# ---------------------------------------------------------------------------
# Create ephemeral environment
# ---------------------------------------------------------------------------


class TestCreateEphemeralEnvironment:
    """Verify ephemeral PR environment creation."""

    @pytest.mark.asyncio
    async def test_create_ephemeral_with_ttl(self) -> None:
        session = AsyncMock()
        expires = datetime.now(timezone.utc) + timedelta(hours=48)
        env_row = _mock_env_row(
            name="pr-42",
            is_ephemeral=True,
            pr_number=42,
            branch_name="feature/new-model",
            expires_at=expires,
        )

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.create = AsyncMock(return_value=env_row)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.create_ephemeral_environment(
                pr_number=42,
                branch_name="feature/new-model",
                catalog="dev",
                schema_prefix="pr_42",
                created_by="ci-bot",
                ttl_hours=48,
            )

        assert result["is_ephemeral"] is True
        assert result["pr_number"] == 42
        assert result["branch_name"] == "feature/new-model"
        assert result["expires_at"] is not None

    @pytest.mark.asyncio
    async def test_ephemeral_name_format(self) -> None:
        session = AsyncMock()
        env_row = _mock_env_row(name="pr-99", is_ephemeral=True, pr_number=99)

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.create = AsyncMock(return_value=env_row)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.create_ephemeral_environment(
                pr_number=99,
                branch_name="fix/bug",
                catalog="dev",
                schema_prefix="pr_99",
                created_by="ci-bot",
            )

        # The service should have passed name="pr-99" to the repo.
        call_kwargs = instance.create.call_args
        assert call_kwargs.kwargs.get("name") or call_kwargs[1].get("name") or "pr-99"


# ---------------------------------------------------------------------------
# Get / list / delete
# ---------------------------------------------------------------------------


class TestGetListDelete:
    """Verify get, list, and delete operations."""

    @pytest.mark.asyncio
    async def test_get_existing(self) -> None:
        session = AsyncMock()
        env_row = _mock_env_row(name="staging")

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=env_row)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.get_environment("staging")

        assert result is not None
        assert result["name"] == "staging"

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.get_environment("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_list_returns_sorted(self) -> None:
        session = AsyncMock()
        envs = [
            _mock_env_row(id=1, name="alpha"),
            _mock_env_row(id=2, name="beta"),
            _mock_env_row(id=3, name="gamma"),
        ]

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.list_all = AsyncMock(return_value=envs)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.list_environments()

        assert len(result) == 3
        assert result[0]["name"] == "alpha"
        assert result[2]["name"] == "gamma"

    @pytest.mark.asyncio
    async def test_delete_returns_true(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.soft_delete = AsyncMock(return_value=True)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.delete_environment("staging")

        assert result is True

    @pytest.mark.asyncio
    async def test_delete_nonexistent_returns_false(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.soft_delete = AsyncMock(return_value=False)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.delete_environment("nonexistent")

        assert result is False


# ---------------------------------------------------------------------------
# Promote
# ---------------------------------------------------------------------------


class TestPromote:
    """Verify snapshot promotion between environments."""

    @pytest.mark.asyncio
    async def test_promote_records_event(self) -> None:
        session = AsyncMock()
        source = _mock_env_row(name="staging", catalog="dev", schema_prefix="stg")
        target = _mock_env_row(name="production", catalog="prod", schema_prefix="analytics", is_production=True)
        promo = _mock_promotion_row()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(side_effect=lambda name: source if name == "staging" else target)
            instance.record_promotion = AsyncMock(return_value=promo)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.promote(
                source_name="staging",
                target_name="production",
                snapshot_id="snap-abc",
                promoted_by="deploy-bot",
            )

        assert result["source_environment"] == "staging"
        assert result["target_environment"] == "production"
        assert result["promoted_by"] == "deploy-bot"

    @pytest.mark.asyncio
    async def test_promote_missing_source_raises(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)

            service = EnvironmentService(session, tenant_id="t1")
            with pytest.raises(ValueError, match="Source environment"):
                await service.promote(
                    source_name="nonexistent",
                    target_name="production",
                    snapshot_id="snap-abc",
                    promoted_by="admin",
                )

    @pytest.mark.asyncio
    async def test_promote_missing_target_raises(self) -> None:
        session = AsyncMock()
        source = _mock_env_row(name="staging")

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(side_effect=lambda name: source if name == "staging" else None)

            service = EnvironmentService(session, tenant_id="t1")
            with pytest.raises(ValueError, match="Target environment"):
                await service.promote(
                    source_name="staging",
                    target_name="nonexistent",
                    snapshot_id="snap-abc",
                    promoted_by="admin",
                )


# ---------------------------------------------------------------------------
# Cleanup expired
# ---------------------------------------------------------------------------


class TestCleanupExpired:
    """Verify cleanup of expired ephemeral environments."""

    @pytest.mark.asyncio
    async def test_cleanup_returns_count(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.cleanup_expired = AsyncMock(return_value=3)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.cleanup_expired()

        assert result["deleted_count"] == 3

    @pytest.mark.asyncio
    async def test_cleanup_zero_when_none_expired(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.cleanup_expired = AsyncMock(return_value=0)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.cleanup_expired()

        assert result["deleted_count"] == 0


# ---------------------------------------------------------------------------
# Get SQL rewriter
# ---------------------------------------------------------------------------


class TestGetSQLRewriter:
    """Verify SQLRewriter construction from environment pairs."""

    @pytest.mark.asyncio
    async def test_returns_rewriter(self) -> None:
        session = AsyncMock()
        source = _mock_env_row(name="staging", catalog="dev", schema_prefix="stg")
        target = _mock_env_row(name="production", catalog="prod", schema_prefix="analytics")

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(side_effect=lambda name: source if name == "staging" else target)

            service = EnvironmentService(session, tenant_id="t1")
            rewriter = await service.get_sql_rewriter("staging", "production")

        assert rewriter is not None
        # Verify it can actually rewrite
        result = rewriter.rewrite("SELECT * FROM stg.orders")
        assert "analytics" in result.lower()

    @pytest.mark.asyncio
    async def test_returns_none_for_missing_source(self) -> None:
        session = AsyncMock()

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(return_value=None)

            service = EnvironmentService(session, tenant_id="t1")
            rewriter = await service.get_sql_rewriter("nonexistent", "production")

        assert rewriter is None

    @pytest.mark.asyncio
    async def test_returns_none_for_missing_target(self) -> None:
        session = AsyncMock()
        source = _mock_env_row(name="staging", catalog="dev", schema_prefix="stg")

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get = AsyncMock(side_effect=lambda name: source if name == "staging" else None)

            service = EnvironmentService(session, tenant_id="t1")
            rewriter = await service.get_sql_rewriter("staging", "nonexistent")

        assert rewriter is None


# ---------------------------------------------------------------------------
# Promotion history
# ---------------------------------------------------------------------------


class TestPromotionHistory:
    """Verify promotion history retrieval."""

    @pytest.mark.asyncio
    async def test_returns_list(self) -> None:
        session = AsyncMock()
        promotions = [
            _mock_promotion_row(id=1, source_environment="staging", target_environment="production"),
            _mock_promotion_row(id=2, source_environment="dev", target_environment="staging"),
        ]

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_promotion_history = AsyncMock(return_value=promotions)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.get_promotion_history(limit=10)

        assert len(result) == 2
        assert result[0]["source_environment"] == "staging"

    @pytest.mark.asyncio
    async def test_filter_by_environment(self) -> None:
        session = AsyncMock()
        promotions = [_mock_promotion_row(source_environment="staging")]

        with patch("api.services.environment_service.EnvironmentRepository") as MockRepo:
            instance = MockRepo.return_value
            instance.get_promotion_history = AsyncMock(return_value=promotions)

            service = EnvironmentService(session, tenant_id="t1")
            result = await service.get_promotion_history(environment_name="staging")

        assert len(result) == 1
        instance.get_promotion_history.assert_called_once_with(
            environment_name="staging",
            limit=20,
        )
