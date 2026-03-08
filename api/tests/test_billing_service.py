"""Tests for api/api/services/billing_service.py

Covers:
- BillingService: customer CRUD, subscription info, portal sessions
- BillingService: webhook event handling (subscription create/update/delete, invoice)
- BillingService: price-to-tier mapping
- StripeUsageReporter: start/stop lifecycle, report_usage (mocked Stripe)
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.config import APISettings
from api.services.billing_service import BillingService, StripeUsageReporter

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def billing_settings() -> APISettings:
    """Settings with billing enabled and Stripe keys configured."""
    return APISettings(
        host="0.0.0.0",
        port=8000,
        debug=True,
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        ai_engine_url="http://localhost:8001",
        ai_engine_timeout=5.0,
        platform_env="dev",
        cors_origins=["http://localhost:3000"],
        billing_enabled=True,
        stripe_secret_key="sk_test_xxx",
        stripe_webhook_secret="whsec_test_xxx",
        stripe_price_id_community="price_community",
        stripe_price_id_team="price_team",
        stripe_price_id_enterprise="price_enterprise",
        stripe_metered_price_id="price_metered",
    )


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Mock async database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result_mock)

    return session


def _make_service(
    session: AsyncMock,
    settings: APISettings,
    tenant_id: str = "test-tenant",
) -> BillingService:
    """Create a BillingService with the given dependencies."""
    return BillingService(session, settings, tenant_id=tenant_id)


# ---------------------------------------------------------------------------
# Customer CRUD
# ---------------------------------------------------------------------------


class TestGetOrCreateCustomer:
    """Verify get_or_create_customer behaviour."""

    @pytest.mark.asyncio
    async def test_returns_existing_customer(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """When a billing record exists, return it without calling Stripe."""
        row = MagicMock()
        row.stripe_customer_id = "cus_existing"
        row.stripe_subscription_id = "sub_existing"
        row.plan_tier = "team"
        row.period_start = datetime(2024, 6, 1, tzinfo=timezone.utc)
        row.period_end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session, billing_settings)
        info = await service.get_or_create_customer()

        assert info["customer_id"] == "cus_existing"
        assert info["subscription_id"] == "sub_existing"
        assert info["plan_tier"] == "team"

    @pytest.mark.asyncio
    async def test_creates_new_customer_via_stripe(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """When no billing record exists, create a Stripe customer and persist."""
        # First call → no existing row; subsequent calls don't matter.
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_stripe = MagicMock()
        mock_stripe.Customer.create.return_value = {"id": "cus_new_123"}

        service = _make_service(mock_session, billing_settings)

        with patch.object(service, "_get_stripe", return_value=mock_stripe):
            info = await service.get_or_create_customer()

        assert info["customer_id"] == "cus_new_123"
        assert info["plan_tier"] == "community"
        assert info["subscription_id"] is None
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()


# ---------------------------------------------------------------------------
# Subscription info
# ---------------------------------------------------------------------------


class TestGetSubscriptionInfo:
    """Verify get_subscription_info behaviour."""

    @pytest.mark.asyncio
    async def test_no_record_returns_community(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """If no billing record exists, return community tier."""
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session, billing_settings)
        info = await service.get_subscription_info()

        assert info["plan_tier"] == "community"
        assert info["status"] == "active"
        assert info["subscription_id"] is None

    @pytest.mark.asyncio
    async def test_record_without_subscription(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """A billing record without Stripe subscription returns active community."""
        row = MagicMock()
        row.plan_tier = "community"
        row.stripe_subscription_id = None
        row.period_start = None
        row.period_end = None

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=result_mock)

        service = _make_service(mock_session, billing_settings)
        info = await service.get_subscription_info()

        assert info["plan_tier"] == "community"
        assert info["status"] == "active"

    @pytest.mark.asyncio
    async def test_record_with_subscription_fetches_live_status(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """When subscription exists, fetch live status from Stripe."""
        row = MagicMock()
        row.plan_tier = "team"
        row.stripe_subscription_id = "sub_live"
        row.period_start = datetime(2024, 6, 1, tzinfo=timezone.utc)
        row.period_end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_stripe = MagicMock()
        mock_stripe.Subscription.retrieve.return_value = {
            "status": "active",
            "cancel_at_period_end": False,
            "current_period_end": 1719792000,
        }

        service = _make_service(mock_session, billing_settings)
        with patch.object(service, "_get_stripe", return_value=mock_stripe):
            info = await service.get_subscription_info()

        assert info["plan_tier"] == "team"
        assert info["status"] == "active"
        assert info["cancel_at_period_end"] is False

    @pytest.mark.asyncio
    async def test_subscription_fetch_failure_returns_unknown(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """If Stripe API fails, status is set to 'unknown'."""
        row = MagicMock()
        row.plan_tier = "team"
        row.stripe_subscription_id = "sub_broken"
        row.period_start = None
        row.period_end = None

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_stripe = MagicMock()
        mock_stripe.Subscription.retrieve.side_effect = Exception("API error")

        service = _make_service(mock_session, billing_settings)
        with patch.object(service, "_get_stripe", return_value=mock_stripe):
            info = await service.get_subscription_info()

        assert info["status"] == "unknown"


# ---------------------------------------------------------------------------
# Portal session
# ---------------------------------------------------------------------------


class TestCreatePortalSession:
    """Verify create_portal_session calls Stripe."""

    @pytest.mark.asyncio
    async def test_creates_portal_session(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """Portal session returns a redirect URL from Stripe."""
        # get_or_create_customer returns existing
        row = MagicMock()
        row.stripe_customer_id = "cus_portal"
        row.stripe_subscription_id = "sub_portal"
        row.plan_tier = "team"
        row.period_start = None
        row.period_end = None

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = row
        mock_session.execute = AsyncMock(return_value=result_mock)

        mock_stripe = MagicMock()
        mock_stripe.billing_portal.Session.create.return_value = {"url": "https://billing.stripe.com/session/ses_xxx"}

        service = _make_service(mock_session, billing_settings)
        with patch.object(service, "_get_stripe", return_value=mock_stripe):
            result = await service.create_portal_session("https://app.ironlayer.app/billing")

        assert result["url"] == "https://billing.stripe.com/session/ses_xxx"
        mock_stripe.billing_portal.Session.create.assert_called_once_with(
            customer="cus_portal",
            return_url="https://app.ironlayer.app/billing",
        )


# ---------------------------------------------------------------------------
# Webhook event handling
# ---------------------------------------------------------------------------


class TestWebhookEventHandling:
    """Verify handle_webhook_event dispatches correctly."""

    @pytest.mark.asyncio
    async def test_subscription_created(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """customer.subscription.created updates the local record."""
        existing_row = MagicMock()
        existing_row.stripe_customer_id = "cus_wh"
        existing_row.stripe_subscription_id = None
        existing_row.plan_tier = "community"

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        event = {
            "type": "customer.subscription.created",
            "data": {
                "object": {
                    "id": "sub_new",
                    "customer": "cus_wh",
                    "items": {"data": [{"price": {"id": "price_team"}}]},
                    "current_period_start": 1717200000,
                    "current_period_end": 1719792000,
                },
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "processed"
        assert existing_row.stripe_subscription_id == "sub_new"
        assert existing_row.plan_tier == "team"
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_subscription_updated(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """customer.subscription.updated updates tier and period."""
        existing_row = MagicMock()
        existing_row.stripe_customer_id = "cus_upd"

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        event = {
            "type": "customer.subscription.updated",
            "data": {
                "object": {
                    "id": "sub_upd",
                    "customer": "cus_upd",
                    "items": {"data": [{"price": {"id": "price_enterprise"}}]},
                    "current_period_start": 1717200000,
                    "current_period_end": 1719792000,
                },
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "processed"
        assert existing_row.plan_tier == "enterprise"

    @pytest.mark.asyncio
    async def test_subscription_deleted_downgrades_to_community(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """customer.subscription.deleted resets to community tier."""
        existing_row = MagicMock()
        existing_row.stripe_customer_id = "cus_del"

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = existing_row
        mock_session.execute = AsyncMock(return_value=result_mock)

        event = {
            "type": "customer.subscription.deleted",
            "data": {
                "object": {"id": "sub_del", "customer": "cus_del"},
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "processed"
        assert existing_row.plan_tier == "community"
        assert existing_row.stripe_subscription_id is None
        assert existing_row.period_start is None
        assert existing_row.period_end is None

    @pytest.mark.asyncio
    async def test_invoice_paid_logs_success(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """invoice.paid returns processed and logs (no side effects)."""
        event = {
            "type": "invoice.paid",
            "data": {
                "object": {
                    "id": "inv_paid",
                    "customer": "cus_paid",
                    "amount_paid": 9900,
                    "currency": "usd",
                },
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "processed"

    @pytest.mark.asyncio
    async def test_invoice_payment_failed_logs_warning(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """invoice.payment_failed returns processed."""
        event = {
            "type": "invoice.payment_failed",
            "data": {
                "object": {
                    "id": "inv_fail",
                    "customer": "cus_fail",
                },
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "processed"

    @pytest.mark.asyncio
    async def test_unhandled_event_type_ignored(self, mock_session: AsyncMock, billing_settings: APISettings) -> None:
        """Unhandled event types return ignored."""
        event = {
            "type": "charge.succeeded",
            "data": {"object": {}},
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        assert result["status"] == "ignored"

    @pytest.mark.asyncio
    async def test_subscription_event_for_unknown_customer(
        self, mock_session: AsyncMock, billing_settings: APISettings
    ) -> None:
        """Subscription event for unknown customer is handled gracefully."""
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result_mock)

        event = {
            "type": "customer.subscription.created",
            "data": {
                "object": {
                    "id": "sub_unknown",
                    "customer": "cus_unknown",
                    "items": {"data": []},
                },
            },
        }

        service = _make_service(mock_session, billing_settings)
        result = await service.handle_webhook_event(event)

        # Should not raise — returns processed even for unknown customer
        assert result["status"] == "processed"


# ---------------------------------------------------------------------------
# Price-to-tier mapping
# ---------------------------------------------------------------------------


class TestPriceIdToTier:
    """Verify _price_id_to_tier returns correct tiers."""

    def test_community_tier(self, billing_settings: APISettings) -> None:
        """Community price ID maps to 'community'."""
        session = AsyncMock()
        service = _make_service(session, billing_settings)
        assert service._price_id_to_tier("price_community") == "community"

    def test_team_tier(self, billing_settings: APISettings) -> None:
        """Team price ID maps to 'team'."""
        session = AsyncMock()
        service = _make_service(session, billing_settings)
        assert service._price_id_to_tier("price_team") == "team"

    def test_enterprise_tier(self, billing_settings: APISettings) -> None:
        """Enterprise price ID maps to 'enterprise'."""
        session = AsyncMock()
        service = _make_service(session, billing_settings)
        assert service._price_id_to_tier("price_enterprise") == "enterprise"

    def test_unknown_price_defaults_to_community(self, billing_settings: APISettings) -> None:
        """Unknown price ID defaults to 'community'."""
        session = AsyncMock()
        service = _make_service(session, billing_settings)
        assert service._price_id_to_tier("price_unknown_xyz") == "community"


# ---------------------------------------------------------------------------
# StripeUsageReporter lifecycle
# ---------------------------------------------------------------------------


class TestStripeUsageReporter:
    """Verify StripeUsageReporter start/stop lifecycle."""

    def test_start_creates_thread(self, billing_settings: APISettings) -> None:
        """start() creates a daemon background thread."""
        session_factory = MagicMock()
        reporter = StripeUsageReporter(
            session_factory=session_factory,
            settings=billing_settings,
            interval_seconds=0.1,
        )

        reporter.start()
        assert reporter._thread is not None
        assert reporter._thread.is_alive()
        assert reporter._thread.daemon is True

        reporter.stop()
        assert reporter._thread is None

    def test_double_start_is_idempotent(self, billing_settings: APISettings) -> None:
        """Calling start() twice does not create a second thread."""
        session_factory = MagicMock()
        reporter = StripeUsageReporter(
            session_factory=session_factory,
            settings=billing_settings,
            interval_seconds=0.1,
        )

        reporter.start()
        thread1 = reporter._thread

        reporter.start()
        thread2 = reporter._thread

        assert thread1 is thread2

        reporter.stop()

    def test_stop_without_start_is_noop(self, billing_settings: APISettings) -> None:
        """stop() without start() does not raise."""
        session_factory = MagicMock()
        reporter = StripeUsageReporter(
            session_factory=session_factory,
            settings=billing_settings,
        )

        reporter.stop()  # Should not raise
        assert reporter._thread is None
