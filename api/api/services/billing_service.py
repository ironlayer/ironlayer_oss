"""Stripe billing integration service.

Provides customer management, subscription lookups, portal session
creation, metered usage reporting, and webhook event processing.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from api.config import APISettings

logger = logging.getLogger(__name__)


class BillingService:
    """Stripe billing operations for a single tenant.

    Parameters
    ----------
    session:
        Active database session.
    settings:
        API settings containing Stripe configuration.
    tenant_id:
        The tenant performing billing operations.
    """

    def __init__(
        self,
        session: AsyncSession,
        settings: APISettings,
        *,
        tenant_id: str,
    ) -> None:
        self._session = session
        self._settings = settings
        self._tenant_id = tenant_id

    def _get_stripe(self) -> Any:
        """Lazily import and configure the Stripe library."""
        import stripe

        stripe.api_key = self._settings.stripe_secret_key.get_secret_value()
        return stripe

    async def get_or_create_customer(self) -> dict[str, Any]:
        """Return the Stripe customer for this tenant, creating if needed.

        Returns
        -------
        dict
            Contains ``customer_id``, ``subscription_id``, ``plan_tier``,
            ``period_start``, ``period_end``.
        """
        from core_engine.state.tables import BillingCustomerTable

        result = await self._session.execute(
            select(BillingCustomerTable).where(BillingCustomerTable.tenant_id == self._tenant_id)
        )
        row = result.scalar_one_or_none()

        if row is not None:
            return {
                "customer_id": row.stripe_customer_id,
                "subscription_id": row.stripe_subscription_id,
                "plan_tier": row.plan_tier,
                "period_start": row.period_start.isoformat() if row.period_start else None,
                "period_end": row.period_end.isoformat() if row.period_end else None,
            }

        # Create a new Stripe customer.
        stripe = self._get_stripe()
        customer = stripe.Customer.create(
            metadata={"ironlayer_tenant_id": self._tenant_id},
        )

        new_row = BillingCustomerTable(
            tenant_id=self._tenant_id,
            stripe_customer_id=customer["id"],
            plan_tier="community",
        )
        self._session.add(new_row)
        await self._session.flush()

        return {
            "customer_id": customer["id"],
            "subscription_id": None,
            "plan_tier": "community",
            "period_start": None,
            "period_end": None,
        }

    async def get_subscription_info(self) -> dict[str, Any]:
        """Return current subscription details for the tenant.

        Returns
        -------
        dict
            Subscription info including tier, status, and period.
        """
        from core_engine.state.tables import BillingCustomerTable

        result = await self._session.execute(
            select(BillingCustomerTable).where(BillingCustomerTable.tenant_id == self._tenant_id)
        )
        row = result.scalar_one_or_none()

        if row is None:
            return {
                "plan_tier": "community",
                "status": "active",
                "subscription_id": None,
                "period_start": None,
                "period_end": None,
            }

        info: dict[str, Any] = {
            "plan_tier": row.plan_tier,
            "subscription_id": row.stripe_subscription_id,
            "period_start": row.period_start.isoformat() if row.period_start else None,
            "period_end": row.period_end.isoformat() if row.period_end else None,
        }

        # Fetch live status from Stripe if subscription exists.
        if row.stripe_subscription_id:
            try:
                stripe = self._get_stripe()
                sub = stripe.Subscription.retrieve(row.stripe_subscription_id)
                info["status"] = sub.get("status", "unknown")
                info["cancel_at_period_end"] = sub.get("cancel_at_period_end", False)
                info["current_period_end"] = sub.get("current_period_end")
            except Exception:
                logger.warning(
                    "Failed to fetch Stripe subscription %s",
                    row.stripe_subscription_id,
                    exc_info=True,
                )
                info["status"] = "unknown"
        else:
            info["status"] = "active"

        return info

    async def create_portal_session(self, return_url: str) -> dict[str, str]:
        """Create a Stripe Customer Portal session.

        Parameters
        ----------
        return_url:
            URL to redirect the customer back to after portal interaction.

        Returns
        -------
        dict
            Contains ``url`` for the portal session.
        """
        customer_info = await self.get_or_create_customer()
        stripe = self._get_stripe()

        session = stripe.billing_portal.Session.create(
            customer=customer_info["customer_id"],
            return_url=return_url,
        )

        return {"url": session["url"]}

    async def create_checkout_session(
        self,
        price_id: str,
        success_url: str,
        cancel_url: str,
        customer_email: str | None = None,
    ) -> dict[str, str]:
        """Create a Stripe Checkout session for a new subscription.

        If the tenant already has a Stripe customer, the session is created
        for that customer.  Otherwise, Stripe creates the customer inline
        during checkout (pre-filled with ``customer_email`` if provided).

        Parameters
        ----------
        price_id:
            The Stripe price ID for the plan tier.
        success_url:
            URL to redirect the user to after successful payment.
        cancel_url:
            URL to redirect the user to on cancellation.
        customer_email:
            Optional email to pre-fill in the Stripe Checkout form.

        Returns
        -------
        dict
            Contains ``checkout_url`` to redirect the customer to.
        """
        from core_engine.state.tables import BillingCustomerTable

        stripe = self._get_stripe()

        # Check if tenant already has a Stripe customer.
        result = await self._session.execute(
            select(BillingCustomerTable).where(BillingCustomerTable.tenant_id == self._tenant_id)
        )
        existing = result.scalar_one_or_none()

        # Determine seat quantity for per-seat pricing.
        quantity = 1
        if price_id == self._settings.stripe_price_id_per_seat:
            from core_engine.state.repository import UserRepository

            user_repo = UserRepository(self._session, tenant_id=self._tenant_id)
            active_count = await user_repo.count_by_tenant()
            quantity = max(active_count, 1)

        session_params: dict[str, Any] = {
            "mode": "subscription",
            "line_items": [{"price": price_id, "quantity": quantity}],
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata": {"ironlayer_tenant_id": self._tenant_id},
        }

        if existing and existing.stripe_customer_id:
            session_params["customer"] = existing.stripe_customer_id
        elif customer_email:
            session_params["customer_email"] = customer_email

        checkout_session = stripe.checkout.Session.create(**session_params)

        # If the tenant doesn't have a billing record yet, create one
        # with the customer ID from the checkout session (if available).
        if existing is None:
            customer_id = checkout_session.get("customer") or ""
            if customer_id:
                new_row = BillingCustomerTable(
                    tenant_id=self._tenant_id,
                    stripe_customer_id=customer_id,
                    plan_tier="community",
                )
                self._session.add(new_row)
                await self._session.flush()

        return {"checkout_url": checkout_session["url"]}

    async def handle_webhook_event(self, event: dict[str, Any]) -> dict[str, str]:
        """Process a Stripe webhook event.

        Supported events:
        - ``customer.subscription.created``
        - ``customer.subscription.updated``
        - ``customer.subscription.deleted``
        - ``invoice.paid``
        - ``invoice.payment_failed``

        Parameters
        ----------
        event:
            The parsed Stripe webhook event object.

        Returns
        -------
        dict
            Contains ``status`` indicating processing result.
        """
        event_type = event.get("type", "")
        data_object = event.get("data", {}).get("object", {})

        if event_type in (
            "customer.subscription.created",
            "customer.subscription.updated",
        ):
            await self._handle_subscription_update(data_object)
            return {"status": "processed"}

        if event_type == "customer.subscription.deleted":
            await self._handle_subscription_deleted(data_object)
            return {"status": "processed"}

        if event_type == "invoice.paid":
            await self._handle_invoice_paid(data_object)
            return {"status": "processed"}

        if event_type == "invoice.payment_failed":
            logger.warning(
                "Payment failed for customer %s (invoice %s)",
                data_object.get("customer"),
                data_object.get("id"),
            )
            return {"status": "processed"}

        logger.debug("Unhandled Stripe event type: %s", event_type)
        return {"status": "ignored"}

    async def _handle_subscription_update(self, subscription: dict[str, Any]) -> None:
        """Update the local billing record for a subscription change.

        Also syncs ``max_seats`` from Stripe metadata if the key
        ``ironlayer_max_seats`` is present.  This allows enterprise sales
        to adjust seat limits directly from the Stripe dashboard.
        """
        from core_engine.state.tables import BillingCustomerTable

        customer_id = subscription.get("customer", "")
        result = await self._session.execute(
            select(BillingCustomerTable).where(BillingCustomerTable.stripe_customer_id == customer_id)
        )
        row = result.scalar_one_or_none()

        if row is None:
            logger.warning("Received subscription event for unknown customer: %s", customer_id)
            return

        row.stripe_subscription_id = subscription.get("id")

        # Derive plan tier from Stripe price ID.
        items = subscription.get("items", {}).get("data", [])
        if items:
            price_id = items[0].get("price", {}).get("id", "")
            row.plan_tier = self._price_id_to_tier(price_id)

        # Update billing period.
        period_start = subscription.get("current_period_start")
        period_end = subscription.get("current_period_end")
        if period_start:
            row.period_start = datetime.fromtimestamp(period_start, tz=UTC)
        if period_end:
            row.period_end = datetime.fromtimestamp(period_end, tz=UTC)

        # Sync max_seats from Stripe metadata if present.
        metadata = subscription.get("metadata") or {}
        max_seats_str = metadata.get("ironlayer_max_seats")
        if max_seats_str is not None:
            try:
                from core_engine.state.repository import TenantConfigRepository

                max_seats_val = int(max_seats_str)
                config_repo = TenantConfigRepository(self._session, row.tenant_id)
                config = await config_repo.get()
                if config is not None:
                    config.max_seats = max_seats_val
                    logger.info(
                        "Synced max_seats=%d from Stripe metadata for tenant %s",
                        max_seats_val,
                        row.tenant_id,
                    )
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid ironlayer_max_seats value in Stripe metadata: %s",
                    max_seats_str,
                )
            except Exception:
                logger.warning(
                    "Failed to sync max_seats from Stripe metadata for tenant %s",
                    row.tenant_id,
                    exc_info=True,
                )

        await self._session.flush()

    async def _handle_subscription_deleted(self, subscription: dict[str, Any]) -> None:
        """Downgrade the tenant to community tier on subscription cancellation."""
        from core_engine.state.tables import BillingCustomerTable

        customer_id = subscription.get("customer", "")
        result = await self._session.execute(
            select(BillingCustomerTable).where(BillingCustomerTable.stripe_customer_id == customer_id)
        )
        row = result.scalar_one_or_none()

        if row is None:
            return

        row.stripe_subscription_id = None
        row.plan_tier = "community"
        row.period_start = None
        row.period_end = None
        await self._session.flush()

    async def _handle_invoice_paid(self, invoice: dict[str, Any]) -> None:
        """Log successful payment (extensible for receipts, notifications)."""
        logger.info(
            "Invoice paid: %s for customer %s (amount: %s %s)",
            invoice.get("id"),
            invoice.get("customer"),
            invoice.get("amount_paid"),
            invoice.get("currency", "usd").upper(),
        )

    def _price_id_to_tier(self, price_id: str) -> str:
        """Map a Stripe price ID to an IronLayer plan tier."""
        tier_map: dict[str, str] = {
            self._settings.stripe_price_id_community: "community",
            self._settings.stripe_price_id_team: "team",
            self._settings.stripe_price_id_enterprise: "enterprise",
        }
        # Per-seat price maps to team tier.
        if self._settings.stripe_price_id_per_seat:
            tier_map[self._settings.stripe_price_id_per_seat] = "team"
        return tier_map.get(price_id, "community")


class StripeUsageReporter:
    """Background thread that aggregates and reports metered usage to Stripe.

    Runs on an hourly interval, queries un-reported usage events from the
    database, and pushes usage records to Stripe's metered billing API.

    Parameters
    ----------
    session_factory:
        Async session factory for database access.
    settings:
        API settings containing Stripe configuration.
    interval_seconds:
        Reporting interval in seconds (default: 3600 = 1 hour).
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: APISettings,
        interval_seconds: float = 3600.0,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._interval = interval_seconds
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the background reporting thread."""
        if self._thread is not None:
            return

        self._stop_event.clear()

        def _run() -> None:
            while not self._stop_event.wait(self._interval):
                try:
                    self._report_usage()
                except Exception:
                    logger.warning("Stripe usage reporting failed", exc_info=True)

        self._thread = threading.Thread(target=_run, name="stripe-usage-reporter", daemon=True)
        self._thread.start()
        logger.info("Stripe usage reporter started (interval=%.0fs)", self._interval)

    def stop(self) -> None:
        """Stop the background reporting thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _report_usage(self) -> None:
        """Aggregate and report usage to Stripe.

        Queries usage events from the last reporting interval, groups by
        tenant, and creates Stripe usage records for metered billing.
        """
        import asyncio

        from core_engine.state.tables import BillingCustomerTable, UsageEventTable
        from sqlalchemy import func as sa_func
        from sqlalchemy import select as sa_select

        async def _async_report() -> None:
            async with self._session_factory() as session:
                # Get all tenants with active Stripe subscriptions.
                customers_result = await session.execute(
                    sa_select(BillingCustomerTable).where(BillingCustomerTable.stripe_subscription_id.isnot(None))
                )
                customers = customers_result.scalars().all()

                if not customers:
                    return

                now = datetime.now(UTC)
                cutoff = datetime.fromtimestamp(now.timestamp() - self._interval, tz=UTC)

                stripe = self._get_stripe()

                for customer in customers:
                    # Count metered events in the interval.
                    count_result = await session.execute(
                        sa_select(sa_func.count(UsageEventTable.event_id)).where(
                            UsageEventTable.tenant_id == customer.tenant_id,
                            UsageEventTable.created_at >= cutoff,
                        )
                    )
                    event_count = count_result.scalar() or 0

                    if event_count == 0:
                        continue

                    if not self._settings.stripe_metered_price_id:
                        continue

                    try:
                        # Find the subscription item for metered pricing.
                        sub = stripe.Subscription.retrieve(
                            customer.stripe_subscription_id,
                            expand=["items.data"],
                        )
                        metered_item = None
                        for item in sub.get("items", {}).get("data", []):
                            if item.get("price", {}).get("id") == self._settings.stripe_metered_price_id:
                                metered_item = item
                                break

                        if metered_item is None:
                            continue

                        stripe.SubscriptionItem.create_usage_record(
                            metered_item["id"],
                            quantity=event_count,
                            timestamp=int(now.timestamp()),
                            action="increment",
                        )

                        logger.info(
                            "Reported %d usage events to Stripe for tenant %s",
                            event_count,
                            customer.tenant_id,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to report usage to Stripe for tenant %s",
                            customer.tenant_id,
                            exc_info=True,
                        )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_async_report())
        except RuntimeError:
            asyncio.run(_async_report())

    def _get_stripe(self) -> Any:
        """Lazily import and configure the Stripe library."""
        import stripe

        stripe.api_key = self._settings.stripe_secret_key.get_secret_value()
        return stripe
