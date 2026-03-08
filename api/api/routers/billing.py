"""Billing endpoints: subscription info, Stripe portal, webhooks, quotas, invoices."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, SettingsDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission
from api.schemas import (
    CheckoutSessionResponse,
    InvoiceListResponse,
    InvoiceResponse,
    PortalSessionResponse,
    QuotasResponse,
    SubscriptionResponse,
)
from api.services.billing_service import BillingService
from api.services.invoice_service import InvoiceService
from api.services.quota_service import QuotaService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/billing", tags=["billing"])


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class PortalRequest(BaseModel):
    """Request body for ``POST /billing/portal``."""

    return_url: str = Field(
        ...,
        description="URL to redirect the user to after leaving the Stripe portal.",
    )


class CheckoutRequest(BaseModel):
    """Request body for ``POST /billing/checkout``."""

    price_id: str = Field(
        ...,
        description="Stripe price ID for the chosen plan tier.",
    )
    success_url: str = Field(
        ...,
        description="URL to redirect to after successful checkout.",
    )
    cancel_url: str = Field(
        ...,
        description="URL to redirect to if the customer cancels.",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class BillingPlanTier(BaseModel):
    """A billing plan tier returned by ``GET /billing/plans``."""

    tier: str
    label: str
    price_label: str
    features: list[str]
    price_id: str | None = None


class BillingPlansResponse(BaseModel):
    """Response for ``GET /billing/plans``."""

    plans: list[BillingPlanTier]


@router.get("/plans", response_model=BillingPlansResponse)
async def get_billing_plans(
    settings: SettingsDep,
) -> BillingPlansResponse:
    """Return available billing plan tiers with Stripe price IDs from server config.

    The frontend should call this instead of hardcoding Stripe price IDs.
    """
    # Use per-seat price ID if configured, fallback to flat team price.
    team_price_id = settings.stripe_price_id_per_seat or settings.stripe_price_id_team
    plans = [
        BillingPlanTier(
            tier="community",
            label="Community",
            price_label="Free",
            features=[
                "1 seat included",
                "5 models",
                "Manual runs",
                "Community support",
            ],
            price_id=None,
        ),
        BillingPlanTier(
            tier="team",
            label="Team",
            price_label="$29/user/mo",
            features=[
                "Up to 10 seats included",
                "Unlimited models",
                "Scheduled runs",
                "AI advisory",
                "Team management",
                "Email support",
            ],
            price_id=team_price_id or None,
        ),
        BillingPlanTier(
            tier="enterprise",
            label="Enterprise",
            price_label="Custom",
            features=[
                "Unlimited seats",
                "Everything in Team",
                "SSO/OIDC",
                "Audit log",
                "Reconciliation",
                "SLA",
                "Dedicated support",
            ],
            price_id=settings.stripe_price_id_enterprise or None,
        ),
    ]
    return BillingPlansResponse(plans=plans)


@router.get("/subscription", response_model=SubscriptionResponse)
async def get_subscription(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, Any]:
    """Return the current subscription info for the authenticated tenant."""
    if not settings.billing_enabled:
        return {
            "plan_tier": "community",
            "status": "active",
            "billing_enabled": False,
        }

    service = BillingService(session, settings, tenant_id=tenant_id)
    info = await service.get_subscription_info()
    info["billing_enabled"] = True
    return info


@router.post("/portal", response_model=PortalSessionResponse)
async def create_portal_session(
    body: PortalRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, str]:
    """Create a Stripe Customer Portal session for subscription management.

    Returns a URL that the frontend should redirect the user to.
    """
    if not settings.billing_enabled:
        raise HTTPException(
            status_code=404,
            detail="Billing is not enabled for this installation.",
        )

    service = BillingService(session, settings, tenant_id=tenant_id)
    return await service.create_portal_session(return_url=body.return_url)


@router.post("/checkout", response_model=CheckoutSessionResponse)
async def create_checkout_session(
    body: CheckoutRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    request: Request,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, str]:
    """Create a Stripe Checkout session for new subscriptions.

    Returns a ``checkout_url`` that the frontend should redirect the user to.
    After completing payment, Stripe redirects back to ``success_url``.
    """
    if not settings.billing_enabled:
        raise HTTPException(
            status_code=404,
            detail="Billing is not enabled for this installation.",
        )

    service = BillingService(session, settings, tenant_id=tenant_id)
    user_email: str | None = None
    if hasattr(request.state, "sub"):
        # Try to resolve user email for Stripe customer pre-fill.
        from core_engine.state.repository import UserRepository

        user_repo = UserRepository(session, tenant_id=tenant_id)
        user = await user_repo.get_by_id(request.state.sub)
        if user:
            user_email = user.email

    return await service.create_checkout_session(
        price_id=body.price_id,
        success_url=body.success_url,
        cancel_url=body.cancel_url,
        customer_email=user_email,
    )


@router.post("/webhooks")
async def stripe_webhook(
    request: Request,
    settings: SettingsDep,
) -> dict[str, str]:
    """Handle incoming Stripe webhook events.

    Validates the webhook signature using the configured webhook secret
    and dispatches the event to the billing service.  This endpoint
    bypasses JWT authentication (validated via Stripe signature instead).
    """
    if not settings.billing_enabled:
        return {"status": "billing_disabled"}

    body = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not sig_header:
        raise HTTPException(status_code=400, detail="Missing Stripe signature")

    try:
        import stripe

        stripe.api_key = settings.stripe_secret_key.get_secret_value()
        event = stripe.Webhook.construct_event(
            payload=body,
            sig_header=sig_header,
            secret=settings.stripe_webhook_secret.get_secret_value(),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except Exception as exc:
        logger.warning("Stripe webhook signature verification failed: %s", exc)
        raise HTTPException(status_code=400, detail="Signature verification failed")

    # Resolve the real tenant_id from the Stripe customer ID embedded in
    # the event.  Most Stripe webhook events attach a ``customer`` field
    # to the data object; for events that don't, we also check the
    # top-level ``data.object.metadata.ironlayer_tenant_id`` which is
    # set during checkout session creation.
    from api.dependencies import get_session_factory

    data_object = event.get("data", {}).get("object", {})
    stripe_customer_id: str | None = data_object.get("customer")

    # Some events nest the customer inside the subscription object.
    if not stripe_customer_id and isinstance(data_object.get("subscription"), dict):
        stripe_customer_id = data_object["subscription"].get("customer")

    session_factory = get_session_factory()
    async with session_factory() as session:
        tenant_id: str | None = None

        if stripe_customer_id:
            from core_engine.state.tables import BillingCustomerTable
            from sqlalchemy import select

            tenant_lookup = await session.execute(
                select(BillingCustomerTable.tenant_id).where(
                    BillingCustomerTable.stripe_customer_id == stripe_customer_id
                )
            )
            tenant_id = tenant_lookup.scalar_one_or_none()

        # Fallback: check metadata set during checkout creation.
        if tenant_id is None:
            metadata = data_object.get("metadata", {}) or {}
            tenant_id = metadata.get("ironlayer_tenant_id")

        if tenant_id is None:
            # Cannot identify the tenant for this event.  Log a warning
            # and return 200 to prevent Stripe from retrying indefinitely
            # (retries are disruptive and won't resolve a missing mapping).
            logger.warning(
                "Stripe webhook event type=%s has no resolvable tenant_id "
                "(stripe_customer_id=%s); skipping processing.",
                event.get("type", "unknown"),
                stripe_customer_id,
            )
            return {"status": "ignored", "reason": "unknown_tenant"}

        # Activate RLS for the resolved tenant before any mutations.
        from core_engine.state.database import set_tenant_context

        await set_tenant_context(session, tenant_id)

        service = BillingService(session, settings, tenant_id=tenant_id)
        webhook_result = await service.handle_webhook_event(event)
        await session.commit()

    return webhook_result


# ---------------------------------------------------------------------------
# Quota endpoints
# ---------------------------------------------------------------------------


@router.get("/quotas", response_model=QuotasResponse)
async def get_quotas(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, Any]:
    """Return current usage vs. effective limits for all quota types.

    Used by the billing page to display usage progress bars and budget info.
    """
    service = QuotaService(session, tenant_id)
    return await service.get_usage_vs_limits()


# ---------------------------------------------------------------------------
# Invoice endpoints
# ---------------------------------------------------------------------------


@router.get("/invoices", response_model=InvoiceListResponse)
async def list_invoices(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_INVOICES)),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Return paginated list of invoices for this tenant."""
    service = InvoiceService(session, tenant_id, settings.invoice_storage_path)
    return await service.list_invoices(limit, offset)


@router.get("/invoices/{invoice_id}", response_model=InvoiceResponse)
async def get_invoice(
    invoice_id: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_INVOICES)),
) -> dict[str, Any]:
    """Return invoice detail including line items."""
    service = InvoiceService(session, tenant_id, settings.invoice_storage_path)
    invoice = await service.get_invoice(invoice_id)
    if invoice is None:
        raise HTTPException(status_code=404, detail=f"Invoice '{invoice_id}' not found")
    return invoice


@router.get("/invoices/{invoice_id}/download")
async def download_invoice_pdf(
    invoice_id: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.VIEW_INVOICES)),
) -> StreamingResponse:
    """Download the PDF for an invoice."""
    service = InvoiceService(session, tenant_id, settings.invoice_storage_path)
    pdf_bytes = await service.get_pdf(invoice_id)
    if pdf_bytes is None:
        raise HTTPException(status_code=404, detail=f"PDF not found for invoice '{invoice_id}'")
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="invoice-{invoice_id}.pdf"'},
    )
