"""Event subscription CRUD endpoints for IronLayer webhook delivery.

Allows tenants to register, list, update, and delete webhook subscriptions
that receive HTTP POST notifications when lifecycle events occur.

All endpoints require ``MANAGE_WEBHOOKS`` permission.
"""

from __future__ import annotations

import logging
from typing import Any

import bcrypt
from core_engine.state.repository import EventSubscriptionRepository
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/event-subscriptions", tags=["event-subscriptions"])


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class EventSubscriptionCreate(BaseModel):
    """Request body for creating a new event subscription."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Human-readable name for this subscription.",
    )
    url: str = Field(
        ...,
        min_length=1,
        max_length=2048,
        description="Webhook endpoint URL (must accept HTTP POST).",
    )
    secret: str | None = Field(
        default=None,
        min_length=8,
        description=(
            "Webhook signing secret (min 8 chars).  Used to compute "
            "HMAC-SHA256 signatures sent in the X-IronLayer-Signature header.  "
            "The secret is bcrypt-hashed at rest."
        ),
    )
    event_types: list[str] | None = Field(
        default=None,
        description=("List of event types to subscribe to.  If null or empty, subscribes to all events."),
    )
    description: str | None = Field(
        default=None,
        max_length=1024,
        description="Optional description of this subscription's purpose.",
    )


class EventSubscriptionUpdate(BaseModel):
    """Request body for updating an event subscription."""

    name: str | None = Field(default=None, min_length=1, max_length=256)
    url: str | None = Field(default=None, min_length=1, max_length=2048)
    secret: str | None = Field(
        default=None,
        min_length=8,
        description="New signing secret.  Omit to leave unchanged.",
    )
    event_types: list[str] | None = Field(default=None)
    active: bool | None = Field(default=None)
    description: str | None = Field(default=None, max_length=1024)


class EventSubscriptionResponse(BaseModel):
    """Serialised event subscription (never includes the secret)."""

    id: int
    tenant_id: str
    name: str
    url: str
    event_types: list[str] | None
    active: bool
    description: str | None
    created_at: str
    updated_at: str


def _row_to_response(row: Any) -> EventSubscriptionResponse:
    """Convert an ORM row to a response model."""
    return EventSubscriptionResponse(
        id=row.id,
        tenant_id=row.tenant_id,
        name=row.name,
        url=row.url,
        event_types=row.event_types,
        active=row.active,
        description=row.description,
        created_at=row.created_at.isoformat() if row.created_at else "",
        updated_at=row.updated_at.isoformat() if row.updated_at else "",
    )


def _hash_secret(secret: str) -> str:
    """Bcrypt hash a webhook signing secret."""
    return bcrypt.hashpw(secret.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=EventSubscriptionResponse,
    status_code=201,
    summary="Create a new event subscription",
)
async def create_subscription(
    body: EventSubscriptionCreate,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> EventSubscriptionResponse:
    """Register a new webhook subscription for lifecycle event delivery."""
    repo = EventSubscriptionRepository(session, tenant_id=tenant_id)

    secret_hash: str | None = None
    if body.secret:
        secret_hash = _hash_secret(body.secret)

    row = await repo.create(
        name=body.name,
        url=body.url,
        secret_hash=secret_hash,
        event_types=body.event_types,
        description=body.description,
    )

    logger.info(
        "Event subscription created: id=%d tenant=%s name=%s url=%s",
        row.id,
        tenant_id,
        body.name,
        body.url,
    )
    return _row_to_response(row)


@router.get(
    "",
    response_model=list[EventSubscriptionResponse],
    summary="List all event subscriptions",
)
async def list_subscriptions(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> list[EventSubscriptionResponse]:
    """Return all event subscriptions for the authenticated tenant."""
    repo = EventSubscriptionRepository(session, tenant_id=tenant_id)
    rows = await repo.list_all()
    return [_row_to_response(r) for r in rows]


@router.get(
    "/{subscription_id}",
    response_model=EventSubscriptionResponse,
    summary="Get a single event subscription",
)
async def get_subscription(
    subscription_id: int,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> EventSubscriptionResponse:
    """Fetch a single event subscription by ID."""
    repo = EventSubscriptionRepository(session, tenant_id=tenant_id)
    row = await repo.get(subscription_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Event subscription {subscription_id} not found",
        )
    return _row_to_response(row)


@router.put(
    "/{subscription_id}",
    response_model=EventSubscriptionResponse,
    summary="Update an event subscription",
)
async def update_subscription(
    subscription_id: int,
    body: EventSubscriptionUpdate,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> EventSubscriptionResponse:
    """Update fields on an existing event subscription."""
    repo = EventSubscriptionRepository(session, tenant_id=tenant_id)

    kwargs: dict[str, Any] = {}
    if body.name is not None:
        kwargs["name"] = body.name
    if body.url is not None:
        kwargs["url"] = body.url
    if body.secret is not None:
        kwargs["secret_hash"] = _hash_secret(body.secret)
    if body.event_types is not None:
        kwargs["event_types"] = body.event_types
    if body.active is not None:
        kwargs["active"] = body.active
    if body.description is not None:
        kwargs["description"] = body.description

    row = await repo.update(subscription_id, **kwargs)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Event subscription {subscription_id} not found",
        )

    logger.info(
        "Event subscription updated: id=%d tenant=%s fields=%s",
        subscription_id,
        tenant_id,
        list(kwargs.keys()),
    )
    return _row_to_response(row)


@router.delete(
    "/{subscription_id}",
    summary="Delete an event subscription",
)
async def delete_subscription(
    subscription_id: int,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_WEBHOOKS)),
) -> dict[str, Any]:
    """Delete an event subscription by ID."""
    repo = EventSubscriptionRepository(session, tenant_id=tenant_id)
    deleted = await repo.delete(subscription_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Event subscription {subscription_id} not found",
        )

    logger.info(
        "Event subscription deleted: id=%d tenant=%s",
        subscription_id,
        tenant_id,
    )
    return {"deleted": True, "subscription_id": subscription_id}
