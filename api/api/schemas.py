"""Shared Pydantic response models for API endpoints.

These schemas ensure that endpoint responses are validated and documented
in the OpenAPI specification.  Routers import from here to avoid duplication.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Cursor pagination (BL-120)
# ---------------------------------------------------------------------------


class CursorPage(BaseModel, Generic[T]):
    """Keyset-paginated response envelope.

    ``next_cursor`` is a base64-encoded opaque token representing the last
    item in the current page.  Pass it as ``?cursor=<token>`` on the next
    request to retrieve the following page.  ``None`` means you are on the
    last page.

    Unlike offset-based pagination, keyset cursors are O(1) regardless of
    page position because the database uses an indexed ``WHERE id < :cursor``
    predicate instead of ``OFFSET N``.

    ``total_hint`` is an optional approximate row count.  It may be stale
    (computed from statistics) and should only be used for UI display, not
    for determining whether more pages exist.
    """

    items: list[T]
    next_cursor: str | None = Field(
        None,
        description="Opaque cursor for the next page. None when this is the last page.",
    )
    total_hint: int | None = Field(
        None,
        description="Approximate total row count (may be stale). For display only.",
    )

# ---------------------------------------------------------------------------
# Plan schemas
# ---------------------------------------------------------------------------


class PlanStepResponse(BaseModel):
    """A single step within an execution plan."""

    step_id: str
    model: str
    run_type: str
    input_range: dict[str, str] | None = None
    depends_on: list[str] = Field(default_factory=list)
    parallel_group: int = 0
    reason: str = ""
    estimated_compute_seconds: float = 0
    estimated_cost_usd: float = 0
    contract_violations: list[dict[str, Any]] = Field(default_factory=list)


class PlanSummaryResponse(BaseModel):
    """Summary statistics for a plan."""

    total_steps: int
    estimated_cost_usd: float
    models_changed: list[str] = Field(default_factory=list)
    contract_violations_count: int = 0
    breaking_contract_violations: int = 0


class ApprovalRecord(BaseModel):
    """A single approval or rejection on a plan."""

    user: str
    action: str | None = None
    comment: str = ""
    timestamp: str | None = None
    at: str | None = None
    rejected: bool | None = None


class PlanResponse(BaseModel):
    """Full plan response."""

    plan_id: str
    base: str | None = None
    target: str | None = None
    summary: PlanSummaryResponse | dict[str, Any] | None = None
    steps: list[PlanStepResponse | dict[str, Any]] = Field(default_factory=list)
    approvals: list[ApprovalRecord | dict[str, Any]] = Field(default_factory=list)
    auto_approved: bool = False
    created_at: str | None = None

    model_config = {"extra": "allow"}


class PlanListItemResponse(BaseModel):
    """Summary item for plan listing."""

    plan_id: str
    base_sha: str | None = None
    target_sha: str | None = None
    total_steps: int = 0
    estimated_cost_usd: float = 0
    models_changed: list[str] = Field(default_factory=list)
    created_at: str | None = None

    model_config = {"extra": "allow"}


class PlanFeedbackResponse(BaseModel):
    """Response for suggestion feedback submission."""

    updated: int
    plan_id: str


class PlanFeedbackStatsResponse(BaseModel):
    """Response for feedback stats."""

    plan_id: str

    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Approval schemas
# ---------------------------------------------------------------------------


class PlanApprovalResponse(BaseModel):
    """Response after approving or rejecting a plan."""

    plan_id: str
    approvals: list[dict[str, Any]] = Field(default_factory=list)
    auto_approved: bool = False
    created_at: str | None = None
    rejected: bool | None = None
    rejected_by: str | None = None
    rejection_reason: str | None = None

    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Run schemas
# ---------------------------------------------------------------------------


class RunRecordResponse(BaseModel):
    """A single run record."""

    run_id: str
    plan_id: str
    step_id: str
    model_name: str
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    input_range: dict[str, str] | None = None
    error_message: str | None = None
    logs_uri: str | None = None
    cluster_used: str | None = None
    executor_version: str = ""
    retry_count: int = 0

    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Billing schemas
# ---------------------------------------------------------------------------


class SubscriptionResponse(BaseModel):
    """Subscription information response."""

    plan_tier: str
    status: str
    subscription_id: str | None = None
    period_start: str | None = None
    period_end: str | None = None
    cancel_at_period_end: bool | None = None
    current_period_end: int | None = None
    billing_enabled: bool = False

    model_config = {"extra": "allow"}


class PortalSessionResponse(BaseModel):
    """Stripe portal session response."""

    url: str


class CheckoutSessionResponse(BaseModel):
    """Stripe checkout session response."""

    checkout_url: str


class QuotaItemResponse(BaseModel):
    """A single quota item."""

    name: str
    event_type: str
    used: int
    limit: int | None = None
    percentage: float | None = None


class LLMBudgetResponse(BaseModel):
    """LLM budget information."""

    daily_used_usd: float
    daily_limit_usd: float | None = None
    monthly_used_usd: float
    monthly_limit_usd: float | None = None


class QuotasResponse(BaseModel):
    """Full quotas response."""

    quotas: list[QuotaItemResponse]
    llm_budget: LLMBudgetResponse


class InvoiceLineItemResponse(BaseModel):
    """A single invoice line item."""

    description: str
    quantity: int | float
    unit_price: float
    amount: float


class InvoiceResponse(BaseModel):
    """Full invoice response."""

    invoice_id: str
    invoice_number: str
    stripe_invoice_id: str | None = None
    period_start: str | None = None
    period_end: str | None = None
    subtotal_usd: float
    tax_usd: float
    total_usd: float
    line_items: list[InvoiceLineItemResponse] | None = None
    status: str
    created_at: str | None = None


class InvoiceListResponse(BaseModel):
    """Paginated invoice list response."""

    invoices: list[InvoiceResponse]
    total: int


# ---------------------------------------------------------------------------
# Team management schemas
# ---------------------------------------------------------------------------


class TeamMemberResponse(BaseModel):
    """A single team member."""

    id: str
    email: str
    display_name: str
    role: str
    is_active: bool
    created_at: str | None = None
    last_login_at: str | None = None


class TeamMembersResponse(BaseModel):
    """Team members list with seat usage information."""

    members: list[TeamMemberResponse]
    total: int
    seat_limit: int | None = None
    seats_used: int


class InviteMemberRequest(BaseModel):
    """Request body for inviting a new team member."""

    email: str = Field(..., min_length=3, max_length=320, description="Email address of the invitee.")
    role: str = Field(
        default="viewer",
        pattern="^(viewer|operator|engineer|admin)$",
        description="Role to assign to the new member.",
    )


class UpdateRoleRequest(BaseModel):
    """Request body for changing a team member's role."""

    role: str = Field(
        ...,
        pattern="^(viewer|operator|engineer|admin)$",
        description="New role for the team member.",
    )
