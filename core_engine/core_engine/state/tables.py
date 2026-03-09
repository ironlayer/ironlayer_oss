"""SQLAlchemy 2.0 ORM table definitions for IronLayer state store.

All tables use the modern ``Mapped`` / ``mapped_column`` declaration style
introduced in SQLAlchemy 2.0.  The ``Base`` declarative base is exported for
use by Alembic migrations and the repository layer.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    false,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Cross-dialect JSON type: uses JSONB on PostgreSQL for GIN indexing and
# query operators, falls back to plain JSON (stored as TEXT) on SQLite.
_JsonType = JSONB().with_variant(JSON(), "sqlite")


def _utcnow() -> datetime:
    """Return the current UTC timestamp (timezone-aware)."""
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Shared declarative base for all IronLayer tables."""


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ModelTable(Base):
    """Registry of known SQL models managed by IronLayer."""

    __tablename__ = "models"

    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    repo_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    current_version: Mapped[str] = mapped_column(String(64), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    time_column: Mapped[str | None] = mapped_column(String(256), nullable=True)
    unique_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    materialization: Mapped[str] = mapped_column(String(64), nullable=False)
    owner: Mapped[str | None] = mapped_column(String(256), nullable=True)
    tags: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    last_modified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "model_name"),
        Index("ix_models_tenant", "tenant_id"),
    )


# ---------------------------------------------------------------------------
# Model versions
# ---------------------------------------------------------------------------


class ModelVersionTable(Base):
    """Immutable version records for individual model revisions."""

    __tablename__ = "model_versions"

    version_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    canonical_sql: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_sql_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "model_name"],
            ["models.tenant_id", "models.model_name"],
            name="fk_model_versions_tenant_model",
            ondelete="CASCADE",
        ),
        Index("ix_model_versions_model_name", "model_name"),
        Index("ix_model_versions_tenant", "tenant_id"),
    )


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


class SnapshotTable(Base):
    """Point-in-time captures of all model versions in an environment."""

    __tablename__ = "snapshots"

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    environment: Mapped[str] = mapped_column(String(64), nullable=False)
    model_versions_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_snapshots_environment", "environment"),
        Index("ix_snapshots_tenant_env", "tenant_id", "environment"),
    )


# ---------------------------------------------------------------------------
# Watermarks
# ---------------------------------------------------------------------------


class WatermarkTable(Base):
    """Partition watermarks tracking incremental progress per model."""

    __tablename__ = "watermarks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    partition_start: Mapped[date] = mapped_column(Date, nullable=False)
    partition_end: Mapped[date] = mapped_column(Date, nullable=False)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "model_name",
            "partition_start",
            "partition_end",
            name="uq_watermarks_tenant_model_range",
        ),
        Index("ix_watermarks_model_name", "model_name"),
    )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class RunTable(Base):
    """Execution run records tracking individual step outcomes."""

    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    plan_id: Mapped[str] = mapped_column(String(64), nullable=False)
    step_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    input_range_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    input_range_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    logs_uri: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    cluster_used: Mapped[str | None] = mapped_column(String(256), nullable=True)
    executor_version: Mapped[str] = mapped_column(String(64), nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    external_run_id: Mapped[str | None] = mapped_column(String(256), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED')",
            name="ck_runs_status",
        ),
        Index("ix_runs_plan_id", "plan_id"),
        Index("ix_runs_model_name", "model_name"),
        Index("ix_runs_tenant_plan", "tenant_id", "plan_id"),
        Index("ix_runs_tenant_model", "tenant_id", "model_name"),
        Index("ix_runs_tenant_model_status", "tenant_id", "model_name", "status"),
        Index("ix_runs_external_run_id", "external_run_id"),
    )


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------


class PlanTable(Base):
    """Persisted execution plans with optional approval tracking."""

    __tablename__ = "plans"

    plan_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    base_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    target_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=False)
    approvals_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    advisory_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    auto_approved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_plans_tenant_created", "tenant_id", "created_at"),
        Index("ix_plans_plan_json_gin", "plan_json", postgresql_using="gin"),
        Index("ix_plans_advisory_json_gin", "advisory_json", postgresql_using="gin"),
    )


# ---------------------------------------------------------------------------
# Locks
# ---------------------------------------------------------------------------


class LockTable(Base):
    """Partition-range advisory locks preventing concurrent writes."""

    __tablename__ = "locks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    range_start: Mapped[date] = mapped_column(Date, nullable=False)
    range_end: Mapped[date] = mapped_column(Date, nullable=False)
    locked_by: Mapped[str] = mapped_column(String(256), nullable=False)
    locked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    ttl_seconds: Mapped[int] = mapped_column(Integer, default=3600, nullable=False)
    force_release_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    preemption_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "model_name",
            "range_start",
            "range_end",
            name="uq_locks_tenant_model_range",
        ),
        Index("ix_locks_model_name", "model_name"),
        Index("ix_locks_tenant_model", "tenant_id", "model_name"),
    )


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


class TelemetryTable(Base):
    """Per-run compute telemetry captured after step execution."""

    __tablename__ = "telemetry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    runtime_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    shuffle_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    input_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    output_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    partition_count: Mapped[int] = mapped_column(Integer, nullable=False)
    cluster_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_telemetry_run_id", "run_id"),
        Index("ix_telemetry_model_name", "model_name"),
        Index("ix_telemetry_tenant_model_captured", "tenant_id", "model_name", "captured_at"),
    )


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


class CredentialTable(Base):
    """Encrypted credential storage for tenant-scoped secrets.

    Credential values are encrypted at the application layer using
    Fernet symmetric encryption before being stored. The encryption
    key is derived from the platform's JWT secret.
    """

    __tablename__ = "credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    credential_name: Mapped[str] = mapped_column(String(256), nullable=False)
    encrypted_value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    last_rotated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "credential_name",
            name="uq_credentials_tenant_name",
        ),
        Index("ix_credentials_tenant", "tenant_id"),
    )


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------


class AuditLogTable(Base):
    """Append-only audit log with tamper-evidence via hash chaining.

    Each entry records a security-relevant action taken within the platform.
    The ``entry_hash`` is a SHA-256 digest of the entry's content fields,
    and ``previous_hash`` links to the preceding entry's hash to form a
    tamper-evident chain per tenant.

    GDPR right-to-erasure notes
    ---------------------------
    ``is_anonymized`` is set to ``True`` when ``actor`` and ``metadata_json``
    have been redacted.  Hash-chain verification (``verify_chain``) skips the
    hash recomputation for anonymized entries — the stored ``entry_hash``
    (computed from original data) is used as-is to advance the chain — so
    the chain remains verifiable for all non-anonymized surrounding entries.
    """

    __tablename__ = "audit_log"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    entity_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    previous_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    entry_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_anonymized: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=false())

    __table_args__ = (
        Index("ix_audit_log_tenant_id", "tenant_id"),
        Index("ix_audit_tenant_created", "tenant_id", "created_at"),
        Index("ix_audit_tenant_action", "tenant_id", "action"),
        Index("ix_audit_entity", "tenant_id", "entity_type", "entity_id"),
    )


# ---------------------------------------------------------------------------
# Token revocations
# ---------------------------------------------------------------------------


class TokenRevocationTable(Base):
    """Revoked JWT tokens tracked by their jti (JWT ID) claim.

    Used for replay protection: when a token is revoked, subsequent
    requests bearing the same jti are rejected by the auth middleware.
    """

    __tablename__ = "token_revocations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    jti: Mapped[str] = mapped_column(String(64), nullable=False)
    revoked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("tenant_id", "jti", name="uq_token_revocations_tenant_jti"),
        Index("ix_token_revocations_jti", "jti"),
        Index("ix_token_revocations_tenant", "tenant_id"),
    )


# ---------------------------------------------------------------------------
# Tenant configuration
# ---------------------------------------------------------------------------


class TenantConfigTable(Base):
    """Per-tenant feature configuration and provisioning state.

    Stores opt-in/opt-out flags for features like LLM integration plus
    lifecycle metadata (creation, soft-delete).  Uses the tenant_id as the
    primary key (one row per tenant).
    """

    __tablename__ = "tenant_config"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    llm_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    llm_monthly_budget_usd: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True, default=None)
    llm_daily_budget_usd: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )
    plan_quota_monthly: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    api_quota_monthly: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    ai_quota_monthly: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    max_seats: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=365)
    updated_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)


# ---------------------------------------------------------------------------
# Reconciliation checks
# ---------------------------------------------------------------------------


class ReconciliationCheckTable(Base):
    """Records reconciliation checks comparing control-plane state to warehouse state.

    Each row captures a single check for a specific run, recording whether the
    control plane's recorded status matches the actual outcome observed in the
    execution backend (e.g., Databricks).
    """

    __tablename__ = "reconciliation_checks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    expected_status: Mapped[str] = mapped_column(String(32), nullable=False)
    warehouse_status: Mapped[str] = mapped_column(String(32), nullable=False)
    discrepancy_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    resolved_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_reconciliation_tenant_run", "tenant_id", "run_id"),
        Index("ix_reconciliation_tenant_unresolved", "tenant_id", "resolved"),
        Index("ix_reconciliation_checked_at", "tenant_id", "checked_at"),
    )


# ---------------------------------------------------------------------------
# AI feedback
# ---------------------------------------------------------------------------


class AIFeedbackTable(Base):
    """Records AI predictions and their actual outcomes for feedback loop.

    Each row captures a single AI prediction (cost, risk, classification) for
    a model step, the actual outcome after execution, and whether the
    suggestion was accepted by the operator.
    """

    __tablename__ = "ai_feedback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_id: Mapped[str] = mapped_column(String(64), nullable=False)
    step_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    feedback_type: Mapped[str] = mapped_column(String(64), nullable=False)  # "cost", "risk", "classification"
    prediction_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    outcome_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    accepted: Mapped[bool | None] = mapped_column(Boolean, nullable=True)  # None = not yet decided
    accuracy_score: Mapped[float | None] = mapped_column(Float, nullable=True)  # 0.0-1.0
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_ai_feedback_tenant_plan", "tenant_id", "plan_id"),
        Index("ix_ai_feedback_tenant_model", "tenant_id", "model_name"),
        Index("ix_ai_feedback_tenant_type", "tenant_id", "feedback_type"),
        Index("ix_ai_feedback_created_at", "tenant_id", "created_at"),
    )


# ---------------------------------------------------------------------------
# LLM usage log
# ---------------------------------------------------------------------------


class LLMUsageLogTable(Base):
    """Per-call LLM usage tracking for budget enforcement.

    Each row records a single LLM API call with token counts and estimated
    cost, enabling per-tenant budget guardrails and usage analytics.
    """

    __tablename__ = "llm_usage_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    call_type: Mapped[str] = mapped_column(String(64), nullable=False)
    model_id: Mapped[str] = mapped_column(String(128), nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_cost_usd: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_llm_usage_tenant_created", "tenant_id", "created_at"),
        Index("ix_llm_usage_tenant_type", "tenant_id", "call_type"),
    )


# ---------------------------------------------------------------------------
# Usage metering
# ---------------------------------------------------------------------------


class UsageEventTable(Base):
    """Metered usage events for billing and quota enforcement.

    Each row captures a single billable action (plan run, apply, AI call,
    etc.) with quantity and optional metadata.  Separate from telemetry
    (which covers compute observability).
    """

    __tablename__ = "usage_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        # Composite index covering (tenant_id, event_type, created_at).
        # ix_usage_events_tenant_type_month was an identical duplicate — dropped by BL-101.
        # ix_usage_events_tenant_created (tenant_id, created_at) was a strict prefix
        # subset of this composite index — dropped by BL-101 (migration 029).
        Index("ix_usage_events_tenant_type_created", "tenant_id", "event_type", "created_at"),
    )


# ---------------------------------------------------------------------------
# Billing
# ---------------------------------------------------------------------------


class BillingCustomerTable(Base):
    """Stripe customer and subscription state per tenant.

    Tracks the mapping between IronLayer tenants and their Stripe
    customer/subscription identifiers, plus the current billing plan tier.
    """

    __tablename__ = "billing_customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    stripe_customer_id: Mapped[str] = mapped_column(String(256), nullable=False)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    plan_tier: Mapped[str] = mapped_column(String(32), nullable=False, default="community")
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        Index("ix_billing_customers_tenant", "tenant_id"),
        Index("ix_billing_customers_stripe_customer", "stripe_customer_id"),
    )


# ---------------------------------------------------------------------------
# Webhook configurations
# ---------------------------------------------------------------------------


class WebhookConfigTable(Base):
    """GitHub (and future provider) webhook configurations per tenant.

    Stores the repository URL, branch filter, hashed secret, encrypted
    secret (for HMAC verification), and automation flags for CI-triggered
    plan generation.

    The ``secret_hash`` column stores a bcrypt hash for config-level
    secret validation.  The ``secret_encrypted`` column stores a
    Fernet-encrypted copy of the plaintext secret so that the webhook
    receiver can compute HMAC-SHA256 digests for signature verification.
    """

    __tablename__ = "webhook_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="github")
    repo_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    branch: Mapped[str] = mapped_column(String(256), nullable=False, default="main")
    secret_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    secret_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_plan: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    auto_apply: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "provider",
            "repo_url",
            "branch",
            name="uq_webhook_configs_tenant_provider_repo_branch",
        ),
        Index("ix_webhook_configs_tenant", "tenant_id"),
        Index("ix_webhook_configs_tenant_provider_repo", "tenant_id", "provider", "repo_url"),
    )


# ---------------------------------------------------------------------------
# Backfill checkpoints
# ---------------------------------------------------------------------------


class BackfillCheckpointTable(Base):
    """Tracks chunked backfill progress for checkpoint-based resume.

    Each row represents a single backfill operation that may span many
    day-sized chunks.  The ``completed_through`` field records the last
    date that was successfully processed, enabling resume from the next
    day on failure without re-executing already-completed chunks.
    """

    __tablename__ = "backfill_checkpoints"

    backfill_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    overall_start: Mapped[date] = mapped_column(Date, nullable=False)
    overall_end: Mapped[date] = mapped_column(Date, nullable=False)
    completed_through: Mapped[date | None] = mapped_column(Date, nullable=True)
    chunk_size_days: Mapped[int] = mapped_column(Integer, default=7, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="RUNNING", nullable=False)
    total_chunks: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_chunks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    cluster_size: Mapped[str | None] = mapped_column(String(32), nullable=True)
    plan_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('RUNNING','COMPLETED','FAILED')",
            name="ck_backfill_checkpoints_status",
        ),
        Index("ix_backfill_checkpoints_tenant_id", "tenant_id", "backfill_id"),
        Index(
            "ix_backfill_checkpoints_tenant_model_status",
            "tenant_id",
            "model_name",
            "status",
        ),
    )


# ---------------------------------------------------------------------------
# Backfill audit
# ---------------------------------------------------------------------------


class BackfillAuditTable(Base):
    """Per-chunk execution history for backfill operations.

    Each row records the outcome of a single chunk within a backfill,
    providing a detailed audit trail of what ran, when, and whether it
    succeeded or failed.
    """

    __tablename__ = "backfill_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    backfill_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    chunk_start: Mapped[date] = mapped_column(Date, nullable=False)
    chunk_end: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('RUNNING','SUCCESS','FAILED')",
            name="ck_backfill_audit_status",
        ),
        Index(
            "ix_backfill_audit_tenant_model_executed",
            "tenant_id",
            "model_name",
            "executed_at",
        ),
        Index("ix_backfill_audit_backfill_id", "tenant_id", "backfill_id"),
    )


# ---------------------------------------------------------------------------
# Schema drift checks
# ---------------------------------------------------------------------------


class SchemaDriftCheckTable(Base):
    """Records schema comparison results between expected and actual table schemas.

    Each row captures a single schema drift detection for a model, recording the
    expected columns (from the model definition or contract), the actual columns
    (from the warehouse), the type of drift, and resolution tracking.
    """

    __tablename__ = "schema_drift_checks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    expected_columns_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    actual_columns_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    drift_type: Mapped[str] = mapped_column(String(64), nullable=False)
    drift_details_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    resolved_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "drift_type IN ('COLUMN_ADDED','COLUMN_REMOVED','TYPE_CHANGED','NONE')",
            name="ck_schema_drift_checks_drift_type",
        ),
        Index("ix_schema_drift_tenant_model", "tenant_id", "model_name"),
        Index("ix_schema_drift_tenant_unresolved", "tenant_id", "resolved"),
        Index("ix_schema_drift_checked_at", "tenant_id", "checked_at"),
    )


# ---------------------------------------------------------------------------
# Reconciliation schedules
# ---------------------------------------------------------------------------


class ReconciliationScheduleTable(Base):
    """Configurable background reconciliation check schedules.

    Each row defines a recurring reconciliation schedule for a tenant, specifying
    the schedule type (run reconciliation vs. schema drift checks), a cron
    expression, and tracking for the last/next execution times.
    """

    __tablename__ = "reconciliation_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    schedule_type: Mapped[str] = mapped_column(String(64), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (Index("ix_recon_schedule_tenant_type", "tenant_id", "schedule_type", unique=True),)


# ---------------------------------------------------------------------------
# Environments
# ---------------------------------------------------------------------------


class EnvironmentTable(Base):
    """First-class environment with catalog/schema mapping for SQL rewriting.

    Each environment maps to a Databricks catalog and schema prefix, enabling
    SQL statements to be rewritten at execution time for environment isolation.
    Plans remain deterministic and environment-agnostic; rewriting happens only
    when SQL is sent to the execution backend.
    """

    __tablename__ = "environments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    catalog: Mapped[str] = mapped_column(String(256), nullable=False)
    schema_prefix: Mapped[str] = mapped_column(String(256), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_production: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_ephemeral: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    branch_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by: Mapped[str] = mapped_column(String(256), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        Index("ix_env_tenant_name", "tenant_id", "name", unique=True),
        Index("ix_env_tenant_ephemeral", "tenant_id", "is_ephemeral"),
        Index("ix_env_expires_at", "expires_at"),
    )


class EnvironmentPromotionTable(Base):
    """Records environment promotion events (snapshot reference copies).

    Promotion copies snapshot references from a source environment to a target
    environment.  The actual data is not duplicated -- only the snapshot metadata
    pointers are recorded, enabling fast and safe promotion workflows.
    """

    __tablename__ = "environment_promotions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source_environment: Mapped[str] = mapped_column(String(128), nullable=False)
    target_environment: Mapped[str] = mapped_column(String(128), nullable=False)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False)
    target_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False)
    promoted_by: Mapped[str] = mapped_column(String(256), nullable=False)
    promoted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)

    __table_args__ = (
        Index("ix_promotion_tenant_source", "tenant_id", "source_environment"),
        Index("ix_promotion_tenant_target", "tenant_id", "target_environment"),
        Index("ix_promotion_promoted_at", "tenant_id", "promoted_at"),
    )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class ModelTestTable(Base):
    """Stores test definitions (can be auto-populated from model headers)."""

    __tablename__ = "model_tests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    test_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    test_type: Mapped[str] = mapped_column(String(64), nullable=False)
    test_config_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    severity: Mapped[str] = mapped_column(String(32), default="BLOCK", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "severity IN ('BLOCK','WARN','INFO')",
            name="ck_model_tests_severity",
        ),
        Index("ix_model_test_tenant_model", "tenant_id", "model_name"),
        Index("ix_model_test_tenant_id", "tenant_id", "test_id", unique=True),
    )


# ---------------------------------------------------------------------------
# Test results
# ---------------------------------------------------------------------------


class TestResultTable(Base):
    """Records test execution results."""

    __tablename__ = "test_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    test_id: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    model_name: Mapped[str] = mapped_column(String(512), nullable=False)
    test_type: Mapped[str] = mapped_column(String(64), nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_test_result_tenant_plan", "tenant_id", "plan_id"),
        Index("ix_test_result_tenant_model", "tenant_id", "model_name"),
        Index("ix_test_result_executed_at", "tenant_id", "executed_at"),
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


class UserTable(Base):
    """Platform user accounts for authentication and identity.

    Users belong to exactly one tenant.  The first user created for a
    tenant is automatically assigned the ADMIN role.  Passwords are
    stored as bcrypt hashes; the plaintext is never persisted.
    """

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    display_name: Mapped[str] = mapped_column(String(256), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="viewer")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),
        Index("ix_users_tenant", "tenant_id"),
        Index("ix_users_email", "email"),
    )


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------


class APIKeyTable(Base):
    """Long-lived API keys for programmatic access (CLI, CI/CD).

    Keys are shown to the user exactly once at creation time.  Only the
    SHA-256 hash of the key is stored; the first 16 characters are kept as
    a prefix to help users identify their keys.
    """

    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    scopes: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_api_keys_tenant", "tenant_id"),
        Index("ix_api_keys_user", "user_id"),
        Index("ix_api_keys_key_hash", "key_hash", unique=True),
        Index("ix_api_keys_prefix", "key_prefix"),
    )


# ---------------------------------------------------------------------------
# Team members
# ---------------------------------------------------------------------------


class TeamMemberTable(Base):
    """Team membership tracking with invitation workflow.

    Records which users belong to a tenant and their role assignment,
    including invitation tracking for pending team members.
    """

    __tablename__ = "team_members"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="viewer")
    invited_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("tenant_id", "user_id", name="uq_team_members_tenant_user"),
        Index("ix_team_members_tenant", "tenant_id"),
        Index("ix_team_members_user", "user_id"),
    )


# ---------------------------------------------------------------------------
# Event subscriptions
# ---------------------------------------------------------------------------


class EventSubscriptionTable(Base):
    """Webhook event subscriptions for external notification delivery.

    Each subscription registers a URL endpoint to receive HTTP POST
    notifications for specified event types.  Secrets are stored as
    bcrypt hashes at rest; the plaintext is never persisted.
    """

    __tablename__ = "event_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    secret_hash: Mapped[str | None] = mapped_column(String(256), nullable=True)
    event_types: Mapped[list | None] = mapped_column(_JsonType, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_event_sub_tenant", "tenant_id"),
        Index("ix_event_sub_tenant_active", "tenant_id", "active"),
    )


# ---------------------------------------------------------------------------
# Customer health
# ---------------------------------------------------------------------------


class CustomerHealthTable(Base):
    """Per-tenant engagement and health tracking for churn prediction.

    Health scores are computed periodically from login recency, plan
    activity, AI adoption, and feature breadth.  A score of 0-100 maps to
    ``active`` (>=60), ``at_risk`` (30-59), or ``churning`` (<30) status.
    """

    __tablename__ = "customer_health"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    health_score: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False, default=100.0)
    health_status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_plan_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_ai_call_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    engagement_metrics_json: Mapped[dict | None] = mapped_column(_JsonType, nullable=True)
    trend_direction: Mapped[str | None] = mapped_column(String(32), nullable=True)
    previous_score: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "health_status IN ('active','at_risk','churning')",
            name="ck_customer_health_health_status",
        ),
        Index("ix_customer_health_tenant", "tenant_id", unique=True),
        Index("ix_customer_health_status", "health_status"),
        Index("ix_customer_health_score", "health_score"),
    )


# ---------------------------------------------------------------------------
# Invoices
# ---------------------------------------------------------------------------


class InvoiceTable(Base):
    """Generated invoice records with line-item detail and PDF storage.

    Each invoice covers a billing period and contains an itemised breakdown
    of usage charges (plan runs, AI calls, LLM cost, API requests).  The
    optional ``pdf_storage_key`` points to a rendered PDF on the filesystem.
    """

    __tablename__ = "invoices"

    invoice_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    stripe_invoice_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    invoice_number: Mapped[str] = mapped_column(String(64), nullable=False)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    subtotal_usd: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    tax_usd: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False, default=0.0)
    total_usd: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    line_items_json: Mapped[dict] = mapped_column(_JsonType, nullable=False)
    pdf_storage_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="generated")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('generated','paid','void')",
            name="ck_invoices_status",
        ),
        Index("ix_invoices_tenant_created", "tenant_id", "created_at"),
        Index("ix_invoices_tenant_number", "tenant_id", "invoice_number", unique=True),
        Index("ix_invoices_stripe_invoice", "stripe_invoice_id"),
        Index("ix_invoices_tenant_period", "tenant_id", "period_start", "period_end"),
    )


# ---------------------------------------------------------------------------
# Event outbox (transactional event persistence)
# ---------------------------------------------------------------------------


class EventOutboxTable(Base):
    """Transactional outbox for guaranteed-delivery event dispatch.

    Events written here are part of the same database transaction as the
    business operation that triggered them, ensuring at-least-once delivery
    even if the process crashes after the transaction commits.

    An ``OutboxPoller`` background task reads ``pending`` entries and
    dispatches them through the in-memory ``EventBus`` handlers, then
    marks them ``delivered``.  Failed entries are retried up to
    ``max_attempts`` times before being marked ``failed``.
    """

    __tablename__ = "event_outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict] = mapped_column(_JsonType, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'delivered', 'failed')",
            name="ck_event_outbox_status",
        ),
        Index("ix_event_outbox_status_created", "status", "created_at"),
        Index("ix_event_outbox_tenant", "tenant_id"),
    )
