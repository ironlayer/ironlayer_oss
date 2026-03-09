"""FastAPI dependency injection for database sessions, AI client, and settings."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable
from typing import Annotated

from core_engine.license.feature_flags import Feature
from core_engine.metering.collector import MeteringCollector
from core_engine.state.database import get_engine, set_tenant_context
from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from api.config import APISettings
from api.services.ai_client import AIServiceClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Settings (request-scoped from app.state)
# ---------------------------------------------------------------------------


def get_settings(request: Request) -> APISettings:
    """Return :class:`APISettings` from app.state (set in lifespan)."""
    return request.app.state.settings


SettingsDep = Annotated[APISettings, Depends(get_settings)]

# ---------------------------------------------------------------------------
# Database session (engine/factory created in lifespan, stored on app.state)
# ---------------------------------------------------------------------------


def init_engine(settings: APISettings) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Create async engine and session factory. Caller stores on app.state.

    BL-096: Pool configuration is read from settings (DB_POOL_SIZE, DB_MAX_OVERFLOW,
    DB_POOL_TIMEOUT) and logged at startup (INFO level) for operational visibility.
    """
    engine = get_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    logger.info(
        "DB connection pool: pool_size=%d max_overflow=%d pool_timeout=%.1fs pool_pre_ping=True",
        settings.db_pool_size,
        settings.db_max_overflow,
        settings.db_pool_timeout,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return engine, session_factory


async def dispose_engine(engine: AsyncEngine) -> None:
    """Dispose the engine pool (call during shutdown)."""
    await engine.dispose()


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """Return the async session factory from app.state."""
    return request.app.state.session_factory


_ENGINE_NOT_INITIALISED = (
    "Database engine has not been initialised. Ensure lifespan has run (app.state.session_factory)."
)


async def _with_session(
    session_factory: async_sessionmaker[AsyncSession],
    tenant_id: str | None = None,
) -> AsyncGenerator[AsyncSession, None]:
    """Internal helper: yield a session with optional tenant RLS context."""
    session = session_factory()
    try:
        if tenant_id is not None:
            await set_tenant_context(session, tenant_id)
        yield session
        await session.commit()
    except Exception:  # Rollback on any error (DB, validation, etc.)
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an ``AsyncSession`` **without** tenant RLS context.

    .. warning:: **No Row-Level Security**

       Sessions produced by this dependency do **not** call
       ``SET LOCAL app.tenant_id``.  Queries executed through this
       session bypass PostgreSQL RLS policies and can read/write rows
       belonging to **any** tenant.  Use this dependency only for
       operations that genuinely require cross-tenant or pre-tenant
       access.

    **Intended usage (via** ``PublicSessionDep`` **):**

    - ``POST /auth/signup`` -- the tenant does not yet exist.
    - ``POST /auth/login`` -- tenant identity is resolved from
      credentials, not from a JWT.
    - ``POST /auth/refresh`` -- the refresh token is validated before
      a tenant context can be established.
    - ``GET /health`` and internal readiness probes.

    For all other (authenticated, tenant-scoped) endpoints, use
    :data:`SessionDep` which calls :func:`get_tenant_session` and
    activates RLS via ``set_tenant_context()``.

    The session commits on clean exit and rolls back on exception.
    """
    session_factory = get_session_factory(request)
    async for session in _with_session(session_factory, tenant_id=None):
        yield session


async def get_tenant_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an ``AsyncSession`` with the RLS tenant context set.

    Extracts ``tenant_id`` from the authenticated request state and executes
    ``SET LOCAL app.tenant_id`` on the session so that PostgreSQL Row-Level
    Security policies restrict all queries to the authenticated tenant's rows.

    This is the primary session dependency for all tenant-scoped endpoints.
    """
    tenant_id = getattr(request.state, "tenant_id", None)
    if tenant_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    session_factory = get_session_factory(request)
    async for session in _with_session(session_factory, tenant_id=tenant_id):
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_tenant_session)]

# WARNING: PublicSessionDep provides a session WITHOUT tenant RLS context.
# Any queries through this session bypass RLS and can see ALL tenant data.
# Only use for pre-authentication endpoints: signup, login, refresh, health.
# See get_db_session() docstring for full details.
PublicSessionDep = Annotated[AsyncSession, Depends(get_db_session)]


async def get_admin_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an ``AsyncSession`` **without** tenant RLS context for admin operations.

    .. warning:: **No Row-Level Security**

       This session has NO Row-Level Security context.  It can read and
       write rows belonging to **any** tenant.  Use only in admin
       endpoints that are protected by ``require_permission(Permission.VIEW_ANALYTICS)``,
       ``require_permission(Permission.MANAGE_HEALTH)``,
       ``require_permission(Permission.VIEW_REPORTS)``, or similar
       admin-only guards.

    **Intended usage (via** ``AdminSessionDep`` **):**

    - ``GET /admin/analytics/*`` -- platform-wide metrics.
    - ``GET /admin/health/*`` -- cross-tenant customer health.
    - ``GET /admin/reports/*`` -- cross-tenant reporting.

    For tenant-scoped endpoints, use :data:`SessionDep` which calls
    :func:`get_tenant_session` and activates RLS via ``set_tenant_context()``.

    The session commits on clean exit and rolls back on exception.
    """
    session_factory = get_session_factory(request)
    async for session in _with_session(session_factory, tenant_id=None):
        yield session


# WARNING: AdminSessionDep provides a session WITHOUT tenant RLS context.
# It can read/write rows belonging to ANY tenant.  Use only in admin
# endpoints protected by admin-only permission guards.
AdminSessionDep = Annotated[AsyncSession, Depends(get_admin_session)]

# ---------------------------------------------------------------------------
# AI service client (created in lifespan, stored on app.state)
# ---------------------------------------------------------------------------


def init_ai_client(settings: APISettings) -> AIServiceClient:
    """Create :class:`AIServiceClient`. Caller stores on app.state."""
    return AIServiceClient(
        base_url=settings.ai_engine_url,
        timeout=settings.ai_engine_timeout,
    )


async def dispose_ai_client(client: AIServiceClient) -> None:
    """Close the AI client's underlying HTTP pool."""
    await client.close()


def get_ai_client(request: Request) -> AIServiceClient:
    """Return the :class:`AIServiceClient` from app.state."""
    return request.app.state.ai_client


AIClientDep = Annotated[AIServiceClient, Depends(get_ai_client)]

# ---------------------------------------------------------------------------
# Metering collector (created in lifespan, stored on app.state)
# ---------------------------------------------------------------------------


def init_metering(
    session_factory: async_sessionmaker[AsyncSession],
) -> MeteringCollector:
    """Create :class:`MeteringCollector`. Caller stores on app.state."""
    from core_engine.metering.collector import DatabaseSink

    sink = DatabaseSink(session_factory)
    collector = MeteringCollector(
        sink=sink,
        flush_interval_seconds=30.0,
        max_buffer_size=500,
    )
    collector.start_background_flush()
    return collector


def dispose_metering(collector: MeteringCollector | None) -> None:
    """Stop the background flush thread and do a final flush."""
    if collector is not None:
        collector.stop_background_flush()


def get_metering_collector(request: Request) -> MeteringCollector:
    """Return the :class:`MeteringCollector` from app.state."""
    return request.app.state.metering


MeteringDep = Annotated[MeteringCollector, Depends(get_metering_collector)]

# ---------------------------------------------------------------------------
# Tenant / user identity (populated by AuthenticationMiddleware)
# ---------------------------------------------------------------------------


def get_tenant_id(request: Request) -> str:
    """Extract tenant_id from authenticated request state."""
    tenant_id = getattr(request.state, "tenant_id", None)
    if tenant_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return tenant_id


TenantDep = Annotated[str, Depends(get_tenant_id)]


def get_user_identity(request: Request) -> str:
    """Extract user identity from authenticated request state."""
    return getattr(request.state, "sub", "anonymous")


UserDep = Annotated[str, Depends(get_user_identity)]

# ---------------------------------------------------------------------------
# RBAC role (populated by AuthenticationMiddleware from JWT "role" claim)
# ---------------------------------------------------------------------------

from api.middleware.rbac import Role, get_user_role  # noqa: E402

RoleDep = Annotated[Role, Depends(get_user_role)]

# ---------------------------------------------------------------------------
# Feature-tier gating
# ---------------------------------------------------------------------------


def require_feature(feature: Feature) -> Callable[..., None]:
    """Return a FastAPI dependency that enforces a billing-tier feature gate.

    Queries the tenant's ``BillingCustomerTable.plan_tier``, maps it to a
    :class:`LicenseTier`, and verifies the requested :class:`Feature` is
    enabled for that tier.  Returns ``None`` on success; raises
    ``HTTPException(403)`` with an upgrade message if the feature is not
    available.

    Usage::

        @router.post("/some-endpoint")
        async def some_endpoint(
            ...,
            _gate: None = Depends(require_feature(Feature.AI_ADVISORY)),
        ):
            ...
    """

    async def _gate(
        session: SessionDep,
        tenant_id: TenantDep,
    ) -> None:
        from core_engine.license.feature_flags import (
            LicenseTier,
            get_required_tier,
            is_feature_enabled,
        )
        from core_engine.state.tables import BillingCustomerTable
        from sqlalchemy import select as sa_select

        result = await session.execute(
            sa_select(BillingCustomerTable.plan_tier).where(
                BillingCustomerTable.tenant_id == tenant_id,
            )
        )
        plan_tier_str: str | None = result.scalar_one_or_none()
        plan_tier_str = plan_tier_str or "community"

        # Map string tier to LicenseTier enum.
        try:
            tier = LicenseTier(plan_tier_str)
        except ValueError:
            tier = LicenseTier.COMMUNITY

        if not is_feature_enabled(tier, feature):
            required_tier = get_required_tier(feature)
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Feature '{feature.value}' requires the "
                    f"{required_tier.value.title()} tier or above. "
                    f"Your current tier is '{tier.value}'. "
                    f"Please upgrade to access this feature."
                ),
            )

    return _gate  # type: ignore[return-value]
