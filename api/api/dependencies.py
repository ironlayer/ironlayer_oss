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

from api.config import APISettings, load_api_settings
from api.services.ai_client import AIServiceClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

_settings_cache: APISettings | None = None


def get_settings() -> APISettings:
    """Return the cached :class:`APISettings` singleton."""
    global _settings_cache  # noqa: PLW0603
    if _settings_cache is None:
        _settings_cache = load_api_settings()
    return _settings_cache


SettingsDep = Annotated[APISettings, Depends(get_settings)]

# ---------------------------------------------------------------------------
# Database session
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine(settings: APISettings) -> AsyncEngine:
    """Create and cache the global async engine."""
    global _engine, _session_factory  # noqa: PLW0603
    _engine = get_engine(settings.database_url)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def dispose_engine() -> None:
    """Dispose the global engine pool (call during shutdown)."""
    global _engine, _session_factory  # noqa: PLW0603
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the global async session factory.

    Used by components that operate outside FastAPI's dependency injection
    (e.g. Starlette middleware) and need direct session access.
    """
    if _session_factory is None:
        raise RuntimeError(
            "Database engine has not been initialised. Ensure init_engine() is called during application startup."
        )
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
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
    if _session_factory is None:
        raise RuntimeError(
            "Database engine has not been initialised. Ensure init_engine() is called during application startup."
        )
    session = _session_factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_tenant_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield an ``AsyncSession`` with the RLS tenant context set.

    Extracts ``tenant_id`` from the authenticated request state and executes
    ``SET LOCAL app.tenant_id`` on the session so that PostgreSQL Row-Level
    Security policies restrict all queries to the authenticated tenant's rows.

    This is the primary session dependency for all tenant-scoped endpoints.
    """
    if _session_factory is None:
        raise RuntimeError(
            "Database engine has not been initialised. Ensure init_engine() is called during application startup."
        )
    tenant_id = getattr(request.state, "tenant_id", None)
    if tenant_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    session = _session_factory()
    try:
        await set_tenant_context(session, tenant_id)
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


SessionDep = Annotated[AsyncSession, Depends(get_tenant_session)]

# WARNING: PublicSessionDep provides a session WITHOUT tenant RLS context.
# Any queries through this session bypass RLS and can see ALL tenant data.
# Only use for pre-authentication endpoints: signup, login, refresh, health.
# See get_db_session() docstring for full details.
PublicSessionDep = Annotated[AsyncSession, Depends(get_db_session)]


async def get_admin_session() -> AsyncGenerator[AsyncSession, None]:
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
    if _session_factory is None:
        raise RuntimeError(
            "Database engine has not been initialised. Ensure init_engine() is called during application startup."
        )
    session = _session_factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


# WARNING: AdminSessionDep provides a session WITHOUT tenant RLS context.
# It can read/write rows belonging to ANY tenant.  Use only in admin
# endpoints protected by admin-only permission guards.
AdminSessionDep = Annotated[AsyncSession, Depends(get_admin_session)]

# ---------------------------------------------------------------------------
# AI service client
# ---------------------------------------------------------------------------

_ai_client: AIServiceClient | None = None


def init_ai_client(settings: APISettings) -> AIServiceClient:
    """Create and cache the global :class:`AIServiceClient`."""
    global _ai_client  # noqa: PLW0603
    _ai_client = AIServiceClient(
        base_url=settings.ai_engine_url,
        timeout=settings.ai_engine_timeout,
    )
    return _ai_client


async def dispose_ai_client() -> None:
    """Close the AI client's underlying HTTP pool."""
    global _ai_client  # noqa: PLW0603
    if _ai_client is not None:
        await _ai_client.close()
        _ai_client = None


def get_ai_client() -> AIServiceClient:
    """Return the cached :class:`AIServiceClient` singleton."""
    if _ai_client is None:
        raise RuntimeError(
            "AI client has not been initialised. Ensure init_ai_client() is called during application startup."
        )
    return _ai_client


AIClientDep = Annotated[AIServiceClient, Depends(get_ai_client)]

# ---------------------------------------------------------------------------
# Metering collector
# ---------------------------------------------------------------------------

_metering_collector: MeteringCollector | None = None


def init_metering(
    session_factory: async_sessionmaker[AsyncSession],
) -> MeteringCollector:
    """Create and cache the global :class:`MeteringCollector`.

    Initialises a :class:`DatabaseSink` backed by the provided session
    factory and starts the background flush thread.
    """
    global _metering_collector  # noqa: PLW0603
    from core_engine.metering.collector import DatabaseSink

    sink = DatabaseSink(session_factory)
    _metering_collector = MeteringCollector(
        sink=sink,
        flush_interval_seconds=30.0,
        max_buffer_size=500,
    )
    _metering_collector.start_background_flush()
    return _metering_collector


def dispose_metering() -> None:
    """Stop the background flush thread and do a final flush."""
    global _metering_collector  # noqa: PLW0603
    if _metering_collector is not None:
        _metering_collector.stop_background_flush()
        _metering_collector = None


def get_metering_collector() -> MeteringCollector:
    """Return the cached :class:`MeteringCollector` singleton."""
    if _metering_collector is None:
        raise RuntimeError(
            "MeteringCollector has not been initialised. Ensure init_metering() is called during application startup."
        )
    return _metering_collector


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
