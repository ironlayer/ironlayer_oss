"""Authentication endpoints: signup, login, refresh, session restore, profile, API keys, revoke.

Public endpoints (signup, login, refresh, session) bypass the auth middleware.
All others require a valid Bearer token.

Security model:
- Access tokens are returned in the JSON response body and stored in memory only.
- Refresh tokens are set as HttpOnly, Secure, SameSite=Strict cookies scoped
  to the ``/api/v1/auth/refresh`` path.  They are never exposed to JavaScript.
- A ``/auth/session`` endpoint allows the frontend to restore a session on page
  load by validating the refresh-token cookie and returning a fresh access token.
"""

from __future__ import annotations

import logging
from typing import Any

from core_engine.state.repository import TokenRevocationRepository
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field

from api.dependencies import PublicSessionDep, SessionDep, TenantDep, UserDep
from api.middleware.login_rate_limiter import LoginRateLimiter
from api.middleware.rbac import Permission, Role, require_permission
from api.services.audit_service import AuditAction, AuditService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Cookie configuration constants
# ---------------------------------------------------------------------------

_REFRESH_COOKIE_KEY = "refresh_token"
_REFRESH_COOKIE_MAX_AGE = 7 * 24 * 3600  # 7 days
_REFRESH_COOKIE_PATH = "/api/v1/auth"  # Sent to refresh + session endpoints

# Module-level login rate limiter instance (in-memory, single-replica).
_login_limiter = LoginRateLimiter()


def _get_client_ip(request: Request) -> str:
    """Extract the client IP, respecting X-Forwarded-For for reverse-proxied deployments."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        # X-Forwarded-For is a comma-separated list; the leftmost is the original client.
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class SignupRequest(BaseModel):
    """Request body for user registration."""

    email: EmailStr = Field(..., description="Email address.")
    password: str = Field(..., min_length=8, description="Password (min 8 characters).")
    display_name: str = Field(..., min_length=1, max_length=256, description="Display name.")


class LoginRequest(BaseModel):
    """Request body for email/password login."""

    email: EmailStr = Field(..., description="Email address.")
    password: str = Field(..., description="Password.")


class TokenResponse(BaseModel):
    """Response containing an access token and user info.

    The refresh token is NOT included in the response body.  It is set
    as an HttpOnly cookie by the endpoint handler.
    """

    access_token: str
    token_type: str = "bearer"
    user: dict[str, Any]
    tenant_id: str


class RefreshResponse(BaseModel):
    """Response from token refresh.

    Contains only the new access token.  The new refresh token is
    rotated via the HttpOnly cookie.
    """

    access_token: str
    token_type: str = "bearer"


class SessionResponse(BaseModel):
    """Response from the session-restore endpoint.

    Returns user info and a fresh access token derived from the
    HttpOnly refresh-token cookie.
    """

    access_token: str
    token_type: str = "bearer"
    user: dict[str, Any]
    tenant_id: str


class UserProfileResponse(BaseModel):
    """Current user profile."""

    id: str
    email: str
    display_name: str
    role: str
    tenant_id: str
    is_active: bool
    email_verified: bool
    created_at: str | None = None
    last_login_at: str | None = None


class CreateAPIKeyRequest(BaseModel):
    """Request body for API key creation."""

    name: str = Field(..., min_length=1, max_length=256, description="Human-readable key name.")
    scopes: list[str] | None = Field(default=None, description="Granted scopes (default: read+write).")


class APIKeyResponse(BaseModel):
    """Response for a single API key."""

    id: str
    name: str
    key_prefix: str
    scopes: list[str] | None = None
    created_at: str | None = None
    last_used_at: str | None = None
    expires_at: str | None = None
    plaintext_key: str | None = None  # Only present on creation


class RevokeTokenRequest(BaseModel):
    """Request body for token revocation."""

    jti: str = Field(..., description="JWT ID (jti claim) of the token to revoke.")
    reason: str | None = Field(default=None, description="Optional reason for revocation.")
    expires_at: str | None = Field(
        default=None,
        description="ISO-8601 expiry of the original token (for automatic cleanup).",
    )


class RevokeTokenResponse(BaseModel):
    """Response after successful token revocation."""

    jti: str
    revoked: bool = True
    message: str = "Token revoked successfully."


# ---------------------------------------------------------------------------
# Public endpoints (no auth required — listed in middleware _PUBLIC_PATHS)
# ---------------------------------------------------------------------------


def _set_refresh_cookie(response: JSONResponse, refresh_token: str) -> None:
    """Set the refresh token as an HttpOnly cookie on the response."""
    response.set_cookie(
        key=_REFRESH_COOKIE_KEY,
        value=refresh_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=_REFRESH_COOKIE_MAX_AGE,
        path=_REFRESH_COOKIE_PATH,
    )


def _clear_refresh_cookie(response: JSONResponse) -> None:
    """Clear the refresh token cookie from the response."""
    response.delete_cookie(
        key=_REFRESH_COOKIE_KEY,
        path=_REFRESH_COOKIE_PATH,
        httponly=True,
        secure=True,
        samesite="strict",
    )


@router.post(
    "/signup",
    response_model=TokenResponse,
    summary="Create a new account",
    status_code=201,
)
async def signup(body: SignupRequest, session: PublicSessionDep) -> JSONResponse:
    """Register a new user, auto-provision a tenant, and return tokens.

    The first user of a new tenant is assigned the ADMIN role.
    The access token is returned in the JSON body.  The refresh token
    is set as an HttpOnly cookie.
    """
    from api.services.auth_service import AuthError, AuthService

    svc = AuthService(session)
    try:
        result = await svc.signup(
            email=body.email,
            password=body.password,
            display_name=body.display_name,
        )
    except AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))

    body_data = TokenResponse(
        access_token=result["access_token"],
        user=result["user"],
        tenant_id=result["tenant_id"],
    )
    response = JSONResponse(content=body_data.model_dump(), status_code=201)
    _set_refresh_cookie(response, result["refresh_token"])
    return response


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Log in with email and password",
)
async def login(body: LoginRequest, request: Request, session: PublicSessionDep) -> JSONResponse:
    """Validate credentials and return an access token.

    Enforces brute-force protection: after 5 consecutive failures for the
    same (email, IP) pair, an exponential backoff is applied (30s -> 900s).

    The access token is returned in the JSON body.  The refresh token
    is set as an HttpOnly cookie.
    """
    from api.services.auth_service import AuthError, AuthService

    client_ip = _get_client_ip(request)

    # Check rate limit BEFORE validating credentials.
    allowed, retry_after = _login_limiter.check_rate_limit(body.email, client_ip)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Too many login attempts. Try again in {retry_after} seconds.",
            headers={"Retry-After": str(retry_after)},
        )

    svc = AuthService(session)
    try:
        result = await svc.login(email=body.email, password=body.password)
    except AuthError as exc:
        # Record the failure for brute-force tracking.
        _login_limiter.record_failure(body.email, client_ip)
        raise HTTPException(status_code=exc.status_code, detail=str(exc))

    # Successful login — reset the failure counter.
    _login_limiter.record_success(body.email, client_ip)

    body_data = TokenResponse(
        access_token=result["access_token"],
        user=result["user"],
        tenant_id=result["tenant_id"],
    )
    response = JSONResponse(content=body_data.model_dump())
    _set_refresh_cookie(response, result["refresh_token"])
    return response


@router.post(
    "/refresh",
    response_model=RefreshResponse,
    summary="Refresh an access token",
)
async def refresh(request: Request, session: PublicSessionDep) -> JSONResponse:
    """Exchange a refresh token (from HttpOnly cookie) for a new token pair.

    The new access token is returned in the JSON body.  The new refresh
    token is rotated via the HttpOnly cookie.
    """
    from api.services.auth_service import AuthError, AuthService

    refresh_token = request.cookies.get(_REFRESH_COOKIE_KEY)
    if not refresh_token:
        raise HTTPException(status_code=401, detail="No refresh token cookie")

    svc = AuthService(session)
    try:
        result = await svc.refresh(refresh_token)
    except AuthError as exc:
        # On invalid/expired refresh token, clear the stale cookie.
        error_response = JSONResponse(
            content={"detail": str(exc)},
            status_code=exc.status_code,
        )
        _clear_refresh_cookie(error_response)
        return error_response

    body_data = RefreshResponse(access_token=result["access_token"])
    response = JSONResponse(content=body_data.model_dump())
    _set_refresh_cookie(response, result["refresh_token"])
    return response


# ---------------------------------------------------------------------------
# Session restore (public — uses cookie, no Bearer token required)
# ---------------------------------------------------------------------------


@router.get(
    "/session",
    response_model=SessionResponse,
    summary="Restore session from HttpOnly refresh cookie",
)
async def restore_session(request: Request, session: PublicSessionDep) -> JSONResponse:
    """Validate the refresh-token cookie and return user info + a fresh access token.

    Called by the frontend on page load to restore a session without
    requiring the user to re-enter credentials.  The refresh token is
    rotated on each call (cookie is updated).
    """
    from api.services.auth_service import AuthError, AuthService

    refresh_token = request.cookies.get(_REFRESH_COOKIE_KEY)
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    svc = AuthService(session)

    # Validate the refresh token and get a new token pair.
    try:
        refreshed = await svc.refresh(refresh_token)
    except AuthError as exc:
        error_response = JSONResponse(
            content={"detail": str(exc)},
            status_code=exc.status_code,
        )
        _clear_refresh_cookie(error_response)
        return error_response

    # Decode the new access token to extract user/tenant info.
    tm = svc._tm
    try:
        claims = tm.validate_token(refreshed["access_token"])
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    # Fetch full user profile.
    try:
        profile = await svc.get_current_user(
            user_id=claims.sub,
            tenant_id=claims.tenant_id,
        )
    except AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))

    body_data = SessionResponse(
        access_token=refreshed["access_token"],
        user={
            "id": profile["id"],
            "email": profile["email"],
            "display_name": profile["display_name"],
            "role": profile["role"],
            "tenant_id": profile["tenant_id"],
        },
        tenant_id=claims.tenant_id,
    )
    response = JSONResponse(content=body_data.model_dump())
    _set_refresh_cookie(response, refreshed["refresh_token"])
    return response


# ---------------------------------------------------------------------------
# Logout (public — clears the cookie)
# ---------------------------------------------------------------------------


@router.post(
    "/logout",
    summary="Log out and clear the refresh cookie",
)
async def logout() -> JSONResponse:
    """Clear the refresh-token cookie to end the session.

    The access token (in-memory only) is discarded by the frontend.
    """
    response = JSONResponse(content={"detail": "Logged out"})
    _clear_refresh_cookie(response)
    return response


# ---------------------------------------------------------------------------
# Authenticated endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/me",
    response_model=UserProfileResponse,
    summary="Get current user profile",
)
async def get_me(
    session: SessionDep,
    tenant_id: TenantDep,
    user: UserDep,
) -> UserProfileResponse:
    """Return the profile of the currently authenticated user."""
    from api.services.auth_service import AuthError, AuthService

    svc = AuthService(session)
    try:
        profile = await svc.get_current_user(user_id=user, tenant_id=tenant_id)
    except AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))

    return UserProfileResponse(**profile)


# ---------------------------------------------------------------------------
# API Key management (ADMIN only)
# ---------------------------------------------------------------------------


@router.post(
    "/api-keys",
    response_model=APIKeyResponse,
    summary="Create an API key",
    status_code=201,
)
async def create_api_key(
    body: CreateAPIKeyRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> APIKeyResponse:
    """Create a new API key.  The plaintext key is returned exactly once.

    Requires MANAGE_SETTINGS permission (ADMIN role).
    """
    from api.services.auth_service import AuthError, AuthService

    svc = AuthService(session)
    try:
        result = await svc.create_api_key(
            user_id=user,
            tenant_id=tenant_id,
            name=body.name,
            scopes=body.scopes,
        )
    except AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))

    # Audit log the API key creation.
    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action=AuditAction.SETTINGS_UPDATED,
        entity_type="api_key",
        entity_id=result["id"],
        reason=f"API key created: {body.name}",
    )

    return APIKeyResponse(**result)


@router.get(
    "/api-keys",
    response_model=list[APIKeyResponse],
    summary="List API keys",
)
async def list_api_keys(
    session: SessionDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> list[APIKeyResponse]:
    """List all non-revoked API keys for the current user.

    Requires MANAGE_SETTINGS permission (ADMIN role).
    """
    from api.services.auth_service import AuthService

    svc = AuthService(session)
    keys = await svc.list_api_keys(user_id=user, tenant_id=tenant_id)
    return [APIKeyResponse(**k) for k in keys]


@router.delete(
    "/api-keys/{key_id}",
    response_model=dict[str, Any],
    summary="Revoke an API key",
)
async def revoke_api_key(
    key_id: str,
    session: SessionDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> dict[str, Any]:
    """Revoke an API key by its ID.

    Requires MANAGE_SETTINGS permission (ADMIN role).
    """
    from api.services.auth_service import AuthService

    svc = AuthService(session)
    revoked = await svc.revoke_api_key(key_id=key_id, tenant_id=tenant_id)

    if not revoked:
        raise HTTPException(status_code=404, detail="API key not found or already revoked.")

    # Audit log the revocation.
    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action=AuditAction.SETTINGS_UPDATED,
        entity_type="api_key",
        entity_id=key_id,
        reason="API key revoked",
    )

    return {"id": key_id, "revoked": True}


# ---------------------------------------------------------------------------
# Token revocation (existing endpoint, preserved)
# ---------------------------------------------------------------------------


@router.post(
    "/revoke",
    response_model=RevokeTokenResponse,
    summary="Revoke a token by jti",
)
async def revoke_token(
    body: RevokeTokenRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    user: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> RevokeTokenResponse:
    """Revoke a JWT token so that subsequent uses are rejected.

    Requires MANAGE_SETTINGS permission (ADMIN role).
    """
    from datetime import datetime

    expires_at_dt = None
    if body.expires_at:
        try:
            expires_at_dt = datetime.fromisoformat(body.expires_at)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid expires_at format: {body.expires_at}",
            )

    repo = TokenRevocationRepository(session, tenant_id=tenant_id)
    await repo.revoke(
        jti=body.jti,
        reason=body.reason,
        expires_at=expires_at_dt,
    )

    # Audit log the revocation.
    audit = AuditService(session, tenant_id=tenant_id, actor=user)
    await audit.log(
        action=AuditAction.TOKEN_REVOKED,
        entity_type="token",
        entity_id=body.jti,
        reason=body.reason or "No reason provided",
    )

    logger.info(
        "Token revoked: jti=%s tenant=%s by=%s reason=%s",
        body.jti,
        tenant_id,
        user,
        body.reason,
    )

    return RevokeTokenResponse(jti=body.jti)
