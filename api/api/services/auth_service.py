"""Authentication service: signup, login, token management, and API keys.

Orchestrates user creation, credential validation, token issuance, and
API key lifecycle.  Uses the existing :class:`TokenManager` from
``security.py`` for JWT generation and validation.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any

from datetime import UTC, datetime

from core_engine.state.repository import (
    APIKeyRepository,
    TenantConfigRepository,
    TokenRevocationRepository,
    UserRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

from api.security import TokenManager
from api.services.audit_service import AuditAction, AuditService

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Raised on authentication or authorisation failures."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


class AuthService:
    """High-level authentication operations.

    Parameters
    ----------
    session:
        An async database session (caller manages transaction).
    token_manager:
        The :class:`TokenManager` for generating/validating tokens.
    """

    def __init__(
        self,
        session: AsyncSession,
        token_manager: TokenManager | None = None,
        settings: Any = None,
    ) -> None:
        self._session = session
        if token_manager is not None:
            self._tm = token_manager
        elif settings is not None:
            from api.middleware.auth import _build_token_config_from_settings
            self._tm = TokenManager(_build_token_config_from_settings(settings))
        else:
            raise TypeError("AuthService requires either token_manager or settings")

    # ------------------------------------------------------------------
    # Signup
    # ------------------------------------------------------------------

    async def signup(
        self,
        email: str,
        password: str,
        display_name: str,
    ) -> dict[str, Any]:
        """Create a new user account and auto-provision a tenant.

        If this is the first user for the tenant (based on email domain or
        new registration), the user is assigned the ADMIN role and a tenant
        config record is created.

        Returns a dict with ``user``, ``access_token``, ``refresh_token``,
        and ``tenant_id``.
        """
        email = email.lower().strip()
        if not email or "@" not in email:
            raise AuthError("A valid email address is required.")
        if len(password) < 8:
            raise AuthError("Password must be at least 8 characters.")
        if not display_name or not display_name.strip():
            raise AuthError("Display name is required.")

        # Check if user already exists (across all tenants).
        user_repo = UserRepository(self._session, tenant_id="__signup__")
        existing = await user_repo.get_by_email_any_tenant(email)
        if existing is not None:
            raise AuthError(
                "An account with this email already exists. Please log in instead.",
                status_code=409,
            )

        # Auto-provision tenant.
        tenant_id = uuid.uuid4().hex[:16]

        # Create tenant config.
        tenant_repo = TenantConfigRepository(self._session, tenant_id=tenant_id)
        await tenant_repo.upsert(llm_enabled=False, updated_by=email)

        # Create the user as ADMIN (first user of the tenant).
        user_repo = UserRepository(self._session, tenant_id=tenant_id)
        user = await user_repo.create(
            email=email,
            password=password,
            display_name=display_name.strip(),
            role="admin",
        )

        # Generate tokens.
        access_token = self._tm.generate_token(
            subject=user.id,
            tenant_id=tenant_id,
            scopes=["read", "write"],
            role=user.role,
        )
        refresh_token = self._tm.generate_refresh_token(
            subject=user.id,
            tenant_id=tenant_id,
        )

        logger.info("User signed up: user=%s tenant=%s email=%s", user.id, tenant_id, email)

        return {
            "user": {
                "id": user.id,
                "email": user.email,
                "display_name": user.display_name,
                "role": user.role,
                "tenant_id": tenant_id,
            },
            "access_token": access_token,
            "refresh_token": refresh_token,
            "tenant_id": tenant_id,
        }

    # ------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------

    async def login(
        self,
        email: str,
        password: str,
        *,
        ip: str | None = None,
    ) -> dict[str, Any]:
        """Validate credentials and return a token pair.

        Parameters
        ----------
        email:
            The user's email address.
        password:
            The user's plaintext password (never stored).
        ip:
            The client IP address, included in the audit log on failure.

        Returns a dict with ``user``, ``access_token``, ``refresh_token``,
        and ``tenant_id``.
        """
        email = email.lower().strip()
        user_repo = UserRepository(self._session, tenant_id="__login__")
        user = await user_repo.verify_password(email, password)

        if user is None:
            # Write audit log for failed authentication attempt.
            # entity_id is SHA-256(email) — never the plaintext — to avoid
            # leaking user data into the audit trail.  A single generic reason
            # is used regardless of whether the email was not found or the
            # password was wrong, preventing oracle attacks.
            email_hash = hashlib.sha256(email.encode("utf-8")).hexdigest()
            audit_meta: dict[str, Any] = {
                "reason": "invalid_credentials",
                "timestamp": datetime.now(UTC).isoformat(),
            }
            if ip is not None:
                audit_meta["ip"] = ip
            audit_svc = AuditService(
                self._session,
                tenant_id="__auth__",
                actor="unauthenticated",
            )
            await audit_svc.log(
                AuditAction.AUTH_FAILED,
                entity_type="user",
                entity_id=email_hash,
                **audit_meta,
            )
            raise AuthError(
                "Invalid email or password.",
                status_code=401,
            )

        if not user.is_active:
            raise AuthError(
                "This account has been deactivated. Contact your administrator.",
                status_code=403,
            )

        # Update last login timestamp.
        await user_repo.update_last_login(user.id)

        # Generate tokens.
        access_token = self._tm.generate_token(
            subject=user.id,
            tenant_id=user.tenant_id,
            scopes=["read", "write"],
            role=user.role,
        )
        refresh_token = self._tm.generate_refresh_token(
            subject=user.id,
            tenant_id=user.tenant_id,
        )

        logger.info("User logged in: user=%s tenant=%s", user.id, user.tenant_id)

        return {
            "user": {
                "id": user.id,
                "email": user.email,
                "display_name": user.display_name,
                "role": user.role,
                "tenant_id": user.tenant_id,
            },
            "access_token": access_token,
            "refresh_token": refresh_token,
            "tenant_id": user.tenant_id,
        }

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    async def refresh(self, refresh_token: str) -> dict[str, Any]:
        """Validate a refresh token and return a fresh token pair.

        Returns a dict with ``access_token`` and ``refresh_token``.
        """
        try:
            claims = await self._tm.validate_token_async(refresh_token)
        except PermissionError as exc:
            raise AuthError(str(exc), status_code=401) from exc

        if "refresh" not in claims.scopes:
            raise AuthError("Not a valid refresh token.", status_code=401)

        # Check revocation BEFORE issuing new tokens (covers both /refresh and /session).
        # The /auth/session endpoint is a public path and therefore bypasses the
        # auth middleware's revocation check.  Without this explicit check a
        # logged-out or admin-revoked refresh token could still restore a session.
        revocation_repo = TokenRevocationRepository(self._session, tenant_id=claims.tenant_id)
        if await revocation_repo.is_revoked(claims.jti):
            raise AuthError("Invalid or expired token.", status_code=401)

        # Verify the user still exists and is active.
        user_repo = UserRepository(self._session, tenant_id=claims.tenant_id)
        user = await user_repo.get_by_id(claims.sub)
        if user is None or not user.is_active:
            raise AuthError("User account not found or deactivated.", status_code=401)

        # Revoke the consumed refresh token before issuing a new pair (BL-051).
        # This prevents refresh token reuse: once a token has been exchanged for
        # a new pair, the old token is immediately invalidated in the revocation
        # store.  Without this, a stolen refresh token remains usable until its
        # natural expiry even after the legitimate user has already rotated.
        # (revocation_repo already constructed above for the pre-check.)
        old_expires_at = datetime.fromtimestamp(claims.exp, tz=UTC)
        await revocation_repo.revoke(
            jti=claims.jti,
            reason="refresh_rotation",
            expires_at=old_expires_at,
        )

        # Generate new token pair.
        new_access = self._tm.generate_token(
            subject=claims.sub,
            tenant_id=claims.tenant_id,
            scopes=["read", "write"],
            role=user.role,
        )
        new_refresh = self._tm.generate_refresh_token(
            subject=claims.sub,
            tenant_id=claims.tenant_id,
        )

        return {
            "access_token": new_access,
            "refresh_token": new_refresh,
        }

    # ------------------------------------------------------------------
    # Current user
    # ------------------------------------------------------------------

    async def get_current_user(
        self,
        user_id: str,
        tenant_id: str,
    ) -> dict[str, Any]:
        """Return the profile of the currently authenticated user."""
        user_repo = UserRepository(self._session, tenant_id=tenant_id)
        user = await user_repo.get_by_id(user_id)
        if user is None:
            raise AuthError("User not found.", status_code=404)

        return {
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
            "role": user.role,
            "tenant_id": user.tenant_id,
            "is_active": user.is_active,
            "email_verified": user.email_verified,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        }

    # ------------------------------------------------------------------
    # API Keys
    # ------------------------------------------------------------------

    async def create_api_key(
        self,
        user_id: str,
        tenant_id: str,
        name: str,
        *,
        scopes: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a new API key for the user.

        Returns a dict with the key metadata plus the ``plaintext_key``
        which is shown exactly once.
        """
        if not name or not name.strip():
            raise AuthError("API key name is required.")

        repo = APIKeyRepository(self._session, tenant_id=tenant_id)
        row, plaintext = await repo.create(
            user_id=user_id,
            name=name.strip(),
            scopes=scopes,
        )

        logger.info(
            "API key created: key_id=%s user=%s tenant=%s prefix=%s",
            row.id,
            user_id,
            tenant_id,
            row.key_prefix,
        )

        return {
            "id": row.id,
            "name": row.name,
            "key_prefix": row.key_prefix,
            "scopes": row.scopes,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "plaintext_key": plaintext,
        }

    async def list_api_keys(
        self,
        user_id: str,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Return all non-revoked API keys for the user."""
        repo = APIKeyRepository(self._session, tenant_id=tenant_id)
        keys = await repo.list_by_user(user_id)
        return [
            {
                "id": k.id,
                "name": k.name,
                "key_prefix": k.key_prefix,
                "scopes": k.scopes,
                "created_at": k.created_at.isoformat() if k.created_at else None,
                "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
                "expires_at": k.expires_at.isoformat() if k.expires_at else None,
            }
            for k in keys
        ]

    async def revoke_api_key(
        self,
        key_id: str,
        tenant_id: str,
    ) -> bool:
        """Revoke an API key.  Returns ``True`` if successful."""
        repo = APIKeyRepository(self._session, tenant_id=tenant_id)
        return await repo.revoke(key_id)
