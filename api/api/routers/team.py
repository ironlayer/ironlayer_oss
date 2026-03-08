"""Team management endpoints: list, invite, remove, and update team members.

All mutation endpoints require the ``MANAGE_SETTINGS`` permission (admin role)
and the ``Feature.TEAM_MANAGEMENT`` tier gate (Team+ plans).  List is available
to any authenticated user with ``READ_PLANS`` permission.
"""

from __future__ import annotations

import logging
from typing import Any

from core_engine.license.feature_flags import Feature
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import SessionDep, SettingsDep, TenantDep, UserDep, require_feature
from api.middleware.rbac import Permission, Role, require_permission
from api.schemas import (
    InviteMemberRequest,
    TeamMemberResponse,
    TeamMembersResponse,
    UpdateRoleRequest,
)
from api.services.team_service import TeamService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/team", tags=["team"])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/members", response_model=TeamMembersResponse)
async def list_members(
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_PLANS)),
) -> dict[str, Any]:
    """List all team members with seat usage information.

    Available to any authenticated user in the tenant.
    """
    service = TeamService(session, settings, tenant_id=tenant_id)
    return await service.list_members()


@router.post("/invite", response_model=TeamMemberResponse)
async def invite_member(
    body: InviteMemberRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    user_identity: UserDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
    _gate: None = Depends(require_feature(Feature.TEAM_MANAGEMENT)),
) -> dict[str, Any]:
    """Invite a new team member.

    Requires admin role and Team+ plan.  Enforces seat quota before creating
    the user.  Syncs Stripe subscription quantity after success.
    """
    service = TeamService(session, settings, tenant_id=tenant_id)
    try:
        return await service.invite_member(
            email=body.email,
            role=body.role,
            invited_by=user_identity,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc


@router.delete("/members/{user_id}", response_model=TeamMemberResponse)
async def remove_member(
    user_id: str,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> dict[str, Any]:
    """Remove a team member (soft-delete).

    Requires admin role.  Sets ``is_active=False`` and syncs Stripe quantity.
    """
    service = TeamService(session, settings, tenant_id=tenant_id)
    try:
        return await service.remove_member(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/members/{user_id}", response_model=TeamMemberResponse)
async def update_member_role(
    user_id: str,
    body: UpdateRoleRequest,
    session: SessionDep,
    settings: SettingsDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_SETTINGS)),
) -> dict[str, Any]:
    """Change a team member's role.

    Requires admin role.
    """
    service = TeamService(session, settings, tenant_id=tenant_id)
    try:
        return await service.update_role(user_id, body.role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
