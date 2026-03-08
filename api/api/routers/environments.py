"""API router for environment management.

Provides endpoints for creating, listing, deleting, and promoting
environments, as well as managing ephemeral PR environments and
cleaning up expired ones.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import SessionDep, TenantDep
from api.middleware.rbac import Permission, Role, require_permission
from api.services.environment_service import EnvironmentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/environments", tags=["environments"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class CreateEnvironmentRequest(BaseModel):
    """Request body for creating a standard environment."""

    name: str = Field(..., min_length=1, max_length=128, description="Unique environment name.")
    catalog: str = Field(..., min_length=1, max_length=256, description="Databricks catalog name.")
    schema_prefix: str = Field(..., min_length=1, max_length=256, description="Schema prefix for table qualification.")
    is_production: bool = Field(False, description="Whether this is a production environment.")
    created_by: str = Field(..., min_length=1, max_length=256, description="Identity of the creator.")


class CreateEphemeralRequest(BaseModel):
    """Request body for creating an ephemeral PR environment."""

    pr_number: int = Field(..., ge=1, description="Pull request number.")
    branch_name: str = Field(..., min_length=1, max_length=256, description="Git branch name.")
    catalog: str = Field(..., min_length=1, max_length=256, description="Databricks catalog for the PR env.")
    schema_prefix: str = Field(..., min_length=1, max_length=256, description="Schema prefix for the PR env.")
    created_by: str = Field(..., min_length=1, max_length=256, description="Identity of the creator.")
    ttl_hours: int = Field(72, ge=1, le=720, description="Time-to-live in hours before auto-expiry.")


class PromoteRequest(BaseModel):
    """Request body for promoting a snapshot between environments."""

    target_environment: str = Field(..., min_length=1, max_length=128, description="Target environment name.")
    snapshot_id: str = Field(..., min_length=1, max_length=128, description="Snapshot ID to promote.")
    promoted_by: str = Field(..., min_length=1, max_length=256, description="Identity of the promoter.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("")
async def create_environment(
    body: CreateEnvironmentRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_ENVIRONMENTS)),
) -> dict[str, Any]:
    """Create a standard environment with catalog/schema mapping."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    try:
        return await service.create_environment(
            name=body.name,
            catalog=body.catalog,
            schema_prefix=body.schema_prefix,
            is_production=body.is_production,
            created_by=body.created_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/ephemeral")
async def create_ephemeral_environment(
    body: CreateEphemeralRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.CREATE_EPHEMERAL_ENVS)),
) -> dict[str, Any]:
    """Create an ephemeral PR environment with auto-expiry."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    try:
        return await service.create_ephemeral_environment(
            pr_number=body.pr_number,
            branch_name=body.branch_name,
            catalog=body.catalog,
            schema_prefix=body.schema_prefix,
            created_by=body.created_by,
            ttl_hours=body.ttl_hours,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("")
async def list_environments(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
    include_deleted: bool = Query(False, description="Include soft-deleted environments."),
) -> list[dict[str, Any]]:
    """List all environments for the current tenant."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    return await service.list_environments(include_deleted=include_deleted)


@router.get("/promotions")
async def get_promotion_history(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
    environment_name: str | None = Query(None, description="Filter by environment name."),
    limit: int = Query(20, ge=1, le=100, description="Maximum records to return."),
) -> list[dict[str, Any]]:
    """Get promotion history across environments."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    return await service.get_promotion_history(
        environment_name=environment_name,
        limit=limit,
    )


@router.get("/{name}")
async def get_environment(
    name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
) -> dict[str, Any]:
    """Get environment details by name."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    env = await service.get_environment(name)
    if env is None:
        raise HTTPException(status_code=404, detail=f"Environment '{name}' not found")
    return env


@router.delete("/{name}")
async def delete_environment(
    name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_ENVIRONMENTS)),
) -> dict[str, Any]:
    """Soft-delete an environment."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    deleted = await service.delete_environment(name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Environment '{name}' not found")
    return {"deleted": True, "name": name}


@router.post("/{name}/promote")
async def promote_environment(
    name: str,
    body: PromoteRequest,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.PROMOTE_ENVIRONMENTS)),
) -> dict[str, Any]:
    """Promote a snapshot from this environment to a target environment.

    Copies snapshot references, NOT data.
    """
    service = EnvironmentService(session, tenant_id=tenant_id)
    try:
        return await service.promote(
            source_name=name,
            target_name=body.target_environment,
            snapshot_id=body.snapshot_id,
            promoted_by=body.promoted_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/cleanup")
async def cleanup_expired(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.MANAGE_ENVIRONMENTS)),
) -> dict[str, Any]:
    """Clean up expired ephemeral environments."""
    service = EnvironmentService(session, tenant_id=tenant_id)
    return await service.cleanup_expired()
