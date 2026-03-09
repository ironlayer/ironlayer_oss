"""Shared HTTP error helpers for consistent API responses."""

from __future__ import annotations

from fastapi import HTTPException


def not_found_404(
    resource: str,
    id: str | None = None,
    *,
    hint: str | None = None,
) -> HTTPException:
    """Return a 404 HTTPException for a missing resource.

    Use across routers so not-found responses are consistent.
    Optional hint gives the client actionable context (e.g. how to create the resource).

    Examples:
        raise not_found_404("Plan", plan_id)
        raise not_found_404("Health data for tenant", tenant_id, hint="Use POST /admin/health/compute to run computation.")
    """
    if id is not None:
        detail = f"{resource} {id} not found"
    else:
        detail = f"{resource} not found"
    if hint:
        detail = f"{detail}. {hint}"
    return HTTPException(status_code=404, detail=detail)
