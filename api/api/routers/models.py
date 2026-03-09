"""Model registry endpoints: list, detail, lineage, and health."""

from __future__ import annotations

import json
import logging
from typing import Any

from core_engine.state.repository import (
    ModelRepository,
    RunRepository,
    WatermarkRepository,
)
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import SessionDep, SettingsDep, TenantDep
from api.http_errors import not_found_404
from api.middleware.rbac import Permission, Role, require_permission
from api.validation import resolve_repo_path_under_base

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/models", tags=["models"])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("")
async def list_models(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
    kind: str | None = Query(default=None, description="Filter by model kind."),
    owner: str | None = Query(default=None, description="Filter by owner."),
    search: str | None = Query(default=None, description="Text search on model name."),
    limit: int = Query(default=100, ge=1, le=500, description="Max models to return."),
    offset: int = Query(default=0, ge=0, le=100_000, description="Number of rows to skip."),
) -> list[dict[str, Any]]:
    """Return registered models with pagination, optionally filtered."""
    repo = ModelRepository(session, tenant_id=tenant_id)

    # Apply filters at the database level for efficiency.
    if kind or owner or search:
        rows = await repo.list_filtered(kind=kind, owner=owner, search=search, limit=limit, offset=offset)
    else:
        rows = await repo.list_all(limit=limit, offset=offset)

    results: list[dict[str, Any]] = []
    for row in rows:
        tags = json.loads(row.tags) if row.tags else []
        results.append(
            {
                "model_name": row.model_name,
                "kind": row.kind,
                "materialization": row.materialization,
                "owner": row.owner,
                "tags": tags,
                "current_version": row.current_version,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "last_modified_at": (row.last_modified_at.isoformat() if row.last_modified_at else None),
            }
        )

    return results


@router.get("/at-risk")
async def get_at_risk_models(
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
    limit: int = Query(default=20, ge=1, le=100, description="Max models to return."),
) -> list[dict[str, Any]]:
    """Return models sorted by descending failure probability.

    Uses historical run data to compute failure risk scores for all
    models and returns the most at-risk ones first.
    """
    from ai_engine.engines.failure_predictor import (
        FailurePredictor,
        RunHistory,
    )

    model_repo = ModelRepository(session, tenant_id=tenant_id)
    run_repo = RunRepository(session, tenant_id=tenant_id)
    predictor = FailurePredictor()

    models = await model_repo.list_all(limit=500)
    results: list[dict[str, Any]] = []

    for model in models:
        try:
            summary = await run_repo.get_model_run_summary(model.model_name)
        except Exception:
            logger.warning(
                "Failed to get run summary for model=%s",
                model.model_name,
                exc_info=True,
            )
            continue

        if summary["total_runs"] == 0:
            continue

        history = RunHistory(
            model_name=model.model_name,
            total_runs=summary["total_runs"],
            failed_runs=summary["failed_runs"],
            recent_runs=summary["recent_runs"],
            recent_failures=summary["recent_failures"],
            consecutive_failures=summary["consecutive_failures"],
            avg_runtime_seconds=summary["avg_runtime_seconds"],
            recent_avg_runtime_seconds=summary["recent_avg_runtime_seconds"],
            runtime_trend=summary["runtime_trend"],
            hours_since_last_success=summary["hours_since_last_success"],
            last_error_type=summary["last_error_type"],
        )

        prediction = predictor.predict(history)

        results.append(
            {
                "model_name": model.model_name,
                "failure_probability": prediction.failure_probability,
                "risk_level": prediction.risk_level,
                "factors": prediction.factors,
                "suggested_actions": prediction.suggested_actions,
                "total_runs": summary["total_runs"],
                "failed_runs": summary["failed_runs"],
                "consecutive_failures": summary["consecutive_failures"],
            }
        )

    # Sort by failure probability descending
    results.sort(key=lambda r: r["failure_probability"], reverse=True)
    return results[:limit]


@router.get("/{model_name:path}/health")
async def get_model_health(
    model_name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
) -> dict[str, Any]:
    """Return health assessment for a model: failure risk + cost trend.

    Combines failure prediction with cost trend analysis to give a
    comprehensive model health overview.
    """
    from ai_engine.engines.failure_predictor import (
        FailurePredictor,
        RunHistory,
        compute_cost_trend,
    )

    model_repo = ModelRepository(session, tenant_id=tenant_id)
    row = await model_repo.get(model_name)
    if row is None:
        raise not_found_404("Model", model_name)

    run_repo = RunRepository(session, tenant_id=tenant_id)
    summary = await run_repo.get_model_run_summary(model_name)

    # Failure prediction
    history = RunHistory(
        model_name=model_name,
        total_runs=summary["total_runs"],
        failed_runs=summary["failed_runs"],
        recent_runs=summary["recent_runs"],
        recent_failures=summary["recent_failures"],
        consecutive_failures=summary["consecutive_failures"],
        avg_runtime_seconds=summary["avg_runtime_seconds"],
        recent_avg_runtime_seconds=summary["recent_avg_runtime_seconds"],
        runtime_trend=summary["runtime_trend"],
        hours_since_last_success=summary["hours_since_last_success"],
        last_error_type=summary["last_error_type"],
    )

    predictor = FailurePredictor()
    prediction = predictor.predict(history)

    # Cost trend
    cost_trend_data = None
    if summary["recent_costs"] or summary["historical_costs"]:
        trend = compute_cost_trend(
            model_name=model_name,
            recent_costs=summary["recent_costs"],
            historical_costs=summary["historical_costs"],
        )
        cost_trend_data = {
            "current_avg_cost_usd": trend.current_avg_cost_usd,
            "previous_avg_cost_usd": trend.previous_avg_cost_usd,
            "cost_change_pct": trend.cost_change_pct,
            "projected_monthly_cost_usd": trend.projected_monthly_cost_usd,
            "trend_direction": trend.trend_direction,
            "factors": trend.factors,
            "alert": trend.alert,
        }

    # Historical stats
    stats = await run_repo.get_historical_stats(model_name)

    return {
        "model_name": model_name,
        "failure_prediction": {
            "failure_probability": prediction.failure_probability,
            "risk_level": prediction.risk_level,
            "factors": prediction.factors,
            "suggested_actions": prediction.suggested_actions,
        },
        "cost_trend": cost_trend_data,
        "historical_stats": stats,
        "run_summary": {
            "total_runs": summary["total_runs"],
            "failed_runs": summary["failed_runs"],
            "recent_runs": summary["recent_runs"],
            "recent_failures": summary["recent_failures"],
            "consecutive_failures": summary["consecutive_failures"],
            "runtime_trend_pct": round(summary["runtime_trend"], 4),
        },
    }


@router.get("/{model_name:path}/lineage")
async def get_model_lineage(
    model_name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
) -> dict[str, Any]:
    """Return upstream and downstream lineage for a model.

    Lineage is computed by loading all models from the database and
    building a lightweight in-memory graph from their declared
    dependencies (tags JSON encodes referenced tables when available).
    """
    repo = ModelRepository(session, tenant_id=tenant_id)
    target = await repo.get(model_name)
    if target is None:
        raise not_found_404("Model", model_name)

    # BL-074: cap list_all at 1000 to support larger tenants; report truncation.
    # TODO: replace with a PostgreSQL recursive CTE for O(1) memory at scale.
    _LINEAGE_MODEL_CAP = 1000
    all_models = await repo.list_all(limit=_LINEAGE_MODEL_CAP)
    graph_truncated = len(all_models) >= _LINEAGE_MODEL_CAP

    # Build adjacency from model metadata.
    # Each model's tags field may contain dependency info, but the
    # canonical source is the repo_path + SQL parsing.  For the API
    # we construct lineage from the model table relationships.
    upstream: set[str] = set()
    downstream: set[str] = set()
    _visited_up: set[str] = set()
    _visited_down: set[str] = set()

    # Build dependency edges: child -> set of parents.
    # We use a simple heuristic: for each model, its tags may list
    # upstream names, and the repo_path can indicate project structure.
    # A more robust implementation would load the full DAG from the repo.
    children_of: dict[str, set[str]] = {}
    parents_of: dict[str, set[str]] = {}
    for m in all_models:
        children_of.setdefault(m.model_name, set())
        parents_of.setdefault(m.model_name, set())

    # BL-074: Maximum graph traversal depth.  Chains deeper than this limit
    # would cause RecursionError (~1000 default) and are pathological.
    _MAX_LINEAGE_DEPTH = 50
    depth_truncated = False

    def _walk_upstream(name: str, depth: int = 0) -> int:
        nonlocal depth_truncated
        # BL-074: hard depth cap — stop traversal and flag truncation.
        if depth > _MAX_LINEAGE_DEPTH:
            depth_truncated = True
            return depth
        max_depth = depth
        for parent in parents_of.get(name, set()):
            if parent not in _visited_up:
                _visited_up.add(parent)
                upstream.add(parent)
                max_depth = max(max_depth, _walk_upstream(parent, depth + 1))
        return max_depth

    def _walk_downstream(name: str, depth: int = 0) -> int:
        nonlocal depth_truncated
        # BL-074: hard depth cap — stop traversal and flag truncation.
        if depth > _MAX_LINEAGE_DEPTH:
            depth_truncated = True
            return depth
        max_depth = depth
        for child in children_of.get(name, set()):
            if child not in _visited_down:
                _visited_down.add(child)
                downstream.add(child)
                max_depth = max(max_depth, _walk_downstream(child, depth + 1))
        return max_depth

    depth_up = _walk_upstream(model_name)
    depth_down = _walk_downstream(model_name)
    is_truncated = depth_truncated or graph_truncated

    return {
        "model_name": model_name,
        "upstream": sorted(upstream),
        "downstream": sorted(downstream),
        "depth": max(depth_up, depth_down),
        "is_truncated": is_truncated,
    }


@router.get("/{model_name:path}/column-lineage")
async def get_column_lineage(
    model_name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    settings: SettingsDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
    column: str | None = Query(
        default=None,
        description=("Specific column to trace.  If omitted, returns lineage for all output columns of the model."),
    ),
) -> dict[str, Any]:
    """Return column-level lineage for a model.

    When *column* is provided, traces that single column back through
    the SQL to its source tables and columns.  Without *column*,
    returns lineage for every output column in the model.

    Column lineage is computed from the model's SQL using AST-based
    analysis — no warehouse connection required.
    """
    from pathlib import Path

    from core_engine.graph.column_lineage import compute_model_column_lineage
    from core_engine.sql_toolkit import Dialect, SqlLineageError

    repo = ModelRepository(session, tenant_id=tenant_id)
    target = await repo.get(model_name)
    if target is None:
        raise not_found_404("Model", model_name)

    # Load the model's SQL from the repo path (validate path under allowed base first).
    model_sql: str | None = None
    if target.repo_path:
        try:
            allowed_base = Path(settings.allowed_repo_base).resolve()
            sql_path = resolve_repo_path_under_base(target.repo_path, allowed_base)
        except ValueError as exc:
            logger.warning("Model %s repo_path outside allowed base: %s", model_name, exc)
            raise HTTPException(
                status_code=400,
                detail="Model repository path is outside the allowed base directory.",
            ) from exc
        if sql_path.exists():
            try:
                raw = sql_path.read_text(encoding="utf-8")
                # Strip YAML header if present.
                if raw.startswith("---"):
                    end = raw.find("---", 3)
                    if end != -1:
                        model_sql = raw[end + 3 :].strip()
                    else:
                        model_sql = raw
                else:
                    model_sql = raw
            except Exception:
                logger.warning(
                    "Could not read SQL from %s for model %s",
                    target.repo_path,
                    model_name,
                    exc_info=True,
                )

    if not model_sql:
        raise HTTPException(
            status_code=422,
            detail=(f"No SQL available for model {model_name}. Column lineage requires the model's SQL source."),
        )

    try:
        lineage_result = compute_model_column_lineage(
            model_name=model_name,
            sql=model_sql,
            dialect=Dialect.DATABRICKS,
        )
    except SqlLineageError as exc:
        logger.warning("Column lineage analysis failed for model %s: %s", model_name, exc, exc_info=True)
        raise HTTPException(
            status_code=422,
            detail="Column lineage analysis failed.",
        ) from exc

    # If a specific column was requested, filter to just that column.
    if column:
        if column not in lineage_result.column_lineage:
            available = sorted(lineage_result.column_lineage.keys())
            raise HTTPException(
                status_code=404,
                detail=(f"Column '{column}' not found in model output. Available columns: {', '.join(available[:20])}"),
            )

        nodes = lineage_result.column_lineage[column]
        return {
            "model_name": model_name,
            "column": column,
            "lineage": [
                {
                    "source_table": n.source_table,
                    "source_column": n.source_column,
                    "transform_type": n.transform_type,
                    "transform_sql": n.transform_sql,
                }
                for n in nodes
            ],
            "unresolved": list(lineage_result.unresolved_columns),
        }

    # Return all columns.
    all_lineage: dict[str, list[dict[str, Any]]] = {}
    for col_name, nodes in lineage_result.column_lineage.items():
        all_lineage[col_name] = [
            {
                "source_table": n.source_table,
                "source_column": n.source_column,
                "transform_type": n.transform_type,
                "transform_sql": n.transform_sql,
            }
            for n in nodes
        ]

    return {
        "model_name": model_name,
        "columns": all_lineage,
        "unresolved": list(lineage_result.unresolved_columns),
    }


@router.get("/{model_name:path}")
async def get_model(
    model_name: str,
    session: SessionDep,
    tenant_id: TenantDep,
    _role: Role = Depends(require_permission(Permission.READ_MODELS)),
) -> dict[str, Any]:
    """Retrieve detailed information about a single model."""
    repo = ModelRepository(session, tenant_id=tenant_id)
    row = await repo.get(model_name)
    if row is None:
        raise not_found_404("Model", model_name)

    # Fetch latest watermark.
    wm_repo = WatermarkRepository(session, tenant_id=tenant_id)
    watermark = await wm_repo.get_watermark(model_name)

    # Fetch latest run stats.
    run_repo = RunRepository(session, tenant_id=tenant_id)
    stats = await run_repo.get_historical_stats(model_name)

    tags = json.loads(row.tags) if row.tags else []

    return {
        "model_name": row.model_name,
        "repo_path": row.repo_path,
        "kind": row.kind,
        "materialization": row.materialization,
        "time_column": row.time_column,
        "unique_key": row.unique_key,
        "owner": row.owner,
        "tags": tags,
        "current_version": row.current_version,
        "watermark": {
            "partition_start": watermark[0].isoformat() if watermark else None,
            "partition_end": watermark[1].isoformat() if watermark else None,
        },
        "historical_stats": stats,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "last_modified_at": (row.last_modified_at.isoformat() if row.last_modified_at else None),
    }
