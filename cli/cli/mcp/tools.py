"""MCP tool implementations for IronLayer.

Each tool is a plain async function that:
1. Accepts JSON-serializable arguments.
2. Imports from core_engine at call time (lazy, to keep startup fast).
3. Returns a ``dict[str, Any]`` result.

The ``TOOL_DEFINITIONS`` list at the bottom provides JSON Schema
descriptions for tool registration with the MCP server.  These
schemas are consumed by :mod:`cli.mcp.server`.

Tool list:
- ``ironlayer_plan``           — Generate an execution plan from git diff.
- ``ironlayer_show``           — Display an existing plan.
- ``ironlayer_lineage``        — Table-level dependency graph.
- ``ironlayer_column_lineage`` — Column-level lineage with optional schema.
- ``ironlayer_diff``           — Semantic SQL diff.
- ``ironlayer_validate``       — Contract validation with optional schema.
- ``ironlayer_models``         — List models with metadata.
- ``ironlayer_transpile``      — Dialect transpilation (e.g. Redshift → Databricks).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _resolve_models_dir(repo_path: str) -> Path:
    """Resolve the models directory from a repository path.

    Looks for a ``models/`` subdirectory first; falls back to the
    repo root.
    """
    repo = Path(repo_path)
    models_dir = repo / "models"
    return models_dir if models_dir.is_dir() else repo


# ---------------------------------------------------------------------------
# Tool: ironlayer_plan
# ---------------------------------------------------------------------------


async def ironlayer_plan(
    repo_path: str,
    *,
    base_ref: str = "HEAD~1",
    target_ref: str = "HEAD",
) -> dict[str, Any]:
    """Generate an execution plan from a git diff.

    Loads models, computes the DAG, detects changed models via git
    diff + content hashing, and produces a deterministic plan.
    """
    from core_engine.diff.change_detector import detect_changes
    from core_engine.graph import build_dag, topological_sort
    from core_engine.loader import load_models_from_directory
    from core_engine.planner.planner import build_plan

    models_dir = _resolve_models_dir(repo_path)

    model_defs = load_models_from_directory(models_dir)
    if not model_defs:
        return {"status": "empty", "message": "No models found", "steps": []}

    dag = build_dag(model_defs)
    order = topological_sort(dag)

    changes = detect_changes(
        models=model_defs,
        dag=dag,
        base_ref=base_ref,
        target_ref=target_ref,
        repo_path=str(repo),
    )

    if not changes.changed_models:
        return {
            "status": "empty",
            "message": "No model changes detected",
            "cosmetic_only": list(changes.cosmetic_only_models),
            "steps": [],
        }

    plan = build_plan(
        models=model_defs,
        dag=dag,
        execution_order=order,
        changed_models=changes.changed_models,
        change_details=changes.change_details,
    )

    return {
        "status": "success",
        "plan_id": plan.plan_id,
        "total_steps": plan.summary.total_steps,
        "estimated_cost_usd": plan.summary.estimated_cost_usd,
        "models_changed": list(changes.changed_models),
        "cosmetic_skipped": list(changes.cosmetic_only_models),
        "steps": [
            {
                "model": step.model,
                "run_type": step.run_type.value,
                "reason": step.reason,
                "estimated_cost_usd": step.estimated_cost_usd,
                "parallel_group": step.parallel_group,
                "contract_violations": step.contract_violations,
            }
            for step in plan.steps
        ],
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_show
# ---------------------------------------------------------------------------


async def ironlayer_show(plan_path: str) -> dict[str, Any]:
    """Display an existing plan from a JSON file."""
    import json

    path = Path(plan_path)
    if not path.exists():
        return {"error": f"Plan file not found: {plan_path}"}

    if not path.is_file():
        return {"error": f"Not a file: {plan_path}"}

    # Guard against extremely large files.
    file_size = path.stat().st_size
    if file_size > 50 * 1024 * 1024:  # 50 MB
        return {"error": f"Plan file too large ({file_size} bytes)"}

    try:
        plan_data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"error": f"Invalid JSON in plan file: {exc}"}

    return {
        "status": "success",
        "plan": plan_data,
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_lineage
# ---------------------------------------------------------------------------


async def ironlayer_lineage(
    repo_path: str,
    model_name: str,
) -> dict[str, Any]:
    """Return upstream and downstream table-level lineage for a model."""
    from core_engine.graph import build_dag, get_downstream, get_upstream
    from core_engine.loader import load_models_from_directory

    models_dir = _resolve_models_dir(repo_path)

    model_defs = load_models_from_directory(models_dir)
    if not model_defs:
        return {"error": "No models found"}

    dag = build_dag(model_defs)
    model_names = {m.name for m in model_defs}

    if model_name not in model_names:
        return {
            "error": f"Model '{model_name}' not found",
            "available_models": sorted(model_names)[:20],
        }

    upstream = sorted(get_upstream(dag, model_name))
    downstream = sorted(get_downstream(dag, model_name))

    return {
        "model": model_name,
        "upstream": upstream,
        "downstream": downstream,
        "upstream_count": len(upstream),
        "downstream_count": len(downstream),
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_column_lineage
# ---------------------------------------------------------------------------


async def ironlayer_column_lineage(
    repo_path: str,
    model_name: str,
    *,
    column: str | None = None,
    schema: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Trace column-level lineage for a model.

    When *column* is specified, traces that single column back through
    the DAG to its source tables.  Without *column*, returns lineage
    for all output columns of the model.

    Optional *schema* mapping ``{table: {column: type}}`` enables
    resolution of ``SELECT *`` and unqualified column references.
    """
    from core_engine.graph import (
        build_dag,
        compute_model_column_lineage,
        trace_column_across_dag,
    )
    from core_engine.loader import load_models_from_directory
    from core_engine.sql_toolkit import Dialect, SqlLineageError

    models_dir = _resolve_models_dir(repo_path)

    model_defs = load_models_from_directory(models_dir)
    if not model_defs:
        return {"error": "No models found"}

    model_map = {m.name: m for m in model_defs}
    if model_name not in model_map:
        return {
            "error": f"Model '{model_name}' not found",
            "available_models": sorted(model_map.keys())[:20],
        }

    target_model = model_map[model_name]
    sql = target_model.clean_sql or target_model.raw_sql
    if not sql:
        return {"error": f"No SQL found for model '{model_name}'"}

    # If tracing a specific column across the DAG:
    if column:
        dag = build_dag(model_defs)
        model_sql_map = {m.name: (m.clean_sql or m.raw_sql) for m in model_defs if m.clean_sql or m.raw_sql}

        try:
            cross_lineage = trace_column_across_dag(
                dag=dag,
                target_model=model_name,
                target_column=column,
                model_sql_map=model_sql_map,
                dialect=Dialect.DATABRICKS,
                schema=schema,
            )
        except SqlLineageError as exc:
            return {"error": f"Column lineage failed: {exc}"}

        return {
            "model": model_name,
            "column": column,
            "lineage_path": [
                {
                    "source_table": n.source_table,
                    "source_column": n.source_column,
                    "transform_type": n.transform_type,
                    "transform_sql": n.transform_sql,
                }
                for n in cross_lineage.lineage_path
            ],
        }

    # All columns for the model:
    try:
        result = compute_model_column_lineage(
            model_name=model_name,
            sql=sql,
            dialect=Dialect.DATABRICKS,
            schema=schema,
        )
    except SqlLineageError as exc:
        return {"error": f"Column lineage failed: {exc}"}

    columns: dict[str, list[dict[str, Any]]] = {}
    for col_name, nodes in result.column_lineage.items():
        columns[col_name] = [
            {
                "source_table": n.source_table,
                "source_column": n.source_column,
                "transform_type": n.transform_type,
                "transform_sql": n.transform_sql,
            }
            for n in nodes
        ]

    return {
        "model": model_name,
        "columns": columns,
        "unresolved": list(result.unresolved_columns),
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_diff
# ---------------------------------------------------------------------------


async def ironlayer_diff(
    old_sql: str,
    new_sql: str,
    *,
    dialect: str = "databricks",
) -> dict[str, Any]:
    """Compute a semantic SQL diff between two statements.

    Returns structural edit operations, whether changes are cosmetic
    only, and column-level changes for SELECT statements.
    """
    from core_engine.sql_toolkit import Dialect, get_sql_toolkit

    try:
        dialect_enum = Dialect(dialect)
    except ValueError:
        return {"error": f"Unsupported dialect '{dialect}'. Valid: databricks, duckdb, redshift"}

    tk = get_sql_toolkit()

    diff_result = tk.differ.diff(old_sql, new_sql, dialect_enum)
    column_changes = tk.differ.extract_column_changes(old_sql, new_sql, dialect_enum)

    return {
        "is_identical": diff_result.is_identical,
        "is_cosmetic_only": diff_result.is_cosmetic_only,
        "edit_count": len(diff_result.edits),
        "edits": [
            {
                "kind": e.kind.value,
                "source_sql": (
                    e.source_sql[:200] + "..." if e.source_sql and len(e.source_sql) > 200 else (e.source_sql or "")
                ),
                "target_sql": (
                    e.target_sql[:200] + "..." if e.target_sql and len(e.target_sql) > 200 else (e.target_sql or "")
                ),
            }
            for e in diff_result.edits
            if e.kind.value != "keep"
        ],
        "column_changes": column_changes,
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_validate
# ---------------------------------------------------------------------------


async def ironlayer_validate(
    repo_path: str,
    *,
    model_name: str | None = None,
    schema: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Validate schema contracts for models in a repository.

    Checks each model's SQL against its declared column contracts.
    When *model_name* is provided, validates only that model.
    When *schema* is provided, uses it for column qualification.

    Returns all contract violations with severity levels.
    """
    from core_engine.loader import load_models_from_directory
    from core_engine.sql_toolkit import Dialect, get_sql_toolkit

    models_dir = _resolve_models_dir(repo_path)

    model_defs = load_models_from_directory(models_dir)
    if not model_defs:
        return {"error": "No models found"}

    if model_name:
        model_defs = [m for m in model_defs if m.name == model_name]
        if not model_defs:
            return {"error": f"Model '{model_name}' not found"}

    tk = get_sql_toolkit()
    all_violations: list[dict[str, Any]] = []
    models_checked = 0

    for model_def in model_defs:
        sql = model_def.clean_sql or model_def.raw_sql
        if not sql:
            continue

        # Safety check the SQL.
        safety = tk.safety_guard.check(sql, Dialect.DATABRICKS)
        if not safety.is_safe:
            for v in safety.violations:
                all_violations.append(
                    {
                        "model": model_def.name,
                        "type": "safety",
                        "violation_type": v.violation_type,
                        "target": v.target,
                        "detail": v.detail,
                        "severity": v.severity,
                    }
                )

        # If schema is provided, qualify columns and check for issues.
        if schema:
            qualify_result = tk.qualifier.qualify_columns(sql, schema, Dialect.DATABRICKS)
            for warning in qualify_result.warnings:
                all_violations.append(
                    {
                        "model": model_def.name,
                        "type": "qualification",
                        "detail": warning,
                        "severity": "warning",
                    }
                )

        # Check column contracts if the model has them.
        if model_def.contract_columns:
            extraction = tk.scope_analyzer.extract_columns(sql, Dialect.DATABRICKS)
            output_cols = set(extraction.output_columns)

            for contract in model_def.contract_columns:
                if contract.name not in output_cols and not extraction.has_star:
                    all_violations.append(
                        {
                            "model": model_def.name,
                            "type": "contract",
                            "column_name": contract.name,
                            "violation_type": "COLUMN_REMOVED",
                            "expected": contract.data_type,
                            "actual": "missing",
                            "severity": "BREAKING",
                        }
                    )

        models_checked += 1

    return {
        "models_checked": models_checked,
        "violation_count": len(all_violations),
        "is_valid": len(all_violations) == 0,
        "violations": all_violations,
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_models
# ---------------------------------------------------------------------------


async def ironlayer_models(
    repo_path: str,
    *,
    kind: str | None = None,
    owner: str | None = None,
) -> dict[str, Any]:
    """List all models in a repository with metadata."""
    from core_engine.loader import load_models_from_directory

    models_dir = _resolve_models_dir(repo_path)

    model_defs = load_models_from_directory(models_dir)

    if kind:
        model_defs = [m for m in model_defs if m.kind.value == kind.upper()]
    if owner:
        model_defs = [m for m in model_defs if m.owner == owner]

    return {
        "total": len(model_defs),
        "models": [
            {
                "name": m.name,
                "kind": m.kind.value,
                "materialization": m.materialization.value,
                "time_column": m.time_column,
                "owner": m.owner,
                "tags": m.tags,
                "dependencies": m.dependencies,
                "output_columns": m.output_columns,
                "file_path": m.file_path,
            }
            for m in model_defs
        ],
    }


# ---------------------------------------------------------------------------
# Tool: ironlayer_transpile
# ---------------------------------------------------------------------------


async def ironlayer_transpile(
    sql: str,
    *,
    source_dialect: str = "redshift",
    target_dialect: str = "databricks",
    pretty: bool = True,
) -> dict[str, Any]:
    """Transpile SQL between dialects.

    Particularly useful for Redshift → Databricks migration:
    converts dialect-specific functions, types, and syntax.
    """
    from core_engine.sql_toolkit import Dialect, get_sql_toolkit

    valid_dialects = {d.value for d in Dialect}
    for name, val in [("source_dialect", source_dialect), ("target_dialect", target_dialect)]:
        if val not in valid_dialects:
            return {"error": f"Unsupported {name} '{val}'. Valid: {', '.join(sorted(valid_dialects))}"}

    tk = get_sql_toolkit()

    source = Dialect(source_dialect)
    target = Dialect(target_dialect)

    result = tk.transpiler.transpile(sql, source, target, pretty=pretty)

    return {
        "output_sql": result.output_sql,
        "source_dialect": result.source_dialect.value,
        "target_dialect": result.target_dialect.value,
        "warnings": result.warnings,
        "fallback_used": result.fallback_used,
    }


# ---------------------------------------------------------------------------
# Tool Definitions (JSON Schema for MCP registration)
# ---------------------------------------------------------------------------


TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "ironlayer_plan",
        "description": (
            "Generate an IronLayer execution plan from a git diff. "
            "Detects changed SQL models and produces a deterministic "
            "plan with execution steps, cost estimates, and contract "
            "validation results."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {
                    "type": "string",
                    "description": "Path to the repository containing SQL models.",
                },
                "base_ref": {
                    "type": "string",
                    "description": "Git ref for the base (before) state. Default: HEAD~1.",
                    "default": "HEAD~1",
                },
                "target_ref": {
                    "type": "string",
                    "description": "Git ref for the target (after) state. Default: HEAD.",
                    "default": "HEAD",
                },
            },
            "required": ["repo_path"],
        },
    },
    {
        "name": "ironlayer_show",
        "description": "Display an existing IronLayer plan from a JSON file.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "plan_path": {
                    "type": "string",
                    "description": "Path to the plan JSON file.",
                },
            },
            "required": ["plan_path"],
        },
    },
    {
        "name": "ironlayer_lineage",
        "description": (
            "Return upstream and downstream table-level lineage for a "
            "model. Shows which models depend on this model and which "
            "models this model depends on."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {
                    "type": "string",
                    "description": "Path to the repository containing SQL models.",
                },
                "model_name": {
                    "type": "string",
                    "description": "Canonical model name (e.g. 'analytics.orders_daily').",
                },
            },
            "required": ["repo_path", "model_name"],
        },
    },
    {
        "name": "ironlayer_column_lineage",
        "description": (
            "Trace column-level lineage for a SQL model. Shows which source "
            "table columns each output column derives from, including "
            "transformation types (direct, expression, aggregation, window, "
            "case, literal). Optionally trace a single column across the "
            "entire model DAG. Provide a schema mapping to resolve "
            "SELECT * and unqualified column references."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {
                    "type": "string",
                    "description": "Path to the repository containing SQL models.",
                },
                "model_name": {
                    "type": "string",
                    "description": "Canonical model name to analyze.",
                },
                "column": {
                    "type": "string",
                    "description": (
                        "Specific column to trace across the DAG. If omitted, returns lineage for all output columns."
                    ),
                },
                "schema": {
                    "type": "object",
                    "description": (
                        "Schema mapping for column resolution: "
                        "{table_name: {column_name: type_string}}. "
                        "Enables resolution of SELECT * and unqualified columns. "
                        'Example: {"orders": {"id": "INT", "amount": "DECIMAL"}}'
                    ),
                    "additionalProperties": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                },
            },
            "required": ["repo_path", "model_name"],
        },
    },
    {
        "name": "ironlayer_diff",
        "description": (
            "Compute a semantic SQL diff between two SQL statements. "
            "Returns whether changes are structural or cosmetic only, "
            "the AST edit operations, and column-level changes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "old_sql": {
                    "type": "string",
                    "description": "The original SQL statement.",
                },
                "new_sql": {
                    "type": "string",
                    "description": "The modified SQL statement.",
                },
                "dialect": {
                    "type": "string",
                    "description": "SQL dialect. Default: 'databricks'.",
                    "enum": ["databricks", "duckdb", "redshift"],
                    "default": "databricks",
                },
            },
            "required": ["old_sql", "new_sql"],
        },
    },
    {
        "name": "ironlayer_validate",
        "description": (
            "Validate schema contracts and SQL safety for models. "
            "Checks column contracts, detects dangerous SQL operations, "
            "and optionally qualifies columns using schema information. "
            "Provide a schema mapping to enable column qualification "
            "and richer validation."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {
                    "type": "string",
                    "description": "Path to the repository containing SQL models.",
                },
                "model_name": {
                    "type": "string",
                    "description": ("Specific model to validate. If omitted, validates all models."),
                },
                "schema": {
                    "type": "object",
                    "description": (
                        "Schema mapping for column qualification: "
                        "{table_name: {column_name: type_string}}. "
                        "Enables richer validation with type checking."
                    ),
                    "additionalProperties": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                },
            },
            "required": ["repo_path"],
        },
    },
    {
        "name": "ironlayer_models",
        "description": (
            "List all SQL models in a repository with metadata including "
            "kind, materialization, owner, tags, dependencies, and "
            "output columns."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {
                    "type": "string",
                    "description": "Path to the repository containing SQL models.",
                },
                "kind": {
                    "type": "string",
                    "description": (
                        "Filter by model kind. "
                        "Options: FULL_REFRESH, INCREMENTAL_BY_TIME_RANGE, "
                        "APPEND_ONLY, MERGE_BY_KEY."
                    ),
                },
                "owner": {
                    "type": "string",
                    "description": "Filter by model owner.",
                },
            },
            "required": ["repo_path"],
        },
    },
    {
        "name": "ironlayer_transpile",
        "description": (
            "Transpile SQL between database dialects. Converts "
            "dialect-specific functions, types, and syntax. Particularly "
            "useful for Redshift to Databricks migrations: GETDATE() → "
            "CURRENT_TIMESTAMP(), NVL() → COALESCE(), CONVERT_TIMEZONE() → "
            "FROM_UTC_TIMESTAMP(), etc."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL statement to transpile.",
                },
                "source_dialect": {
                    "type": "string",
                    "description": "Source SQL dialect. Default: 'redshift'.",
                    "enum": ["databricks", "duckdb", "redshift"],
                    "default": "redshift",
                },
                "target_dialect": {
                    "type": "string",
                    "description": "Target SQL dialect. Default: 'databricks'.",
                    "enum": ["databricks", "duckdb", "redshift"],
                    "default": "databricks",
                },
                "pretty": {
                    "type": "boolean",
                    "description": "Format output with indentation. Default: true.",
                    "default": True,
                },
            },
            "required": ["sql"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool dispatch map
# ---------------------------------------------------------------------------

TOOL_DISPATCH: dict[str, Any] = {
    "ironlayer_plan": ironlayer_plan,
    "ironlayer_show": ironlayer_show,
    "ironlayer_lineage": ironlayer_lineage,
    "ironlayer_column_lineage": ironlayer_column_lineage,
    "ironlayer_diff": ironlayer_diff,
    "ironlayer_validate": ironlayer_validate,
    "ironlayer_models": ironlayer_models,
    "ironlayer_transpile": ironlayer_transpile,
}
