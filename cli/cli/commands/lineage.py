"""``ironlayer lineage`` -- display upstream/downstream lineage."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer

from cli.display import (
    display_cross_model_column_lineage,
    display_lineage,
)
from cli.helpers import console
from cli.state import get_json_output


def lineage_command(
    repo: Path = typer.Argument(
        ...,
        help="Path to the repository containing SQL models.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    model: str = typer.Option(
        ...,
        "--model",
        "-m",
        help="Canonical model name to trace lineage for.",
    ),
    column: str | None = typer.Option(
        None,
        "--column",
        "-c",
        help=(
            "Column name to trace.  When provided, switches to column-level "
            "lineage mode.  Without --column, shows table-level lineage."
        ),
    ),
    depth: int = typer.Option(
        50,
        "--depth",
        help="Maximum traversal depth for cross-model column tracing.",
        min=1,
        max=200,
    ),
) -> None:
    """Display upstream and downstream lineage for a model."""
    from core_engine.graph import build_dag, get_downstream, get_upstream
    from core_engine.loader import load_models_from_directory

    models_dir = repo / "models"
    if not models_dir.is_dir():
        models_dir = repo

    try:
        model_defs = load_models_from_directory(models_dir)
    except Exception as exc:
        console.print(f"[red]Failed to load models: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    if not model_defs:
        console.print("[yellow]No models found.[/yellow]")
        raise typer.Exit(code=0)

    dag = build_dag(model_defs)
    model_names = {m.name for m in model_defs}

    if model not in model_names:
        console.print(f"[red]Model '{model}' not found in repository.[/red]")
        available = ", ".join(sorted(model_names)[:10])
        if model_names:
            console.print(f"[dim]Available models: {available}[/dim]")
        raise typer.Exit(code=3)

    if column is not None:
        from core_engine.graph import (
            trace_column_across_dag,
        )
        from core_engine.sql_toolkit import Dialect

        model_sql_map: dict[str, str] = {}
        for m in model_defs:
            sql = m.clean_sql if m.clean_sql else m.raw_sql
            if sql:
                model_sql_map[m.name] = sql

        if model not in model_sql_map:
            console.print(f"[red]No SQL found for model '{model}'.[/red]")
            raise typer.Exit(code=3)

        try:
            cross_lineage = trace_column_across_dag(
                dag=dag,
                target_model=model,
                target_column=column,
                model_sql_map=model_sql_map,
                dialect=Dialect.DATABRICKS,
                max_depth=depth,
            )
        except Exception as exc:
            console.print(f"[red]Column lineage failed: {exc}[/red]")
            raise typer.Exit(code=3) from exc

        if get_json_output():
            result: dict[str, Any] = {
                "model": model,
                "column": column,
                "lineage_path": [
                    {
                        "column": node.column,
                        "source_table": node.source_table,
                        "source_column": node.source_column,
                        "transform_type": node.transform_type,
                        "transform_sql": node.transform_sql,
                    }
                    for node in cross_lineage.lineage_path
                ],
            }
            sys.stdout.write(json.dumps(result, indent=2) + "\n")
        else:
            display_cross_model_column_lineage(console, cross_lineage)
        return

    upstream = sorted(get_upstream(dag, model))
    downstream = sorted(get_downstream(dag, model))

    if get_json_output():
        result = {
            "model": model,
            "upstream": upstream,
            "downstream": downstream,
        }
        sys.stdout.write(json.dumps(result, indent=2) + "\n")
    else:
        display_lineage(console, model, upstream, downstream)
