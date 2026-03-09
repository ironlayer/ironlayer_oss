"""``ironlayer models`` -- list models in a repository."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from cli.display import display_model_list
from cli.helpers import console
from cli.state import get_json_output


def models_command(
    repo: Path = typer.Argument(
        ...,
        help="Path to the repository containing SQL models.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
) -> None:
    """List all models discovered in a repository."""
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

    if get_json_output():
        rows = [
            {
                "name": m.name,
                "kind": m.kind.value,
                "materialization": m.materialization.value,
                "time_column": m.time_column,
                "owner": m.owner,
                "tags": m.tags,
                "dependencies": m.dependencies,
            }
            for m in model_defs
        ]
        sys.stdout.write(json.dumps(rows, indent=2) + "\n")
    else:
        display_model_list(console, model_defs)
