"""``ironlayer show`` -- display plan summary."""

from __future__ import annotations

import sys
from pathlib import Path

import typer

from cli.display import display_plan_summary
from cli.helpers import console
from cli.state import get_json_output


def show_command(
    plan_path: Path = typer.Argument(
        ...,
        help="Path to a plan JSON file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
    ),
) -> None:
    """Display a human-readable summary of a plan."""
    from core_engine.planner import deserialize_plan

    try:
        plan_json = plan_path.read_text(encoding="utf-8")
        execution_plan = deserialize_plan(plan_json)
    except Exception as exc:
        console.print(f"[red]Failed to read plan: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    if get_json_output():
        sys.stdout.write(plan_json.strip() + "\n")
    else:
        display_plan_summary(console, execution_plan)
