"""``ironlayer apply`` -- execute a plan."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from cli.display import display_run_results
from cli.helpers import (
    console,
    format_input_range,
    load_model_sql_map,
    resolve_model_sql,
)
from cli.state import emit_metrics, get_env, get_json_output


def apply_command(
    plan_path: Path = typer.Argument(
        ...,
        help="Path to the plan JSON file to execute.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
    ),
    repo: Path = typer.Option(
        ...,
        "--repo",
        help="Path to the git repository containing SQL model definitions.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    approve_by: str | None = typer.Option(
        None,
        "--approve-by",
        help="Name of the person approving this plan execution.",
    ),
    auto_approve: bool = typer.Option(
        False,
        "--auto-approve",
        help="Skip manual approval (only allowed in dev environment).",
    ),
    override_cluster: str | None = typer.Option(
        None,
        "--override-cluster",
        help="Override the cluster/warehouse used for execution.",
    ),
) -> None:
    """Execute a previously generated plan."""
    from core_engine.config import load_settings
    from core_engine.executor import LocalExecutor
    from core_engine.models.run import RunRecord, RunStatus
    from core_engine.planner import deserialize_plan

    try:
        plan_json = plan_path.read_text(encoding="utf-8")
        execution_plan = deserialize_plan(plan_json)
    except Exception as exc:
        console.print(f"[red]Failed to read plan: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    try:
        sql_map = load_model_sql_map(repo)
    except Exception as exc:
        console.print(f"[red]Failed to load models from repo: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    if not auto_approve and get_env() != "dev":
        if not approve_by:
            console.print("[red]Non-dev environments require --approve-by or --auto-approve.[/red]")
            raise typer.Exit(code=3)
        console.print(f"Plan approved by: [bold]{approve_by}[/bold]")

    if execution_plan.summary.total_steps == 0:
        console.print("[green]Plan has zero steps -- nothing to execute.[/green]")
        raise typer.Exit(code=0)

    settings = load_settings(env=get_env())

    emit_metrics(
        "apply.started",
        {
            "plan_id": execution_plan.plan_id,
            "total_steps": execution_plan.summary.total_steps,
            "env": get_env(),
            "approved_by": approve_by or ("auto" if auto_approve else "dev-default"),
        },
    )

    run_records: list[dict] = []
    failed = False

    with LocalExecutor(db_path=settings.local_db_path) as executor:
        for idx, step in enumerate(execution_plan.steps, start=1):
            step_label = f"[{idx}/{execution_plan.summary.total_steps}] {step.model}"

            if failed:
                run_records.append(
                    {
                        "model": step.model,
                        "status": "CANCELLED",
                        "duration_seconds": 0.0,
                        "input_range": format_input_range(step.input_range),
                        "retries": 0,
                    }
                )
                continue

            with console.status(f"Executing {step_label}...", spinner="dots"):
                parameters: dict[str, str] = {}
                if step.input_range is not None:
                    parameters["start_date"] = step.input_range.start.isoformat()
                    parameters["end_date"] = step.input_range.end.isoformat()

                if override_cluster:
                    parameters["cluster_id"] = override_cluster

                model_sql = resolve_model_sql(step.model, sql_map)
                record: RunRecord = executor.execute_step(
                    step=step,
                    sql=model_sql,
                    parameters=parameters,
                )

            duration = 0.0
            if record.started_at and record.finished_at:
                duration = (record.finished_at - record.started_at).total_seconds()

            run_records.append(
                {
                    "model": step.model,
                    "status": record.status.value,
                    "duration_seconds": round(duration, 2),
                    "input_range": format_input_range(step.input_range),
                    "retries": record.retry_count,
                }
            )

            emit_metrics(
                "step.completed",
                {
                    "plan_id": execution_plan.plan_id,
                    "step_id": step.step_id,
                    "model": step.model,
                    "status": record.status.value,
                    "duration_seconds": round(duration, 2),
                },
            )

            if record.status == RunStatus.FAIL:
                failed = True
                console.print(f"[red]Step {step_label} failed: {record.error_message}[/red]")

    emit_metrics(
        "apply.completed",
        {
            "plan_id": execution_plan.plan_id,
            "failed": failed,
            "steps_executed": len([r for r in run_records if r["status"] != "CANCELLED"]),
        },
    )

    if get_json_output():
        sys.stdout.write(json.dumps(run_records, indent=2, default=str) + "\n")
    else:
        display_run_results(console, run_records)

    if failed:
        raise typer.Exit(code=3)
