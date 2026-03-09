"""``ironlayer plan`` -- generate execution plan from git diff."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import typer

from cli.display import display_plan_summary
from cli.helpers import console, load_stored_token, parse_date
from cli.state import emit_metrics, get_env, get_json_output


def plan_command(
    repo: Path = typer.Argument(
        ...,
        help="Path to the git repository containing SQL models.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    base: str = typer.Argument(
        ...,
        help="Base git ref (commit SHA or branch) representing the current state.",
    ),
    target: str = typer.Argument(
        ...,
        help="Target git ref (commit SHA or branch) representing the desired state.",
    ),
    out: Path = typer.Option(
        Path("plan.json"),
        "--out",
        "-o",
        help="Output path for the generated plan JSON.",
    ),
    as_of_date: str | None = typer.Option(
        None,
        "--as-of-date",
        help="Reference date for date arithmetic (YYYY-MM-DD). Defaults to today.",
    ),
) -> None:
    """Generate a deterministic execution plan from a git diff."""
    from core_engine.config import load_settings
    from core_engine.diff import compute_structural_diff
    from core_engine.git import get_changed_files, get_file_at_commit, validate_repo
    from core_engine.graph import build_dag
    from core_engine.loader import load_models_from_directory
    from core_engine.parser import compute_canonical_hash
    from core_engine.planner import PlannerConfig, generate_plan, serialize_plan

    try:
        validate_repo(repo)

        models_dir = repo / "models"
        if not models_dir.is_dir():
            models_dir = repo
        models = load_models_from_directory(models_dir)
        if not models:
            console.print("[yellow]No models found. Nothing to plan.[/yellow]")
            raise typer.Exit(code=0)

        dag = build_dag(models)
        changed_files = get_changed_files(repo, base, target)
        sql_changed = [cf for cf in changed_files if cf.path.endswith(".sql")]

        model_map = {m.name: m for m in models}
        changed_model_names = set()
        for cf in sql_changed:
            for m in models:
                if m.file_path.endswith(cf.path) or cf.path.endswith(m.file_path):
                    changed_model_names.add(m.name)

        previous_versions: dict[str, str] = {}
        current_versions: dict[str, str] = {}
        for m in models:
            current_versions[m.name] = m.content_hash

        for m_name in changed_model_names:
            m_def = model_map[m_name]
            try:
                old_sql = get_file_at_commit(repo, m_def.file_path, base)
                previous_versions[m_name] = compute_canonical_hash(old_sql)
            except Exception:
                pass

        for m in models:
            if m.name not in changed_model_names:
                previous_versions[m.name] = m.content_hash
                current_versions[m.name] = m.content_hash

        diff_result = compute_structural_diff(previous_versions, current_versions)
        ref_date = parse_date(as_of_date, "as-of-date") if as_of_date else date.today()

        settings = load_settings(env=get_env())
        planner_config = PlannerConfig(
            default_lookback_days=settings.default_lookback_days,
        )

        execution_plan = generate_plan(
            models=model_map,
            diff_result=diff_result,
            dag=dag,
            watermarks={},
            run_stats={},
            config=planner_config,
            base=base,
            target=target,
            as_of_date=ref_date,
        )

        plan_json = serialize_plan(execution_plan)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(plan_json, encoding="utf-8")

        emit_metrics(
            "plan.generated",
            {
                "plan_id": execution_plan.plan_id,
                "total_steps": execution_plan.summary.total_steps,
                "estimated_cost_usd": execution_plan.summary.estimated_cost_usd,
            },
        )

        if get_json_output():
            sys.stdout.write(plan_json + "\n")
        else:
            display_plan_summary(console, execution_plan)
            console.print(f"\nPlan written to [bold]{out}[/bold]")

            if not load_stored_token():
                from cli.cloud import load_stored_token as _cloud_token

                if not _cloud_token():
                    console.print(
                        "\n[dim]Tip: Get AI-powered cost estimates and risk scoring"
                        " -- run [bold]ironlayer login[/bold] to connect to"
                        " IronLayer Cloud.[/dim]"
                    )

        raise typer.Exit(code=0)

    except typer.Exit:
        raise
    except Exception as exc:
        console.print(f"[red]Error generating plan: {exc}[/red]")
        emit_metrics("plan.error", {"error": str(exc)})
        raise typer.Exit(code=3) from exc
