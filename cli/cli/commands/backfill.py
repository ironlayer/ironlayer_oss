"""``ironlayer backfill``, ``backfill-chunked``, ``backfill-resume``, ``backfill-history``."""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import typer

from cli.display import display_run_results
from cli.helpers import console, load_model_sql_map, parse_date, resolve_model_sql
from cli.state import emit_metrics, get_env, get_json_output


def backfill_command(
    model: str = typer.Option(
        ...,
        "--model",
        "-m",
        help="Canonical model name to backfill.",
    ),
    start: str = typer.Option(
        ...,
        "--start",
        help="Start date for the backfill range (YYYY-MM-DD, inclusive).",
    ),
    end: str = typer.Option(
        ...,
        "--end",
        help="End date for the backfill range (YYYY-MM-DD, inclusive).",
    ),
    repo: Path = typer.Option(
        ...,
        "--repo",
        help="Path to the git repository containing SQL model definitions.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    cluster: str | None = typer.Option(
        None,
        "--cluster",
        help="Override the cluster/warehouse used for execution.",
    ),
) -> None:
    """Run a targeted backfill for a single model over a date range."""
    from core_engine.config import load_settings
    from core_engine.executor import LocalExecutor
    from core_engine.models.plan import DateRange, PlanStep, RunType, compute_deterministic_id
    from core_engine.models.run import RunStatus

    start_date = parse_date(start, "start")
    end_date = parse_date(end, "end")

    if start_date > end_date:
        console.print("[red]Start date must not be after end date.[/red]")
        raise typer.Exit(code=3)

    input_range = DateRange(start=start_date, end=end_date)
    step_id = compute_deterministic_id(model, "backfill", start, end)
    compute_deterministic_id("backfill", model, start, end)

    step = PlanStep(
        step_id=step_id,
        model=model,
        run_type=RunType.INCREMENTAL,
        input_range=input_range,
        depends_on=[],
        parallel_group=0,
        reason=f"manual backfill {start} to {end}",
        estimated_compute_seconds=0.0,
        estimated_cost_usd=0.0,
    )

    console.print(f"Backfilling [bold]{model}[/bold] from [cyan]{start_date}[/cyan] to [cyan]{end_date}[/cyan]")

    settings = load_settings(env=get_env())
    with LocalExecutor(db_path=settings.local_db_path) as executor:
        emit_metrics("backfill.started", {"model": model, "start": start, "end": end})

        parameters: dict[str, str] = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if cluster:
            parameters["cluster_id"] = cluster

        sql_map = load_model_sql_map(repo)
        model_sql = resolve_model_sql(model, sql_map)

        with console.status(f"Executing backfill for {model}...", spinner="dots"):
            record = executor.execute_step(
                step=step,
                sql=model_sql,
                parameters=parameters,
            )

        duration = 0.0
        if record.started_at and record.finished_at:
            duration = (record.finished_at - record.started_at).total_seconds()

        run_records = [
            {
                "model": model,
                "status": record.status.value,
                "duration_seconds": round(duration, 2),
                "input_range": f"{start_date} .. {end_date}",
                "retries": record.retry_count,
            }
        ]

        emit_metrics(
            "backfill.completed",
            {"model": model, "status": record.status.value, "duration_seconds": round(duration, 2)},
        )

        if get_json_output():
            sys.stdout.write(json.dumps(run_records, indent=2, default=str) + "\n")
        else:
            display_run_results(console, run_records)

        if record.status == RunStatus.FAIL:
            console.print(f"[red]Backfill failed: {record.error_message}[/red]")
            raise typer.Exit(code=3)

        console.print("[green]Backfill completed successfully.[/green]")


def backfill_chunked_command(
    model: str = typer.Option(
        ...,
        "--model",
        "-m",
        help="Canonical model name to backfill.",
    ),
    start: str = typer.Option(
        ...,
        "--start",
        help="Start date for the backfill range (YYYY-MM-DD, inclusive).",
    ),
    end: str = typer.Option(
        ...,
        "--end",
        help="End date for the backfill range (YYYY-MM-DD, inclusive).",
    ),
    repo: Path = typer.Option(
        ...,
        "--repo",
        help="Path to the git repository containing SQL model definitions.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    chunk_days: int = typer.Option(
        7,
        "--chunk-days",
        help="Number of days per chunk (default 7).",
        min=1,
    ),
    cluster: str | None = typer.Option(
        None,
        "--cluster",
        help="Override the cluster/warehouse used for execution.",
    ),
) -> None:
    """Run a chunked backfill with checkpoint-based resume capability."""
    from core_engine.config import load_settings
    from core_engine.executor import LocalExecutor
    from core_engine.models.plan import DateRange, PlanStep, RunType, compute_deterministic_id
    from core_engine.models.run import RunStatus

    start_date = parse_date(start, "start")
    end_date = parse_date(end, "end")

    if start_date > end_date:
        console.print("[red]Start date must not be after end date.[/red]")
        raise typer.Exit(code=3)

    chunks: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end_date)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)

    backfill_id = compute_deterministic_id("chunked_backfill", model, start, end, str(chunk_days))

    console.print(
        f"Chunked backfill [bold]{model}[/bold] "
        f"from [cyan]{start_date}[/cyan] to [cyan]{end_date}[/cyan] "
        f"({len(chunks)} chunks of {chunk_days} day(s))"
    )

    settings = load_settings(env=get_env())
    with LocalExecutor(db_path=settings.local_db_path) as executor:
        sql_map = load_model_sql_map(repo)
        model_sql = resolve_model_sql(model, sql_map)

        emit_metrics(
            "backfill_chunked.started",
            {"model": model, "start": start, "end": end, "chunk_days": chunk_days, "total_chunks": len(chunks)},
        )

        run_records: list[dict] = []
        failed = False
        completed_through: date | None = None

        for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            chunk_label = f"[{i}/{len(chunks)}] {model} ({chunk_start} .. {chunk_end})"

            if failed:
                run_records.append(
                    {
                        "model": model,
                        "status": "CANCELLED",
                        "duration_seconds": 0.0,
                        "input_range": f"{chunk_start} .. {chunk_end}",
                        "retries": 0,
                        "chunk": i,
                    }
                )
                continue

            step_id = compute_deterministic_id(
                model, "chunk", chunk_start.isoformat(), chunk_end.isoformat()
            )
            input_range = DateRange(start=chunk_start, end=chunk_end)
            step = PlanStep(
                step_id=step_id,
                model=model,
                run_type=RunType.INCREMENTAL,
                input_range=input_range,
                depends_on=[],
                parallel_group=0,
                reason=f"chunked backfill chunk {i}/{len(chunks)}",
                estimated_compute_seconds=0.0,
                estimated_cost_usd=0.0,
            )

            parameters: dict[str, str] = {
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
            }
            if cluster:
                parameters["cluster_id"] = cluster

            with console.status(f"Executing {chunk_label}...", spinner="dots"):
                record = executor.execute_step(
                    step=step,
                    sql=model_sql,
                    parameters=parameters,
                )

            duration = 0.0
            if record.started_at and record.finished_at:
                duration = (record.finished_at - record.started_at).total_seconds()

            run_records.append(
                {
                    "model": model,
                    "status": record.status.value,
                    "duration_seconds": round(duration, 2),
                    "input_range": f"{chunk_start} .. {chunk_end}",
                    "retries": record.retry_count,
                    "chunk": i,
                }
            )

            emit_metrics(
                "backfill_chunked.chunk_completed",
                {
                    "model": model,
                    "chunk": i,
                    "total_chunks": len(chunks),
                    "status": record.status.value,
                    "duration_seconds": round(duration, 2),
                },
            )

            if record.status == RunStatus.FAIL:
                failed = True
                console.print(f"[red]Chunk {chunk_label} failed: {record.error_message}[/red]")
                console.print(
                    f"[yellow]Resume from this point with:[/yellow]\n"
                    f"  ironlayer backfill-resume --backfill-id {backfill_id}"
                )
            else:
                completed_through = chunk_end
                console.print(f"  [green]✓[/green] Chunk {i}/{len(chunks)} completed")

        emit_metrics(
            "backfill_chunked.completed",
            {
                "model": model,
                "backfill_id": backfill_id,
                "failed": failed,
                "completed_chunks": len([r for r in run_records if r["status"] == "SUCCESS"]),
                "total_chunks": len(chunks),
            },
        )

        if get_json_output():
            result = {
                "backfill_id": backfill_id,
                "model": model,
                "status": "FAILED" if failed else "COMPLETED",
                "completed_through": completed_through.isoformat() if completed_through else None,
                "total_chunks": len(chunks),
                "completed_chunks": len([r for r in run_records if r["status"] == "SUCCESS"]),
                "runs": run_records,
            }
            sys.stdout.write(json.dumps(result, indent=2, default=str) + "\n")
        else:
            display_run_results(console, run_records)

        if failed:
            raise typer.Exit(code=3)

        console.print("[green]All chunks completed successfully.[/green]")


def backfill_resume_command(
    backfill_id: str = typer.Option(
        ...,
        "--backfill-id",
        help="Backfill identifier from a previous chunked backfill.",
    ),
    api_url: str = typer.Option(
        "http://localhost:8000",
        "--api-url",
        help="IronLayer API base URL.",
        envvar="IRONLAYER_API_URL",
    ),
) -> None:
    """Resume a previously failed chunked backfill."""
    from cli.helpers import api_request

    console.print(f"Resuming backfill [bold]{backfill_id}[/bold]...")

    result = api_request(
        "POST",
        api_url,
        f"/api/v1/backfills/{backfill_id}/resume",
    )

    emit_metrics("backfill_resume.completed", {"backfill_id": backfill_id})

    if get_json_output():
        sys.stdout.write(json.dumps(result, indent=2, default=str) + "\n")
    else:
        runs = result.get("runs", [])
        if runs:
            display_run_results(console, runs)
        else:
            console.print("[green]Backfill resumed successfully.[/green]")


def backfill_history_command(
    model: str = typer.Option(
        ...,
        "--model",
        "-m",
        help="Canonical model name to retrieve history for.",
    ),
    api_url: str = typer.Option(
        "http://localhost:8000",
        "--api-url",
        help="IronLayer API base URL.",
        envvar="IRONLAYER_API_URL",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        help="Maximum number of history entries to retrieve.",
        min=1,
        max=100,
    ),
) -> None:
    """Show backfill history for a model."""
    from datetime import datetime

    from rich.table import Table

    from cli.helpers import api_request

    console.print(f"Backfill history for [bold]{model}[/bold]")

    result = api_request(
        "GET",
        api_url,
        f"/api/v1/backfills/history/{model}",
        params={"limit": limit},
    )

    if get_json_output():
        sys.stdout.write(json.dumps(result, indent=2, default=str) + "\n")
    else:
        if not result:
            console.print("[yellow]No backfill history found.[/yellow]")
            return

        table = Table(title=f"Backfill History: {model}")
        table.add_column("Plan ID", style="dim", max_width=16)
        table.add_column("Start Date")
        table.add_column("End Date")
        table.add_column("Status")
        table.add_column("Created")

        for entry in result:
            plan_id = str(entry.get("plan_id", ""))[:16]
            start_date = entry.get("start_date", "-")
            end_date = entry.get("end_date", "-")
            status = entry.get("status", "-")
            created = entry.get("created_at", "-")
            if created and created != "-":
                try:
                    created = datetime.fromisoformat(created).strftime("%Y-%m-%d %H:%M")
                except (ValueError, TypeError):
                    pass

            status_style = "green" if status == "SUCCESS" else "red" if status == "FAIL" else "yellow"
            table.add_row(
                plan_id,
                str(start_date),
                str(end_date),
                f"[{status_style}]{status}[/{status_style}]",
                str(created),
            )

        console.print(table)
