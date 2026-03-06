"""Rich output formatting for the IronLayer CLI.

All functions write to a :class:`rich.console.Console` instance (typically
bound to *stderr*) so that machine-readable output on *stdout* is never
polluted with human-readable decoration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

if TYPE_CHECKING:
    from core_engine.models.model_definition import ModelDefinition
    from core_engine.models.plan import Plan
    from core_engine.sql_toolkit._types import (
        ColumnLineageResult,
        CrossModelColumnLineage,
    )


# ---------------------------------------------------------------------------
# Status colour mapping
# ---------------------------------------------------------------------------

_STATUS_COLOURS: dict[str, str] = {
    "SUCCESS": "green",
    "FAIL": "red",
    "RUNNING": "yellow",
    "PENDING": "dim",
    "CANCELLED": "dim red",
}


def _coloured_status(status: str) -> str:
    """Return a Rich markup string with the status colour-coded."""
    colour = _STATUS_COLOURS.get(status, "white")
    return f"[{colour}]{status}[/{colour}]"


# ---------------------------------------------------------------------------
# Plan summary
# ---------------------------------------------------------------------------


def display_plan_summary(console: Console, plan: Plan) -> None:
    """Render a plan overview with a step-by-step table.

    Parameters
    ----------
    console:
        Rich console to write to (typically stderr).
    plan:
        The execution plan to display.
    """
    # Header panel.
    header_lines = [
        f"[bold]Plan ID:[/bold]  {plan.plan_id[:16]}...",
        f"[bold]Base:[/bold]     {plan.base[:12] if plan.base else '(none)'}",
        f"[bold]Target:[/bold]   {plan.target[:12] if plan.target else '(none)'}",
        f"[bold]Steps:[/bold]    {plan.summary.total_steps}",
        f"[bold]Est. Cost:[/bold] ${plan.summary.estimated_cost_usd:.4f}",
    ]
    console.print(
        Panel(
            "\n".join(header_lines),
            title="Execution Plan",
            border_style="blue",
        )
    )

    if not plan.steps:
        console.print("[dim]No steps in this plan.[/dim]")
        return

    # Step table.
    table = Table(
        title="Plan Steps",
        show_lines=False,
        pad_edge=True,
        expand=False,
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Model", style="bold")
    table.add_column("Run Type")
    table.add_column("Input Range")
    table.add_column("Group", justify="center")
    table.add_column("Reason")
    table.add_column("Est. Cost", justify="right")

    for idx, step in enumerate(plan.steps, start=1):
        input_range_str = f"{step.input_range.start} .. {step.input_range.end}" if step.input_range else "-"
        run_type_style = "cyan" if step.run_type.value == "INCREMENTAL" else "magenta"

        table.add_row(
            str(idx),
            step.model,
            f"[{run_type_style}]{step.run_type.value}[/{run_type_style}]",
            input_range_str,
            str(step.parallel_group),
            step.reason,
            f"${step.estimated_cost_usd:.4f}",
        )

    console.print(table)

    # Contract violations section.
    if plan.summary.contract_violations_count > 0:
        console.print()  # blank line before violations section
        violations_table = Table(
            title="Schema Contract Violations",
            show_lines=False,
            pad_edge=True,
            expand=False,
        )
        violations_table.add_column("Model", style="bold")
        violations_table.add_column("Column")
        violations_table.add_column("Violation")
        violations_table.add_column("Severity")
        violations_table.add_column("Expected")
        violations_table.add_column("Actual")

        for step in plan.steps:
            for v in step.contract_violations:
                severity = v.get("severity", "INFO")
                if severity == "BREAKING":
                    sev_style = "red bold"
                elif severity == "WARNING":
                    sev_style = "yellow"
                else:
                    sev_style = "dim"

                violations_table.add_row(
                    step.model,
                    v.get("column_name", "?"),
                    v.get("violation_type", "?"),
                    f"[{sev_style}]{severity}[/{sev_style}]",
                    v.get("expected", "-"),
                    v.get("actual", "-"),
                )

        console.print(violations_table)

        breaking = plan.summary.breaking_contract_violations
        if breaking > 0:
            console.print(
                f"[red bold]{breaking} BREAKING contract violation(s) detected.[/red bold] "
                f"Models with STRICT contracts will block plan apply."
            )

    # Summary footer.
    summary_parts = [
        f"[bold]{plan.summary.total_steps}[/bold] step(s)",
        f"estimated total cost [bold]${plan.summary.estimated_cost_usd:.4f}[/bold]",
    ]
    if plan.summary.contract_violations_count > 0:
        summary_parts.append(f"[red]{plan.summary.contract_violations_count} contract violation(s)[/red]")
    console.print("\n" + ", ".join(summary_parts))


# ---------------------------------------------------------------------------
# Run results
# ---------------------------------------------------------------------------


def display_run_results(console: Console, runs: list[dict]) -> None:
    """Render an execution results table.

    Parameters
    ----------
    console:
        Rich console to write to.
    runs:
        List of run result dicts with keys: model, status, duration_seconds,
        input_range, retries.
    """
    if not runs:
        console.print("[dim]No runs to display.[/dim]")
        return

    table = Table(
        title="Execution Results",
        show_lines=False,
        pad_edge=True,
        expand=False,
    )
    table.add_column("Model", style="bold")
    table.add_column("Status")
    table.add_column("Duration", justify="right")
    table.add_column("Input Range")
    table.add_column("Retries", justify="center")

    succeeded = 0
    failed = 0

    for run in runs:
        status = run.get("status", "PENDING")
        duration = run.get("duration_seconds", 0.0)
        retries = run.get("retries", 0)

        if status == "SUCCESS":
            succeeded += 1
        elif status == "FAIL":
            failed += 1

        table.add_row(
            run.get("model", "?"),
            _coloured_status(status),
            f"{duration:.2f}s",
            run.get("input_range", "-"),
            str(retries),
        )

    console.print(table)

    # Summary line.
    total = len(runs)
    parts: list[str] = [f"[bold]{total}[/bold] step(s)"]
    if succeeded:
        parts.append(f"[green]{succeeded} succeeded[/green]")
    if failed:
        parts.append(f"[red]{failed} failed[/red]")
    cancelled = total - succeeded - failed
    if cancelled > 0:
        parts.append(f"[dim]{cancelled} cancelled[/dim]")

    console.print(" | ".join(parts))


# ---------------------------------------------------------------------------
# Model list
# ---------------------------------------------------------------------------


def display_model_list(console: Console, models: list[ModelDefinition]) -> None:
    """Render a table of all discovered models.

    Parameters
    ----------
    console:
        Rich console to write to.
    models:
        List of :class:`ModelDefinition` objects.
    """
    if not models:
        console.print("[dim]No models found.[/dim]")
        return

    table = Table(
        title=f"Models ({len(models)})",
        show_lines=False,
        pad_edge=True,
        expand=False,
    )
    table.add_column("Name", style="bold")
    table.add_column("Kind")
    table.add_column("Materialization")
    table.add_column("Time Column")
    table.add_column("Owner")
    table.add_column("Tags")

    for m in models:
        kind_style = "cyan" if "INCREMENTAL" in m.kind.value else "magenta"
        tags_str = ", ".join(m.tags) if m.tags else "-"

        table.add_row(
            m.name,
            f"[{kind_style}]{m.kind.value}[/{kind_style}]",
            m.materialization.value,
            m.time_column or "-",
            m.owner or "-",
            tags_str,
        )

    console.print(table)


# ---------------------------------------------------------------------------
# Lineage tree
# ---------------------------------------------------------------------------


def display_lineage(
    console: Console,
    model_name: str,
    upstream: list[str],
    downstream: list[str],
) -> None:
    """Render a lineage tree showing upstream and downstream models.

    Parameters
    ----------
    console:
        Rich console to write to.
    model_name:
        The focal model name.
    upstream:
        Sorted list of upstream model names.
    downstream:
        Sorted list of downstream model names.
    """
    tree = Tree(f"[bold yellow]{model_name}[/bold yellow]", guide_style="dim")

    # Upstream branch.
    if upstream:
        upstream_branch = tree.add("[bold blue]upstream[/bold blue]")
        for name in upstream:
            upstream_branch.add(f"[blue]{name}[/blue]")
    else:
        tree.add("[dim]no upstream dependencies[/dim]")

    # Downstream branch.
    if downstream:
        downstream_branch = tree.add("[bold green]downstream[/bold green]")
        for name in downstream:
            downstream_branch.add(f"[green]{name}[/green]")
    else:
        tree.add("[dim]no downstream dependents[/dim]")

    console.print(Panel(tree, title="Lineage", border_style="yellow"))

    console.print(f"[bold]{len(upstream)}[/bold] upstream, [bold]{len(downstream)}[/bold] downstream")


# ---------------------------------------------------------------------------
# Migration report
# ---------------------------------------------------------------------------


def display_migration_report(
    console: Console,
    migrated: list[dict[str, str]],
    skipped: list[dict[str, str]],
    warnings: list[str],
) -> None:
    """Display a summary of a migration operation.

    Parameters
    ----------
    console:
        Rich console to write to (typically stderr).
    migrated:
        List of dicts with keys ``name``, ``source``, ``output``, ``status``
        for each successfully migrated model.
    skipped:
        List of dicts with keys ``name``, ``source``, ``reason`` for each
        model that was skipped.
    warnings:
        Free-form warning messages to display after the table.
    """
    total = len(migrated) + len(skipped)

    if total == 0:
        console.print("[dim]No models found to migrate.[/dim]")
        return

    # Build main table combining migrated and skipped entries.
    table = Table(
        title=f"Migration Report ({total} model(s))",
        show_lines=False,
        pad_edge=True,
        expand=False,
    )
    table.add_column("Model Name", style="bold")
    table.add_column("Source Path")
    table.add_column("Output Path")
    table.add_column("Status")

    for entry in migrated:
        table.add_row(
            entry.get("name", "?"),
            entry.get("source", "-"),
            entry.get("output", "-"),
            f"[green]{entry.get('status', 'migrated')}[/green]",
        )

    for entry in skipped:
        table.add_row(
            entry.get("name", "?"),
            entry.get("source", "-"),
            "-",
            f"[dim]{entry.get('reason', 'skipped')}[/dim]",
        )

    console.print(table)

    # Summary line.
    parts: list[str] = [f"[bold]{total}[/bold] model(s) found"]
    if migrated:
        parts.append(f"[green]{len(migrated)} migrated[/green]")
    if skipped:
        parts.append(f"[dim]{len(skipped)} skipped[/dim]")

    console.print(" | ".join(parts))

    # Warnings section.
    if warnings:
        console.print()
        console.print("[yellow bold]Warnings:[/yellow bold]")
        for warning in warnings:
            console.print(f"  [yellow]- {warning}[/yellow]")


# ---------------------------------------------------------------------------
# Column-level lineage
# ---------------------------------------------------------------------------

_TRANSFORM_STYLES: dict[str, str] = {
    "direct": "green",
    "expression": "cyan",
    "aggregation": "magenta",
    "window": "magenta",
    "case": "yellow",
    "literal": "dim",
}


def display_column_lineage(
    console: Console,
    model_name: str,
    column_lineage: ColumnLineageResult,
) -> None:
    """Render column-level lineage for a single model.

    Shows each output column and the source columns it derives from,
    including the transformation type (direct, expression, aggregation,
    window, case, literal).

    Parameters
    ----------
    console:
        Rich console to write to (typically stderr).
    model_name:
        The focal model name.
    column_lineage:
        The :class:`ColumnLineageResult` to display.
    """
    tree = Tree(
        f"[bold yellow]{model_name}[/bold yellow]  column lineage",
        guide_style="dim",
    )

    if not column_lineage.column_lineage:
        tree.add("[dim]no column lineage computed[/dim]")
        console.print(Panel(tree, title="Column Lineage", border_style="yellow"))
        return

    for col_name, nodes in sorted(column_lineage.column_lineage.items()):
        col_branch = tree.add(f"[bold white]{col_name}[/bold white]")

        for node in nodes:
            transform_style = _TRANSFORM_STYLES.get(node.transform_type, "white")
            transform_label = f"[{transform_style}]{node.transform_type}[/{transform_style}]"

            if node.source_table and node.source_column:
                source_label = f"[blue]{node.source_table}[/blue].[cyan]{node.source_column}[/cyan]"
            elif node.source_column:
                source_label = f"[cyan]{node.source_column}[/cyan]"
            elif node.source_table:
                source_label = f"[blue]{node.source_table}[/blue]"
            else:
                source_label = "[dim]literal/unknown[/dim]"

            col_branch.add(f"{transform_label} ← {source_label}")

    console.print(Panel(tree, title="Column Lineage", border_style="yellow"))

    # Summary.
    total_cols = len(column_lineage.column_lineage)
    unresolved = len(column_lineage.unresolved_columns)
    parts = [f"[bold]{total_cols}[/bold] column(s) traced"]
    if unresolved:
        parts.append(f"[yellow]{unresolved} unresolved[/yellow]")
    console.print(", ".join(parts))

    if column_lineage.unresolved_columns:
        console.print(f"[dim]Unresolved: {', '.join(column_lineage.unresolved_columns)}[/dim]")


def display_cross_model_column_lineage(
    console: Console,
    result: CrossModelColumnLineage,
) -> None:
    """Render cross-model column lineage for a single column.

    Shows the full trace from target model/column back through
    upstream models to source tables.

    Parameters
    ----------
    console:
        Rich console to write to (typically stderr).
    result:
        The :class:`CrossModelColumnLineage` to display.
    """
    tree = Tree(
        f"[bold yellow]{result.target_model}[/bold yellow].[bold white]{result.target_column}[/bold white]",
        guide_style="dim",
    )

    if not result.lineage_path:
        tree.add("[dim]no lineage path found[/dim]")
        console.print(Panel(tree, title="Cross-Model Column Lineage", border_style="yellow"))
        return

    # Group lineage nodes by source table for cleaner display.
    by_table: dict[str | None, list] = {}
    for node in result.lineage_path:
        by_table.setdefault(node.source_table, []).append(node)

    for table, nodes in by_table.items():
        table_label = f"[blue]{table}[/blue]" if table else "[dim]unknown source[/dim]"
        table_branch = tree.add(table_label)

        for node in nodes:
            transform_style = _TRANSFORM_STYLES.get(node.transform_type, "white")
            col_label = f"[cyan]{node.source_column}[/cyan]" if node.source_column else "[dim]?[/dim]"
            transform_label = f"[{transform_style}]{node.transform_type}[/{transform_style}]"
            table_branch.add(f"{col_label}  ({transform_label})")

    console.print(Panel(tree, title="Cross-Model Column Lineage", border_style="yellow"))
    console.print(f"[bold]{len(result.lineage_path)}[/bold] lineage hop(s) traced")


# ---------------------------------------------------------------------------
# Check engine results
# ---------------------------------------------------------------------------

_SEVERITY_ICONS: dict[str, str] = {
    "error": "\u2717",
    "warning": "\u26a0",
    "info": "\u2139",
}

_SEVERITY_COLOURS: dict[str, str] = {
    "error": "red",
    "warning": "yellow",
    "info": "dim",
}


def display_check_results(console: Console, result: Any) -> None:
    """Render check engine results with Rich formatting.

    Parameters
    ----------
    console:
        Rich console to write to (typically stderr).
    result:
        A ``CheckResult`` object from the Rust check engine (PyO3),
        with attributes: ``passed``, ``elapsed_ms``, ``project_type``,
        ``total_files_checked``, ``total_files_skipped_cache``,
        ``total_errors``, ``total_warnings``, ``total_infos``,
        ``diagnostics`` (list of ``CheckDiagnostic``).
    """
    status = "[green]PASSED[/green]" if result.passed else "[red]FAILED[/red]"
    console.print(f"\n\u26a1 IronLayer Check \u2014 {status}  ({result.elapsed_ms}ms)\n")

    console.print(
        f"  Files: {result.total_files_checked} checked, "
        f"{result.total_files_skipped_cache} cached  "
        f"({result.project_type} project)"
    )

    if not result.diagnostics:
        console.print("\n  [green]\u2713 No issues found.[/green]\n")
        return

    by_file: dict[str, list[Any]] = {}
    for d in result.diagnostics:
        by_file.setdefault(d.file_path, []).append(d)

    for file_path, diags in sorted(by_file.items()):
        console.print(f"\n  [bold]{file_path}[/bold]")
        for d in diags:
            severity_str = str(d.severity).lower()
            if "." in severity_str:
                severity_str = severity_str.rsplit(".", maxsplit=1)[-1].lower()
            icon = _SEVERITY_ICONS.get(severity_str, "?")
            colour = _SEVERITY_COLOURS.get(severity_str, "white")
            loc = f":{d.line}" if d.line > 0 else ""
            col = f":{d.column}" if d.column > 0 else ""
            console.print(
                f"    [{colour}]{icon} {d.rule_id}[/{colour}] "
                f"[dim]{file_path}{loc}{col}[/dim]  {d.message}"
            )
            if d.suggestion:
                console.print(f"      [dim]\u2192 {d.suggestion}[/dim]")

    console.print(
        f"\n\u2500\u2500 {result.total_errors} error(s), "
        f"{result.total_warnings} warning(s), "
        f"{result.total_infos} info(s)  "
        f"({result.elapsed_ms}ms)\n"
    )
