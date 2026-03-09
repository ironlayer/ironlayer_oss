"""``ironlayer check`` — run the Rust-powered check engine against SQL models.

Validates SQL models, YAML schemas, naming conventions, ref() integrity,
and project structure using the fast parallel Rust engine via PyO3.
Falls back to a pure Python implementation if the Rust extension is
unavailable.  Human-readable output goes to stderr via Rich; JSON/SARIF
output goes to stdout in ``--json`` mode.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer
from rich.console import Console

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Rust extension availability
# ---------------------------------------------------------------------------

_RUST_AVAILABLE = False
try:
    from ironlayer_check_engine import CheckConfig as RustCheckConfig
    from ironlayer_check_engine import CheckEngine as RustCheckEngine
    from ironlayer_check_engine import quick_check as rust_quick_check

    _RUST_AVAILABLE = True
except ImportError:
    RustCheckEngine = None  # type: ignore[assignment,misc]
    RustCheckConfig = None  # type: ignore[assignment,misc]
    rust_quick_check = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Check command
# ---------------------------------------------------------------------------


def check_command(
    repo: Path = typer.Argument(
        ...,
        help="Path to the project root directory.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    config_path: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to ironlayer.check.toml config file.",
    ),
    fix: bool = typer.Option(
        False,
        "--fix",
        help="Auto-fix fixable rule violations in place.",
    ),
    changed_only: bool = typer.Option(
        False,
        "--changed-only",
        help="Only check files modified in the current git working tree.",
    ),
    no_cache: bool = typer.Option(
        False,
        "--no-cache",
        help="Disable the content-addressable check cache.",
    ),
    max_diagnostics: int | None = typer.Option(
        None,
        "--max-diagnostics",
        help="Maximum number of diagnostics to report (0 = unlimited).",
    ),
    select: str | None = typer.Option(
        None,
        "--select",
        "-s",
        help="Comma-separated rule IDs or categories to include.",
    ),
    exclude_rules: str | None = typer.Option(
        None,
        "--exclude",
        "-e",
        help="Comma-separated rule IDs or categories to exclude.",
    ),
    fail_on_warn: bool = typer.Option(
        False,
        "--fail-on-warn",
        help="Treat warnings as failures (exit code 1).",
    ),
    output_format: str = typer.Option(
        "text",
        "--format",
        "-f",
        help="Output format: text, json, or sarif.",
    ),
) -> None:
    """Run quality checks against SQL models using the Rust check engine.

    Validates SQL headers, syntax, safety, ref integrity, naming
    conventions, YAML schemas, and project structure.  Results are
    displayed with Rich formatting (text mode) or emitted as JSON/SARIF
    to stdout.

    Examples::

        ironlayer check .
        ironlayer check ./my-project --fix
        ironlayer check . --changed-only
        ironlayer check . --format json
        ironlayer check . --select HDR,SQL --fail-on-warn
    """
    from cli.state import emit_metrics, get_json_output

    # If the global --json flag is set, override format to json.
    if get_json_output():
        output_format = "json"

    if output_format not in ("text", "json", "sarif"):
        console.print(
            f"[red]Invalid format '{output_format}'. Must be one of: text, json, sarif.[/red]"
        )
        raise typer.Exit(code=3)

    start_time = time.monotonic()

    if not _RUST_AVAILABLE:
        console.print(
            "[yellow]Note: Rust check engine unavailable, "
            "using Python fallback (slower).[/yellow]"
        )
        _run_python_fallback(
            repo=repo,
            fail_on_warn=fail_on_warn,
            output_format=output_format,
        )
        return

    # Build configuration from Rust defaults, then apply CLI overrides.
    try:
        config = RustCheckConfig()
        if fix:
            config.fix = True
        if changed_only:
            config.changed_only = True
        if no_cache:
            config.no_cache = True
        if max_diagnostics is not None:
            config.max_diagnostics = max_diagnostics
        if select is not None:
            config.select = select
        if exclude_rules is not None:
            config.exclude_rules = exclude_rules
        if fail_on_warn:
            config.fail_on_warnings = True
    except Exception as exc:
        console.print(f"[red]Failed to create check config: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    # Run the Rust check engine.
    try:
        engine = RustCheckEngine(config)
        result = engine.check(str(repo))
    except Exception as exc:
        console.print(f"[red]Check engine error: {exc}[/red]")
        emit_metrics("check_error", {"error": str(exc)})
        raise typer.Exit(code=3) from exc

    elapsed_total_ms = int((time.monotonic() - start_time) * 1000)

    # Emit metrics.
    emit_metrics(
        "check_complete",
        {
            "passed": result.passed,
            "project_type": result.project_type,
            "total_files_checked": result.total_files_checked,
            "total_files_skipped_cache": result.total_files_skipped_cache,
            "total_errors": result.total_errors,
            "total_warnings": result.total_warnings,
            "total_infos": result.total_infos,
            "elapsed_ms": result.elapsed_ms,
            "elapsed_total_ms": elapsed_total_ms,
            "fix": fix,
            "changed_only": changed_only,
        },
    )

    # Output results.
    if output_format == "json":
        sys.stdout.write(result.to_json() + "\n")
    elif output_format == "sarif":
        sys.stdout.write(result.to_sarif_json() + "\n")
    else:
        from cli.display import display_check_results

        display_check_results(console, result)

    # Exit code: 0 = passed, 1 = failures found.
    if not result.passed:
        raise typer.Exit(code=1)
    if fail_on_warn and result.total_warnings > 0:
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Python fallback (no Rust extension available)
# ---------------------------------------------------------------------------


def _run_python_fallback(
    *,
    repo: Path,
    fail_on_warn: bool,
    output_format: str,
) -> None:
    """Run checks using the pure Python implementation.

    This is a limited fallback that uses existing core_engine modules
    (model_loader, ref_resolver, sql_guard) when the Rust extension
    is not available (e.g., unsupported platform).
    """
    from core_engine.loader import load_models_from_directory
    from core_engine.parser.sql_guard import check_sql_safety

    start_time = time.monotonic()

    models_dir = repo / "models"
    if not models_dir.is_dir():
        models_dir = repo

    try:
        models = load_models_from_directory(models_dir)
    except Exception as exc:
        console.print(f"[red]Failed to load models: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    if not models:
        console.print("[dim]No models found.[/dim]")
        return

    diagnostics: list[dict[str, Any]] = []

    for model in models:
        sql = model.clean_sql or model.raw_sql
        if not sql:
            continue

        # Safety checks.
        violations = check_sql_safety(sql)
        for v in violations:
            diagnostics.append(
                {
                    "rule_id": f"SAF-{v.violation_type}",
                    "message": f"{v.violation_type}: {v.detail}",
                    "severity": v.severity.lower(),
                    "file_path": model.file_path or model.name,
                    "line": 0,
                    "column": 0,
                }
            )

    elapsed_ms = int((time.monotonic() - start_time) * 1000)
    errors = sum(1 for d in diagnostics if d["severity"] == "critical")
    warnings = sum(1 for d in diagnostics if d["severity"] in ("high", "medium"))
    passed = errors == 0

    if output_format == "json":
        output = {
            "passed": passed,
            "project_type": "unknown",
            "total_files_checked": len(models),
            "total_files_skipped_cache": 0,
            "total_errors": errors,
            "total_warnings": warnings,
            "total_infos": 0,
            "elapsed_ms": elapsed_ms,
            "diagnostics": diagnostics,
        }
        sys.stdout.write(json.dumps(output, indent=2, sort_keys=True) + "\n")
    else:
        status = "[green]PASSED[/green]" if passed else "[red]FAILED[/red]"
        console.print(f"\nIronLayer Check (Python fallback) — {status}  ({elapsed_ms}ms)")
        console.print(f"  Files: {len(models)} checked")
        if diagnostics:
            for d in diagnostics:
                console.print(f"    {d['rule_id']}  {d['file_path']}  {d['message']}")
        else:
            console.print("  [green]No issues found.[/green]")
        console.print(
            f"\n-- {errors} error(s), {warnings} warning(s)  ({elapsed_ms}ms)\n"
        )

    if not passed:
        raise typer.Exit(code=1)
    if fail_on_warn and warnings > 0:
        raise typer.Exit(code=1)
