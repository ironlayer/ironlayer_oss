"""IronLayer CLI application -- Typer-based developer interface.

Provides commands for plan generation, inspection, execution, backfill,
model listing, and lineage traversal.  Human-readable output goes to
*stderr* via Rich; machine-readable artefacts (plan JSON, metrics) go to
files on disk so that pipelines can compose cleanly.
"""

from __future__ import annotations

from pathlib import Path

import typer

from cli.state import set_global_options
from core_engine.config import PlatformEnv

# ---------------------------------------------------------------------------
# App & sub-apps
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="ironlayer",
    help="IronLayer - AI-Native Databricks Transformation Control Plane",
    no_args_is_help=True,
)

from cli.commands.migrate import migrate_app
from cli.commands.mcp import mcp_app

app.add_typer(migrate_app, name="migrate")
app.add_typer(mcp_app, name="mcp")

# ---------------------------------------------------------------------------
# Register commands
# ---------------------------------------------------------------------------

from cli.commands.apply import apply_command
from cli.commands.auth import login_command, logout_command, whoami_command
from cli.commands.check import check_command
from cli.commands.backfill import (
    backfill_chunked_command,
    backfill_command,
    backfill_history_command,
    backfill_resume_command,
)
from cli.commands.dev import dev_command
from cli.commands.init import init_command
from cli.commands.lineage import lineage_command
from cli.commands.models import models_command
from cli.commands.plan import plan_command
from cli.commands.show import show_command

app.command(name="init")(init_command)
app.command(name="check")(check_command)
app.command(name="dev")(dev_command)
app.command(name="plan")(plan_command)
app.command(name="show")(show_command)
app.command(name="apply")(apply_command)
app.command(name="backfill")(backfill_command)
app.command("backfill-chunked")(backfill_chunked_command)
app.command("backfill-resume")(backfill_resume_command)
app.command("backfill-history")(backfill_history_command)
app.command(name="models")(models_command)
app.command(name="lineage")(lineage_command)
app.command(name="login")(login_command)
app.command(name="logout")(logout_command)
app.command(name="whoami")(whoami_command)


# ---------------------------------------------------------------------------
# Global options callback
# ---------------------------------------------------------------------------


@app.callback()
def _global_options(
    json_mode: bool = typer.Option(
        False,
        "--json/--no-json",
        help="Emit structured JSON to stdout instead of human-readable output.",
    ),
    metrics_file: Path | None = typer.Option(
        None,
        "--metrics-file",
        help="Write metrics events to this file (JSONL).",
        envvar="PLATFORM_METRICS_FILE",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        help="Environment override (dev | staging | prod). 'production' is accepted and normalized to prod.",
        envvar="PLATFORM_ENV",
    ),
) -> None:
    """Global options applied to every command."""
    # Normalize and validate env using core PlatformEnv (accepts "production" -> prod).
    norm = "prod" if env and env.strip().lower() == "production" else (env or "dev").strip().lower()
    try:
        platform_env = PlatformEnv(norm)
    except ValueError:
        raise typer.BadParameter(
            f"Invalid env '{env}'. Use dev, staging, or prod (or production)."
        ) from None
    set_global_options(json_output=json_mode, metrics_file=metrics_file, env=platform_env.value)
