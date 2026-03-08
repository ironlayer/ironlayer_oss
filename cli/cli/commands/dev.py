"""``platform dev`` -- local development server.

Starts the full IronLayer stack locally with zero external dependencies:

  * SQLite for metadata storage (no PostgreSQL required)
  * DuckDB for SQL execution (no Databricks required)
  * Embedded FastAPI API server via uvicorn
  * Optional AI engine on a separate port
  * Optional frontend dev server (if ``frontend/`` exists)

The developer gets plan/apply/UI working against example models with
a single command and no Docker.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

logger = logging.getLogger(__name__)


def dev_command(
    port: int = typer.Option(
        8000,
        "--port",
        "-p",
        help="API server port.",
    ),
    ai_port: int = typer.Option(
        8001,
        "--ai-port",
        help="AI engine port (if AI is enabled).",
    ),
    no_ai: bool = typer.Option(
        False,
        "--no-ai",
        help="Disable the AI advisory engine.",
    ),
    no_ui: bool = typer.Option(
        False,
        "--no-ui",
        help="Skip starting the frontend dev server.",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Host to bind the API server to.",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        help="Enable auto-reload on code changes.",
    ),
) -> None:
    """Start a local development server with zero external dependencies."""
    console = Console(stderr=True)

    # Detect project configuration.
    project_root = Path.cwd()
    ironlayer_config = project_root / ".ironlayer" / "config.yaml"
    env_file = project_root / ".env"

    if not ironlayer_config.exists() and not env_file.exists():
        console.print(
            "[red]No IronLayer project found in the current directory.[/red]\n"
            "[dim]Run [bold]platform init[/bold] first to scaffold a project.[/dim]"
        )
        raise typer.Exit(code=3)

    # Set up environment for local mode.
    _setup_local_env(project_root, port, ai_port, no_ai)

    # Build the services table.
    services = _build_services_table(host, port, ai_port, no_ai, no_ui, project_root)
    console.print(
        Panel(
            services,
            title="IronLayer Dev Server",
            border_style="blue",
        )
    )

    console.print("[dim]Press Ctrl+C to stop all services.[/dim]\n")

    # Start services.
    _shutdown = threading.Event()

    def _handle_sigint(signum: int, frame: object) -> None:
        console.print("\n[yellow]Shutting down...[/yellow]")
        _shutdown.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        import uvicorn

        uvicorn_config = uvicorn.Config(
            "api.main:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info",
            access_log=False,
        )
        server = uvicorn.Server(uvicorn_config)

        console.print(f"[green]\u2713[/green] API server starting on http://{host}:{port}")
        console.print(f"[green]\u2713[/green] OpenAPI docs at http://{host}:{port}/docs")
        console.print(f"[green]\u2713[/green] Readiness probe at http://{host}:{port}/ready")

        # Run the API server (blocking).
        server.run()

    except ImportError as exc:
        console.print(f"[red]Missing dependency: {exc}[/red]")
        console.print("[dim]Install the API package: pip install -e api/[/dim]")
        raise typer.Exit(code=3) from exc
    except KeyboardInterrupt:
        console.print("[yellow]Dev server stopped.[/yellow]")
    except Exception as exc:
        console.print(f"[red]Server error: {exc}[/red]")
        raise typer.Exit(code=3) from exc

    console.print("[green]Dev server stopped cleanly.[/green]")


def _setup_local_env(
    project_root: Path,
    port: int,
    ai_port: int,
    no_ai: bool,
) -> None:
    """Configure environment variables for local-only operation.

    Sets SQLite as the state store and overrides database URLs so that
    the API server runs without PostgreSQL.
    """
    state_db = project_root / ".ironlayer" / "state.db"
    state_db.parent.mkdir(parents=True, exist_ok=True)

    # Force local SQLite mode.
    os.environ.setdefault("PLATFORM_STATE_STORE_TYPE", "local")
    os.environ["PLATFORM_DATABASE_URL"] = f"sqlite+aiosqlite:///{state_db}"

    # Set dev environment.
    os.environ.setdefault("PLATFORM_ENV", "dev")

    # API configuration — port is always set explicitly from the CLI arg.
    os.environ.setdefault("API_HOST", "127.0.0.1")
    os.environ["API_PORT"] = str(port)
    os.environ.setdefault("API_CORS_ORIGINS", '["http://localhost:3000"]')

    # Disable auth in dev mode for local convenience.
    os.environ.setdefault("API_AUTH_MODE", "dev")

    # JWT secret (deterministic for local dev -- not for production!).
    os.environ.setdefault(
        "API_JWT_SECRET",
        "ironlayer-local-dev-secret-not-for-production",
    )

    # Disable rate limiting in local dev.
    os.environ.setdefault("API_RATE_LIMIT_ENABLED", "false")

    # AI engine.
    if no_ai:
        os.environ["AI_ENGINE_URL"] = ""
        os.environ["AI_LLM_ENABLED"] = "false"
    else:
        os.environ.setdefault("AI_ENGINE_URL", f"http://127.0.0.1:{ai_port}")

    # DuckDB for local execution.
    duckdb_path = project_root / ".ironlayer" / "local.duckdb"
    os.environ.setdefault("PLATFORM_LOCAL_DB_PATH", str(duckdb_path))

    # Load .env file if it exists (lower priority than explicit overrides).
    env_file = project_root / ".env"
    if env_file.exists():
        _load_dotenv(env_file)


def _load_dotenv(env_file: Path) -> None:
    """Load environment variables from a .env file.

    Only sets variables that are not already present in ``os.environ``
    (existing values take precedence).
    """
    try:
        content = env_file.read_text(encoding="utf-8")
    except OSError:
        return

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


def _build_services_table(
    host: str,
    port: int,
    ai_port: int,
    no_ai: bool,
    no_ui: bool,
    project_root: Path,
) -> Table:
    """Build a Rich table showing the services that will be started."""
    table = Table(show_header=True, header_style="bold", expand=False)
    table.add_column("Service", style="bold")
    table.add_column("URL")
    table.add_column("Status")

    table.add_row(
        "API Server",
        f"http://{host}:{port}",
        "[green]starting[/green]",
    )

    state_db = project_root / ".ironlayer" / "state.db"
    table.add_row(
        "Database",
        f"SQLite ({state_db.name})",
        "[green]local[/green]",
    )

    duckdb_path = project_root / ".ironlayer" / "local.duckdb"
    table.add_row(
        "Executor",
        f"DuckDB ({duckdb_path.name})",
        "[green]local[/green]",
    )

    if no_ai:
        table.add_row("AI Engine", "-", "[dim]disabled[/dim]")
    else:
        table.add_row(
            "AI Engine",
            f"http://{host}:{ai_port}",
            "[yellow]optional[/yellow]",
        )

    if no_ui:
        table.add_row("Frontend", "-", "[dim]disabled[/dim]")
    else:
        frontend_dir = project_root / "frontend"
        if frontend_dir.is_dir():
            table.add_row(
                "Frontend",
                "http://localhost:3000",
                "[yellow]manual start[/yellow]",
            )
        else:
            table.add_row("Frontend", "-", "[dim]not found[/dim]")

    return table
