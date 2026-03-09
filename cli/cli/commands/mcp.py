"""``ironlayer mcp serve`` -- MCP server for AI assistant integration."""

from __future__ import annotations

import typer

from cli.helpers import console
from cli.state import get_json_output

mcp_app = typer.Typer(
    name="mcp",
    help="MCP (Model Context Protocol) server for AI assistant integration.",
    no_args_is_help=True,
)


@mcp_app.command("serve")
def mcp_serve_command(
    transport: str = typer.Option(
        "stdio",
        "--transport",
        "-t",
        help="Transport type: 'stdio' (default) or 'sse'.",
    ),
    port: int = typer.Option(
        3333,
        "--port",
        "-p",
        help="Port for SSE transport (ignored for stdio).",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Bind address for SSE transport. Use 0.0.0.0 for all interfaces.",
    ),
) -> None:
    """Start the IronLayer MCP server."""
    import asyncio

    try:
        from cli.mcp.server import run_sse, run_stdio
    except SystemExit as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    if transport == "stdio":
        if not get_json_output():
            console.print("[dim]Starting IronLayer MCP server (stdio)...[/dim]")
        asyncio.run(run_stdio())
    elif transport == "sse":
        console.print(f"[bold]Starting IronLayer MCP server (SSE) on {host}:{port}[/bold]")
        asyncio.run(run_sse(host=host, port=port))
    else:
        console.print(f"[red]Unknown transport '{transport}'. Use 'stdio' or 'sse'.[/red]")
        raise typer.Exit(code=1)
