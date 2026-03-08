"""MCP server creation and transport handlers.

Provides two transport options:
- **stdio** — standard input/output for local tool use (Claude Code, Cursor).
- **SSE** — Server-Sent Events over HTTP for remote access.

Usage::

    # stdio (default — for Claude Code / Cursor config):
    ironlayer mcp serve

    # SSE (for remote access):
    ironlayer mcp serve --transport sse --port 3333
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _ensure_mcp_installed() -> None:
    """Raise a helpful error if the ``mcp`` extra is not installed."""
    try:
        import mcp  # noqa: F401
    except ImportError:
        raise SystemExit(
            "The 'mcp' extra is required for MCP server support.\nInstall it with: pip install ironlayer[mcp]"
        )


def create_server() -> Any:
    """Create and configure the MCP server with all IronLayer tools.

    Returns
    -------
    mcp.server.Server
        A configured MCP server ready to run on any transport.
    """
    _ensure_mcp_installed()

    from mcp.server import Server
    from mcp.types import TextContent, Tool

    from cli.mcp.tools import TOOL_DEFINITIONS, TOOL_DISPATCH

    server = Server("ironlayer")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        """Return the list of available IronLayer tools."""
        return [
            Tool(
                name=defn["name"],
                description=defn["description"],
                inputSchema=defn["inputSchema"],
            )
            for defn in TOOL_DEFINITIONS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Dispatch a tool call to the appropriate handler."""
        handler = TOOL_DISPATCH.get(name)
        if handler is None:
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": f"Unknown tool: {name}"}),
                )
            ]

        try:
            result = await handler(**arguments)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as exc:
            logger.error("Tool '%s' failed: %s", name, exc, exc_info=True)
            result = {"error": f"{type(exc).__name__}: {exc}"}

        return [
            TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str),
            )
        ]

    return server


async def run_stdio() -> None:
    """Run the MCP server on stdio transport.

    This is the primary transport for local tool use with Claude Code
    and Cursor.  The server reads JSON-RPC messages from stdin and
    writes responses to stdout.
    """
    _ensure_mcp_installed()

    from mcp.server.stdio import stdio_server

    server = create_server()

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


async def run_sse(host: str = "127.0.0.1", port: int = 3333) -> None:
    """Run the MCP server on SSE (Server-Sent Events) transport.

    This transport exposes the server over HTTP for remote access.
    Useful for self-hosting or testing from browser-based tools.

    Parameters
    ----------
    host:
        Bind address.  Default ``127.0.0.1`` (localhost only).
        Use ``0.0.0.0`` to listen on all interfaces (requires
        explicit ``--host 0.0.0.0``).
    port:
        HTTP port.  Default ``3333``.
    """
    _ensure_mcp_installed()

    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route

    import uvicorn

    server = create_server()
    sse_transport = SseServerTransport("/messages/")

    async def handle_sse(request: Any) -> Any:
        async with sse_transport.connect_sse(request.scope, request.receive, request._send) as streams:
            await server.run(
                streams[0],
                streams[1],
                server.create_initialization_options(),
            )

    app = Starlette(
        debug=False,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse_transport.handle_post_message),
        ],
    )

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    uv_server = uvicorn.Server(config)
    await uv_server.serve()
