"""Shared CLI helpers: console, credentials, API client, date parsing."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

console = Console(stderr=True)


def parse_date(value: str, label: str) -> date:
    """Parse a YYYY-MM-DD string into a date, raising on failure."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        console.print(f"[red]Invalid {label} date '{value}': {exc}[/red]")
        raise typer.Exit(code=3) from exc


def credentials_path() -> Path:
    """Return the path to the stored credentials file."""
    return Path.home() / ".ironlayer" / "credentials.json"


def load_stored_token() -> str | None:
    """Load the access token from secure credential storage.

    BL-105: Delegates to :func:`cli.cloud.load_full_credentials` which
    checks the OS keychain first, then falls back to the TOML config file.
    Also transparently migrates any legacy ``credentials.json`` on first call.
    Returns ``None`` on any error to avoid crashing the CLI.
    """
    from cli.cloud import load_full_credentials

    try:
        creds = load_full_credentials()
    except Exception:
        return None
    if creds is None:
        return None
    return creds.get("access_token")


def save_credentials(
    api_url: str,
    access_token: str,
    refresh_token: str,
    email: str,
) -> None:
    """Persist credentials to secure storage (OS keychain with TOML fallback).

    BL-105: Delegates to :func:`cli.cloud.save_full_credentials`.
    The legacy plaintext JSON file is no longer written.
    """
    from cli.cloud import save_full_credentials

    save_full_credentials(api_url, access_token, refresh_token, email)


def api_request(
    method: str,
    api_url: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    """Send an HTTP request to the IronLayer API and return the JSON response.

    Auth token: IRONLAYER_API_TOKEN env var, or stored credentials.
    """
    import httpx

    headers: dict[str, str] = {"Content-Type": "application/json"}
    token = os.environ.get("IRONLAYER_API_TOKEN") or load_stored_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"{api_url.rstrip('/')}{path}"
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.request(
                method,
                url,
                headers=headers,
                json=body,
                params=params,
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            detail = exc.response.json().get("detail", detail)
        except Exception:
            pass
        console.print(f"[red]API error ({exc.response.status_code}): {detail}[/red]")
        raise typer.Exit(code=3) from exc
    except httpx.ConnectError as exc:
        console.print(f"[red]Cannot connect to API at {api_url}: {exc}[/red]")
        raise typer.Exit(code=3) from exc


def load_model_sql_map(repo_path: Path) -> dict[str, str]:
    """Load all model definitions from a repo and return a {model_name: clean_sql} map."""
    from core_engine.loader import load_models_from_directory

    models_dir = repo_path / "models"
    if not models_dir.is_dir():
        models_dir = repo_path
    model_list = load_models_from_directory(models_dir)
    return {m.name: m.clean_sql for m in model_list}


def resolve_model_sql(model_name: str, sql_map: dict[str, str]) -> str:
    """Look up model SQL from the preloaded map, raising on missing models."""
    sql = sql_map.get(model_name)
    if not sql:
        available = ", ".join(sorted(sql_map.keys())[:10])
        suffix = "..." if len(sql_map) > 10 else ""
        console.print(
            f"[red]Model '{model_name}' not found in repo. Available models: {available}{suffix}[/red]"
        )
        raise typer.Exit(code=3)
    return sql


def format_input_range(input_range: object) -> str:
    """Format an optional DateRange as a human-readable string."""
    if input_range is None:
        return "-"
    start = getattr(input_range, "start", None)
    end = getattr(input_range, "end", None)
    if start is not None and end is not None:
        return f"{start} .. {end}"
    return "-"
