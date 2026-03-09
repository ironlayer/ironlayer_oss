"""``ironlayer login``, ``logout``, ``whoami`` -- authentication."""

from __future__ import annotations

import json

import typer

from cli.helpers import console, credentials_path


def login_command(
    api_url: str = typer.Option(
        ...,
        "--api-url",
        help="IronLayer API base URL (e.g. https://api.ironlayer.app).",
        envvar="IRONLAYER_API_URL",
        prompt="IronLayer API URL",
    ),
    email: str = typer.Option(
        ...,
        "--email",
        help="Account email address.",
        prompt="Email",
    ),
    password: str = typer.Option(
        ...,
        "--password",
        help="Account password.",
        prompt="Password",
        hide_input=True,
    ),
) -> None:
    """Authenticate with a IronLayer API server and store credentials locally."""
    import httpx

    from cli.cloud import save_full_credentials

    login_url = f"{api_url.rstrip('/')}/api/v1/auth/login"
    console.print(f"[dim]Authenticating with {api_url} …[/dim]")

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                login_url,
                json={"email": email, "password": password},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            detail = exc.response.json().get("detail", detail)
        except (json.JSONDecodeError, ValueError, KeyError):
            pass
        console.print(f"[red]Login failed ({exc.response.status_code}): {detail}[/red]")
        raise typer.Exit(code=1) from exc
    except httpx.ConnectError as exc:
        console.print(
            f"[red]Could not connect to {api_url}. Check the URL and ensure the API is running.[/red]"
        )
        raise typer.Exit(code=1) from exc

    access_token = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")
    user = data.get("user", {})

    if not access_token:
        console.print("[red]Login response did not include an access token.[/red]")
        raise typer.Exit(code=1)

    # BL-105: Store credentials via OS keychain (with TOML fallback).
    save_full_credentials(api_url, access_token, refresh_token, email)

    console.print(f"[green]✓ Logged in as {user.get('display_name', email)}[/green]")
    console.print(f"[dim]  Tenant:      {data.get('tenant_id', 'unknown')}[/dim]")
    console.print(f"[dim]  Role:        {user.get('role', 'unknown')}[/dim]")
    console.print(f"[dim]  Credentials: stored in OS keychain[/dim]")


def logout_command() -> None:
    """Remove stored credentials."""
    from cli.cloud import delete_full_credentials

    # BL-105: Remove from keychain + TOML file (also removes any legacy JSON).
    delete_full_credentials()
    console.print("[green]✓ Logged out — credentials removed.[/green]")


def whoami_command() -> None:
    """Show the currently authenticated user."""
    import httpx

    from cli.cloud import load_full_credentials

    # BL-105: Load from OS keychain / TOML (migrates legacy JSON automatically).
    creds = load_full_credentials()
    if creds is None:
        console.print("[yellow]Not logged in. Run [bold]ironlayer login[/bold] first.[/yellow]")
        raise typer.Exit(code=1)

    api_url = creds.get("api_url", "")
    token = creds.get("access_token", "")
    stored_email = creds.get("email", "unknown")

    if not api_url or not token:
        console.print("[yellow]Credentials incomplete. Run [bold]ironlayer login[/bold] again.[/yellow]")
        raise typer.Exit(code=1)

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{api_url.rstrip('/')}/api/v1/auth/me",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            user = resp.json()
    except Exception:
        console.print(f"[dim]API URL:  {api_url}[/dim]")
        console.print(f"[dim]Email:   {stored_email}[/dim]")
        console.print("[yellow]Could not reach API — showing cached info only.[/yellow]")
        return

    console.print(f"[green]✓ {user.get('display_name', stored_email)}[/green]")
    console.print(f"[dim]  Email:   {user.get('email', stored_email)}[/dim]")
    console.print(f"[dim]  Tenant:  {user.get('tenant_id', 'unknown')}[/dim]")
    console.print(f"[dim]  Role:    {user.get('role', 'unknown')}[/dim]")
    console.print(f"[dim]  API URL: {api_url}[/dim]")
