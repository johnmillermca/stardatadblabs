"""
rbacctl — command-line interface for the RBAC Control Plane.

Usage:
  rbacctl user create alice
  rbacctl user bind alice analyst
  rbacctl user bind alice etl_writer --service kafka
  rbacctl user roles alice
  rbacctl sync --user alice
  rbacctl sync --dry-run
  rbacctl role list
  rbacctl role create myrole --display-name "My Role" --perm doris:SELECT --perm kafka:CONSUME
  rbacctl role add-perm <role_id> doris:SELECT
  rbacctl role remove-perm <role_id> doris:SELECT
  rbacctl role add-members data_engineer alice bob carol
  rbacctl role add-members platform_admin bob --service spark --expires-days 30
  rbacctl audit --limit 20

Authentication:
  Set RBAC_TOKEN env var to your raw API token (or master token).
  Set RBAC_URL  env var to the API base URL (default: http://localhost:8080).
"""
from __future__ import annotations

import json
import os
import sys
from typing import Optional

import httpx
import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="rbacctl",
    help="RBAC Control Plane — manage users, roles and service access",
    add_completion=True,
)
user_app  = typer.Typer(help="Manage platform users")
role_app  = typer.Typer(help="Manage roles and permissions")
sync_app  = typer.Typer(help="Push RBAC state to services")
audit_app = typer.Typer(help="Query audit log")
token_app = typer.Typer(help="Manage API tokens")

app.add_typer(user_app,  name="user")
app.add_typer(role_app,  name="role")
app.add_typer(sync_app,  name="sync")
app.add_typer(audit_app, name="audit")
app.add_typer(token_app, name="token")

console = Console()


# ── HTTP client ────────────────────────────────────────────

def _base_url() -> str:
    return os.environ.get("RBAC_URL", "http://localhost:8080").rstrip("/")


def _token() -> str:
    t = os.environ.get("RBAC_TOKEN", "")
    if not t:
        rprint("[red]Error: RBAC_TOKEN environment variable not set.[/red]")
        rprint("  export RBAC_TOKEN=<your-api-token>")
        raise SystemExit(1)
    return t


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=_base_url(),
        headers={"Authorization": f"Bearer {_token()}"},
        timeout=30.0,
    )


def _check(resp: httpx.Response, ok=(200, 201)) -> dict:
    if resp.status_code not in ok:
        try:
            body = resp.json()
            detail = body.get("detail", resp.text)
        except Exception:
            detail = resp.text
        rprint(f"[red]Error {resp.status_code}:[/red] {detail}")
        raise SystemExit(1)
    return resp.json()


def _print_table(title: str, rows: list[dict], columns: list[str]) -> None:
    table = Table(title=title, show_header=True, header_style="bold cyan")
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(row.get(c, "")) for c in columns])
    console.print(table)


# ── user commands ──────────────────────────────────────────

@user_app.command("list")
def user_list(q: str = typer.Option("", "--filter", "-f", help="Filter by username")):
    """List all users."""
    with _client() as c:
        resp = _check(c.get("/api/v1/users", params={"q": q}))
    _print_table("Users", resp, ["id", "username", "display_name", "email", "enabled", "created_at"])


@user_app.command("get")
def user_get(username: str):
    """Show a single user."""
    with _client() as c:
        resp = _check(c.get(f"/api/v1/users/{username}"))
    rprint(resp)


@user_app.command("create")
def user_create(
    username: str,
    display_name: Optional[str] = typer.Option(None, "--name", "-n"),
    email: Optional[str] = typer.Option(None, "--email", "-e"),
):
    """Create a new user."""
    payload = {"username": username}
    if display_name:
        payload["display_name"] = display_name
    if email:
        payload["email"] = email
    with _client() as c:
        resp = _check(c.post("/api/v1/users", json=payload), ok=(201,))
    rprint(f"[green]✓[/green] User [bold]{username}[/bold] created (id={resp['id']})")


@user_app.command("delete")
def user_delete(username: str, yes: bool = typer.Option(False, "--yes", "-y")):
    """Delete a user (also removes bindings)."""
    if not yes:
        typer.confirm(f"Delete user '{username}' and all their bindings?", abort=True)
    with _client() as c:
        _check(c.delete(f"/api/v1/users/{username}"))
    rprint(f"[green]✓[/green] User [bold]{username}[/bold] deleted")


@user_app.command("enable")
def user_enable(username: str):
    """Enable a disabled user."""
    with _client() as c:
        resp = _check(c.patch(f"/api/v1/users/{username}", json={"enabled": True}))
    rprint(f"[green]✓[/green] User [bold]{username}[/bold] enabled")


@user_app.command("disable")
def user_disable(username: str):
    """Disable a user (revokes cache, preserves bindings)."""
    with _client() as c:
        resp = _check(c.patch(f"/api/v1/users/{username}", json={"enabled": False}))
    rprint(f"[yellow]✓[/yellow] User [bold]{username}[/bold] disabled")


@user_app.command("bind")
def user_bind(
    username: str,
    role: str,
    service: Optional[str] = typer.Option(
        None, "--service", "-s",
        help="Restrict to one service (doris/kafka/opensearch/spark)",
    ),
    expires_days: Optional[int] = typer.Option(None, "--expires-days", "-d",
                                               help="Auto-expire binding after N days"),
):
    """Bind a role to a user."""
    from datetime import datetime, timedelta, timezone
    payload: dict = {"role_name": role}
    if service:
        payload["service_name"] = service
    if expires_days:
        payload["expires_at"] = (
            datetime.now(timezone.utc) + timedelta(days=expires_days)
        ).isoformat()
    with _client() as c:
        resp = _check(c.post(f"/api/v1/users/{username}/bindings", json=payload), ok=(201,))
    scope = f" (service={service})" if service else " (all services)"
    rprint(f"[green]✓[/green] Role [bold]{role}[/bold] bound to [bold]{username}[/bold]{scope}")


@user_app.command("unbind")
def user_unbind(username: str, binding_id: int):
    """Remove a specific role binding by ID."""
    with _client() as c:
        _check(c.delete(f"/api/v1/users/{username}/bindings/{binding_id}"))
    rprint(f"[green]✓[/green] Binding {binding_id} removed from [bold]{username}[/bold]")


@user_app.command("bindings")
def user_bindings(username: str):
    """Show all role bindings for a user."""
    with _client() as c:
        resp = _check(c.get(f"/api/v1/users/{username}/bindings"))
    _print_table(
        f"Bindings for {username}",
        resp,
        ["id", "role_name", "service_name", "granted_by", "granted_at", "expires_at"],
    )


@user_app.command("roles")
def user_roles(username: str):
    """Show effective roles and permissions (uses cache)."""
    with _client() as c:
        resp = _check(c.get(f"/api/v1/users/{username}/roles"))
    cached_label = " [dim](cached)[/dim]" if resp.get("cached") else ""
    rprint(f"\n[bold]{username}[/bold]{cached_label}")
    rprint(f"  Roles: [cyan]{', '.join(resp['roles']) or 'none'}[/cyan]")
    if resp["permissions"]:
        _print_table(
            "Effective Permissions",
            resp["permissions"],
            ["service", "permission", "resource_scope"],
        )


# ── role commands ──────────────────────────────────────────

@role_app.command("list")
def role_list(q: str = typer.Option("", "--filter", "-f")):
    """List all roles."""
    with _client() as c:
        resp = _check(c.get("/api/v1/roles", params={"q": q}))
    rows = [{"id": r["id"], "name": r["name"], "display_name": r["display_name"],
             "perms": len(r["permissions"])} for r in resp]
    _print_table("Roles", rows, ["id", "name", "display_name", "perms"])


@role_app.command("get")
def role_get(role_id: int):
    """Show full detail for a role including its permissions."""
    with _client() as c:
        resp = _check(c.get(f"/api/v1/roles/{role_id}"))
    rprint(f"\n[bold]{resp['name']}[/bold] (id={resp['id']}): {resp.get('description','')}")
    _print_table(
        "Permissions",
        resp["permissions"],
        ["service_name", "permission_name", "resource_scope"],
    )


@role_app.command("create")
def role_create(
    name: str,
    display_name: str = typer.Option(..., "--display-name", "-n"),
    description: Optional[str] = typer.Option(None, "--description", "-d"),
    perms: Optional[list[str]] = typer.Option(
        None, "--perm", "-p",
        help=(
            "Permission to grant in 'service:NAME' format. "
            "Repeat for multiple permissions. "
            "Example: --perm doris:SELECT --perm kafka:CONSUME"
        ),
    ),
):
    """Create a new role with optional initial permissions."""
    permissions = [{"permission_name": p} for p in (perms or [])]
    payload = {
        "name": name,
        "display_name": display_name,
        "description": description,
        "permissions": permissions,
    }
    with _client() as c:
        resp = _check(c.post("/api/v1/roles", json=payload), ok=(201,))
    rprint(f"[green]✓[/green] Role [bold]{name}[/bold] created (id={resp['id']})")
    if perms:
        rprint(f"  Permissions granted: [cyan]{', '.join(perms)}[/cyan]")


@role_app.command("add-perm")
def role_add_perm(
    role_id: int,
    permission: str = typer.Argument(
        ..., help="Permission in 'service:NAME' format, e.g. doris:SELECT"
    ),
):
    """Grant a permission to a role by name (e.g. doris:SELECT)."""
    parts = permission.split(":", 1)
    if len(parts) != 2:
        rprint(f"[red]Error:[/red] Use 'service:NAME' format, e.g. doris:SELECT")
        raise SystemExit(1)
    service_name, perm_name = parts
    with _client() as c:
        resp = _check(
            c.post(f"/api/v1/roles/{role_id}/permissions/{service_name}/{perm_name}"),
            ok=(200, 201),
        )
    rprint(f"[green]✓[/green] Permission [cyan]{permission}[/cyan] added to role {role_id}")


@role_app.command("remove-perm")
def role_remove_perm(
    role_id: int,
    permission: str = typer.Argument(
        ..., help="Permission in 'service:NAME' format, e.g. doris:SELECT"
    ),
    yes: bool = typer.Option(False, "--yes", "-y"),
):
    """Revoke a permission from a role by name (e.g. doris:SELECT)."""
    if not yes:
        typer.confirm(f"Remove permission '{permission}' from role {role_id}?", abort=True)
    parts = permission.split(":", 1)
    if len(parts) != 2:
        rprint(f"[red]Error:[/red] Use 'service:NAME' format, e.g. doris:SELECT")
        raise SystemExit(1)
    service_name, perm_name = parts
    with _client() as c:
        resp = _check(
            c.delete(f"/api/v1/roles/{role_id}/permissions/{service_name}/{perm_name}"),
            ok=(200,),
        )
    rprint(f"[green]✓[/green] Permission [cyan]{permission}[/cyan] removed from role {role_id}")


@role_app.command("add-members")
def role_add_members(
    role_name: str,
    usernames: list[str] = typer.Argument(..., help="One or more usernames to bind to the role"),
    service: Optional[str] = typer.Option(
        None, "--service", "-s",
        help="Restrict binding to one service (doris/kafka/opensearch/spark)",
    ),
    expires_days: Optional[int] = typer.Option(
        None, "--expires-days", "-d",
        help="Auto-expire all new bindings after N days",
    ),
):
    """Bind multiple users to a role in a single call."""
    from datetime import datetime, timedelta, timezone
    payload: dict = {"usernames": usernames}
    if service:
        payload["service_name"] = service
    if expires_days:
        payload["expires_at"] = (
            datetime.now(timezone.utc) + timedelta(days=expires_days)
        ).isoformat()
    with _client() as c:
        resp = _check(c.post(f"/api/v1/users/roles/{role_name}/members", json=payload))

    table = Table(title=f"Bulk bind → {role_name}", show_header=True, header_style="bold cyan")
    table.add_column("username")
    table.add_column("status")
    table.add_column("binding_id")
    colour_map = {"bound": "green", "already_exists": "yellow", "user_not_found": "red"}
    for r in resp["results"]:
        colour = colour_map.get(r["status"], "white")
        table.add_row(
            r["username"],
            f"[{colour}]{r['status']}[/{colour}]",
            str(r["binding_id"]) if r.get("binding_id") else "—",
        )
    console.print(table)
    rprint(
        f"[green]bound {resp['bound']}[/green]  "
        f"[yellow]skipped {resp['skipped']}[/yellow]  "
        f"[red]errors {resp['errors']}[/red]"
    )
    if resp["errors"]:
        raise SystemExit(1)


@role_app.command("delete")
def role_delete(role_id: int, yes: bool = typer.Option(False, "--yes", "-y")):
    """Delete a role (also removes all bindings to it)."""
    if not yes:
        typer.confirm(f"Delete role {role_id}?", abort=True)
    with _client() as c:
        _check(c.delete(f"/api/v1/roles/{role_id}"))
    rprint(f"[green]✓[/green] Role {role_id} deleted")


@role_app.command("perms")
def role_perms(service: Optional[str] = typer.Option(None, "--service", "-s")):
    """List available permissions (optionally filtered by service)."""
    with _client() as c:
        if service:
            resp = _check(c.get(f"/api/v1/services/{service}/permissions"))
        else:
            resp = []
            svcs = _check(c.get("/api/v1/services"))
            for svc in svcs:
                p = _check(c.get(f"/api/v1/services/{svc['name']}/permissions"))
                resp.extend(p)
    _print_table("Available Permissions", resp, ["service_name", "name", "description"])


# ── sync commands ──────────────────────────────────────────

@sync_app.command("run")
def sync_run(
    username: Optional[str] = typer.Option(None, "--user", "-u"),
    service:  Optional[str] = typer.Option(None, "--service", "-s"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Show what would change"),
):
    """
    Push RBAC state to services.

    Examples:
      rbacctl sync run                    # full platform sync
      rbacctl sync run --user alice       # sync alice to all services
      rbacctl sync run --service doris    # sync all users to Doris
      rbacctl sync run --dry-run          # preview only
    """
    payload = {"dry_run": dry_run}
    if username:
        payload["username"] = username
    if service:
        payload["service"] = service

    with _client() as c:
        resp = _check(c.post("/api/v1/sync", json=payload))

    results = resp["results"]
    errors  = resp["errors"]

    table = Table(title="Sync Results", show_header=True, header_style="bold cyan")
    for col in ["username", "service", "status", "detail"]:
        table.add_column(col)
    for r in results:
        colour = {"synced": "green", "skipped": "dim", "error": "red",
                  "dry_run": "yellow"}.get(r["status"], "white")
        table.add_row(
            r["username"], r["service"],
            f"[{colour}]{r['status']}[/{colour}]",
            r.get("detail") or "",
        )
    console.print(table)

    if errors:
        rprint(f"[red]⚠ {errors} error(s) during sync[/red]")
        raise SystemExit(1)
    elif dry_run:
        rprint("[yellow]Dry run complete — no changes applied[/yellow]")
    else:
        rprint(f"[green]✓ Sync complete ({len(results)} tasks)[/green]")


# ── audit commands ─────────────────────────────────────────

@audit_app.command("log")
def audit_log(
    actor:  Optional[str] = typer.Option(None, "--actor"),
    action: Optional[str] = typer.Option(None, "--action"),
    limit:  int = typer.Option(50, "--limit", "-n"),
):
    """Show audit log entries."""
    params = {"limit": limit}
    if actor:
        params["actor"] = actor
    if action:
        params["action"] = action
    with _client() as c:
        resp = _check(c.get("/api/v1/audit", params=params))
    _print_table("Audit Log", resp, ["ts", "actor", "action", "target_type", "target_id"])


# ── token commands ─────────────────────────────────────────

@token_app.command("create")
def token_create(
    name: str,
    scopes: str = typer.Option("read,write", "--scopes"),
    expires_days: Optional[int] = typer.Option(None, "--expires-days"),
):
    """Create a new API token."""
    payload = {
        "name": name,
        "scopes": scopes.split(","),
        "expires_days": expires_days,
    }
    with _client() as c:
        resp = _check(c.post("/api/v1/auth/tokens", json=payload), ok=(200, 201))
    rprint(f"[green]✓[/green] Token [bold]{name}[/bold] created")
    rprint(f"  Raw token (save this — shown only once):")
    rprint(f"  [bold yellow]{resp['raw_token']}[/bold yellow]")


@token_app.command("revoke")
def token_revoke(name: str):
    """Revoke an API token."""
    with _client() as c:
        _check(c.delete(f"/api/v1/auth/tokens/{name}"))
    rprint(f"[green]✓[/green] Token [bold]{name}[/bold] revoked")


# ── services ───────────────────────────────────────────────

@app.command("services")
def list_services():
    """List registered services."""
    with _client() as c:
        resp = _check(c.get("/api/v1/services"))
    _print_table("Services", resp, ["id", "name", "display_name", "enabled"])


if __name__ == "__main__":
    app()
