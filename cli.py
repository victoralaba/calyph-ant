# cli.py
"""
Calyphant CLI.

Provides a command-line interface for automation and CI/CD integration.
All commands that need a database connection accept --url or read
the CALYPHANT_TARGET_URL environment variable.

Commands:
  connect    — test a PostgreSQL connection URL
  schema     — dump schema from a database
  migrate    — apply pending migrations
  rollback   — rollback last applied migration
  diff       — compare two database schemas
  backup     — create a backup
  restore    — restore from a backup file
  extensions — list or enable extensions
  seed       — run the super admin seed script
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import print as rprint

app = typer.Typer(
    name="calyphant",
    help="Calyphant — PostgreSQL workspace CLI",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

console = Console()
err_console = Console(stderr=True, style="bold red")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_url(url: str | None) -> str:
    resolved = url or os.environ.get("CALYPHANT_TARGET_URL")
    if not resolved:
        err_console.print(
            "No database URL provided. Pass --url or set CALYPHANT_TARGET_URL."
        )
        raise typer.Exit(1)
    return resolved


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

@app.command()
def connect(
    url: Optional[str] = typer.Option(None, "--url", "-u", help="PostgreSQL connection URL"),
):
    """Test a PostgreSQL connection and display server metadata."""
    from domains.connections.service import test_connection

    target_url = _get_url(url)
    console.print(f"[dim]Testing connection...[/dim]")

    result = _run(test_connection(target_url))

    if result.success:
        table = Table(title="Connection OK", show_header=False)
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        table.add_row("Host", result.host or "—")
        table.add_row("Port", str(result.port or "—"))
        table.add_row("Database", result.database or "—")
        table.add_row("PostgreSQL", result.pg_version or "—")
        table.add_row("Provider", result.cloud_provider or "unknown")
        console.print(table)

        if result.capabilities:
            exts = result.capabilities.get("extensions", [])
            if exts:
                console.print(f"\n[dim]{len(exts)} extension(s) installed[/dim]")
    else:
        err_console.print(f"Connection failed: {result.error}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

@app.command()
def schema(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    schema_name: str = typer.Option("public", "--schema", "-s"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Write SQL to file"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Dump the schema of a database as SQL or JSON."""
    import asyncio as _asyncio

    target_url = _get_url(url)
    console.print(f"[dim]Introspecting schema...[/dim]")

    from domains.schema.introspection import introspect_database

    snapshot = asyncio.run(
        asyncio.to_thread(introspect_database, target_url, schema_name)
    )

    if json_output:
        import dataclasses
        data = {
            "database": snapshot.database,
            "pg_version": snapshot.pg_version,
            "tables": len(snapshot.tables),
            "schemas": snapshot.schemas,
        }
        console.print_json(json.dumps(data))
        return

    if output:
        lines = [f"-- Schema dump: {snapshot.database}", f"-- {snapshot.pg_version}", ""]
        for t in snapshot.tables:
            if t.kind != "table":
                continue
            col_parts = [
                f'    "{c.name}" {c.data_type}'
                + (" PRIMARY KEY" if c.is_primary_key else "")
                + ("" if c.nullable else " NOT NULL")
                + (f" DEFAULT {c.default}" if c.default else "")
                for c in t.columns
            ]
            lines.append(f'CREATE TABLE IF NOT EXISTS "{t.schema}"."{t.name}" (')
            lines.append(",\n".join(col_parts))
            lines.append(");")
            lines.append("")
        output.write_text("\n".join(lines))
        console.print(f"[green]Schema written to {output}[/green]")
    else:
        table = Table(title=f"Schema: {snapshot.database}")
        table.add_column("Table", style="cyan")
        table.add_column("Kind")
        table.add_column("Columns", justify="right")
        table.add_column("Rows (est.)", justify="right")
        for t in snapshot.tables:
            table.add_row(
                t.name, t.kind, str(len(t.columns)),
                str(t.row_count_estimate or "—")
            )
        console.print(table)


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------

@app.command()
def migrate(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    connection_id: str = typer.Argument(..., help="Calyphant connection ID (UUID)"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """Apply all pending migrations for a connection."""
    console.print(f"[dim]Loading pending migrations...[/dim]")
    console.print("[yellow]Use the API or web UI to apply migrations.[/yellow]")
    console.print(
        "CLI migration execution requires a running Calyphant API instance. "
        "Connect via: POST /migrations/{connection_id}/apply-all"
    )


@app.command()
def rollback(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    connection_id: str = typer.Argument(...),
    migration_id: str = typer.Argument(..., help="Migration ID to roll back"),
):
    """Roll back a specific migration."""
    console.print(
        "Use the API: POST /migrations/{connection_id}/{migration_id}/rollback"
    )


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

@app.command()
def diff(
    source_url: str = typer.Argument(..., help="Source database URL"),
    target_url: str = typer.Argument(..., help="Target database URL"),
    schema_name: str = typer.Option("public", "--schema", "-s"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Write SQL to file"),
):
    """Compare two PostgreSQL schemas and output the diff SQL."""
    from domains.schema.diff import diff_schemas

    console.print("[dim]Computing schema diff...[/dim]")
    result = _run(diff_schemas(source_url, target_url, schema_name=schema_name))

    if result.error:
        err_console.print(f"Diff failed: {result.error}")
        raise typer.Exit(1)

    if not result.changes:
        console.print("[green]Schemas are identical.[/green]")
        return

    console.print(f"\n[bold]{result.summary}[/bold]\n")

    if result.has_destructive_changes:
        console.print("[bold red]⚠  Contains destructive changes[/bold red]\n")

    table = Table()
    table.add_column("Kind", style="cyan")
    table.add_column("Object")
    table.add_column("Table")
    table.add_column("Destructive", justify="center")
    for change in result.changes:
        table.add_row(
            change.kind.value,
            change.object_name,
            change.table_name or "—",
            "[red]YES[/red]" if change.is_destructive else "no",
        )
    console.print(table)

    if output:
        output.write_text(result.sql)
        console.print(f"\n[green]Migration SQL written to {output}[/green]")
    elif result.sql:
        console.print("\n[dim]-- Migration SQL --[/dim]")
        console.print(result.sql)


# ---------------------------------------------------------------------------
# backup
# ---------------------------------------------------------------------------

@app.command()
def backup(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    label: str = typer.Option("cli-backup", "--label", "-l"),
    fmt: str = typer.Option("calyph", "--format", "-f", help="calyph or sql"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
    schema_only: bool = typer.Option(False, "--schema-only"),
):
    """Create a database backup and write it locally."""
    import asyncpg
    from domains.backups.engine import _build_calyph, pg_dump_sql

    target_url = _get_url(url)
    console.print(f"[dim]Creating {fmt} backup...[/dim]")

    async def _do_backup():
        if fmt == "calyph":
            pg_conn = await asyncpg.connect(dsn=target_url, timeout=30.0)
            try:
                return await _build_calyph(
                    pg_conn=pg_conn,
                    label=label,
                    schema_only=schema_only,
                    # Pass db_url explicitly so full DDL introspection works
                    # from the CLI without a DB session or connection_id.
                    db_url=target_url,
                )
            finally:
                await pg_conn.close()
        else:
            return await pg_dump_sql(target_url, schema_only=schema_only)

    data = _run(_do_backup())

    ext = ".calyph.gz" if fmt == "calyph" else ".sql.gz"
    out_path = output or Path(f"{label}{ext}")
    out_path.write_bytes(data)
    size_kb = len(data) / 1024
    console.print(f"[green]Backup written to {out_path} ({size_kb:.1f} KB)[/green]")

# ---------------------------------------------------------------------------
# extensions
# ---------------------------------------------------------------------------

extensions_app = typer.Typer(help="Manage PostgreSQL extensions")
app.add_typer(extensions_app, name="extensions")


@extensions_app.command("list")
def extensions_list(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    installed_only: bool = typer.Option(False, "--installed"),
):
    """List available (or installed) extensions."""
    import asyncpg
    from domains.extensions.registry import list_extensions

    target_url = _get_url(url)

    async def _list():
        conn = await asyncpg.connect(dsn=target_url, timeout=10.0)
        try:
            return await list_extensions(conn)
        finally:
            await conn.close()

    data = _run(_list())
    exts = data["extensions"]
    if installed_only:
        exts = [e for e in exts if e["installed"]]

    table = Table(title=f"Extensions ({'installed' if installed_only else 'all'})")
    table.add_column("Name", style="cyan")
    table.add_column("Category")
    table.add_column("Version")
    table.add_column("Installed", justify="center")
    for ext in exts:
        table.add_row(
            ext["display_name"],
            ext["category"],
            ext["installed_version"] or ext["default_version"] or "—",
            "[green]✓[/green]" if ext["installed"] else "—",
        )
    console.print(table)


@extensions_app.command("enable")
def extensions_enable(
    name: str = typer.Argument(..., help="Extension name (e.g. vector, uuid-ossp)"),
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    schema_name: str = typer.Option("public", "--schema", "-s"),
):
    """Enable a PostgreSQL extension."""
    import asyncpg
    from domains.extensions.registry import enable_extension

    target_url = _get_url(url)

    async def _enable():
        conn = await asyncpg.connect(dsn=target_url, timeout=10.0)
        try:
            return await enable_extension(conn, name, schema_name)
        finally:
            await conn.close()

    try:
        result = _run(_enable())
        console.print(
            f"[green]Extension '{result['display_name']}' enabled "
            f"(v{result['installed_version']})[/green]"
        )
    except ValueError as exc:
        err_console.print(str(exc))
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------

@app.command()
def seed(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Run the super admin seed script (creates default tiers, super admin user)."""
    if not confirm:
        typer.confirm(
            "This will create a super admin user and seed default data. Continue?",
            abort=True,
        )
    console.print("[dim]Running seed script...[/dim]")

    import importlib
    import scripts.seed as seed_module
    _run(seed_module.main())
    console.print("[green]Seed complete.[/green]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
