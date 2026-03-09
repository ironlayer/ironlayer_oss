"""``ironlayer migrate`` -- import models from dbt, raw SQL, SQLMesh."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import typer

from cli.display import display_migration_report
from cli.helpers import console
from cli.state import emit_metrics, get_json_output

if TYPE_CHECKING:
    from core_engine.models.model_definition import ModelDefinition

migrate_app = typer.Typer(
    name="migrate",
    help="Import models from external tools into IronLayer format.",
    no_args_is_help=True,
)

_SQL_TABLE_REF_PATTERN = re.compile(
    r"""
    (?:FROM|JOIN)\s+               # FROM or JOIN keyword
    (?:`([^`]+)`                   # backtick-quoted identifier
    | "([^"]+)"                    # double-quote-quoted identifier
    | (                            # unquoted identifier
        [a-zA-Z_]\w*               # first segment
        (?:\.[a-zA-Z_]\w*)*        # optional dotted segments (schema.table)
      )
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SQL_KEYWORDS: frozenset[str] = frozenset(
    {
        "select", "where", "group", "order", "having", "limit",
        "union", "except", "intersect", "with", "on", "using", "as",
        "inner", "outer", "left", "right", "cross", "full", "natural", "lateral",
    }
)


def _extract_sql_table_refs(sql: str) -> list[str]:
    """Extract table references from FROM and JOIN clauses in raw SQL."""
    seen: set[str] = set()
    refs: list[str] = []
    for match in _SQL_TABLE_REF_PATTERN.finditer(sql):
        table_name = match.group(1) or match.group(2) or match.group(3) or ""
        table_name = table_name.strip()
        if not table_name or table_name.lower() in _SQL_KEYWORDS or table_name.startswith("("):
            continue
        if table_name not in seen:
            seen.add(table_name)
            refs.append(table_name)
    refs.sort()
    return refs


def _generate_ironlayer_file(
    model: ModelDefinition,
    output_dir: Path,
    original_sql: str | None = None,
) -> Path:
    """Generate a IronLayer-formatted .sql file from a ModelDefinition."""
    parts = model.name.split(".")
    if len(parts) >= 2:
        rel_path = Path(*parts[:-1]) / f"{parts[-1]}.sql"
    else:
        rel_path = Path(f"{parts[0]}.sql")
    output_path = output_dir / rel_path

    header_lines: list[str] = [
        f"-- name: {model.name}",
        f"-- kind: {model.kind.value}",
        f"-- materialization: {model.materialization.value}",
    ]
    if model.time_column:
        header_lines.append(f"-- time_column: {model.time_column}")
    if model.unique_key:
        header_lines.append(f"-- unique_key: {model.unique_key}")
    if model.partition_by:
        header_lines.append(f"-- partition_by: {model.partition_by}")
    if model.incremental_strategy:
        header_lines.append(f"-- incremental_strategy: {model.incremental_strategy}")
    if model.owner:
        header_lines.append(f"-- owner: {model.owner}")
    if model.tags:
        header_lines.append(f"-- tags: {', '.join(model.tags)}")
    if model.dependencies:
        header_lines.append(f"-- dependencies: {', '.join(model.dependencies)}")
    if model.exposures:
        exp_data = [
            {"name": e.name, "type": e.type, "url": e.url, "label": e.label}
            for e in model.exposures
        ]
        header_lines.append("-- exposures: " + json.dumps(exp_data, ensure_ascii=False))
    if model.pre_hooks:
        raw = "\n".join(model.pre_hooks)
        escaped = raw.replace("\\", "\\\\").replace("\n", "\\n")
        header_lines.append("-- pre_hook_sql: " + escaped)
    if model.post_hooks:
        raw = "\n".join(model.post_hooks)
        escaped = raw.replace("\\", "\\\\").replace("\n", "\\n")
        header_lines.append("-- post_hook_sql: " + escaped)

    sql_body = original_sql or model.clean_sql or model.raw_sql
    if not sql_body.strip():
        sql_body = f"-- No SQL body available for model: {model.name}"
    file_content = "\n".join(header_lines) + "\n\n" + sql_body + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(file_content, encoding="utf-8")
    return output_path


@migrate_app.command("from-dbt")
def migrate_from_dbt(
    project_path: Path = typer.Argument(
        ...,
        help="Path to the dbt project directory (contains dbt_project.yml).",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    output: Path = typer.Option(
        Path("./models"),
        "--output",
        "-o",
        help="Output directory for generated IronLayer model files.",
    ),
    tag_filter: str | None = typer.Option(
        None,
        "--tag",
        help="Only migrate models with this tag.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be migrated without writing files.",
    ),
) -> None:
    """Migrate dbt models into IronLayer format."""
    from core_engine.loader.dbt_loader import (
        DbtManifestError,
        discover_dbt_manifest,
        load_models_from_dbt_manifest,
    )

    try:
        manifest_path = discover_dbt_manifest(project_path)
        if manifest_path is None:
            console.print(
                f"[red]No manifest.json found in '{project_path}'.[/red]\n"
                f"[dim]Run 'dbt compile' or 'dbt build' first to generate "
                f"the manifest artifact.[/dim]"
            )
            raise typer.Exit(code=3)

        console.print(f"Found manifest at [bold]{manifest_path}[/bold]")
        if not get_json_output():
            console.print(
                "[dim]Scope: dbt models (table/view/incremental). Exposures and hooks are "
                "preserved as metadata; hooks are not executed by ironlayer apply. "
                "Tests, seeds, snapshots, and ephemeral models are not migrated. See docs.[/dim]"
            )

        tag_filter_list: list[str] | None = None
        if tag_filter:
            tag_filter_list = [t.strip() for t in tag_filter.split(",") if t.strip()]

        model_defs = load_models_from_dbt_manifest(manifest_path, tag_filter=tag_filter_list)

        if not model_defs:
            msg = "No models found in the dbt manifest"
            if tag_filter:
                msg += f" matching tag '{tag_filter}'"
            console.print(f"[yellow]{msg}.[/yellow]")
            raise typer.Exit(code=0)

        migrated: list[dict[str, str]] = []
        skipped: list[dict[str, str]] = []
        warnings: list[str] = []
        output_dir = output.resolve()

        for model in model_defs:
            sql_body = model.clean_sql or model.raw_sql
            if not sql_body.strip():
                skipped.append({"name": model.name, "source": model.file_path, "reason": "no SQL content"})
                warnings.append(f"Model '{model.name}' has no SQL content and was skipped.")
                continue
            parts = model.name.split(".")
            rel_path = str(Path(*parts[:-1]) / f"{parts[-1]}.sql") if len(parts) >= 2 else f"{parts[0]}.sql"
            out_path = output_dir / rel_path
            if dry_run:
                migrated.append({"name": model.name, "source": model.file_path, "output": str(out_path), "status": "dry-run"})
            else:
                written_path = _generate_ironlayer_file(model, output_dir, original_sql=sql_body)
                migrated.append({"name": model.name, "source": model.file_path, "output": str(written_path), "status": "migrated"})

        if get_json_output():
            sys.stdout.write(json.dumps({"migrated": migrated, "skipped": skipped, "warnings": warnings}, indent=2) + "\n")
        else:
            display_migration_report(console, migrated, skipped, warnings)
            if not dry_run and migrated:
                console.print(f"\nFiles written to [bold]{output_dir}[/bold]")

        emit_metrics(
            "migrate.from_dbt",
            {"source": str(project_path), "migrated": len(migrated), "skipped": len(skipped), "dry_run": dry_run, "tag_filter": tag_filter},
        )
    except typer.Exit:
        raise
    except DbtManifestError as exc:
        console.print(f"[red]dbt manifest error: {exc}[/red]")
        raise typer.Exit(code=3) from exc
    except Exception as exc:
        console.print(f"[red]Migration failed: {exc}[/red]")
        emit_metrics("migrate.from_dbt.error", {"error": str(exc)})
        raise typer.Exit(code=3) from exc


@migrate_app.command("from-sql")
def migrate_from_sql(
    sql_dir: Path = typer.Argument(
        ...,
        help="Directory containing raw .sql files.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    output: Path = typer.Option(
        Path("./models"),
        "--output",
        "-o",
        help="Output directory for generated IronLayer model files.",
    ),
    default_materialization: str = typer.Option(
        "TABLE",
        "--materialization",
        help="Default materialization for imported models (TABLE, VIEW, INSERT_OVERWRITE, MERGE).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be migrated without writing files.",
    ),
) -> None:
    """Migrate raw SQL files into IronLayer format with inferred dependencies."""
    from core_engine.models.model_definition import Materialization, ModelDefinition, ModelKind

    try:
        mat_upper = default_materialization.upper().strip()
        try:
            materialization = Materialization(mat_upper)
        except ValueError:
            valid = ", ".join(m.value for m in Materialization)
            console.print(f"[red]Invalid materialization '{default_materialization}'. Valid options: {valid}[/red]")
            raise typer.Exit(code=3)

        if materialization in (Materialization.TABLE, Materialization.VIEW):
            kind = ModelKind.FULL_REFRESH
        elif materialization == Materialization.INSERT_OVERWRITE:
            kind = ModelKind.INCREMENTAL_BY_TIME_RANGE
        elif materialization == Materialization.MERGE:
            kind = ModelKind.MERGE_BY_KEY
        else:
            kind = ModelKind.FULL_REFRESH

        sql_files = sorted(sql_dir.rglob("*.sql"))
        if not sql_files:
            console.print(f"[yellow]No .sql files found under '{sql_dir}'.[/yellow]")
            raise typer.Exit(code=0)

        console.print(f"Found [bold]{len(sql_files)}[/bold] SQL file(s) under [bold]{sql_dir}[/bold]")
        migrated: list[dict[str, str]] = []
        skipped: list[dict[str, str]] = []
        warnings: list[str] = []
        output_dir = output.resolve()

        for sql_file in sql_files:
            try:
                sql_content = sql_file.read_text(encoding="utf-8")
            except OSError as exc:
                skipped.append({"name": sql_file.name, "source": str(sql_file), "reason": f"read error: {exc}"})
                warnings.append(f"Could not read '{sql_file}': {exc}")
                continue
            if not sql_content.strip():
                skipped.append({"name": sql_file.name, "source": str(sql_file), "reason": "empty file"})
                continue
            rel_path = sql_file.relative_to(sql_dir)
            stem_parts = list(rel_path.parent.parts) + [rel_path.stem]
            model_name = ".".join(stem_parts)
            dependencies = _extract_sql_table_refs(sql_content)
            effective_kind = kind
            effective_materialization = materialization
            if kind == ModelKind.INCREMENTAL_BY_TIME_RANGE:
                effective_kind = ModelKind.FULL_REFRESH
                effective_materialization = Materialization.TABLE
                if not any(w.startswith("Materialization INSERT_OVERWRITE") for w in warnings):
                    warnings.append(
                        "Materialization INSERT_OVERWRITE requires a time_column. Falling back to FULL_REFRESH/TABLE. Edit generated headers to set time_column and kind."
                    )
            if kind == ModelKind.MERGE_BY_KEY:
                effective_kind = ModelKind.FULL_REFRESH
                effective_materialization = Materialization.TABLE
                if not any(w.startswith("Materialization MERGE") for w in warnings):
                    warnings.append(
                        "Materialization MERGE requires a unique_key. Falling back to FULL_REFRESH/TABLE. Edit generated headers to set unique_key and kind."
                    )
            model_def = ModelDefinition(
                name=model_name,
                kind=effective_kind,
                materialization=effective_materialization,
                dependencies=dependencies,
                file_path=str(sql_file),
                raw_sql=sql_content,
                clean_sql=sql_content,
            )
            name_parts = model_name.split(".")
            out_rel = str(Path(*name_parts[:-1]) / f"{name_parts[-1]}.sql") if len(name_parts) >= 2 else f"{name_parts[0]}.sql"
            out_path = output_dir / out_rel
            if dry_run:
                migrated.append({"name": model_name, "source": str(sql_file), "output": str(out_path), "status": "dry-run"})
            else:
                written_path = _generate_ironlayer_file(model_def, output_dir, original_sql=sql_content)
                migrated.append({"name": model_name, "source": str(sql_file), "output": str(written_path), "status": "migrated"})

        if get_json_output():
            sys.stdout.write(json.dumps({"migrated": migrated, "skipped": skipped, "warnings": warnings}, indent=2) + "\n")
        else:
            display_migration_report(console, migrated, skipped, warnings)
            if not dry_run and migrated:
                console.print(f"\nFiles written to [bold]{output_dir}[/bold]")

        emit_metrics(
            "migrate.from_sql",
            {"source": str(sql_dir), "migrated": len(migrated), "skipped": len(skipped), "dry_run": dry_run, "materialization": mat_upper},
        )
    except typer.Exit:
        raise
    except Exception as exc:
        console.print(f"[red]Migration failed: {exc}[/red]")
        emit_metrics("migrate.from_sql.error", {"error": str(exc)})
        raise typer.Exit(code=3) from exc


@migrate_app.command("from-sqlmesh")
def migrate_from_sqlmesh(
    project_path: Path = typer.Argument(
        ...,
        help="Path to the SQLMesh project root directory.",
        exists=True,
        file_okay=False,
        resolve_path=True,
    ),
    output: Path = typer.Option(
        Path("./models"),
        "--output",
        "-o",
        help="Output directory for generated IronLayer model files.",
    ),
    tag: str | None = typer.Option(
        None,
        "--tag",
        help="Only migrate models with this tag.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be migrated without writing files.",
    ),
) -> None:
    """Migrate a SQLMesh project into IronLayer format."""
    from core_engine.loader.sqlmesh_loader import (
        SQLMeshLoadError,
        discover_sqlmesh_project,
        load_models_from_sqlmesh_project,
    )

    try:
        config_path = discover_sqlmesh_project(project_path)
        if config_path is None:
            console.print(
                f"[red]No SQLMesh config file found in '{project_path}'.[/red]\n"
                "Expected config.yaml, config.yml, or config.py."
            )
            raise typer.Exit(code=3)
        console.print(f"Found SQLMesh project: [bold]{config_path}[/bold]")
        model_defs = load_models_from_sqlmesh_project(project_path, tag_filter=tag)
        if not model_defs:
            console.print("[yellow]No models found in the SQLMesh project.[/yellow]")
            if tag:
                console.print(f"[yellow]Tag filter: '{tag}'[/yellow]")
            raise typer.Exit(code=0)
        console.print(f"Found [bold]{len(model_defs)}[/bold] model(s) to migrate")
        migrated: list[dict[str, str]] = []
        skipped: list[dict[str, str]] = []
        warnings: list[str] = []
        output_dir = output.resolve()
        for model_def in model_defs:
            sql_body = model_def.clean_sql or model_def.raw_sql
            if sql_body and sql_body.startswith("-- Python model:"):
                warnings.append(f"Model '{model_def.name}' is a Python model. Manual SQL conversion required.")
            name_parts = model_def.name.split(".")
            out_rel = str(Path(*name_parts[:-1]) / f"{name_parts[-1]}.sql") if len(name_parts) >= 2 else f"{name_parts[0]}.sql"
            out_path = output_dir / out_rel
            if dry_run:
                migrated.append({"name": model_def.name, "source": model_def.file_path or "", "output": str(out_path), "status": "dry-run"})
            else:
                written_path = _generate_ironlayer_file(model_def, output_dir)
                migrated.append({"name": model_def.name, "source": model_def.file_path or "", "output": str(written_path), "status": "migrated"})
        if get_json_output():
            sys.stdout.write(json.dumps({"migrated": migrated, "skipped": skipped, "warnings": warnings}, indent=2) + "\n")
        else:
            display_migration_report(console, migrated, skipped, warnings)
            if not dry_run and migrated:
                console.print(f"\nFiles written to [bold]{output_dir}[/bold]")
        emit_metrics(
            "migrate.from_sqlmesh",
            {"source": str(project_path), "migrated": len(migrated), "skipped": len(skipped), "dry_run": dry_run, "tag_filter": tag},
        )
    except typer.Exit:
        raise
    except SQLMeshLoadError as exc:
        console.print(f"[red]SQLMesh project error: {exc}[/red]")
        raise typer.Exit(code=3) from exc
    except Exception as exc:
        console.print(f"[red]Migration failed: {exc}[/red]")
        emit_metrics("migrate.from_sqlmesh.error", {"error": str(exc)})
        raise typer.Exit(code=3) from exc
