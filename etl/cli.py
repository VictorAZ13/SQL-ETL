# etl/cli.py
import sys
import datetime as dt
from typing import Optional,List
import os, json
import tomllib
import pandas as pd
from pathlib import Path
import typer
from rich.table import Table
from rich.console import Console

# --- Fallback para ejecutar como script directo (python etl/cli.py ...) ---
try:
    from etl.db import get_conn
except ImportError:
    import pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from etl.db import get_conn

app = typer.Typer(no_args_is_help=True)
console = Console()

def _default_batch() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M")

@app.command("run-batch")
def run_batch(
    batch_id: str = typer.Option(_default_batch(), "--batch-id", help="ID del lote"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Ejecuta y revierte (no persiste)"),
    truncate_staging: bool = typer.Option(False, "--truncate-staging", help="Limpia staging (si está implementado)"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="DSN para sobrescribir el del entorno (.env)"),
):

    try:
        
        conn = get_conn(dsn) # Conexión a la BD
        conn.autocommit = True  # control manual de BEGIN/COMMIT/ROLLBACK
        with conn.cursor() as cur:
            if truncate_staging:
                # Si tienes un SP real, descomenta/ajusta:
                # cur.execute("CALL proyecto_etl.sp_truncate_staging(%s);", (batch_id,))
                console.log("[yellow]truncate-staging: (simulado hoy)[/]")
            # Transacción explícita
            cur.execute("BEGIN;")
            cur.execute("CALL proyecto_etl.sp_orquestar_batch(%s);", (batch_id,))
            if dry_run:
                cur.execute("ROLLBACK;")
                console.log(f"[cyan][DRY-RUN][/cyan] Batch {batch_id} ejecutado y revertido.")
            else:
                cur.execute("COMMIT;")
                console.log(f"[green][OK][/green] Batch {batch_id} ejecutado y confirmado.")
        conn.close()
    except Exception:
        console.print_exception(show_locals=False)
        sys.exit(1)

@app.command("show-audit")
def show_audit(
    dsn: Optional[str] = typer.Option(None, "--dsn", help="DSN para sobrescribir el del entorno (.env)"),
):
    """
    Muestra los pasos de la última corrida desde proyecto_etl.audit_run / audit_step.
    """
    try:
        conn = get_conn(dsn)
        with conn, conn.cursor() as cur:
            cur.execute("""
                WITH last AS (
                  SELECT run_id,status
                  FROM proyecto_etl.audit_run
                  ORDER BY started_at DESC
                  LIMIT 1
                )
                SELECT s.table_name,l.status, s.rows_inserted, s.duration_ms
                FROM proyecto_etl.audit_step s
                JOIN last l ON s.run_id = l.run_id
                ORDER BY l.run_id;
            """)
            rows = cur.fetchall()

        if not rows:
            console.print("[yellow]No hay corridas registradas en auditoría todavía.[/]")
            return

        table = Table(title="Última corrida (audit_step)")
        table.add_column("#", justify="right")
        table.add_column("Paso")
        table.add_column("Estado")
        table.add_column("Filas", justify="right")
        table.add_column("ms", justify="right")

        for order, status, rows_insert, ms in rows:
            badge = "[green]OK[/]" if (status or "").upper() == "OK" else "[red]FAIL[/]"
            table.add_row(str(order), badge, str(rows_insert or 0), str(ms or 0))

        console.print(table)
    except Exception:
        console.print_exception(show_locals=False)
        sys.exit(1)

@app.command("snapshot-kpis")
def snapshot_kpis(
    run_id: Optional[int] = typer.Option(None, "--run-id", help="run_id a snapshootear"),
    batch_id: Optional[str] = typer.Option(None, "--batch-id", help="batch_id a snapshootear (toma el último run)"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Override DSN"),
):
   
    if not run_id and not batch_id:
        console.print("[red]Debes pasar --run-id o --batch-id[/]")
        raise typer.Exit(2)

    try:
        conn = get_conn(dsn); conn.autocommit = True
        with conn.cursor() as cur:
            if run_id:
                cur.execute("CALL proyecto_etl.sp_snapshot_kpis(%s);", (run_id,))
                console.log(f"[green]OK[/green] snapshot por run_id={run_id}")
            else:
                cur.execute("CALL proyecto_etl.sp_snapshot_kpis_by_batch(%s);", (batch_id,))
                console.log(f"[green]OK[/green] snapshot por batch_id={batch_id}")
        conn.close()
    except Exception:
        console.print_exception(show_locals=False)
        sys.exit(1)

@app.command("compare-variacion")
def compare_variacion(
    base_run: int = typer.Option(..., "--base-run", help="run_id base (anterior)"),
    target_run: int = typer.Option(..., "--target-run", help="run_id target (reciente)"),
    level: Optional[str] = typer.Option(None, "--level", help="grupo|curso|depto|profesor"),
    periodo: Optional[str] = typer.Option(None, "--periodo", help="Ej: 2024-II"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Override DSN"),
    top: int = typer.Option(50, "--top", help="Máximo de filas a mostrar"),
):
    """
    Compara KPIs entre dos corridas y muestra variación absoluta y porcentual.
    """
    try:
        conn = get_conn(dsn)
        with conn, conn.cursor() as cur:
            cur.execute("""
                SELECT kpi_name, kpi_level, periodo_id, dept_id, curso_id, grupo_code, profesor_id,
                       base_value, target_value, var_abs, var_pct
                FROM proyecto_etl.fn_kpi_variacion(%s, %s, %s, %s)
                ORDER BY kpi_name, kpi_level, periodo_id NULLS LAST, dept_id NULLS LAST, curso_id NULLS LAST
                LIMIT %s;
            """, (base_run, target_run, level, periodo, top))
            rows = cur.fetchall()

        if not rows:
            console.print("[yellow]No hay KPIs comparables para esos run_id/filtros.[/]")
            return

        table = Table(title=f"Variación de KPIs (base={base_run} → target={target_run})")
        for col in ["kpi_name","kpi_level","periodo_code","dept_code","curso_code","grupo_code","profesor_dni",
                    "base_value","target_value","var_abs","var_pct"]:
            table.add_column(col, justify="right" if col in {"base_value","target_value","var_abs","var_pct"} else "left")

        for r in rows:
            base_v, tgt_v, v_abs, v_pct = r[7], r[8], r[9], r[10]
            table.add_row(
                r[0], r[1], r[2] or "", r[3] or "", r[4] or "", r[5] or "", r[6] or "",
                f"{base_v:.4f}" if base_v is not None else "",
                f"{tgt_v:.4f}"  if tgt_v  is not None else "",
                f"{v_abs:.4f}"  if v_abs  is not None else "",
                (f"{v_pct*100:.2f}%" if v_pct is not None else "")
            )
        console.print(table)
    except Exception:
        console.print_exception(show_locals=False)
        sys.exit(1)
        
def _get_exports_dir() -> Path:
    cfg_path = Path("config.toml")
    if cfg_path.exists():
        with cfg_path.open("rb") as f:
            data = tomllib.load(f)
        exp = data.get("etl", {}).get("exports_dir", "exports")
    else:
        exp = "exports"
    p = Path(exp)
    p.mkdir(parents=True, exist_ok=True)
    return p

# Utilidad: resolver run_id desde batch_id (último run del batch)
def _resolve_run_id(conn, batch_id: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT run_id
            FROM proyecto_etl.audit_run
            WHERE batch_id = %s
            ORDER BY started_at DESC
            LIMIT 1;
        """, (batch_id,))
        row = cur.fetchone()
        return row[0] if row else None

@app.command("export-kpis")
def export_kpis(
    run_id: Optional[int] = typer.Option(None, "--run-id", help="run_id a exportar"),
    batch_id: Optional[str] = typer.Option(None, "--batch-id", help="Si no pasas run_id, usa el último run de este batch"),
    format: str = typer.Option("csv", "--format", case_sensitive=False, help="csv|parquet|json"),
    out: Optional[Path] = typer.Option(None, "--out", help="Carpeta destino; por defecto usa etl.exports_dir de config.toml"),
    level: Optional[str] = typer.Option(None, "--level", help="grupo|curso|depto|profesor"),
    periodo: Optional[str] = typer.Option(None, "--periodo", help="Ej: 2024-II"),
    kpi: Optional[List[str]] = typer.Option(None, "--kpi", "-k", help="Puedes repetir: -k fill_rate -k tasa_aprobacion"),
    dept: Optional[str] = typer.Option(None, "--dept", help="Filtra por dept_code"),
    curso: Optional[str] = typer.Option(None, "--curso", help="Filtra por curso_code"),
    profesor: Optional[str] = typer.Option(None, "--profesor", help="Filtra por profesor_dni"),
    dsn: Optional[str] = typer.Option(None, "--dsn", help="Override DSN"),
    limit: int = typer.Option(0, "--limit", help="0 = sin límite; útil para pruebas"),
):
    """
    Exporta KPIs desde proyecto_etl.kpi_snapshot a CSV/Parquet/JSON.
    """
    format = format.lower()
    if format not in {"csv","parquet","json"}:
        console.print("[red]--format debe ser uno de: csv|parquet|json[/]")
        raise typer.Exit(2)

    try:
        conn = get_conn(dsn)
        with conn:
            # Resolver run_id si no se pasó
            if run_id is None:
                if not batch_id:
                    console.print("[red]Debes pasar --run-id o --batch-id[/]")
                    raise typer.Exit(2)
                run_id = _resolve_run_id(conn, batch_id)
                if run_id is None:
                    console.print(f"[yellow]No se encontró run para batch_id={batch_id}[/]")
                    raise typer.Exit(1)

            # Construir query dinámica con filtros
            where = ["run_id = %s"]
            params: List[object] = [run_id]
            if level:
                where.append("kpi_level = %s");      params.append(level)
            if periodo:
                where.append("periodo_id = %s");   params.append(periodo)
            if dept:
                where.append("dept_id = %s");      params.append(dept)
            if curso:
                where.append("curso_id = %s");     params.append(curso)
            if profesor:
                where.append("profesor_id = %s");   params.append(profesor)
            if kpi:
                where.append("kpi_name = ANY(%s)");  params.append(kpi)

            sql = f"""
                SELECT run_id, batch_id, kpi_name, kpi_level,
                       periodo_id, dept_id, curso_id, grupo_code, profesor_id,
                       kpi_value, created_at, tags_json
                FROM proyecto_etl.kpi_snapshot
                WHERE {" AND ".join(where)}
                ORDER BY kpi_name, kpi_level, periodo_id NULLS LAST, dept_id NULLS LAST, curso_id NULLS LAST
            """
            if limit and limit > 0:
                sql += f" LIMIT {int(limit)}"

            # Traer datos
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []

        if not rows:
            console.print("[yellow]No hay filas para exportar con esos filtros.[/]")
            raise typer.Exit(0)

        # DataFrame para exportar
        df = pd.DataFrame(rows, columns=cols)

        # Carpeta destino
        exports_root = out if out else _get_exports_dir()
        target_dir = exports_root / f"run_{run_id}"
        target_dir.mkdir(parents=True, exist_ok=True)

        # Nombre de archivo
        lvl = level or "all"
        per = (periodo or "all").replace("/", "-")
        base_name = f"kpis_run_{run_id}_{lvl}_{per}"

        # Escritura por formato
        path = None
        if format == "csv":
            path = target_dir / f"{base_name}.csv"
            # UTF-8 con BOM juega bien con Excel en Windows
            df.to_csv(path, index=False, encoding="utf-8-sig")
        elif format == "json":
            path = target_dir / f"{base_name}.json"
            df.to_json(path, orient="records", force_ascii=False, date_format="iso", indent=2)
        else:  # parquet
            try:
                import pyarrow  # noqa: F401  (solo para verificar instalación)
            except Exception:
                console.print("[red]Necesitas pyarrow para --format parquet. Instala: pip install pyarrow[/]")
                raise typer.Exit(2)
            path = target_dir / f"{base_name}.parquet"
            df.to_parquet(path, index=False, engine="pyarrow")

        # Feedback
        console.log(f"[green]OK[/green] Exportado {len(df):,} filas → {path}")
        # Vista previa (máx 10)
        preview = df.head(10)
        table = Table(title=f"Preview ({min(10, len(df))} de {len(df)})")
        for c in preview.columns:
            table.add_column(str(c))
        for _, r in preview.iterrows():
            table.add_row(*[("" if pd.isna(v) else str(v)) for v in r.tolist()])
        console.print(table)

    except Exception:
        console.print_exception(show_locals=False)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()

