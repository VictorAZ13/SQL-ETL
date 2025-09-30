# etl/cli.py
import sys
import datetime as dt
from typing import Optional

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
    """
    Llama al SP: CALL proyecto_etl.sp_orquestar_batch(:batch_id)
    con control explícito de transacción para soportar --dry-run de forma segura.
    """
    try:
        # Usamos conexión sin context manager para evitar commits implícitos.
        conn = get_conn(dsn)
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
                  SELECT run_id
                  FROM proyecto_etl.audit_run
                  ORDER BY started_at DESC
                  LIMIT 1
                )
                SELECT s.table_name, s.rows_inserted,s.rows_updated,s.rows_skipped, s.duration_ms
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

        for order, name, status, rows_aff, ms in rows:
            badge = "[green]OK[/]" if (status or "").upper() == "OK" else "[red]FAIL[/]"
            table.add_row(str(order), name, badge, str(rows_aff or 0), str(ms or 0))

        console.print(table)
    except Exception:
        console.print_exception(show_locals=False)
        sys.exit(1)

if __name__ == "__main__":
    app()
