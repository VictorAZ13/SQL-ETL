# etl/db.py
from __future__ import annotations
import os, psycopg, tomllib
from pathlib import Path
from getpass import getpass
from dotenv import load_dotenv, find_dotenv

ETL_DEBUG = os.getenv("ETL_DEBUG") == "1"
print(ETL_DEBUG)

# Carga .env desde la raíz del proyecto (si existe)
_DOTENV_PATH = find_dotenv(usecwd=True)
load_dotenv(_DOTENV_PATH or None)

if ETL_DEBUG:
    print(f"[debug] .env: {_DOTENV_PATH or 'no encontrado'}")

def _dsn_from_pg_env() -> str | None:
    host = os.getenv("PGHOST")
    db   = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pw   = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")
    if host and db and user and pw:
        if ETL_DEBUG:
            print(f"[debug] usando PG* (host={host}, db={db}, user={user})")
        return f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    return None

def _dsn_from_config() -> str | None:
    cfg = Path("config.toml")
    if not cfg.exists():
        return None
    # ✅ Forma correcta: tomllib.load() con archivo binario
    with cfg.open("rb") as f:
        data = tomllib.load(f)
    dsn = data.get("etl", {}).get("dsn")
    if ETL_DEBUG:
        print(f"[debug] etl.dsn en config.toml: {'sí' if dsn else 'no'}")
    return dsn

def _prompt_for_dsn(existing_host: str | None = None,
                    existing_db: str | None = None,
                    existing_user: str | None = None) -> str:
    print("No encontré credenciales. Ingrésalas para esta sesión:")
    host = existing_host or input("Host [localhost]: ") or "localhost"
    port = input("Port [5432]: ") or "5432"
    db   = existing_db   or input("Database: ")
    user = existing_user or input("User: ")
    pw   = getpass("Password: ")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"

def resolve_dsn(flag_dsn: str | None = None) -> str:
    # 1) Flag --dsn
    if flag_dsn:
        if ETL_DEBUG: print("[debug] DSN por --dsn")
        return flag_dsn
    # 2) ETL_DSN del entorno / .env
    if os.getenv("ETL_DSN"):
        if ETL_DEBUG: print("[debug] DSN por ETL_DSN")
        return os.environ["ETL_DSN"]
    # 3) Variables PG*
    dsn = _dsn_from_pg_env()
    if dsn:
        return dsn
    # 4) config.toml (último recurso, idealmente sin password)
    dsn = _dsn_from_config()
    if dsn:
        return dsn
    # 5) Prompt (para local). Para bloquear prompts en CI: ETL_NO_PROMPT=1
    if os.getenv("ETL_NO_PROMPT") == "1":
        raise RuntimeError("Sin credenciales y ETL_NO_PROMPT=1 (no se permite prompt).")
    if ETL_DEBUG: print("[debug] pidiendo credenciales por consola…")
    return _prompt_for_dsn(
        existing_host=os.getenv("PGHOST"),
        existing_db=os.getenv("PGDATABASE"),
        existing_user=os.getenv("PGUSER"),
    )

def get_conn(dsn: str | None = None) -> psycopg.Connection:
    return psycopg.connect(resolve_dsn(dsn), application_name="etl_cli")
