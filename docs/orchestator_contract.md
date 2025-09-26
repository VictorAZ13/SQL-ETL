# Contrato del Orquestador (App-céntrico)

> **Alcance (SÍ)**: orquestar pre-checks → CALL SQL → post-checks → exportar evidencias → (opcional) refresh MV.
> **No-alcance (NO)**: DDL/creación de tablas, creación de constraints/triggers/vistas. (Se manejan en `sql/constraints.sql` y `sql/triggers.sql` en D2.)

## 1) Entradas
- **Fuente**: archivos en `data/` (p.ej., `data/estudiantes_sintetico.csv`).
- **Formatos**: CSV o Parquet.
- **Catálogos**: (opcional) `datasets/departamentos.csv`, `datasets/cursos.csv`, etc. solo lectura.
- **Conexión**: variables de entorno tipo Postgres:
  - `DATABASE_URL` *o* (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`).

## 2) CLI / Flags (estándar)
| flag | tipo | default | descripción |
|---|---|---|---|
| `--input` | path | `data/estudiantes_sintetico.csv` | archivo o carpeta de entrada |
| `--format` | enum(`csv`,`parquet`) | `csv` | formato de entrada |
| `--delimiter` | str | `,` | separador CSV |
| `--schema` | str | `etl_demo` | esquema destino/staging |
| `--truncate-staging` | bool | `false` | si `true`, hace `TRUNCATE` de staging **antes** de cargar |
| `--dry-run` | bool | `false` | ejecuta pre-checks y preview de upsert **sin** persistir |
| `--run-id` | uuid | autogenerado | permite reusar un run_id externo |
| `--export-dir` | path | `exports/` | carpeta raíz de evidencias |
| `--refresh-mv` | bool | `false` | si `true`, refresca MV al final (si existen) |
| `--seed` | int | `42` | solo para generación de dataset sintético (fuera del alcance del run) |
| `--verbose` | bool | `false` | logging detallado a consola |

## 3) Pasos del pipeline (orden canónico)
1. **Setup**  
   - Generar `run_id` (UUID v4) si no viene por flag.  
   - Construir `run_folder = exports/<YYYYMMDD_HHMMSS>_<run_id>/`.
2. **Pre-checks (bloqueantes/warn)**  
   - Ejecutar consultas definidas en `sql/etl_quality_checks.sql` (bloque PRE).  
   - Si **alguna métrica bloqueante > 0**, **abortar** con `exit_code=2` y exportar resultados.
3. **Cargar / Transformar (SQL/CALL)**  
   - **No DDL**. Llamar a procedimientos/funciones existentes (p.ej., `CALL etl_demo.sp_cargar_estudiantes(truncate_staging => :flag)`).
   - Registrar conteos `inserted/updated/skipped` en `etl_logs`.
4. **Post-checks**  
   - Ejecutar bloque POST (delta, revalidación FK, etc.). Si bloqueantes > 0 → `exit_code=2`.
5. **Exports**  
   - Guardar artefactos de evidencias (ver sección 4).
6. **(Opcional) Refresh MV**  
   - Si `--refresh-mv`, refrescar vistas materializadas definidas en `docs` (lista a completar en D2/D3).
7. **Fin / Reporte**  
   - Escribir resumen JSON (`run_summary.json`) y CSVs de métricas. Cerrar `etl_logs`.

## 4) Salidas / Artefactos
Dentro de `exports/<timestamp>_<run_id>/`:
- `run_summary.json`:
  ```json
  {
    "run_id": "<uuid>",
    "started_at": "<iso8601>",
    "ended_at": "<iso8601>",
    "status": "ok|warn|fail",
    "inserted": 0,
    "updated": 0,
    "skipped": 0,
    "errors": 0,
    "gates": { "blocking_failed": false, "warnings": 0 },
    "paths": {
      "pre_checks_csv": "dq_pre_checks.csv",
      "post_checks_csv": "dq_post_checks.csv",
      "logs_csv": "etl_logs.csv"
    }
  }
dq_pre_checks.csv y dq_post_checks.csv (columnas: metric, severity, value, expected, status).

etl_logs.csv (extracto de la tabla etl_logs de la BD).

(Opcional) dumps ad-hoc: preview_upsert.csv, delta_resumen.csv.

## 5) Tabla etl_logs (especificación mínima)

Definición funcional (no ejecutar hoy). Se materializa en D2.

### Campos:

- run_id UUID, step TEXT (setup|pre|load|post|export|refresh|done)
- status TEXT (started|ok|warn|fail)
- inserted INT, updated INT, skipped INT, errors INT,
- metric JSONB (clave→valor para métricas específicas),
- started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
- error_msg TEXT, extra JSONB.

**Claves**: (run_id, step, started_at).

## 6) Métricas estandarizadas (nombres y severidad)

Prefijo dq_, fase pre|post, p.ej.:

dq_pre_nulos_criticos_alumnos — bloqueante

dq_pre_duplicados_por_dni — bloqueante

dq_pre_dni_formato_invalido — bloqueante

dq_pre_genero_fuera_dominio — warning

dq_pre_nota_fuera_rango — bloqueante

dq_pre_fk_departamento_huerfana — bloqueante

dq_post_delta_staging_destino — bloqueante

dq_post_revalidacion_fk — bloqueante

Regla de gates: si cualquier métrica bloqueante > 0, el orquestador devuelve exit_code=2 y status=fail.

## 7) Errores / Exit codes

0: éxito (sin bloqueantes; warnings permitidos).

1: argumentos inválidos / archivo no encontrado.

2: fallas de gates bloqueantes (pre/post).

3: error SQL (CALL/TRANSACCIONAL).

4: errores de export (E/S).

5: otros inesperados.

## 8) Idempotencia / Reentrancia

--dry-run no persiste cambios; solo pre-checks y plan de upsert.

Con --truncate-staging=false, el upsert debe ser reentrante (clave natural: dni).

Reprocesos con el mismo run_id deben no duplicar logs; se registran como warn si se detecta reintento.

## 9) Parámetros abiertos (completar en D2/D3)

Nombre real de tablas staging/destino.

Lista de MV a refrescar.

Valores esperados por métrica (para status=ok/warn/fail en export).