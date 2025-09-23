-- FILE: etl_quality_checks.sql
-- INTENT: Especificación de checks pre/post (conteos = 0 → OK).
-- PRE: nulos críticos, duplicados por DNI, FKs huérfanas, rangos/dominios.
-- POST: conteos staging/destino, delta, revalidación FK, drift simple.
-- SEVERIDAD: bloqueante | warning (gates de calidad).
-- RUN_ORDER: 30
-- ==== PRECHECKS (staging/demostración) =======================================
-- [PRE] dq_pre_nulos_criticos_alumnos | severidad=bloqueante | salida=COUNT(*)
-- Campos críticos: dni, nombre, apellido
SELECT COUNT(*) AS dq_pre_nulos_criticos_alumnos
FROM etl_demo.demo_estudiantes
WHERE dni IS NULL OR nombre IS NULL OR apellido IS NULL;

-- [PRE] dq_pre_duplicados_por_dni | severidad=bloqueante | salida=COUNT(*)
SELECT COUNT(*) AS dq_pre_duplicados_por_dni
FROM (
  SELECT dni
  FROM etl_demo.demo_estudiantes
  WHERE dni IS NOT NULL
  GROUP BY dni
  HAVING COUNT(*) > 1
) d;

-- [PRE] dq_pre_dni_formato_invalido | severidad=bloqueante | salida=COUNT(*)
-- Regla: DNI debe ser 8 dígitos
SELECT COUNT(*) AS dq_pre_dni_formato_invalido
FROM etl_demo.demo_estudiantes
WHERE dni IS NOT NULL AND (dni !~ '^[0-9]{8}$');

-- [PRE] dq_pre_genero_fuera_dominio | severidad=warning | salida=COUNT(*)
-- Dominio: 'M','F' (permitimos NULL)
SELECT COUNT(*) AS dq_pre_genero_fuera_dominio
FROM etl_demo.demo_estudiantes
WHERE genero IS NOT NULL AND genero NOT IN ('M','F');

-- [PRE] dq_pre_nota_fuera_rango | severidad=bloqueante | salida=COUNT(*)
-- Rango de nota: 0..20 (NULL permitido)
SELECT COUNT(*) AS dq_pre_nota_fuera_rango
FROM etl_demo.demo_estudiantes
WHERE nota IS NOT NULL AND NOT (nota BETWEEN 0 AND 20);

-- [PRE] dq_pre_fk_departamento_huerfana | severidad=bloqueante | salida=COUNT(*)
-- TODO: Ajustar nombres reales si tu tabla/columna difiere.
-- Ejemplo asumiendo demo_estudiantes.departamento_id -> departamentos.id
-- SELECT COUNT(*) AS dq_pre_fk_departamento_huerfana
-- FROM etl_demo.demo_estudiantes e
-- LEFT JOIN etl_demo.departamentos d ON d.id = e.departamento_id
-- WHERE e.departamento_id IS NOT NULL AND d.id IS NULL;

-- ==== POSTCHECKS (tras upsert a destino) =====================================
-- [POST] dq_post_delta_staging_destino | severidad=bloqueante | salida=COUNT(*)
-- TODO: Definir tablas reales staging/destino. Placeholder:
-- SELECT ABS( (SELECT COUNT(*) FROM etl_demo.stg_estudiantes)
--           - (SELECT COUNT(*) FROM etl_demo.dim_estudiantes) ) AS dq_post_delta_staging_destino;

-- [POST] dq_post_revalidacion_fk | severidad=bloqueante | salida=COUNT(*)
-- Repetimos huerfanas tras la carga; debe ser 0
-- TODO: Mismo join de PRE, aplicado sobre destino.

-- [POST] dq_post_logs_consistentes | severidad=warning | salida=COUNT(*)
-- Consistencia básica: inserted + updated + skipped == filas consideradas
-- TODO: Depende de etl_logs. Placeholder de especificación.
