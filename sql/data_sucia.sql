-- (Opcional) limpieza por si estabas en la misma sesión
DROP TABLE IF EXISTS t_calificacion;
DROP TABLE IF EXISTS t_matricula;
DROP TABLE IF EXISTS t_grp;
DROP TABLE IF EXISTS t_per;
DROP TABLE IF EXISTS t_cur;
DROP TABLE IF EXISTS t_est;

-- Semilla reproducible
SELECT setseed(0.24);

-- Bases temporales "limpias"
CREATE TEMP TABLE t_est AS
SELECT format('DNI%08s', i) AS estudiante_dni,
       format('Estudiante %s', i) AS nombre
FROM generate_series(1,300) i;

CREATE TEMP TABLE t_cur AS
SELECT format('CUR%04s', i) AS curso_code,
       format('Curso %s', i) AS nombre,
       3 + (random()*3)::int AS creditos,
       format('DEPT_%02s', ((random()*4)::int+1)) AS dept_code
FROM generate_series(1,30) i;

CREATE TEMP TABLE t_per AS
SELECT unnest(ARRAY['2024-II','2025-I'])::text AS periodo_code,
       DATE '2024-08-01' AS fecha_inicial,
       DATE '2024-12-15' AS fecha_final;

-- 1) CURSO con duplicados BK, FK rota y créditos inválidos
SELECT curso_code, nombre, creditos, dept_code,
       'batch_20250928' AS batch_id, 'curso_base' AS source_file
FROM t_cur
UNION ALL
SELECT curso_code, nombre, creditos, dept_code,
       'batch_20250928','curso_dup'
FROM (
  SELECT * FROM t_cur ORDER BY random() LIMIT 3
) d
UNION ALL
SELECT 'CUR9999','Curso Fantasma',4,'DEPT_XX',
       'batch_20250928','curso_fk_rota'
UNION ALL
SELECT 'CUR0000','Curso Cero',0,'DEPT_01',
       'batch_20250928','curso_credito_cero'
;

-- 2) GRUPO con capacidades inválidas y periodo inexistente
CREATE TEMP TABLE t_grp AS
SELECT format('G%05s', row_number() OVER (ORDER BY p.periodo_code, c.curso_code, i.gs)) AS grupo_code,
       p.periodo_code,
       c.curso_code,
       30 + (random()*16)::int AS capacidad
FROM t_cur c
CROSS JOIN t_per p
CROSS JOIN LATERAL (SELECT generate_series(1,1) AS gs) i
ORDER BY p.periodo_code, c.curso_code;

SELECT grupo_code, periodo_code, curso_code, capacidad,
       'batch_20250928' AS batch_id, 'grupo_base' AS source_file
FROM t_grp
UNION ALL
SELECT 'G99998','2024-II',(SELECT curso_code FROM t_cur LIMIT 1), -5,
       'batch_20250928','grupo_cap_neg'
UNION ALL
SELECT 'G99999','2026-I',(SELECT curso_code FROM t_cur LIMIT 1), 35,
       'batch_20250928','grupo_periodo_fk'
;

-- 3) ESTUDIANTE con DNI inválidos y duplicados
SELECT estudiante_dni, nombre,
       'batch_20250928' AS batch_id, 'est_base' AS source_file
FROM t_est
UNION ALL
SELECT estudiante_dni, nombre,
       'batch_20250928','est_dup'
FROM (
  SELECT * FROM t_est ORDER BY random() LIMIT 2
) d
UNION ALL
SELECT 'DNI123', 'Estudiante Inválido',
       'batch_20250928','est_dni_inval'
;

-- 4) MATRÍCULA con duplicados y grupo inexistente
WITH pick AS (
  SELECT e.estudiante_dni, g.grupo_code
  FROM t_est e
  JOIN (SELECT grupo_code FROM t_grp ORDER BY random() LIMIT 20) g ON true
  ORDER BY random() LIMIT 150
)
SELECT estudiante_dni, grupo_code,
       'batch_20250928' AS batch_id, 'mat_base' AS source_file
FROM pick
UNION ALL
SELECT estudiante_dni, grupo_code,
       'batch_20250928','mat_dup'
FROM (
  SELECT * FROM pick ORDER BY random() LIMIT 5
) d
UNION ALL
SELECT (SELECT estudiante_dni FROM t_est LIMIT 1), 'GXXXXX',
       'batch_20250928','mat_fk_rota'
;

-- 5) CALIFICACIÓN fuera de rango
SELECT estudiante_dni, grupo_code, 21.0 AS nota,
       'batch_20250928' AS batch_id, 'cal_fuera_rango' AS source_file
FROM (
  SELECT e.estudiante_dni,
         (SELECT grupo_code FROM t_grp ORDER BY random() LIMIT 1) AS grupo_code
  FROM t_est e
  ORDER BY random() LIMIT 5
) s
;
