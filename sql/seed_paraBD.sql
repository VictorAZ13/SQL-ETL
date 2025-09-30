-- Limpieza por si quedaron TEMP de otra sesión
DROP TABLE IF EXISTS tmp_calificacion;
DROP TABLE IF EXISTS tmp_matricula;
DROP TABLE IF EXISTS tmp_profesor_asignacion;
DROP TABLE IF EXISTS tmp_grupo;
DROP TABLE IF EXISTS tmp_estudiante;
DROP TABLE IF EXISTS tmp_profesor;
DROP TABLE IF EXISTS tmp_curso;
DROP TABLE IF EXISTS tmp_periodo;
DROP TABLE IF EXISTS tmp_carrera;
DROP TABLE IF EXISTS tmp_departamento;

-- Reproducible
SELECT setseed(0.42);

-- ====== TEMP sin esquema ======
CREATE TEMP TABLE IF NOT EXISTS tmp_departamento(dept_code text primary key, nombre text) ON COMMIT DROP;
INSERT INTO tmp_departamento
SELECT format('DEPT_%02s', i), format('Departamento %s', i)
FROM generate_series(1,5) i;

CREATE TEMP TABLE IF NOT EXISTS tmp_carrera(carr_code text primary key, nombre text, dept_code text) ON COMMIT DROP;
INSERT INTO tmp_carrera
SELECT format('CARR_%02s', i), format('Carrera %s', i),
       (SELECT dept_code FROM tmp_departamento ORDER BY random() LIMIT 1)
FROM generate_series(1,12) i;

CREATE TEMP TABLE IF NOT EXISTS tmp_periodo(periodo_code text primary key, fecha_inicial date, fecha_final date) ON COMMIT DROP;
INSERT INTO tmp_periodo VALUES
('2024-I','2024-03-01','2024-07-31'),
('2024-II','2024-08-01','2024-12-15'),
('2025-I','2025-03-01','2025-07-31')
ON CONFLICT (periodo_code) DO NOTHING;

CREATE TEMP TABLE IF NOT EXISTS tmp_curso(curso_code text primary key, nombre text, creditos int, dept_code text) ON COMMIT DROP;
INSERT INTO tmp_curso
SELECT format('CUR%04s', i), format('Curso %s', i),
       3 + (random()*3)::int,
       (SELECT dept_code FROM tmp_departamento ORDER BY random() LIMIT 1)
FROM generate_series(1,100) i
ON CONFLICT (curso_code) DO NOTHING;

CREATE TEMP TABLE IF NOT EXISTS tmp_profesor(profesor_dni text primary key, nombre text) ON COMMIT DROP;
INSERT INTO tmp_profesor
SELECT format('DNI%08s', 5000+i), format('Profesor %s', i)
FROM generate_series(1,120) i
ON CONFLICT (profesor_dni) DO NOTHING;

CREATE TEMP TABLE IF NOT EXISTS tmp_estudiante(estudiante_dni text primary key, nombre text) ON COMMIT DROP;
INSERT INTO tmp_estudiante
SELECT format('DNI%08s', i), format('Estudiante %s', i)
FROM generate_series(1,2000) i
ON CONFLICT (estudiante_dni) DO NOTHING;

-- ====== Grupos deterministas (2 por curso x periodo) ======
CREATE TEMP TABLE IF NOT EXISTS tmp_grupo(
  grupo_code text primary key,
  periodo_code text,
  curso_code text,
  capacidad int
) ON COMMIT DROP;

INSERT INTO tmp_grupo
SELECT format('G%05s', row_number() OVER (ORDER BY p.periodo_code, c.curso_code, gs)),
       p.periodo_code, c.curso_code,
       30 + (random()*16)::int
FROM tmp_curso c
CROSS JOIN tmp_periodo p
CROSS JOIN LATERAL (SELECT generate_series(1,2) AS gs) s
ON CONFLICT (grupo_code) DO NOTHING;

-- ====== Asignación de profesor (1 por grupo) ======
CREATE TEMP TABLE IF NOT EXISTS tmp_profesor_asignacion(
  grupo_code text,
  profesor_dni text,
  UNIQUE (grupo_code, profesor_dni)
) ON COMMIT DROP;

INSERT INTO tmp_profesor_asignacion
SELECT g.grupo_code,
       (SELECT profesor_dni FROM tmp_profesor ORDER BY random() LIMIT 1)
FROM tmp_grupo g
ON CONFLICT (grupo_code, profesor_dni) DO NOTHING;

-- ====== Matrícula (muestreo realista 60–95% de capacidad) ======
CREATE TEMP TABLE IF NOT EXISTS tmp_matricula(
  estudiante_dni text,
  grupo_code text,
  UNIQUE (estudiante_dni, grupo_code)
) ON COMMIT DROP;

WITH target AS (
  SELECT grupo_code, (capacidad * (0.60 + 0.35*random()))::int AS n
  FROM tmp_grupo
)
INSERT INTO tmp_matricula
SELECT e.estudiante_dni, t.grupo_code
FROM target t
CROSS JOIN LATERAL (
  SELECT estudiante_dni
  FROM tmp_estudiante
  ORDER BY random()
  LIMIT t.n
) e
ON CONFLICT (estudiante_dni, grupo_code) DO NOTHING;

-- ====== Calificación (0..20, media ~13) ======
CREATE TEMP TABLE IF NOT EXISTS tmp_calificacion(
  estudiante_dni text,
  grupo_code text,
  nota numeric(4,1)
) ON COMMIT DROP;

INSERT INTO tmp_calificacion
SELECT m.estudiante_dni, m.grupo_code,
       round(GREATEST(0, LEAST(20, 10 + 6*random() + 4*(random()-0.5)))::numeric, 1)
FROM tmp_matricula m;

-- ====== Para exportar desde DBeaver (Result Grid → Export → CSV) ======
-- Ejecuta cada SELECT y usa el asistente de exportación.
SELECT *, 'batch_20250928' AS batch_id FROM tmp_departamento;
-- CSV 1
SELECT *, 'batch_20250928' AS batch_id FROM tmp_carrera;
-- CSV 2
SELECT *, 'batch_20250928' AS batch_id FROM tmp_periodo;
-- CSV 3
SELECT *, 'batch_20250928' AS batch_id FROM tmp_curso;
-- CSV 4
SELECT *, 'batch_20250928' AS batch_id FROM tmp_profesor;
-- CSV 5
SELECT *, 'batch_20250928' AS batch_id FROM tmp_estudiante;
-- CSV 6
SELECT *, 'batch_20250928' AS batch_id FROM tmp_grupo;
-- CSV 7
SELECT *, 'batch_20250928' AS batch_id FROM tmp_profesor_asignacion;
-- CSV 8
SELECT *, 'batch_20250928' AS batch_id FROM tmp_matricula;
-- CSV 9
SELECT *, 'batch_20250928' AS batch_id FROM tmp_calificacion;
