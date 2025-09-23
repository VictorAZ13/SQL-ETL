-- FILE: constraints.sql
-- INTENT: Declarar/activar constraints con enfoque idempotente.
-- ORDER: Antes de triggers y vistas/MV.
-- IDEMPOTENCIA: Usar NOT VALID → VALIDATE para minimizar bloqueos.
-- SAFE NOTES: Validar en ventanas; documentar fallos en docs/etl_run.md.
-- RUN_ORDER: 10

--CONSTRAINS
-- [SECTION] CONSTRAINTS: <demo_estudiantes "copia de estudiantes para pruebas">
-- PRE: <estudiantes> existe
-- POST (D2): VALIDATE CONSTRAINT; registrar resultados en docs/etl_run.md
-- SOURCE: sql/others/query_dia9_3.sql  (líneas X–Y)
-- SQL original (copiado sin cambios) ↓↓↓
-- ...
ALTER TABLE etl_demo.demo_estudiantes
  ADD CONSTRAINT nn_dni    CHECK (dni IS NOT NULL) NOT VALID,
  ADD CONSTRAINT chk_gen   CHECK (genero IN ('M','F') OR genero IS NULL) NOT VALID,
  ADD CONSTRAINT chk_nota  CHECK (nota BETWEEN 0 AND 20 OR nota IS NULL) NOT VALID;

ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT nn_dni;
ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT chk_gen;
ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT chk_nota;


