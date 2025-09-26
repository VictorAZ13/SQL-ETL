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


--UNICIDAD DE BK (REGLAS O CLAVES DE NEGOCIO)
--BK No hay duplicicidad de alumnos en los grupos /grupo_id enlazado a un curso y grupo por lo que no solo representa un grupo sino el grupo y el curso)
ALTER TABLE proyecto_etl.matricula
  ADD CONSTRAINT uq_matricula_estudiante_grupo
  UNIQUE (estudiante_id,grupo_id);

--BK para asegurar que un profe no se duplique en los grupo o viceversa que un grupo no tenga profesores duplicados
ALTER TABLE proyecto_etl.profesor_asignacion
  ADD CONSTRAINT uq_asignacion_grupo
  UNIQUE (grupo_id,profesor_id);

--BK para asegurar un unico rol por grupo
ALTER TABLE proyecto_etl.profesor_asignacion
  ADD CONSTRAINT uq_asignacion_grupo_rol
  UNIQUE (grupo_id,rol);

--BK para asegurar que exista solo una calificación por matricula
ALTER TABLE proyecto_etl.calificacion
  ADD CONSTRAINT uq_calificacion
  UNIQUE (matricula_id);

--BK para que un grupo en un periodo puede tener infinidad de cursos no duplicados
ALTER TABLE proyecto_etl.grupo
  ADD CONSTRAINT uq_grupo_periodo_curso
  UNIQUE (grupo_code,curso_id,periodo_id);

-- Acciones que se realizarán en caso haya cambios en tablas padre
ALTER TABLE proyecto_etl.carrera
  ADD CONSTRAINT fk_dept_id
  FOREIGN KEY (dept_id)
  REFERENCES proyecto_etl.departamento(dept_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.curso
  ADD CONSTRAINT fk_dept_id_curso
  FOREIGN KEY (dept_id)
  REFERENCES proyecto_etl.departamento(dept_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.estudiante
  ADD CONSTRAINT fk_carr_id
  FOREIGN KEY (carr_id)
  REFERENCES proyecto_etl.carrera(carr_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.grupo
  ADD CONSTRAINT fk_per_id
  FOREIGN KEY (periodo_id)
  REFERENCES proyecto_etl.periodo(periodo_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.grupo
  ADD CONSTRAINT fk_curso_id
  FOREIGN KEY (curso_id)
  REFERENCES proyecto_etl.curso(curso_id)
  ON DELETE RESTRICT
  ON UPDATE NOT ACTION 
  NOT VALID;

ALTER TABLE proyecto_etl.profesor_asignacion
  ADD CONSTRAINT fk_grupo_id
  FOREIGN KEY (grupo_id)
  REFERENCES proyecto_etl.grupo(grupo_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION 
  NOT VALID;

ALTER TABLE proyecto_etl.profesor_asignacion
  ADD CONSTRAINT fk_profesor_id
  FOREIGN KEY (profesor_id)
  REFERENCES proyecto_etl.profesor(profesor_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.matricula
  ADD CONSTRAINT fk_estudiante_id
  FOREIGN KEY (estudiante_id)
  REFERENCES proyecto_etl.estudiante(estudiante_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.matricula
  ADD CONSTRAINT fk_grupo_id_matr
  FOREIGN KEY (grupo_id)
  REFERENCES proyecto_etl.grupo(grupo_id)
  ON DELETE RESTRICT
  ON UPDATE NO ACTION
  NOT VALID;

ALTER TABLE proyecto_etl.calificacion
  ADD CONSTRAINT fk_matricula_id
  FOREIGN KEY (matricula_id)
  REFERENCES proyecto_etl.matricula(matricula_id)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
  NOT VALID;


-- CHECKS
-- Check para saber la nota de 0 a 20
ALTER TABLE proyecto_etl.calificacion
  ADD CONSTRAINT ck_calificacion_nota
  CHECK (nota IS NULL OR BETWEEN 0 AND 20) NOT VALID; 

-- Check creditos 
ALTER TABLE proyecto_etl.curso
  ADD CONSTRAINT ck_creditos_no_0
  CHECK (creditos > 0) NOT VALID;

-- Check capacidad
ALTER TABLE proyecto_etl.grupo
  ADD CONSTRAINT ck_grupo_cap
  CHECK (grupo_cap >= 0) NOT VALID;

-- Fecha periodo
ALTER TABLE proyecto_etl.periodo
  ADD CONSTRAINT ck_fecha_fin_mayor_fecha_ini
  CHECK(fecha_inicial IS NULL OR fecha_final IS NULL OR fecha_inicial < fecha_final) NOT VALID;

