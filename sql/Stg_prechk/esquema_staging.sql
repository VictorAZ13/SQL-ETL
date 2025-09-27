--- CREAR ESQUEMAS DE STAGING
CREATE SCHEMA IF NOT EXISTS stg;

-- Crear Stagings para cada tabla
-- STAGING Periodo

CREATE TABLE stg.stg_periodo(
    -- BK y columnas upsert
    periodo_code text,
    fecha_inicial date,
    fecha_final date,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_periodo_bk
  on stg.stg_periodo (periodo_code);

create index if not exists ix_stg_periodo_batch
  on stg.stg_periodo (batch_id);

---STAGING DEPARTAMENTO
CREATE TABLE stg.stg_departamento(
    -- BK y columnas upsert
    dept_code text,
    dept_name text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_departamento_bk
  on stg.stg_departamento (dept_code);

create index if not exists ix_stg_departamento_batch
  on stg.stg_departamento (batch_id);

---STAGING CARRERA
CREATE TABLE stg.stg_carrera(
    -- BK y columnas upsert
    carr_code text,
    carr_name text,
    dept_code text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_carrera_bk
  on stg.stg_carrera (carr_code);

create index if not exists ix_stg_carrera_batch
  on stg.stg_carrera (batch_id);

create index if not exists ix_stg_carrera_fk_dep
  on stg.stg_carrera (dept_code);

---STAGING Curso
CREATE TABLE stg.stg_curso(
    -- BK y columnas upsert
    curso_code text,
    curso_name text,
    creditos int,
    dept_code text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_curso_bk
  on stg.stg_curso (curso_code);

create index if not exists ix_stg_curso_batch
  on stg.stg_curso (batch_id);

create index if not exists ix_stg_curso_fk_dep
  on stg.stg_curso (dept_code);

---STAGING PROFESOR
CREATE TABLE stg.stg_profesor(
    -- BK y columnas upsert
    profesor_dni text,
    profesor_name text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_profesor_bk
  on stg.stg_profesor (profesor_dni);

create index if not exists ix_stg_profesor_batch
  on stg.stg_profesor (batch_id);

---STAGING ESTUDIANTE
CREATE TABLE stg.stg_estudiante(
    -- BK y columnas upsert
    estudiante_dni text,
    estudiante_name text,
    carr_code text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_estudiante_bk
  on stg.stg_estudiante (estudiante_dni);

create index if not exists ix_stg_estudiante_batch
  on stg.stg_estudiante (batch_id);

create index if not exists ix_stg_estudiante_fk_carr
  on stg.stg_estudiante (carr_code);

---STAGING GRUPO
CREATE TABLE stg.stg_grupo(
    -- BK y columnas upsert
    grupo_code text,
    periodo_code text,
    curso_code text,
    grupo_cap int,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_grupo_bk
  on stg.stg_grupo (grupo_code,periodo_code,curso_code);

create index if not exists ix_stg_grupo_batch
  on stg.stg_grupo (batch_id);

create index if not exists ix_stg_grupo_fk_per
  on stg.stg_grupo (periodo_code);

create index if not exists ix_stg_grupo_fk_cur
  on stg.stg_grupo (curso_code);

---STAGING profesor_asignacion
CREATE TABLE stg.stg_profesor_asignacion(
    -- BK y columnas upsert
    grupo_code text,
    profesor_dni text,
    rol text,
    -- METADATA
    batch_id text, -- id de la carga
    source_system text,  -- nombre del origen (la orquestación captura eso)
    source_file text, --archivo de origen
    source_row int, --cual es el indice en el archivo origen
    extract_ts timestamptz, --cuando se hizo la extracción
    load_ts timestamptz default now(), --fecha de carga
    op_type text, --En caso haga I/U/D en mi pipeline
    record_hash text, --hash para deduplicados (crea una firma para evaluar igualdades en la información)
    is_quarantined boolean default false, --por si no paso prechecks
    reject_reason text --razon de la cuarentena
);
-- index para acelerar inserts, updates (Pues Motor SQL viaja por filas y demora mas mientras el indice se encuentre mas profundo)
create index if not exists ix_stg_profesor_asignacion_bk
  on stg.stg_profesor_asignacion (grupo_code,profesor_dni);

create index if not exists ix_stg_profesor_asignacion_batch
  on stg.stg_profesor_asignacion (batch_id);

create index if not exists ix_stg_profesor_asignacion_fk_prof
  on stg.stg_profesor_asignacion (profesor_dni);

create index if not exists ix_stg_profesor_asignacion_fk_grup
  on stg.stg_profesor_asignacion (grupo_code);

--STAGIN MATRICULA
CREATE TABLE stg.stg_matricula (

  -- BK compuesta en destino: (estudiante_dni, grupo_id)
  estudiante_dni   text,         -- FK por código → estudiante
  grupo_code       text,         -- FK por código → grupo

  -- ===== Metadatos (row-level) =====
  batch_id         text,         -- id del lote que orquesta D4+
  source_system    text,         -- alias del origen (csv_manual, siga, etc.)
  source_file      text,         -- archivo/flujo de donde vino
  source_row       int,          -- nro. de línea en el archivo origen
  extract_ts       timestamptz,  -- cuándo se extrajo
  load_ts          timestamptz default now(), -- cuándo se cargó a staging
  op_type          text,         -- I/U/D si usas CDC (opcional)
  record_hash      text,         -- hash para dedup exacto (opcional)
  is_quarantined   boolean default false, -- marcado por pre-checks
  reject_reason    text          -- motivo (breve) si va a cuarentena
);

-- Índices (no únicos) para acelerar pre-checks y upserts
create index if not exists ix_stg_matricula_bk
  on stg.stg_matricula (estudiante_dni, grupo_code);

create index if not exists ix_stg_matricula_batch
  on stg.stg_matricula (batch_id);

create index if not exists ix_stg_matricula_fk_est
  on stg.stg_matricula (estudiante_dni);

create index if not exists ix_stg_matricula_fk_grp
  on stg.stg_matricula (grupo_code);

--STAGIN calificacion
create table stg.stg_calificacion (

  -- BK compuesta en destino: (estudiante_dni, grupo_id)
  estudiante_dni   text,
  grupo_code text,         
  nota       int,         

  -- ===== Metadatos (row-level) =====
  batch_id         text,         -- id del lote que orquesta D4+
  source_system    text,         -- alias del origen (csv_manual, siga, etc.)
  source_file      text,         -- archivo/flujo de donde vino
  source_row       int,          -- nro. de línea en el archivo origen
  extract_ts       timestamptz,  -- cuándo se extrajo
  load_ts          timestamptz default now(), -- cuándo se cargó a staging
  op_type          text,         -- I/U/D si usas CDC (opcional)
  record_hash      text,         -- hash para dedup exacto (opcional)
  is_quarantined   boolean default false, -- marcado por pre-checks
  reject_reason    text          -- motivo (breve) si va a cuarentena
);

-- Índices (no únicos) para acelerar pre-checks y upserts
create index if not exists ix_stg_calificacion_fk  --fk compuesta
  on stg.stg_calificacion (estudiante_dni,grupo_code);

create index if not exists ix_stg_calificacion_batch
  on stg.stg_calificacion (batch_id);

