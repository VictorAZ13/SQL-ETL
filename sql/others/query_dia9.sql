--DIA 9 CONSTRAINS PROCEDIMIENTOS ALMACENADOS
--BLOQUE 1
---CREACIÓN DEL ESQUEMA Y EXTENSIONES A USAR DE LOS STAGINGS
CREATE SCHEMA IF NOT EXISTS etl_demo;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

--CREACIÓN DE LAS TABLAS STAGINGS(TABLAS DE ALMACENAMIENTO RAPIDO)
DROP TABLE IF EXISTS etl_demo.staging_estudiantes CASCADE;
CREATE TABLE etl_demo.staging_estudiantes (
    id_text text,
    dni text,
    nombre text,
    apellido text,
    genero text,
    curso_id int,
    nota numeric,
    created_at timestamptz DEFAULT now()
);

--CREACIÓN TABLA ORIGEN DE EJEMPLO (demo_estudiantes)
DROP TABLE IF EXISTS etl_demo.demo_estudiantes CASCADE;
CREATE TABLE etl_demo.demo_estudiantes (
    estudiantes_id bigserial PRIMARY KEY,
    dni text UNIQUE,
    nombre text NOT NULL,
    apellido text NOT NULL,
    genero text,
    curso_id int,
    nota numeric(5,2),
    nota_1 numeric(4,2),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- LOGS (con run_id ??? UUD)
DROP TABLE IF EXISTS etl_demo.etl_logs CASCADE;
CREATE TABLE etl_demo.etl_logs(
    log_id bigserial PRIMARY KEY,
    run_id uuid DEFAULT get_random_uuid(),
    tabla text,
    accion text,
    filas_afectadas INT,
    status text CHECK(status in ('ok','failed')),
    msg text,
    ts timestamptz DEFAULT now()
);