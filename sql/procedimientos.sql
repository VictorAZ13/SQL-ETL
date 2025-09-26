-- FILE: procedimientos.sql
-- INTENT: Wrapper delgado para orquestador (opcional).
-- FLUJO CANÓNICO: cleanup → dedup → upsert → logs → (opcional) refresh MV / truncate staging.
-- SCOPE: Sin DDL; DDL permanece en constraints/triggers/vistas.
-- IDEMPOTENCIA: Cada etapa debe ser reentrante; controlar por claves naturales/run_id.
-- RUN_ORDER: 40

-- [SECTION] PROCEDURE/FUNCTION: <etl_demo.fn_normaliza_nota / etl_demo.sp_cargar_estudiantes >
-- PRE: constraints válidas; staging listo
-- POST: logs registrados; salidas en /exports (D2/D3)
-- DEPENDE DE: <constraints and quality checks>
-- SOURCE: sql/others/query_dia9_2.sql (líneas X–Y)
-- SQL original (copiado sin cambios) ↓↓↓
-- ...

CREATE OR REPLACE FUNCTION etl_demo.fn_normaliza_nota(n numeric)
RETURNS numeric 
LANGUAGE plpgsql AS $$
BEGIN
    IF n IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN GREATEST(0,LEAST(1,n/20.0));
END $$;

--PROCEDIMIENTO DE INSERCION
CREATE OR REPLACE PROCEDURE etl_demo.sp_cargar_estudiantes(truncate_staging boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE 
    v_run uuid := gen_random_uuid();
    v_upsert int := 0;
BEGIN
    WITH cleaned AS(
        SELECT
            trim(id_text) AS id_text,
            trim(dni) AS dni,
            initcap(trim(nombre)) AS nombre,
            initcap(trim(apellido)) AS apellido,
            upper(trim(genero)) AS genero,
            curso_id,
            CASE WHEN nota IS NULL THEN NULL
                WHEN nota < 0 THEN 0
                ELSE nota END AS nota,
            etl_demo.fn_normaliza_nota(nota) AS nota_1
        FROM etl_demo.staging_estudiantes
    ),
    filtered AS(
        SELECT *
        FROM cleaned 
        WHERE dni IS NOT NULL 
            AND coalesce(length(apellido),0) > 0
    )
    INSERT INTO etl_demo.demo_estudiantes(dni,nombre,apellido,genero,curso_id,nota,nota_1,updated_at)
    SELECT
        f.dni,
        f.nombre,
        f.apellido,
        CASE WHEN f.genero IN ('M','F') THEN f.genero ELSE NULL END,
        f.curso_id,
        LEAST(20,COALESCE(f.nota,0))::numeric(5,2),
        f.nota_1,
        now()
    FROM filtered f 
    ON CONFLICT (dni) DO UPDATE 
        SET nombre = EXCLUDED.nombre,
            apellido  = EXCLUDED.apellido,
            genero    = EXCLUDED.genero,
            curso_id  = EXCLUDED.curso_id,
            nota      = EXCLUDED.nota,
            nota_1    = EXCLUDED.nota_1,
            updated_at= now();
    GET DIAGNOSTICS v_upsert  = ROW_COUNT;

    INSERT INTO etl_demo.etl_logs(run_id,tabla,accion,filas_afectadas,status,msg)
    VALUES (v_run,'demo_estudiantes','upsert',v_upsert,'ok','carga staging-destino con limpieza basica');
    IF truncate_staging THEN
        TRUNCATE etl_demo.staging_estudiantes;
        INSERT INTO etl_demo.etl_logs(run_id,tabla,accion,filas_afectadas,status,msg)
        VALUES (v_run,'staging_estudiantes','truncate',0,'ok','TRUNCATE staging');
    END IF;

    EXCEPTION WHEN OTHERS THEN
    INSERT INTO etl_demo.etl_logs(run_id,tabla,accion,filas_afectadas,status,msg)
    VALUES (v_run,'demo_estudiantes','upsert',0,'failed',SQLERRM);
    RAISE;
END$$;


--ejecutar funcion
CALL etl_demo.sp_cargar_estudiantes(false);

-- Conteos y verificación
SELECT COUNT(*) AS destino_filas FROM etl_demo.demo_estudiantes;
SELECT dni,nombre,apellido,genero,nota,nota_1 FROM etl_demo.demo_estudiantes ORDER BY dni;
SELECT * FROM etl_demo.etl_logs ORDER BY ts DESC LIMIT 5;