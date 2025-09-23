-- FILE: triggers.sql
-- INTENT: Triggers separados por responsabilidad.
-- AUDITORÍA: INSERT/UPDATE/DELETE → tabla audit_* (NO mezclar con orquestación).
-- UPDATED_AT: Trigger específico por tabla (independiente de auditoría).
-- ORDEN: Después de constraints válidos.
-- RUN_ORDER: 20
--TRIGGER: cambio de fecha de actualización

-- [GROUP] Auditoría (NO orquestación)
-- PRE: tablas audit_* existen
-- SOURCE: sql/others/.sql
-- SQL original ↓↓↓
-- ...

-- [GROUP] updated_at (independiente de auditoría)
-- PRE: columna updated_at existe
-- SOURCE: sql/others/query_dia9_3.sql
-- SQL original ↓↓↓
-- ...

CREATE OR REPLACE FUNCTION etl_demo.trg_set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_set_updated_at ON etl_demo.demo_estudiantes;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON etl_demo.demo_estudiantes
FOR EACH ROW EXECUTE FUNCTION etl_demo.trg_set_updated_at();

--trigger inserción de tabla auditoria (que se modifico)
CREATE OR REPLACE FUNCTION etl_demo.trg_audit_estudiantes()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO etl_demo.log_estudiantes(estudiante_id, op, old_row, new_row)
  VALUES (COALESCE(NEW.estudiantes_id, OLD.estudiantes_id), TG_OP, to_jsonb(OLD), to_jsonb(NEW));
  RETURN CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN NEW ELSE OLD END;
END$$;

DROP TRIGGER IF EXISTS trg_audit_insupd ON etl_demo.demo_estudiantes;
CREATE TRIGGER trg_audit_insupd
AFTER INSERT OR UPDATE ON etl_demo.demo_estudiantes
FOR EACH ROW EXECUTE FUNCTION etl_demo.trg_audit_estudiantes();
