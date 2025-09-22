--PARTE A TRIGGERS (AUDITORIA + UPDATED_AT)

CREATE TABLE IF NOT EXISTS etl_demo.log_estudiantes(
    log_id bigserial PRIMARY KEY,
    estudiantes_id bigint,
    op text,
    old_row jsonb,
    new_row jsonb,
    ts timestamptz DEFAULT now()
);

--TRIGGER: cambio de fecha de actualización
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


--CONSTRAINS
ALTER TABLE etl_demo.demo_estudiantes
  ADD CONSTRAINT nn_dni    CHECK (dni IS NOT NULL) NOT VALID,
  ADD CONSTRAINT chk_gen   CHECK (genero IN ('M','F') OR genero IS NULL) NOT VALID,
  ADD CONSTRAINT chk_nota  CHECK (nota BETWEEN 0 AND 20 OR nota IS NULL) NOT VALID;

ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT nn_dni;
ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT chk_gen;
ALTER TABLE etl_demo.demo_estudiantes VALIDATE CONSTRAINT chk_nota;
