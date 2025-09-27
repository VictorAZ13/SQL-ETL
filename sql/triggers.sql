-- FILE: triggers.sql
-- INTENT: Triggers separados por responsabilidad.
-- AUDITORÍA: INSERT/UPDATE/DELETE → tabla audit_* (NO mezclar con orquestación).
-- UPDATED_AT: Trigger específico por tabla (independiente de auditoría).
-- ORDEN: Después de constraints válidos.
-- RUN_ORDER: 20
--TRIGGER: cambio de fecha de actualización

-- Tablas de auditoria
-- Resumen por corrida
create table if not exists proyecto_etl.audit_run(
  run_id      bigserial primary key,
  batch_id    text,
  started_at  timestamptz default now(),
  finished_at timestamptz,
  status      text,     -- ok | error
  notes       text
);

-- Detalle por tabla/paso
create table if not exists proyecto_etl.audit_step(
  step_id      bigserial primary key,
  run_id       bigint references proyecto_etl.audit_run(run_id),
  table_name   text,
  rows_input   int,
  rows_inserted int,
  rows_updated  int,
  rows_skipped  int,
  started_at   timestamptz default now(),
  finished_at  timestamptz,
  duration_ms  int,
  notes        text
);

--TRIGGERS

-- Función genérica updated_at
create or replace function util.fn_set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

-- Crea el trigger only-where-available (si la tabla tiene updated_at)
do $$
declare r record;
begin
  for r in
    select table_schema, table_name
    from information_schema.columns
    where table_schema = 'proyecto_etl' and column_name = 'updated_at'
  loop
    execute format('drop trigger if exists trg_set_updated_at on %I.%I;', r.table_schema, r.table_name);
    execute format(
      'create trigger trg_set_updated_at before update on %I.%I
         for each row execute function util.fn_set_updated_at();',
      r.table_schema, r.table_name
    );
  end loop;
end $$;
