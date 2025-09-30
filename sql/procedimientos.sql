-- FILE: procedimientos.sql
-- INTENT: Wrapper delgado para orquestador (opcional).
-- FLUJO CANÓNICO: cleanup → dedup → upsert → logs → (opcional) refresh MV / truncate staging.
-- SCOPE: Sin DDL; DDL permanece en constraints/triggers/vistas.
-- IDEMPOTENCIA: Cada etapa debe ser reentrante; controlar por claves naturales/run_id.
-- RUN_ORDER: 40

-- Quitar duplicados
-- PERIODO  (BK: periodo_code)
create or replace view stg.vw_stg_periodo_dedup as
with r as (
  select p.*,
         row_number() over (
           partition by util.fn_normal_valores(p.periodo_code) --BK
           order by p.load_ts desc nulls last,
                    p.source_row desc nulls last,
                    p.extract_ts desc nulls last,
                    coalesce(p.source_file,'')
         ) rn
  from stg.stg_periodo p
  where not p.is_quarantined
)
select * from r where rn = 1;

-- DEPARTAMENTO  (BK: dept_code)
create or replace view stg.vw_stg_departamento_dedup as
with r as (
  select d.*,
         row_number() over (
           partition by util.fn_normal_valores(d.dept_code)
           order by d.load_ts desc nulls last,
                    d.source_row desc nulls last,
                    d.extract_ts desc nulls last,
                    coalesce(d.source_file,'')
         ) rn
  from stg.stg_departamento d
  where not d.is_quarantined
)
select * from r where rn = 1;

-- CARRERA  (BK: carr_code)
create or replace view stg.vw_stg_carrera_dedup as
with r as (
  select c.*,
         row_number() over (
           partition by util.fn_normal_valores(c.carr_code)
           order by c.load_ts desc nulls last,
                    c.source_row desc nulls last,
                    c.extract_ts desc nulls last,
                    coalesce(c.source_file,'')
         ) rn
  from stg.stg_carrera c
  where not c.is_quarantined
)
select * from r where rn = 1;

-- CURSO  (BK: curso_code)
create or replace view stg.vw_stg_curso_dedup as
with r as (
  select c.*,
         row_number() over (
           partition by util.fn_normal_valores(c.curso_code)
           order by c.load_ts desc nulls last,
                    c.source_row desc nulls last,
                    c.extract_ts desc nulls last,
                    coalesce(c.source_file,'')
         ) rn
  from stg.stg_curso c
  where not c.is_quarantined
)
select * from r where rn = 1;

-- PROFESOR  (BK: profesor_dni)
create or replace view stg.vw_stg_profesor_dedup as
with r as (
  select p.*,
         row_number() over (
           partition by util.fn_normal_valores(p.profesor_dni)
           order by p.load_ts desc nulls last,
                    p.source_row desc nulls last,
                    p.extract_ts desc nulls last,
                    coalesce(p.source_file,'')
         ) rn
  from stg.stg_profesor p
  where not p.is_quarantined
)
select * from r where rn = 1;

-- ESTUDIANTE  (BK: estudiante_dni)
create or replace view stg.vw_stg_estudiante_dedup as
with r as (
  select e.*,
         row_number() over (
           partition by util.fn_normal_valores(e.estudiante_dni)
           order by e.load_ts desc nulls last,
                    e.source_row desc nulls last,
                    e.extract_ts desc nulls last,
                    coalesce(e.source_file,'')
         ) rn
  from stg.stg_estudiante e
  where not e.is_quarantined
)
select * from r where rn = 1;

-- GRUPO  (BK compuesta: grupo_code, curso_code, periodo_code)
create or replace view stg.vw_stg_grupo_dedup as
with r as (
  select g.*,
         row_number() over (
           partition by
             util.fn_normal_valores(g.grupo_code),
             util.fn_normal_valores(g.curso_code),
             util.fn_normal_valores(g.periodo_code)
           order by g.load_ts desc nulls last,
                    g.source_row desc nulls last,
                    g.extract_ts desc nulls last,
                    coalesce(g.source_file,'')
         ) rn
  from stg.stg_grupo g
  where not g.is_quarantined
)
select * from r where rn = 1;

-- PROFESOR_ASIGNACION  (BK compuesta: grupo_code, profesor_dni)
create or replace view stg.vw_stg_profesor_asignacion_dedup as
with r as (
  select pa.*,
         row_number() over (
           partition by
             util.fn_normal_valores(pa.grupo_code),
             util.fn_normal_valores(pa.profesor_dni)
           order by pa.load_ts desc nulls last,
                    pa.source_row desc nulls last,
                    pa.extract_ts desc nulls last,
                    coalesce(pa.source_file,'')
         ) rn
  from stg.stg_profesor_asignacion pa
  where not pa.is_quarantined
)
select * from r where rn = 1;

-- MATRICULA  (BK compuesta: estudiante_dni, grupo_code)
create or replace view stg.vw_stg_matricula_dedup as
with r as (
  select m.*,
         row_number() over (
           partition by
             util.fn_normal_valores(m.estudiante_dni),
             util.fn_normal_valores(m.grupo_code)
           order by m.load_ts desc nulls last,
                    m.source_row desc nulls last,
                    m.extract_ts desc nulls last,
                    coalesce(m.source_file,'')
         ) rn
  from stg.stg_matricula m
  where not m.is_quarantined
)
select * from r where rn = 1;

-- CALIFICACION  (en staging: (estudiante_dni, grupo_code); en 3FN será por matricula_id)
create or replace view stg.vw_stg_calificacion_dedup as
with r as (
  select c.*,
         row_number() over (
           partition by
             util.fn_normal_valores(c.estudiante_dni),
             util.fn_normal_valores(c.grupo_code)
           order by c.load_ts desc nulls last,
                    c.source_row desc nulls last,
                    c.extract_ts desc nulls last,
                    coalesce(c.source_file,'')
         ) rn
  from stg.stg_calificacion c
  where not c.is_quarantined
)
select * from r where rn = 1;

--UPSERTS
begin;

-- 1) DIMENSIONES (orden: depto, periodo, carrera, curso, profesor, estudiante)

-- DEPARTAMENTO (BK: dept_code)
insert into proyecto_etl.departamento (dept_code, dept_name)
select util.fn_normal_valores(v.dept_code), v.dept_name
from stg.vw_stg_departamento_dedup v
on conflict (dept_code) do update
set dept_name = excluded.dept_name;

-- PERIODO (BK: periodo_code)
insert into proyecto_etl.periodo (periodo_code, fecha_inicial, fecha_final)
select util.fn_normal_valores(v.periodo_code), v.fecha_inicial, v.fecha_final
from stg.vw_stg_periodo_dedup v
on conflict (periodo_code) do update
set fecha_inicial = excluded.fecha_inicial,
    fecha_final   = excluded.fecha_final;

-- CARRERA (BK: carr_code)  FK: dept_id (lookup por dept_code)
insert into proyecto_etl.carrera (carr_code, carr_name, dept_id)
select util.fn_normal_valores(v.carr_code)   as carr_code,
       v.carr_name,
       d.dept_id
from stg.vw_stg_carrera_dedup v
join proyecto_etl.departamento d
  on d.dept_code = util.fn_normal_valores(v.dept_code)
on conflict (carr_code) do update
set carr_name = excluded.carr_name,
    dept_id   = excluded.dept_id;

-- CURSO (BK: curso_code)  FK: dept_id (lookup por dept_code)
insert into proyecto_etl.curso (curso_code, curso_name, creditos, dept_id)
select util.fn_normal_valores(v.curso_code)  as curso_code,
       v.curso_name,
       v.creditos,
       d.dept_id
from stg.vw_stg_curso_dedup v
join proyecto_etl.departamento d
  on d.dept_code = util.fn_normal_valores(v.dept_code)
on conflict (curso_code) do update
set curso_name = excluded.curso_name,
    creditos   = excluded.creditos,
    dept_id    = excluded.dept_id;

-- PROFESOR (BK: profesor_dni)
insert into proyecto_etl.profesor (profesor_dni, profesor_name)
select util.fn_normal_valores(v.profesor_dni), v.profesor_name
from stg.vw_stg_profesor_dedup v
on conflict (profesor_dni) do update
set profesor_name = excluded.profesor_name;

-- ESTUDIANTE (BK: estudiante_dni)  FK: carr_id (lookup por carr_code)
insert into proyecto_etl.estudiante (estudiante_dni, estudiante_name, carr_id)
select util.fn_normal_valores(v.estudiante_dni) as estudiante_dni,
       v.estudiante_name,
       c.carr_id
from stg.vw_stg_estudiante_dedup v
join proyecto_etl.carrera c
  on c.carr_code = util.fn_normal_valores(v.carr_code)
-- where v.batch_id = 'TU_BATCH'
on conflict (estudiante_dni) do update
set estudiante_name = excluded.estudiante_name,
    carr_id         = excluded.carr_id;

-- 2) HECHOS (orden: grupo, profesor_asignacion, matricula, calificacion)

-- GRUPO (BK compuesta: grupo_code, curso_id, periodo_id)
insert into proyecto_etl.grupo (grupo_code, curso_id, periodo_id, grupo_cap)
select util.fn_normal_valores(v.grupo_code) as grupo_code,
       cu.curso_id,
       pe.periodo_id,
       v.grupo_cap
from stg.vw_stg_grupo_dedup v
join proyecto_etl.curso   cu on cu.curso_code   = util.fn_normal_valores(v.curso_code)
join proyecto_etl.periodo pe on pe.periodo_code = util.fn_normal_valores(v.periodo_code)
on conflict (grupo_code, curso_id, periodo_id) do update
set grupo_cap = excluded.grupo_cap;

-- PROFESOR_ASIGNACION (BK compuesta: grupo_id, profesor_id)
insert into proyecto_etl.profesor_asignacion (grupo_id, profesor_id, rol)
select g.grupo_id,
       p.profesor_id,
       v.rol
from stg.vw_stg_profesor_asignacion_dedup v
join proyecto_etl.grupo    g on g.grupo_code    = util.fn_normal_valores(v.grupo_code)
join proyecto_etl.profesor p on p.profesor_dni  = util.fn_normal_valores(v.profesor_dni)
on conflict (grupo_id, profesor_id) do update
set rol = excluded.rol;

-- MATRICULA (BK compuesta: estudiante_id, grupo_id)
insert into proyecto_etl.matricula (estudiante_id, grupo_id)
select e.estudiante_id,
       g.grupo_id
from stg.vw_stg_matricula_dedup v
join proyecto_etl.estudiante e on e.estudiante_dni = util.fn_normal_valores(v.estudiante_dni)
join proyecto_etl.grupo      g on g.grupo_code     = util.fn_normal_valores(v.grupo_code)
on conflict (estudiante_id, grupo_id) do nothing;

-- CALIFICACION (BK: matricula_id)
insert into proyecto_etl.calificacion (matricula_id, nota)
select m.matricula_id,
       v.nota
from stg.vw_stg_calificacion_dedup v
join proyecto_etl.estudiante e on e.estudiante_dni = util.fn_normal_valores(v.estudiante_dni)
join proyecto_etl.grupo      g on g.grupo_code     = util.fn_normal_valores(v.grupo_code)
join proyecto_etl.matricula  m on m.estudiante_id = e.estudiante_id
                              and m.grupo_id      = g.grupo_id
on conflict (matricula_id) do update
set nota = excluded.nota;

commit;

-- :::::::::::::::::::::::::::::::::::::::::::::
--PROCEDIMIENTO ALMACENADO
-- :::::::::::::::::::::::::::::::::::::::::::::
create or replace procedure proyecto_etl.sp_orquestar_batch(p_batch text default null)
language plpgsql
as $$
declare
  v_run_id   bigint;
  t0         timestamptz;
  v_in       int;
  v_ins      int;
  v_upd      int;
  v_skipped  int;
  
  -- helper para filtro por batch
  -- (usaremos "where (p_batch is null or v.batch_id = p_batch)" en cada SELECT)
begin
  -- === inicio de corrida ===
  insert into proyecto_etl.audit_run(batch_id) values (p_batch)
  returning run_id into v_run_id;

  -- =====================================
  -- 0) CLEANUP mínimo (marca quarantine)
  -- =====================================
  -- Reglas de dominio rápidas (idempotentes)
  update stg.stg_curso
     set is_quarantined = true, reject_reason = 'DOMINIO: creditos<=0'
   where (p_batch is null or batch_id = p_batch)
     and creditos is not null and creditos <= 0;

  update stg.stg_calificacion
     set is_quarantined = true, reject_reason = 'DOMINIO: nota fuera 0..20'
   where (p_batch is null or batch_id = p_batch)
     and nota is not null and not (nota between 0 and 20);

  update stg.stg_periodo
     set is_quarantined = true, reject_reason = 'REGLA: fecha_inicial>fecha_final'
   where (p_batch is null or batch_id = p_batch)
     and fecha_inicial > fecha_final;

  -- =====================================
  -- 1) DIMENSIONES
  -- =====================================

  -- DEPARTAMENTO
  t0 := clock_timestamp();
  select count(*) into v_in --INTO POR QUE?
    from stg.vw_stg_departamento_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.departamento (dept_code, dept_name) --
    select util.fn_normal_valores(v.dept_code), v.dept_name
      from stg.vw_stg_departamento_dedup v
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (dept_code) do update
       set dept_name = excluded.dept_name
    returning (xmax = 0) as inserted
  )
  select
    count(*) filter (where inserted), --FILTER POR QUE?
    count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'departamento', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- PERIODO
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_periodo_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.periodo (periodo_code, fecha_inicial, fecha_final)
    select util.fn_normal_valores(v.periodo_code), v.fecha_inicial, v.fecha_final
      from stg.vw_stg_periodo_dedup v
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (periodo_code) do update
       set fecha_inicial = excluded.fecha_inicial,
           fecha_final   = excluded.fecha_final
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'periodo', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- CARRERA
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_carrera_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.carrera (carr_code, carr_name, dept_id)
    select util.fn_normal_valores(v.carr_code),
           v.carr_name,
           d.dept_id
      from stg.vw_stg_carrera_dedup v
      join proyecto_etl.departamento d
        on d.dept_code = util.fn_normal_valores(v.dept_code)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (carr_code) do update
       set carr_name = excluded.carr_name,
           dept_id   = excluded.dept_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'carrera', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- CURSO
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_curso_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.curso (curso_code, curso_name, creditos, dept_id)
    select util.fn_normal_valores(v.curso_code),
           v.curso_name,
           v.creditos,
           d.dept_id
      from stg.vw_stg_curso_dedup v
      join proyecto_etl.departamento d
        on d.dept_code = util.fn_normal_valores(v.dept_code)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (curso_code) do update
       set curso_name = excluded.curso_name,
           creditos   = excluded.creditos,
           dept_id    = excluded.dept_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'curso', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- PROFESOR
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_profesor_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.profesor (profesor_dni, profesor_name)
    select util.fn_normal_valores(v.profesor_dni),
           v.profesor_name
      from stg.vw_stg_profesor_dedup v
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (profesor_dni) do update
       set profesor_name = excluded.profesor_name
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'profesor', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- ESTUDIANTE
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_estudiante_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.estudiante (estudiante_dni, estudiante_name, carr_id)
    select util.fn_normal_valores(v.estudiante_dni),
           v.estudiante_name,
           c.carr_id
      from stg.vw_stg_estudiante_dedup v
      join proyecto_etl.carrera c
        on c.carr_code = util.fn_normal_valores(v.carr_code)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (estudiante_dni) do update
       set estudiante_name = excluded.estudiante_name,
           carr_id         = excluded.carr_id
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'estudiante', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- =====================================
  -- 2) HECHOS
  -- =====================================

  -- GRUPO
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_grupo_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.grupo (grupo_code, curso_id, periodo_id, grupo_cap)
    select util.fn_normal_valores(v.grupo_code),
           cu.curso_id,
           pe.periodo_id,
           v.grupo_cap
      from stg.vw_stg_grupo_dedup v
      join proyecto_etl.curso   cu on cu.curso_code   = util.fn_normal_valores(v.curso_code)
      join proyecto_etl.periodo pe on pe.periodo_code = util.fn_normal_valores(v.periodo_code)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (grupo_code, curso_id, periodo_id) do update
       set grupo_cap = excluded.grupo_cap
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'grupo', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- PROFESOR_ASIGNACION
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_profesor_asignacion_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.profesor_asignacion (grupo_id, profesor_id, rol)
    select g.grupo_id,
           p.profesor_id,
           v.rol
      from stg.vw_stg_profesor_asignacion_dedup v
      join proyecto_etl.grupo    g on g.grupo_code   = util.fn_normal_valores(v.grupo_code)
      join proyecto_etl.profesor p on p.profesor_dni = util.fn_normal_valores(v.profesor_dni)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (grupo_id, profesor_id) do update
       set rol = excluded.rol
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'profesor_asignacion', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- MATRICULA
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_matricula_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.matricula (estudiante_id, grupo_id)
    select e.estudiante_id,
           g.grupo_id
      from stg.vw_stg_matricula_dedup v
      join proyecto_etl.estudiante e on e.estudiante_dni = util.fn_normal_valores(v.estudiante_dni)
      join proyecto_etl.grupo      g on g.grupo_code     = util.fn_normal_valores(v.grupo_code)
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (estudiante_id, grupo_id) do nothing
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)  -- en DO NOTHING, "not inserted" ~ conflictos
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'matricula', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- CALIFICACION
  t0 := clock_timestamp();
  select count(*) into v_in
    from stg.vw_stg_calificacion_dedup v
   where (p_batch is null or v.batch_id = p_batch);

  with d as (
    insert into proyecto_etl.calificacion (matricula_id, nota)
    select m.matricula_id,
           v.nota
      from stg.vw_stg_calificacion_dedup v
      join proyecto_etl.estudiante e on e.estudiante_dni = util.fn_normal_valores(v.estudiante_dni)
      join proyecto_etl.grupo      g on g.grupo_code     = util.fn_normal_valores(v.grupo_code)
      join proyecto_etl.matricula  m on m.estudiante_id = e.estudiante_id
                                    and m.grupo_id      = g.grupo_id
     where (p_batch is null or v.batch_id = p_batch)
    on conflict (matricula_id) do update
       set nota = excluded.nota
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
  into v_ins, v_upd
  from d;

  insert into proyecto_etl.audit_step
    (run_id, table_name, rows_input, rows_inserted, rows_updated, rows_skipped, finished_at, duration_ms)
  values
    (v_run_id, 'calificacion', v_in, v_ins, v_upd, null, clock_timestamp(),
     extract(milliseconds from (clock_timestamp()-t0))::int);

  -- === fin ok ===
  update proyecto_etl.audit_run
     set finished_at = now(), status = 'ok'
   where run_id = v_run_id;

exception when others then
  update proyecto_etl.audit_run
     set finished_at = now(), status = 'error', notes = sqlerrm
   where run_id = v_run_id;
  raise;
end;
$$;


