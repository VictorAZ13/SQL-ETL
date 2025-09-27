-- 01_prechecks.sql
\echo '=========================================================='
\echo 'PRE-CHECKS STAGING (nulos BK, duplicados BK, huérfanas, dominio, formato)'
\echo '=========================================================='

-- =========================
-- 1) NULOS CRÍTICOS (BK)
-- =========================
\echo '1) Nulos críticos en BK'
select 'stg_periodo' tbl, 'periodo_code null' chk, count(*) n
from stg.stg_periodo where periodo_code is null
union all
select 'stg_departamento','dept_code null', count(*) from stg.stg_departamento where dept_code is null
union all
select 'stg_carrera','carr_code null', count(*) from stg.stg_carrera where carr_code is null
union all
select 'stg_curso','curso_code null', count(*) from stg.stg_curso where curso_code is null
union all
select 'stg_profesor','profesor_dni null', count(*) from stg.stg_profesor where profesor_dni is null
union all
select 'stg_estudiante','estudiante_dni null', count(*) from stg.stg_estudiante where estudiante_dni is null
union all
select 'stg_grupo','grupo_code null', count(*) from stg.stg_grupo where grupo_code is null
union all
select 'stg_profesor_asignacion','grupo_code|profesor_dni null', count(*) 
from stg.stg_profesor_asignacion where grupo_code is null or profesor_dni is null
union all
select 'stg_matricula','estudiante_dni|grupo_code null', count(*) 
from stg.stg_matricula where estudiante_dni is null or grupo_code is null
union all
select 'stg_calificacion','estudiante_dni|grupo_code null', count(*) 
from stg.stg_calificacion where estudiante_dni is null or grupo_code is null
;

-- Muestras
select * from stg.stg_curso where curso_code is null limit 5;
select * from stg.stg_estudiante where estudiante_dni is null limit 5;

-- =========================
-- 2) DUPLICADOS DE BK
-- =========================
\echo '2) Duplicados de BK'
-- Dimensiones
select 'stg_departamento' tbl, dept_code bk, count(*) cnt
from stg.stg_departamento group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_carrera', carr_code, count(*) 
from stg.stg_carrera group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_curso', curso_code, count(*) 
from stg.stg_curso group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_profesor', profesor_dni, count(*) 
from stg.stg_profesor group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_estudiante', estudiante_dni, count(*) 
from stg.stg_estudiante group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_periodo', periodo_code, count(*) 
from stg.stg_periodo group by 2 having count(*)>1 order by 3 desc limit 20;

-- Hechos (BK compuestas)
select 'stg_grupo' tbl, grupo_code||'|'||periodo_code||'|'||curso_code bk, count(*) cnt
from stg.stg_grupo group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_profesor_asignacion', grupo_code||'|'||profesor_dni bk, count(*) 
from stg.stg_profesor_asignacion group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_matricula', estudiante_dni||'|'||grupo_code bk, count(*) 
from stg.stg_matricula group by 2 having count(*)>1 order by 3 desc limit 20;

select 'stg_calificacion', estudiante_dni||'|'||grupo_code bk, count(*) 
from stg.stg_calificacion group by 2 having count(*)>1 order by 3 desc limit 20;

-- =========================
-- 3) FKs HUÉRFANAS (por CÓDIGO)
-- =========================
\echo '3) FKs huérfanas por código'
-- carrera -> departamento
select 'stg_carrera' tbl, c.carr_code hijo_bk, c.dept_code fk_code
from stg.stg_carrera c
left join stg.stg_departamento d on d.dept_code = c.dept_code
where c.dept_code is not null and d.dept_code is null
limit 20;

-- curso -> departamento
select 'stg_curso', c.curso_code, c.dept_code
from stg.stg_curso c
left join stg.stg_departamento d on d.dept_code = c.dept_code
where c.dept_code is not null and d.dept_code is null
limit 20;

-- grupo -> curso / periodo
select 'stg_grupo', g.grupo_code, g.curso_code
from stg.stg_grupo g
left join stg.stg_curso c on c.curso_code = g.curso_code
where g.curso_code is not null and c.curso_code is null
limit 20;

select 'stg_grupo', g.grupo_code, g.periodo_code
from stg.stg_grupo g
left join stg.stg_periodo p on p.periodo_code = g.periodo_code
where g.periodo_code is not null and p.periodo_code is null
limit 20;

-- estudiante -> carrera
select 'stg_estudiante', e.estudiante_dni, e.carr_code
from stg.stg_estudiante e
left join stg.stg_carrera c on c.carr_code = e.carr_code
where e.carr_code is not null and c.carr_code is null
limit 20;

-- profesor_asignacion -> grupo / profesor
select 'stg_profesor_asignacion', pa.grupo_code, pa.profesor_dni
from stg.stg_profesor_asignacion pa
left join stg.stg_grupo g on g.grupo_code = pa.grupo_code
left join stg.stg_profesor pr on pr.profesor_dni = pa.profesor_dni
where (pa.grupo_code is not null and g.grupo_code is null)
   or (pa.profesor_dni is not null and pr.profesor_dni is null)
limit 20;

-- matricula -> estudiante / grupo
select 'stg_matricula', m.estudiante_dni, m.grupo_code
from stg.stg_matricula m
left join stg.stg_estudiante e on e.estudiante_dni = m.estudiante_dni
left join stg.stg_grupo g on g.grupo_code = m.grupo_code
where (m.estudiante_dni is not null and e.estudiante_dni is null)
   or (m.grupo_code is not null and g.grupo_code is null)
limit 20;

-- calificacion -> (estudiante, grupo)
select 'stg_calificacion', c.estudiante_dni, c.grupo_code
from stg.stg_calificacion c
left join stg.stg_matricula m
  on m.estudiante_dni = c.estudiante_dni and m.grupo_code = c.grupo_code
where (c.estudiante_dni is not null and c.grupo_code is not null)
  and m.estudiante_dni is null
limit 20;

-- =========================
-- 4) DOMINIO / REGLAS
-- =========================
\echo '4) Dominio / reglas'
-- creditos > 0
select 'stg_curso' tbl, count(*) creditos_invalidos
from stg.stg_curso where creditos is not null and creditos <= 0;

-- nota 0..20
select 'stg_calificacion' tbl, count(*) notas_fuera_rango
from stg.stg_calificacion where nota is not null and not (nota between 0 and 20);

-- grupo_cap >= 0
select 'stg_grupo' tbl, count(*) capacidad_invalida
from stg.stg_grupo where grupo_cap is not null and grupo_cap < 0;

-- periodo: fecha_inicial <= fecha_final
select 'stg_periodo' tbl, count(*) fechas_invertidas
from stg.stg_periodo
where fecha_inicial is not null and fecha_final is not null
  and fecha_inicial > fecha_final;

-- =========================
-- 5) FORMATO / CANÓNICAS (muestras)
-- =========================
\echo '5) Formato / canónicas (muestras)'
-- DNI 8 dígitos
select estudiante_dni from stg.stg_estudiante
where estudiante_dni is not null and estudiante_dni !~ '^[0-9]{8}$' limit 20;

-- códigos con espacios o minúsculas
select curso_code from stg.stg_curso
where curso_code ~ '\s' or curso_code != upper(trim(curso_code)) limit 20;

\echo '==================== FIN PRE-CHECKS ====================='
