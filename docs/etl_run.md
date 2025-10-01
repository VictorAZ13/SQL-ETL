# ETL Runbook

## Inventario de esquema (borrador)
- Tablas: …
- Vistas/MV: …
- Triggers: …
- Constraints pendientes de VALIDATE: …

## Brechas
- Staging → Orquestación (SP) → KPIs → Snapshot → Variación → Export.

## Resultados de VALIDATE (D1 - placeholder)
- `ALTER TABLE … VALIDATE CONSTRAINT …`: pendiente de ejecución (D2)

## Resultados de checks (D1 - tabla vacía)
| métrica | severidad | esperado | observado | status |
|--------|-----------|----------|-----------|--------|
| nulos_críticos_alumnos | bloqueante | 0 | - | pendiente |
| duplicados_por_DNI | bloqueante | 0 | - | pendiente |
| fks_huérfanas | bloqueante | 0 | - | pendiente |
| notas_fuera_rango | warning | 0 | - | pendiente |
| delta_staging_vs_destino | bloqueante | 0 | - | pendiente |


## Bitácora (D1)
- Logros:
- Dificultades/Bloqueos:
- Observaciones/Decisiones:
- Evidencias adjuntas:
  
| paso | archivo                             | depende de            |
| ---- | ----------------------------------- | --------------------- |
| 1    | sql/constraints.sql                 | DDL tablas            |
| 2    | sql/triggers.sql                    | constraints           |
| 3    | sql/procedimientos.sql              | constraints, triggers |
| 4    | sql/etl\_quality\_checks.sql (pre)  | staging listo         |
| 5    | sql/etl\_quality\_checks.sql (post) | carga realizada       |

## Casos borde (Dataset sintético D1)
| caso                                  | qty | check que dispara                 | severidad     |
|---------------------------------------|----:|-----------------------------------|---------------|
| DNI nulo                              |  2 | dq_pre_nulos_criticos_alumnos     | bloqueante    |
| Nombre/Apellido nulo                  |  3 | dq_pre_nulos_criticos_alumnos     | bloqueante    |
| DNI duplicado                         |  3 | dq_pre_duplicados_por_dni         | bloqueante    |
| DNI formato inválido (no 8 dígitos)   |  3 | dq_pre_dni_formato_invalido       | bloqueante    |
| Género fuera de dominio               |  2 | dq_pre_genero_fuera_dominio       | warning       |
| Nota fuera de rango (<0 o >20)        |  4 | dq_pre_nota_fuera_rango           | bloqueante    |
| FK departamento huérfana              |  2 | dq_pre_fk_departamento_huerfana   | bloqueante    |
| Ruido de limpieza (espacios/case)     |  4 | (no bloquea; valida normalización)| warning       |

**Totales sintéticos esperados (borrador):**
- nulos_criticos_alumnos = 5
- duplicados_por_dni = 3 (claves repetidas en grupos de 2+)
- dni_formato_invalido = 3
- genero_fuera_dominio = 2
- nota_fuera_rango = 4
- fk_departamento_huerfana = 2

## Glosario (BK/SK/relaciones)
| Tabla                | SK (PK técnica)  | BK (negocio)                             | FKs                                      | NOT NULL mínimos                    | Notas/decisiones                            |
| -------------------- | ---------------- | ---------------------------------------- | ---------------------------------------- | ----------------------------------- | ------------------------------------------- |
| periodo              | periodo\_id      | periodo\_code                            | –                                        | periodo\_code, fechas               | CHECK fechas                                |
| departamento         | dept\_id         | dept\_code                               | –                                        | dept\_code, dept\_name              | –                                           |
| carrera              | carr\_id         | carr\_code                               | dept\_id → departamento                  | carr\_code, dept\_id                | –                                           |
| curso                | curso\_id        | curso\_code                              | dept\_id → departamento                  | curso\_code, dept\_id, creditos     | CHECK créditos                              |
| profesor             | profesor\_id     | profesor\_dni                            | –                                        | profesor\_dni, profesor\_name       | –                                           |
| estudiante           | estudiante\_id   | estudiante\_dni                          | carr\_id → carrera                       | estudiante\_dni, carr\_id           | –                                           |
| grupo                | grupo\_id        | (grupo\_code, curso_id, periodo_id)                              | periodo\_id → periodo; curso\_id → curso | grupo\_code, periodo\_id, curso\_id | CHECK capacidad                             |
| profesor\_asignacion | asignacion\_id   | (grupo\_id, profesor\_id) *(+ opc. rol)* | grupo → grupo; profesor → profesor       | grupo\_id, profesor\_id, rol        | Ver reglas de unicidad                      |
| matricula            | matricula\_id    | (estudiante\_id, grupo\_id)              | estudiante → estudiante; grupo → grupo   | estudiante\_id, grupo\_id           | Evitar duplicidad                           |
| calificacion         | calificacion\_id | (matricula\_id)                          | matricula → matricula                    | matricula\_id, nota                 | 1 nota por matrícula (o definir componente) |

## Constraints posibles
| Tabla                | Constraint          | Tipo   | Columnas/Referencia            | Severidad | Estado | Comentario                         |
| -------------------- | ------------------- | ------ | ------------------------------ | --------- | ------ | ---------------------------------- |
| periodo              | uq\_periodo\_bk     | UNIQUE | periodo\_code                  | Bloq.     | Hecho   | –                                  |
| departamento         | uq\_depto\_bk       | UNIQUE | dept\_code                     | Bloq.     | Hecho   | –                                  |
| carrera              | uq\_carrera\_bk     | UNIQUE | carr\_code                     | Bloq.     | Hecho   | –                                  |
| curso                | uq\_curso\_bk       | UNIQUE | curso\_code                    | Bloq.     | Hecho   | –                                  |
| profesor             | uq\_profesor\_bk    | UNIQUE | profesor\_dni                  | Bloq.     | Hecho   | –                                  |
| estudiante           | uq\_estud\_bk       | UNIQUE | estudiante\_dni                | Bloq.     | Hecho   | –                                  |
| grupo                | uq\_grupo_periodo_curso       | UNIQUE | (grupo\_code, curso_id, periodo_id)                    | Bloq.     | Hecho   | –                                  |
| profesor\_asignacion | uq\_asignacion\_grupo      | UNIQUE | (grupo\_id, profesor\_id)      | Bloq.     | Hecho   | + opc. **UNIQUE(grupo\_id, rol)**  |
| matricula            | uq\_matricula\_estudiante\_grupo   | UNIQUE | (estudiante\_id, grupo\_id)    | Bloq.     | Hecho   | –                                  |
| calificacion         | uq\_calif\_bk       | UNIQUE | (matricula\_id)                | Bloq.     | Hecho   | Si 1 nota/matrícula                |
| curso                | ck\_curso\_creditos | CHECK  | creditos > 0                   | (W/B)     | Plan   | Según negocio                      |
| grupo                | ck\_grupo\_cap      | CHECK  | grupo\_cap >= 0                | (W/B)     | Plan   | –                                  |
| periodo              | ck\_periodo\_fechas | CHECK  | fecha\_inicial <= fecha\_final | (W/B)     | Plan   | –                                  |
| calificacion         | ck\_calif\_nota     | CHECK  | nota BETWEEN 0 AND 20          | Bloq.     | Plan   | –                                  |
| **FKs (todas)**      | fk\_\*              | FK     | ya creadas (VALID)             | Bloq.     | Hecho  | Añadir acciones (RESTRICT/CASCADE) |

## Indices del ETL 
| Tabla                | Índice sugerido     | Objetivo                                       |
| -------------------- | ------------------- | ---------------------------------------------- |
| carrera              | idx\_carrera\_dept  | Joins por depto                                |
| curso                | idx\_curso\_dept    | Joins por depto                                |
| estudiante           | idx\_estud\_carr    | Joins por carrera                              |
| grupo                | idx\_grupo\_periodo | Filtrar por periodo                            |
| grupo                | idx\_grupo\_curso   | Filtrar por curso                              |
| profesor\_asignacion | idx\_asig\_grupo    | Join por grupo                                 |
| profesor\_asignacion | idx\_asig\_prof     | Join por profesor                              |
| matricula            | idx\_mat\_est       | Join por estudiante                            |
| matricula            | idx\_mat\_grupo     | Join por grupo                                 |
| calificacion         | idx\_calif\_mat     | Join por matrícula                             |
| **(KPI)**            | compuesto a definir | Soporte a VIEW/MV (se afina con consulta real) |

## Matriz de decisión por FK
| Tabla hija          | Columna (FK)  | Tabla padre  | ON DELETE | ON UPDATE | Estado | Justificación                           |
| ------------------- | ------------- | ------------ | --------- | --------- | ------ | --------------------------------------- |
| carrera             | dept_id       | departamento | RESTRICT  | NO ACTION | —      | Catálogo con uso no se borra            |
| curso               | dept_id       | departamento | RESTRICT  | NO ACTION | —      | Evitar cursos huérfanos                 |
| estudiante          | carr_id       | carrera      | RESTRICT  | NO ACTION | —      | Estudiante requiere carrera válida      |
| grupo               | periodo_id    | periodo      | RESTRICT  | NO ACTION | —      | No eliminar periodos con grupos         |
| grupo               | curso_id      | curso        | RESTRICT  | NO ACTION | —      | Grupo depende del curso                 |
| profesor_asignacion | grupo_id      | grupo        | CASCADE   | NO ACTION | —      | Asignaciones dependen del grupo         |
| profesor_asignacion | profesor_id   | profesor     | RESTRICT  | NO ACTION | —      | Evitar borrar profesor con asignaciones |
| matricula           | estudiante_id | estudiante   | RESTRICT  | NO ACTION | —      | Protege histórico de matrículas         |
| matricula           | grupo_id      | grupo        | RESTRICT  | NO ACTION | —      | Primero gestionar matrículas            |
| calificacion        | matricula_id  | matricula    | CASCADE   | NO ACTION | —      | Nota sin matrícula no tiene sentido     |

## CHECKS VALIDADAS (D2)

| Tabla        | Constraint                   | Regla                             | Severidad | Estado | Comentario          |
| ------------ | ---------------------------- | --------------------------------- | --------- | ------ | ------------------- |
| calificacion | ck_calificacion_nota         | `nota BETWEEN 0 AND 20`           | **Bloq.** |        | `nota` NOT NULL   |
| curso        | ck_creditos_no_0             | `creditos > 0`                    | Warn/Bloq |        | Política de negocio |
| grupo        | ck_grupo_cap                 | `grupo_cap >= 0`                  | Warn      |        | –                   |
| periodo      | ck_fecha_fin_mayor_fecha_ini | `fecha_inicial < fecha_final` | Warn/Bloq |        | Decide `<`   |


## Indices de las FKs (Seguimiento de las vistas)

| Tabla               | Índice          | Objetivo                   | Estado |
| ------------------- | --------------- | -------------------------- | ------ |
| carrera             | (dept_id)       | Joins con departamento     |        |
| curso               | (dept_id)       | Joins con departamento     |        |
| estudiante          | (carr_id)       | Joins con carrera          |        |
| grupo               | (periodo_id)    | Filtro por periodo         |        |
| grupo               | (curso_id)      | Filtro por curso           |        |
| profesor_asignacion | (grupo_id)      | Join con grupo             |        |
| profesor_asignacion | (profesor_id)   | Filtro por profesor        |        |
| matricula           | (estudiante_id) | Filtro/join por estudiante |        |
| matricula           | (grupo_id)      | Join con grupo             |        |
| calificacion        | (matricula_id)  | Join con matrícula         |        |

## KPI Elegido (Carga Docente)
| KPI           | Consulta objetivo                        | Tabla | Índice                 | Justificación                                                                               | Estado |
| ------------- | ---------------------------------------- | ----- | ---------------------- | ------------------------------------------------------------------------------------------- | ------ |
| Carga Docente | Top profesores por alumnos en un periodo | grupo | (curso_id, periodo_id) | Acelera filtros combinados en `grupo` antes de unir con `matricula` y `profesor_asignacion` |        |


## KPI de apoyo y variables de apoyo
| Concepto              | Definición                                    | Decisión           |
| --------------------- | --------------------------------------------- | ------------------ |
| alumnos_total         | suma de matrículas en grupos del profesor     | **No DISTINCT**    |
| alumnos_únicos        | estudiantes únicos del profesor en el periodo | Métrica secundaria |
| grupos_total          | count(distinct grupo_id)                      | –                  |
| tasa_ocupación_global | sum(matriculados)/sum(capacidad)              | **Principal**      |
| promedio_ocupación    | avg(matriculados/capacidad por grupo)         | Complementaria     |
| grupos_a_tope         | count(grupo con matriculados >= capacidad)    | –                  |
| cortes                | por carrera, curso, departamento              | Sí                 |
| filtros mínimos       | periodo (obligatorio)                         | Sí                 |
| roles                 | incluir TITULAR/AUX (o catálogo elegido)      | Decide y documenta |
