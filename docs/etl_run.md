# ETL Runbook

## Inventario de esquema (borrador)
- Tablas: …
- Vistas/MV: …
- Triggers: …
- Constraints pendientes de VALIDATE: …

## Brechas
- …

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

## Evidencias del run
- (D1: capturas/listados de estructura, encabezados SQL)

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
