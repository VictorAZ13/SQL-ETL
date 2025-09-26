# ETL - SQL

En este repositorio fue creado con el propósito de aprender acerca de pipeline en ambitos aplicados, partiendo de la idea de la transformación y el guardado de csv o excel a base de datos sql que podran ser consultados y tratados para **reportes o BI**.


## Arquitectura (alto nivel)

PostgreSQL (schema proyecto_etl) con SRP:
DDL base: tablas 3FN para periodo, departamento, carrera, curso, profesor, estudiante, grupo, profesor_asignacion, matricula, calificacion.

Constraints: BK/SK, FKs con acciones, CHECKs de dominio.

Índices: por PK/UNIQUE y FKs; 1 índice KPI.

Runbook (docs/etl_run.md): evidencias de validaciones y decisiones.

ETL app-céntrico (D5–D7): Python orchestration (flags; pre/post-checks; logs).

Capa de consulta (D6): 1 VIEW + 1 MV para KPIs; REFRESH controlado.

Diagrama sugerido: fuentes (CSV) → staging → sp_cargar_* (UPSERT) → tablas destino → VIEW/MV → BI.

## 🔧 Requisitos
- Python 3.11+  
- PostgreSQL 14+  
- DBeaver (cliente gráfico para SQL **opcional**)  
- VS Code (editor personal recomendado)  

---

## Pasos de ejecución (borrador)
1) Pre-checks de calidad (SQL)
2) Carga: `CALL sp_etl_run(...);` (wrapper thin) o pasos app-céntricos
3) Post-checks, export a `/exports`, refresh de vistas/

---
## 📌 Próximos pasos
- Poyecto SQL <-> Orquestador (Python)

---
## Avances del proyecto
 **Dia 1**: 
- [X] Estructura creada
- [x] Plantilla `docs/etl_run.md`
- [x] Encabezados en `sql/*.sql`
- [x] 1er commit “init scaffolding”
## Orquestador (app-céntrico) — Contrato
Ver **docs/orchestrator_contract.md**.  
No-alcance: el orquestador no ejecuta DDL (constraints/triggers/vistas); solo orquesta pre/post checks, llamadas SQL y exporta evidencias.

---
 **Dia 2**: 
- [x] Claves BK compuestas
- [x] FKs indices y constraints (Acciones referenciales)
- [x] KPI escogido
- [ ] 1er commit “init scaffolding”
- [ ] 
## Orquestador (app-céntrico) — Contrato
Ver **docs/orchestrator_contract.md**.  
No-alcance: el orquestador no ejecuta DDL (constraints/triggers/vistas); solo orquesta pre/post checks, llamadas SQL y exporta evidencias.


## Plan de implementación

D2: aplicar constraints (BK, FKs con acciones), CHECKs, índices; actualizar runbook.

D3: preparar staging + pre-checks (nulos críticos, duplicados BK, FKs huérfanas por código, fuera de rango); remediar.

D4: functions set-based + procedure orquestador SQL (cleanup→dedup→upsert→logs), triggers (auditoría/updated_at).

D5–D7: orquestación Python (flags --dry-run, --truncate-staging, pre/post-checks, export a /exports), VIEW/MV KPIs, medición “antes/después”, README final.

---

##  Objetivos de aprendizaje
- Practicar consultas SQL básicas (SELECT, WHERE, ORDER BY, LIMIT, etc.).
- Integrar Python con PostgreSQL para construir un pipeline ETL.
- Preparar la base para análisis y visualización de datos.
- Aprender buenas prácticas de versionado y organización de proyectos.

---

## 📚 Avances de conocimiento
- **Día 1**: Setup de proyecto, importación de dataset y primeras consultas SQL.  
  - SELECT, WHERE, ORDER BY, LIMIT.  
  - Primer commit del repo.
- **Día 2**: Funciones de agregación y agrupación (SQL)
  - Practiqué funciones de agregación: `COUNT`, `MIN`, `MAX`, `AVG`, `SUM`.
  - Aprendí a usar `GROUP BY` para resumir datos categóricos (género, curso, departamento).
  - Usé `HAVING` para filtrar resultados después de agrupar.
  - Combiné `GROUP BY + HAVING + ORDER BY` para generar rankings (ej. curso con más alumnos, departamento con mejor promedio).
  - Reto resuelto: encontrar el departamento con mejor promedio de calificación entre los que tienen más de 8 alumnos.
- **Día 3**:JOINS en SQL: Contenido del día

    - Repaso conceptual de INNER, LEFT, RIGHT y FULL OUTER JOIN.
    - Creación e importación de dataset (CSV → BDEAVER).
- **Día 3**: JOINS en SQL  
  - Repaso conceptual de INNER, LEFT, RIGHT y FULL OUTER JOIN.  
  - Creación e importación de dataset (CSV → DBeaver).  
  - Consultas iniciales:  
    - Estudiantes con cursos (INNER JOIN).  
    - Estudiantes sin curso (LEFT JOIN).  
    - Cursos sin profesor (LEFT JOIN).  
    - Profesores sin curso (LEFT JOIN + filtro).  
  - Mini-proyecto: unir estudiantes, cursos y profesores con 2 JOINS encadenados.  
  - Aprendizajes clave:  
    - LEFT JOIN es el más útil en reporting, muy similar a `merge` de pandas o BUSCARV de Excel.  
    - INNER JOIN asegura consistencia de datos, útil para limpieza.  
    - RIGHT/FULL OUTER JOIN tienen menos uso en la práctica.  

- **Día 4**: Subconsultas y CTEs  
  - Practiqué **subconsultas en SELECT y WHERE** para cálculos intermedios.  
  - Usé **CTEs (WITH)** para estructurar consultas complejas en pasos más legibles.  
  - Construí rankings de alumnos dentro de cada curso usando subquery + ORDER BY.  
  - Ejercicio clave: detectar cursos con notas sobre el promedio global utilizando CTEs.  
  - Aprendizaje: las CTEs facilitan lectura y mantenimiento, sobre todo frente a subconsultas anidadas.

- **Día 5**: Funciones de agregación avanzadas y primeras Window Functions  
  - Revisé `AVG`, `SUM`, `MAX`, `MIN` aplicadas como **funciones ventana**.  
  - Introducción a `ROW_NUMBER`, `RANK`, `DENSE_RANK`.  
  - Comparé resultados por curso y a nivel global.  
  - Practiqué acumulados y promedios móviles con `ROWS` y `RANGE`.  
  - Entendí cómo las funciones ventana **no reducen filas** (a diferencia de GROUP BY).  
  - Ejemplo trabajado: promedio acumulado y ranking oficial de estudiantes por nota.  

- **Día 6**: Práctica combinada de JOINS + funciones ventana  
  - Combiné joins (estudiantes, cursos, profesores) con funciones ventana.  
  - Ejemplo: ranking de profesores según el promedio de notas de sus cursos.  
  - Practiqué diferencias entre `INNER` y `LEFT` al cruzar con tablas con valores faltantes.  
  - Usé funciones de agregación + ventana para validar consistencia de datos.  
  - Aprendizaje clave: **las funciones ventana pueden convivir con joins sin problemas, pero el orden de ejecución importa**.

- **Día 7**: Plantillas SQL y repaso de Window Functions I  
  - Organicé mis consultas en archivos (`sql/`).  
  - Consolidé ejemplos de `RANK`, `DENSE_RANK`, `ROW_NUMBER`.  
  - Repaso de funciones de agregación con ventana (`SUM OVER`, `AVG OVER`).  
  - Preparación para entrar a analíticas avanzadas (Día 8).  
  - Aprendizaje: estructurar código SQL en archivos versionados ayuda a reutilizar plantillas y mantener orden.

- **Día 8**: Window Functions II y performance  
  - Practiqué funciones analíticas avanzadas: `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTILE`.  
  - Usé `ROWS` y `RANGE` para definir frames en acumulados y promedios móviles.  
  - Resolví casos de Top-N por curso y deduplicación de registros con `ROW_NUMBER`.  
  - Introducción al análisis de performance con `EXPLAIN` y `EXPLAIN ANALYZE`.  
  - Creé un índice (`curso_id, nota DESC`) y observé cómo optimiza el plan, cambiando de **Sort completo** a **Incremental Sort**.  
  - Guardé planes de ejecución en JSON (`perf/`) y documenté notas en el README.  