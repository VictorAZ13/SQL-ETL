# ETL - SQL

En este repositorio fue creado con el propósito de aprender acerca de pipeline en ambitos aplicados, partiendo de la idea de la transformación y el guardado de csv o excel a base de datos sql que podran ser consultados y tratados para **reportes o BI**.

##  Objetivos de aprendizaje
- Practicar consultas SQL básicas (SELECT, WHERE, ORDER BY, LIMIT, etc.).
- Integrar Python con PostgreSQL para construir un pipeline ETL.
- Preparar la base para análisis y visualización de datos.
- Aprender buenas prácticas de versionado y organización de proyectos.


## Estructura del proyecto

datasets/
etl/
sql/
exports/ "se añadirá mas tarde"

## 🔧 Requisitos
- Python 3.10+  
- PostgreSQL 14+  
- DBeaver (cliente gráfico para SQL)  
- VS Code (editor recomendado)  

---

## ▶️ Uso básico
1. Crear la base de datos `school_db` en PostgreSQL.  
2. Importar dataset de ejemplo (`datasets/students.csv`).  
3. Ejecutar las consultas de `sql/01-select-queries.sql` en DBeaver.  
4. (Más adelante) correr scripts de `/etl` para automatizar el pipeline.

---

## 📚 Avances
- **Día 1**: Setup de proyecto, importación de dataset y primeras consultas SQL.  
  - SELECT, WHERE, ORDER BY, LIMIT.  
  - Primer commit del repo.

---

## 📌 Próximos pasos
- Añadir más consultas SQL (JOIN, GROUP BY, agregaciones).  
- Crear primer script ETL en Python (`etl/pipeline.py`).  
- Configurar exportaciones en `/exports`.
