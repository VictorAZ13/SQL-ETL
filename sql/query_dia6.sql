--- CTE aplicado
--- Ejemplo simple --
WITH promedios AS (
    SELECT curso_id, AVG(nota) AS promedio
    FROM estudiantes
    GROUP BY curso_id
)

SELECT * FROM promedios
WHERE promedio > 10;

--Reto Bloque 1
WITH promedios AS(
    SELECT curso_id, AVG(nota) AS promedio
    FROM estudiantes
    GROUP BY curso_id
)

SELECT e.id, e.nombre,e.nota,p.curso_id, p.promedio
FROM estudiantes e 
LEFT JOIN promedios p ON e.curso_id = p.curso_id
WHERE e.nota > p.promedio
ORDER BY p.curso_id; 

--CTE recursivo
WITH RECURSIVE numeros AS(
    --caso Base
    SELECT 1 AS n
    UNION ALL 

    SELECT n + 1
    FROM numeros
    WHERE n < 5
)

SELECT * FROM numeros;


--CTE Recursivo Aplicado
WITH RECURSIVE jerarquia AS(
    SELECT id, nombre,parent_id, 0 AS nivel, CAST(id AS VARCHAR) AS path
    FROM departamentos
    WHERE parent_id IS NULL

    UNION ALL

    SELECT d.id, d.nombre,d.parent_id,
    j.nivel + 1,
    j.path || '>' || d.id
    FROM departamentos d 
    INNER JOIN jerarquia j ON d.parent_id = j.id
)

SELECT * FROM jerarquia ORDER BY path;

--Vistas
CREATE VIEW vista_promedios AS
SELECT curso_id, AVG(nota) AS promedio
FROM estudiantes
GROUP BY curso_id;

SELECT * FROM vista_promedios WHERE promedio > 10;


-- Vista Materializadas (DBEAVER)
CREATE MATERIALIZED VIEW vista_mtr_promedios AS
SELECT curso_id, AVG(nota) AS promedio
FROM estudiantes
GROUP BY curso_id;

--Reto bloque 3
drop materialized view if exists vista_total_al_prof cascade; 
CREATE MATERIALIZED VIEW vista_total_al_prof AS
select p.id, p.nombre_profesor, COUNT(e.id) AS total_alumnos
FROM estudiantes e 
LEFT JOIN cursos c ON e.curso_id = c.id
LEFT JOIN profesores p ON c.profesor_id = p.id
GROUP BY p.id;


--SUPER RETO
SELECT c.id,c.nombre_curso,vmp.profesor_id, vmp.nombre_profesor,vmp.total_alumnos, RANK(vmp.total_alumnos) OVER (PARTITION BY c.id ORDER BY vmp.total_alumnos) AS ranking_alumnos
FROM cursos c
LEFT JOIN vista_total_al_prof vmp ON c.profesor_id = vmp.id
GROUP BY c.id HAVING ranking_alumnos <= 3;

