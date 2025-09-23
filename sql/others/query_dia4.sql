--SUBCONSULTAS
--Alumnos con mayor nota al promedio
SELECT e.nombre, e.nota FROM estudiantes e WHERE e.nota > (SELECT AVG(nota) FROM estudiantes);

--Alumnos con mayor nota de su curso
SELECT e.curso_id, e.nombre,e.nota FROM estudiantes e WHERE e.nota > (SELECT AVG(nota) FROM estudiantes WHERE curso_id = e.curso_id) ORDER BY curso_id DESC;

-- Alumnos que tienen la maxima nota de cada curso
SELECT e.curso_id, e.nombre,e.nota FROM estudiantes e WHERE e.nota = (SELECT MAX(nota) FROM estudiantes WHERE curso_id = e.curso_id) ORDER BY curso_id DESC;

--Subconsultas en FROM
SELECT *
FROM (
    SELECT curso_id, AVG(nota) AS promedio_nota
    FROM estudiantes GROUP BY curso_id
) c
ORDER BY c.promedio_nota DESC;

--Reto de Bloque 2
SELECT t.curso_id,t.promedio_cursos
FROM (
    SELECT curso_id, AVG(nota) AS promedio_cursos
    FROM estudiantes
    GROUP BY curso_id
) t
WHERE t.promedio_cursos > (
    SELECT AVG(c.promedio_cursos)
    FROM (
        SELECT curso_id, AVG(nota) AS promedio_cursos
        FROM estudiantes
        GROUP BY curso_id
    ) c
)

-- --Subconsultas en SELECT
-- SELECT nombre,nota,
-- (SELECT AVG(nota) FROM estudiantes) AS promedio_general
-- FROM estudiantes
-- WHERE nota > (
--     SELECT AVG(nota) FROM estudiantes
-- );


--RETO DEL DIA (Funciona en DBeaver)
SELECT t.nombre, t.total_alumnos
FROM (
    SELECT p.nombre_profesor AS nombre, COUNT(e.id) AS total_alumnos
    FROM profesores p LEFT JOIN cursos c ON p.id = c.profesor_id LEFT JOIN estudiantes e ON c.id = e.curso_id
    GROUP BY p.nombre_profesor
) t
WHERE t.total_alumnos > (
    SELECT AVG(total_alumnos)
    FROM (
        SELECT COUNT(e.id) AS total_alumnos
        FROM profesores p LEFT JOIN cursos c ON p.id = c.profesor_id LEFT JOIN estudiantes e ON c.id = e.curso_id
        GROUP BY p.nombre_profesor
    ) x
);

SELECT c.nombre_curso, c.promedio_notas, (SELECT AVG(nota) FROM estudiantes) AS promedio_global
FROM (
    SELECT c.nombre_curso AS nombre_curso, AVG(e.nota) AS promedio_notas
    FROM cursos c LEFT JOIN estudiantes e ON c.id = e.curso_id
    GROUP BY c.nombre_curso
) c;