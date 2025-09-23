--Estudiantes con cursos asignados
SELECT e.id, e.nombre, c.nombre_curso, c.profesor_id FROM estudiantes e INNER JOIN cursos c on e.curso_id = c.id;

--Cursos con profesores asignados se obvian profes que no esten asignados
SELECT c.id,c.nombre_curso,p.nombre_profesor FROM cursos c LEFT JOIN profesores p ON c.profesor_id = p.id; 

-- Cursos asignados a estudiantes (Si algun curso no fue asignado aparecera como null)
SELECT e.id,e.nombre,c.nombre_curso FROM estudiantes e RIGHT JOIN cursos c ON e.curso_id = c.id;

-- Profesores con su curso asignado
SELECT p.id,p.nombre_profesor,c.nombre_curso  FROM profesores p FULL OUTER JOIN cursos c ON p.id = c.profesor_id;

--Practica Joins complejos
SELECT e.id AS DNI, e.nombre AS nombre_estudiante,c.nombre_curso AS nombre_curso, p.nombre_profesor AS Nombre_Profesor,p.departamento FROM estudiantes e INNER JOIN cursos c ON e.curso_id = c.id LEFT JOIN profesores p ON c.profesor_id = p.id;

--Ver solo los nulos

SELECT p.nombre_profesor FROM profesores p LEFT JOIN cursos c ON p.id = c.profesor_id WHERE c.profesor_id IS NULL;

-- Cantidad de estudiantes por curso
SELECT c.nombre_curso, COUNT(e.id) FROM cursos c LEFT JOIN estudiantes e ON c.id = e.curso_id GROUP BY c.nombre_curso; 

--Cantidad de cursos por profesor
SELECT p.nombre_profesor, COUNT(c.profesor_id) FROM profesores p LEFT JOIN cursos c ON p.id = c.profesor_id GROUP BY p.nombre_profesor;

--Ejercicio

SELECT e.nombre AS Nombre_Estudiante, c.nombre_curso, p.nombre_profesor FROM estudiantes e LEFT JOIN cursos c ON e.curso_id = c.id LEFT JOIN profesores p ON c.profesor_id = p.id;

--reto ranking de profesores con más alumnos asignados

SELECT p.nombre_profesor, COUNT(e.id) AS total_alumnos FROM profesores p INNER JOIN cursos c ON p.id = c.profesor_id LEFT JOIN estudiantes e ON c.id = e.curso_id GROUP BY p.nombre_profesor ORDER BY total_alumnos DESC;