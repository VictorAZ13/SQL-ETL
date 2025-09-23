-- FUNCIONES DE AGREGACIÓN
-- total de filas (practica de count)
SELECT COUNT(*) AS total_estudiantes FROM estudiantes;

-- edad minima y máxima
SELECT MIN(edad) AS edad_minima, MAX(edad) AS edad_max FROM estudiantes;

-- promedio de edad
SELECT AVG(edad) AS promedio_edad FROM estudiantes;

--suma de edad
SELECT SUM(edad) AS suma_edad FROM estudiantes;


--Clausulas
--Total alumnos agrupados por género
SELECT genero,COUNT(*) AS total_estudiantes FROM estudiantes GROUP BY genero;

--Promedio de edad por curso
SELECT curso,AVG(edad) AS promedio_curso FROM estudiantes GROUP BY curso;

--conteo de alumnos y promedio de calificación por departamento
SELECT departamento,COUNT(*) AS total_estudiantes, AVG(calificacion) AS promedio_calificacion FROM estudiantes GROUP BY departamento;

--reto del dia curso con mayor cantidad de alumnos
SELECT curso,COUNT(*) AS total_curso FROM estudiantes GROUP BY curso ORDER BY total_curso DESC LIMIT 1;

--Filtrado de grupos
--Cursos con mas de 10 alumnos
SELECT curso, COUNT(*) AS total_alumnos FROM estudiantes GROUP BY curso HAVING COUNT(*)>10;

--Generos con promedio mayor a 20
SELECT genero,AVG(edad) AS promedio_edad FROM estudiantes GROUP BY genero HAVING AVG(edad)>20;

--Departamentos sin ningun alumno con calificación desaprobatoria y con una cantidad de alumnos mayor a 5
SELECT departamento,MIN(calificacion) AS nota_minima, COUNT(*) AS total_alumnos FROM estudiantes GROUP BY departamento HAVING MIN(calificacion) > 10.5 AND  COUNT(*) > 5;

--Minireto de ranking de cursos por cantidad de alumnos solo los que tengan mas de 10
SELECT curso, COUNT(*) AS total_alumnos FROM estudiantes GROUP BY curso HAVING COUNT(*)>10 ORDER BY total_alumnos DESC;


--Reto del dia
SELECT departamento, AVG(calificacion) AS promedio_calificacion, COUNT(*) AS total_alumnos FROM estudiantes GROUP BY departamento HAVING COUNT(*)>8 ORDER BY promedio_calificacion DESC LIMIT 1;