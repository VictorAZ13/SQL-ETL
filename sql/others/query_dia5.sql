-- Window Functions SQL (EJEMPLO)
SELECT curso_id,nombre,nota, DENSE_RANK() OVER (PARTITION BY curso_id ORDER BY nota DESC) AS ranking_curso
FROM estudiantes;

--Funciones de RANKING
--RANK()
SELECT nombre,curso_id, RANK() OVER (PARTITION BY curso_id ORDER BY nota DESC) AS ranking_curso, RANK() OVER(ORDER BY nota DESC) AS ranking_global
FROM estudiantes;

--ROW NUMBER()
SELECT ROW_NUMBER() OVER (PARTITION BY p.nombre_profesor ORDER BY c.id ASC) AS posicion_profesor, p.nombre_profesor, c.nombre_curso,e.nombre
FROM estudiantes e 
LEFT JOIN cursos c ON e.curso_id = c.id 
LEFT JOIN profesores p ON c.profesor_id = p.id;

--DENSE_RANK()
SELECT nombre,nota, DENSE_RANK() OVER(ORDER BY nota DESC) AS rankig_oficial_global
FROM estudiantes;

--FUNCIONES DE AGREGACION AVG()
SELECT nombre, nota, AVG(nota) OVER() AS promedio_notas,AVG(nota) OVER(ORDER BY nota DESC) AS promedio_acumulado_general
FROM estudiantes;

--SUM()
SELECT nombre, nota, SUM(nota) OVER (PARTITION BY curso_id) AS suma_notas_curso
FROM estudiantes;

--MAX Y MIN()
SELECT nombre, nota, MAX(nota) OVER(PARTITION BY curso_id) AS maxima_nota_curso,MIN(nota) OVER(PARTITION BY curso_id) AS minima_nota_curso
FROM estudiantes;

--ROWS O RANGE BETWEEN ## AND ##
SELECT e.nombre,e.nota,c.nombre_curso,
SUM(e.nota) OVER(PARTITION BY e.curso_id ORDER BY e.nota ASC
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS suma_acumulada,
AVG(e.nota) OVER(ORDER BY e.nota DESC
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS promedio_global,
AVG(e.nota) OVER (ORDER BY e.nota DESC
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS promedio_movil_3,
AVG(e.nota) OVER (PARTITION BY curso_id ORDER BY nota DESC
ROWS  BETWEEN 2 PRECEDING AND CURRENT ROW) AS promedio_movil_curso_3,
MAX(e.nota) OVER (ORDER BY e.nota ASC
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS maximo_global,
MAX (e.nota) OVER (ORDER BY e.nota ASC
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS maximo__until_now

FROM estudiantes e
LEFT JOIN cursos c ON e.curso_id = c.id
ORDER BY c.nombre_curso;
 
