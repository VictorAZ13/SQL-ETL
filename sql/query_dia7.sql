--Funciones parte 2
--LAG/LEAD/FIRST/LAST S
--LAG (SINTAXIS LAG("columna",k(cuantos valores hacia atras),default(valor por default)))
SELECT curso_id, id AS estudiante_id, nota,
LAG(nota,1,0) OVER(PARTITION BY curso_id ORDER BY id) AS nota_anterior,
nota - LAG(nota,1,0) OVER(PARTITION BY curso_id ORDER BY id) AS diff_nota
FROM estudiantes
WHERE curso_id =2
ORDER BY curso_id, id;

--LEAD (SINTAXIS LEAD("columna",k(cuantos valores hacia delante),default(valor por default)))
WITH totales_curso AS (
    SELECT curso_id, COUNT(id) AS total_alumnos
    FROM estudiantes
    GROUP BY curso_id
)
SELECT curso_id, total_alumnos,
       LEAD(total_alumnos, 1, 0) OVER (ORDER BY curso_id) AS total_siguiente,
       total_alumnos - LEAD(total_alumnos,1,0) OVER (ORDER BY curso_id) AS diff_con_siguiente
FROM totales_curso;


--FIRST/LAST VALUE devuelve el primer y ultimo valor por un rango de filas que indiques en RANGE/ROWNS BETWEEN ## AND ##
SELECT curso_id, id, nota,
LAST_VALUE(nota) OVER (PARTITION BY curso_id ORDER BY nota ASC
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mejor_nota,
FIRST_VALUE(nota) OVER (PARTITION BY curso_id ORDER BY nota ASC
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS peor_nota
FROM estudiantes
ORDER BY curso_id,nota ASC, id DESC;

--NTILE
SELECT curso_id, id, nota,
       NTILE(4) OVER (
         PARTITION BY curso_id
         ORDER BY nota DESC, id DESC
       ) AS cuartil
FROM estudiantes
WHERE curso_id = 2
ORDER BY curso_id, nota DESC, id DESC;

--PERCENT_RANK() Y CUME_DIST()
SELECT id,nota,curso_id,
RANK() OVER (PARTITION BY curso_id ORDER BY nota DESC) AS ranking,
PERCENT_RANK() OVER(PARTITION curso_id ORDER BY nota DESC) AS ranking_posicion,
CUME_DIST() OVER(PARTITION BY curso_id ORDER BY nota DESC) AS ranking_acumulado
FROM estudiantes
ORDER BY curso_id,nota DESC;


--