-- Mayores a 25
SELECT * FROM estudiantes WHERE edad > 25;

-- solo nombre y grado
SELECT nombre, grado FROM estudiantes;

-- ordenarlos por edad
SELECT * FROM estudiantes ORDER BY edad;

-- reto del dia obtener al mas joven

SELECT * FROM estudiantes ORDER BY edad ASC LIMIT 1;
