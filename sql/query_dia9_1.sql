--INSERCIÓN SUCIA
TRUNCATE etl_demo.staging_estudiantes;

INSERT INTO etl_demo.staging_estudiantes(id_text,dni,nombre,apellido,genero,curso_id,nota)
VALUES
('E001','12345678','  ana ','perez','f',101, 18.5),   -- espacios, minúsculas
('E002','12345679','PEDRO','GARCIA','M',101, 21),     -- nota > 20
('E003','12345679','maria','lopez','F',102, 15),      -- dni duplicado
('E004','12345680','LUIS','',       'M',103, NULL),   -- apellido vacío
('E005',NULL,'sofia','  diaz ','x', 104, 12);         -- género inválido

-- VERIFICAR STAGING
-- 1) Conteo esperado: 5
SELECT COUNT(*) AS filas_staging FROM etl_demo.staging_estudiantes;

-- 2) Vista rápida de “suciedad”
SELECT
  dni, nombre, apellido, genero, curso_id, nota
FROM etl_demo.staging_estudiantes
ORDER BY dni NULLS LAST;
