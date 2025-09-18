
DROP TABLE IF EXISTS estudiantes CASCADE;
DROP TABLE IF EXISTS cursos CASCADE;
DROP TABLE IF EXISTS profesores CASCADE;
DROP TABLE IF EXISTS departamentos CASCADE;


CREATE TABLE departamentos(
    id INT PRIMARY KEY,
    nombre VARCHAR(80),
    parent_id INT NULL  
);

CREATE TABLE profesores(
    id INT PRIMARY KEY,
    nombre_profesor VARCHAR(80),
    id_departamento INT,
    FOREIGN KEY(id_departamento) REFERENCES departamentos(id)
);

CREATE TABLE cursos(
    id INT PRIMARY KEY,
    nombre_curso VARCHAR(80),
    profesor_id INT,
    FOREIGN KEY (profesor_id) REFERENCES profesores(id)
);

CREATE TABLE estudiantes (
    id INT PRIMARY KEY,
    nombre VARCHAR(80),
    edad INT,
    genero VARCHAR(20),
    curso_id INT,
    nota FLOAT,
    FOREIGN KEY (curso_id) REFERENCES cursos(id)
);


