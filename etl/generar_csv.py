import csv
import random

columnas = ["id","nombre","edad","genero","curso","departamento","calificacion","grado"]

nombres = ["Victor","Oscar","Flavia","Yeny","Juan","Nieves","Diana","Jose","Cesar"]
generos = ["M","F"]
cursos = ["Fisica","Matematicas","Diplomado de ML","Diplomado Estadistica","Data Science"]
departamentos = ["Ingenieria Industrial","Arquitectura","Ciencas de la Computación","Ingeniería Económica","Administración de Negocios"]
grados = ["Egresado","Estudiante","Master","Doctor"]

with open("estudiantes.csv","w",newline="",encoding="utf-8") as archivo:
    writer = csv.writer(archivo)
    writer.writerow(columnas)

    for i in range(1,41):
        fila = [
            i,
            random.choice(nombres),
            random.randint(17,30),
            random.choice(generos),
            random.choice(cursos),
            random.choice(departamentos),
            random.randint(0,20),
            random.choice(grados)
        ]
        writer.writerow(fila)

