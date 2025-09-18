import csv
import random
import os


os.makedirs("datasets", exist_ok=True)

nombres_estudiantes = ["Victor","Oscar","Flavia","Yeny","Juan","Nieves","Diana","Jose","Cesar"]
nombres_profesores = ["Perez","Gomez","Lopez","Torres","Fernandez","Salas","Ramos","Cruz","Castro"]
departamentos = ["Ingeniería Industrial","Arquitectura","Ciencias de la Computación",
                 "Ingeniería Económica","Administración de Negocios"]
cursos = ["Física","Matemáticas","Diplomado ML","Diplomado Estadística","Data Science"]

# --- Profesores.csv ---
with open("datasets/profesores.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id","nombre_profesor","departamento"])
    for i in range(1, len(nombres_profesores)+1):
<<<<<<< HEAD
        writer.writerow([i, nombres_profesores[i-1], random.choice(departamentos)])
=======
        writer.writerow([i, nombres_profesores[i-1], 
        random.randint(1,len(departamentos))])
>>>>>>> dev

# --- Cursos.csv ---
with open("datasets/cursos.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id","nombre_curso","profesor_id"])
    for i in range(1, len(cursos)+1):
        writer.writerow([i, cursos[i-1], random.randint(1, len(nombres_profesores))])

# --- Estudiantes.csv ---
with open("datasets/estudiantes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id","nombre","edad","genero","curso_id","nota"])
<<<<<<< HEAD
    for i in range(1, 41):  # 40 estudiantes
=======
    for i in range(1, 41): 
>>>>>>> dev
        writer.writerow([
            i,
            random.choice(nombres_estudiantes),
            random.randint(17,30),
            random.choice(["M","F"]),
<<<<<<< HEAD
            random.randint(1, len(cursos)),  # curso_id FK
            random.randint(0,20)
        ])
=======
            random.randint(1, len(cursos)),  
            random.randint(0,20)
        ])

# ---Departamentos.csv --
with open("datasets/departamentos.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id","nombre","parent_id"])
    for i in range(1, len(cursos)+1):
        writer.writerow([i, departamentos[i-1], "NULL"])

>>>>>>> dev
