# FleetLogix · Proyecto Integrador M2

**Henry Data Science · Módulo 2 (SQL)** — Laura Garmendia

FleetLogix es una empresa de transporte y logística que opera una flota de 200 vehículos y hace entregas de última milla en 5 ciudades de Colombia. Recibimos una base PostgreSQL con el modelo relacional (6 tablas vacías) y la tarea de poblarla con datos sintéticos coherentes.

## Estado del proyecto

| Avance | Tema | Estado |
|---|---|---|
| 1 | Análisis del modelo y generación de datos sintéticos | ✅ Completo |

## Avance 1: qué se hizo

**1. Análisis del modelo relacional**
- Documentación de las relaciones (cardinalidades) y de todos los constraints: PK, FK, UNIQUE, NOT NULL, DEFAULT e índices.
- Diagrama Entidad-Relación en notación pata de gallo.
- Patrones de negocio implícitos en el modelo y 7 mejoras posibles sin modificar la estructura base.

**2. Generación de datos sintéticos**: script en Python con Faker, pandas y numpy.

| Tabla | Tipo | Registros |
|---|---|---:|
| vehicles | maestra | 200 |
| drivers | maestra | 400 |
| routes | maestra | 50 |
| trips | transaccional | 100.000 |
| deliveries | transaccional | 400.000 |
| maintenance | transaccional | 5.000 |
| **Total** | | **505.650** |

- Los datos respetan las reglas del negocio: vehículos y conductores sin viajes superpuestos, viajes solo con conductores contratados y con licencia vigente, peso dentro de la capacidad del vehículo, entre 2 y 6 entregas por viaje y entregas dentro del horario de su viaje.
- Carga con `COPY` en una sola transacción: si algo falla, la base queda sin cambios.
- Se corrigieron errores de la plantilla original. Están detallados en el notebook, sección 2.1.

**3. Control de calidad**
- 31 validaciones en SQL (integridad referencial, consistencia temporal, horarios, capacidad y reglas de negocio): **31 de 31 OK**.
- Revisión de distribuciones (entregas por viaje, hora de salida, viajes por mes, retrasos) y log de carga.

## Estructura del repositorio

```
ProyectoM2_LauraGarmendia/
├── README.md
├── fleetlogix_db_schema.sql            # Esquema provisto (6 tablas)
├── 1. Notebooks/
│   ├── Avance_1_DataGeneration.ipynb   # Análisis, generación y control de calidad
│   └── generation_summary.json         # Resumen de la carga y de las validaciones
├── 2. Scripts/
│   └── 01_data_generation.py           # Generación y carga de datos
└── 3. Documentación/
    ├── diagrama_ER_fleetlogix.png      # Diagrama Entidad-Relación
    └── data_dictionary.xlsx            # Diccionario de datos
```

## Cómo ejecutarlo

**Requisitos:** PostgreSQL 16 (en este proyecto, en un contenedor Docker con la imagen `postgres:16`) y Python 3.10 o superior.

1. Crear la base `fleetlogix` y ejecutar `fleetlogix_db_schema.sql` (por ejemplo, desde DBeaver).
2. Instalar las librerías:
   ```
   pip install psycopg2-binary faker pandas numpy matplotlib
   ```
3. Ejecutar el notebook `1. Notebooks/Avance_1_DataGeneration.ipynb`, que pide la contraseña de PostgreSQL al conectarse. Otra opción es correr el script:
   ```
   export FLEETLOGIX_DB_PASSWORD="tu_contraseña"
   python "2. Scripts/01_data_generation.py"
   ```

El script usa una semilla fija (`SEED = 42`): genera siempre los mismos datos. Se puede re-ejecutar sin duplicar registros, porque vacía las tablas antes de cargar.

## Tecnologías

PostgreSQL 16 · Docker · DBeaver · Python (pandas, numpy, Faker, psycopg2, matplotlib) · Jupyter · VS Code
