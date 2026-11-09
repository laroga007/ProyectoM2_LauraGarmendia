# FleetLogix · Proyecto Integrador M2

**Henry Data Science · Módulo 2 (SQL)** — Laura Garmendia

FleetLogix es una empresa de transporte y logística que opera una flota de 200 vehículos y hace entregas de última milla en 5 ciudades de Colombia. El proyecto parte de una base PostgreSQL con el modelo relacional (6 tablas vacías): se pobló con datos sintéticos coherentes, se usó para responder preguntas de negocio con SQL y se llevó a un Data Warehouse en Snowflake.

## Estado del proyecto

| Avance | Tema | Estado |
|---|---|---|
| 1 | Análisis del modelo y generación de datos sintéticos | ✅ Completo |
| 2 | Análisis y optimización de queries SQL | ✅ Completo |
| 3 | Data Warehouse en Snowflake y pipeline ETL | ✅ Completo |

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
- Se realizaron ajustes sobre el script base para garantizar la coherencia de los datos; están detallados en el notebook, sección 2.1.

**3. Control de calidad**
- 31 validaciones en SQL (integridad referencial, consistencia temporal, horarios, capacidad y reglas de negocio): **31 de 31 OK**.
- Revisión de distribuciones (entregas por viaje, hora de salida, viajes por mes, retrasos) y log de carga.

## Avance 2: qué se hizo

**1. Ejecución y análisis de 8 queries** (de las 12 propuestas): Q1, Q2, Q3, Q4, Q5, Q8, Q9 y Q12. Cada una se documentó con su problema de negocio, su resultado y su interpretación. La Q12 se eligió frente a la Q10 porque, en estos datos, los viajes se asignan por disponibilidad y no por desempeño del conductor.

**Hallazgos principales:**
- **Q2:** de las 23 licencias que devuelve, 14 ya están vencidas. Se propone una versión que las distingue.
- **Q5:** la gran diferencia de viajes entre conductores se explica por la antigüedad.
- **Q8:** el retraso es estructural (12-15% de entregas con más de 30 min, todos los días).
- **Q9:** se ajustó la agregación para calcular viajes y mantenimientos por separado. La versión inicial combinaba ambas tablas en el mismo JOIN, multiplicaba filas y sobrestimaba los costos de mantenimiento ~600 veces.
- **Q12:** el domingo concentra entregas a primera hora (viajes del sábado), aunque ese día casi no se despacha.

**2. Optimización con índices**
- Cada query se midió con `EXPLAIN ANALYZE` en DBeaver, antes y después.
- Se evaluaron los 5 índices propuestos: 2 se usaban y 3 no (tablas muy chicas o un índice parcial que la Q12 no podía usar). El set final tiene **2 de los propuestos y 3 nuevos**, incluidos índices cubrientes (`INCLUDE`), más un ajuste de `random_page_cost` para disco SSD.

| Query | Antes (ms) | Después (ms) | Mejora |
|---|---:|---:|---:|
| Q3 | 22,0 | 15,8 | 28% |
| Q4 | 37,2 | 33,8 | 9% |
| Q8 | 75,8 | 43,3 | 43% |
| Q9 ajustada | 38,3 | 17,5 | 54% |
| Q12 | 38,5 | 22,3 | 42% |
| **Q9 inicial → ajustada + índices** | **801,9** | **17,5** | **98%** |

Q1, Q2 (tablas de 200 y 400 filas) y Q5 (necesita leer todos los viajes) no mejoran con índices; el análisis completo está en el notebook.

## Avance 3: qué se hizo

**1. Modelo dimensional (estrella, metodología Kimball)**
- Tabla de hechos `fact_deliveries`: una fila por entrega completada, con tiempos, retraso, puntualidad, distancia, combustible, entregas por hora, costo e ingreso estimados.
- 6 dimensiones: `dim_date` (con los festivos de Colombia), `dim_time` (por minuto, con turnos), `dim_vehicle` y `dim_driver` (SCD tipo 2), `dim_route` y `dim_customer` (SCD tipo 1).
- Claves sustitutas en todas las dimensiones; los hechos se unen a la versión de la dimensión vigente en la fecha de la entrega.

**2. Implementación en Snowflake**
- Warehouse multi-clúster (1 a 3 clústeres) con suspensión y reactivación automáticas.
- Time Travel de 30 días para toda la base.
- Vistas seguras por rol: Ventas (`SALES_ANALYST`) ve solo datos comerciales y Operaciones (`OPERATIONS_ANALYST`) el detalle completo.

**3. Pipeline ETL en Python** (`05_etl_pipeline.py`)
- Extrae de PostgreSQL, guarda el lote crudo en JSON (`VARIANT`), calcula métricas, aplica controles de calidad, carga dimensiones y hechos con `write_pandas` + `MERGE` y precalcula totales diarios.
- Idempotente: re-ejecutar un día actualiza las filas y no las duplica.
- Carga histórica inicial y cargas diarias; queda programado con `schedule` (todos los días a las 02:00) o con `cron`.
- Conexión a Snowflake por par de claves; ninguna credencial queda en el código.

**4. Verificación**: conciliación de cantidades y pesos con PostgreSQL, hechos sin clave de dimensión, prueba de SCD tipo 2, Time Travel y permisos por rol. Todo está en el notebook del avance 3.

Se realizaron ajustes sobre los scripts base (claves de dimensión, SCD tipo 2, granularidad de `dim_time`, reparto de métricas del viaje entre sus entregas y carga en bloque); están detallados en los comentarios de cada script y en el notebook.

## Estructura del repositorio

```
ProyectoM2_LauraGarmendia/
├── README.md
├── fleetlogix_db_schema.sql            # Esquema provisto (6 tablas)
├── 1. Notebooks/
│   ├── Avance_1_DataGeneration.ipynb   # Análisis, generación y control de calidad
│   ├── Avance_2_SQLAnalysis.ipynb      # Análisis de queries y optimización
│   ├── Avance_3_DataWarehouse.ipynb    # Modelo estrella, ETL y verificación en Snowflake
│   └── generation_summary.json         # Resumen de la carga y de las validaciones
├── 2. Scripts/
│   ├── 01_data_generation.py           # Generación y carga de datos
│   ├── 02_queries_analysis.sql         # Las 8 queries comentadas (incluye Q9 ajustada)
│   ├── 03_optimization_indexes.sql     # Índices, ajuste de configuración y resultados
│   ├── 04_dimensional_model.sql        # Data Warehouse en Snowflake (tablas, vistas y roles)
│   └── 05_etl_pipeline.py              # Pipeline ETL PostgreSQL → Snowflake
└── 3. Documentación/
    ├── diagrama_ER_fleetlogix.png      # Diagrama Entidad-Relación
    ├── data_dictionary.xlsx            # Diccionario de datos
    ├── diagrama_estrella_fleetlogix.png # Diagrama del modelo estrella
    ├── capturas_avance2/               # Evidencia de EXPLAIN ANALYZE en DBeaver
    └── capturas_avance3/               # Evidencia del Data Warehouse en Snowsight
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
4. Avance 2: ejecutar `2. Scripts/03_optimization_indexes.sql` en DBeaver (o psql) para crear los índices, y después el notebook `1. Notebooks/Avance_2_SQLAnalysis.ipynb`.

5. Avance 3 (requiere una cuenta de Snowflake, edición Enterprise):
   - Ejecutar `2. Scripts/04_dimensional_model.sql` en un worksheet de Snowsight.
   - Instalar las librerías: `pip install "snowflake-connector-python[pandas]" schedule`.
   - Generar un par de claves, registrar la pública en el usuario de Snowflake (`ALTER USER ... SET RSA_PUBLIC_KEY`) y guardar la privada fuera del repositorio.
   - Ejecutar el notebook `1. Notebooks/Avance_3_DataWarehouse.ipynb`, o el pipeline desde la terminal:
     ```
     python "2. Scripts/05_etl_pipeline.py" --full --until 2026-10-14   # carga histórica
     python "2. Scripts/05_etl_pipeline.py" --date 2026-10-15            # un día puntual
     python "2. Scripts/05_etl_pipeline.py" --schedule                   # todos los días a las 02:00
     ```

El script de generación usa una semilla fija (`SEED = 42`): genera siempre los mismos datos. Se puede re-ejecutar sin duplicar registros, porque vacía las tablas antes de cargar.

## Tecnologías

PostgreSQL 16 · Docker · DBeaver · Snowflake · Python (pandas, numpy, Faker, psycopg2, snowflake-connector, schedule, matplotlib) · Jupyter · VS Code
