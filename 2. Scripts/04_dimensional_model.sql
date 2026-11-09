-- =====================================================
-- FLEETLOGIX - AVANCE 3: DATA WAREHOUSE (MODELO ESTRELLA)
-- Motor: Snowflake (edición Enterprise) · Ejecutado en un worksheet de Snowsight
-- =====================================================
-- Contenido:
--   1. Warehouse con escalado automático, base y esquema (Time Travel de 30 días)
--   2. Dimensiones: fecha, hora, vehículo, conductor, ruta y cliente
--   3. Tabla de hechos fact_deliveries (una fila por entrega completada)
--   4. Staging en JSON (VARIANT) y tabla de totales diarios
--   5. Carga de dim_date y dim_time (se generan aquí, no vienen de PostgreSQL)
--   6. Vistas seguras y roles (Ventas / Operaciones)
--   7. Verificación
-- Las dimensiones vehículo, conductor, ruta y cliente y la tabla de hechos
-- se cargan con el pipeline 05_etl_pipeline.py.
--
-- Ajustes realizados sobre el script base:
--   * Claves sustitutas (vehicle_key, driver_key, route_key, customer_key) con
--     IDENTITY: así cada versión de un conductor o vehículo (SCD tipo 2) tiene
--     su propia clave, distinta del ID de PostgreSQL.
--   * dim_customer: en el origen no existe un ID de cliente; la clave natural
--     es customer_name y la clave sustituta es customer_key.
--   * dim_time con un registro por minuto (1.440 filas); time_key = HHMM.
--   * dim_date con los festivos de Colombia 2024-2027 (Ley Emiliani).
--   * Time Travel de 30 días definido a nivel de base de datos: todas las
--     tablas lo heredan.
--   * Permisos completos para los roles (uso del warehouse, la base y el
--     esquema), necesarios para que las vistas se puedan consultar.
-- Nota: Snowflake registra PRIMARY KEY y REFERENCES pero no los hace cumplir;
-- la unicidad y la integridad las garantiza el pipeline (MERGE por clave).
-- =====================================================


-- =====================================================
-- 1. WAREHOUSE, BASE Y ESQUEMA
-- =====================================================
USE ROLE ACCOUNTADMIN;

-- Escalado automático (Enterprise): arranca con 1 clúster y agrega hasta 3
-- si se acumulan consultas en cola. Se suspende tras 60 s sin uso y se
-- reactiva solo cuando llega una consulta (no consume créditos en reposo).
CREATE OR REPLACE WAREHOUSE FLEETLOGIX_WH WITH
    WAREHOUSE_SIZE      = 'XSMALL'
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 3
    SCALING_POLICY      = 'STANDARD'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Time Travel: permite consultar o recuperar los datos como estaban
-- hasta 30 días atrás. Lo heredan el esquema y todas las tablas.
CREATE DATABASE IF NOT EXISTS FLEETLOGIX_DW DATA_RETENTION_TIME_IN_DAYS = 30;
CREATE SCHEMA IF NOT EXISTS FLEETLOGIX_DW.ANALYTICS;

USE WAREHOUSE FLEETLOGIX_WH;
USE DATABASE FLEETLOGIX_DW;
USE SCHEMA ANALYTICS;


-- =====================================================
-- 2. DIMENSIONES
-- =====================================================

-- Dimensión Fecha: un registro por día (2024-2027)
CREATE OR REPLACE TABLE dim_date (
    date_key        INT PRIMARY KEY,     -- AAAAMMDD, ej. 20261015
    full_date       DATE NOT NULL,
    day_of_week     INT,                 -- 1 = lunes ... 7 = domingo (ISO)
    day_name        VARCHAR(10),
    day_of_month    INT,
    day_of_year     INT,
    week_of_year    INT,
    month_num       INT,
    month_name      VARCHAR(10),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,
    holiday_name    VARCHAR(50),
    fiscal_quarter  INT,                 -- en Colombia el año fiscal coincide
    fiscal_year     INT                  -- con el calendario (enero-diciembre)
);

-- Dimensión Tiempo: un registro por minuto del día, para análisis por turno
CREATE OR REPLACE TABLE dim_time (
    time_key          INT PRIMARY KEY,   -- HHMM, ej. 1430
    hour              INT,
    minute            INT,
    second            INT,
    time_of_day       VARCHAR(20),       -- 'Madrugada', 'Mañana', 'Tarde', 'Noche'
    hour_24           VARCHAR(5),        -- '14:30'
    hour_12           VARCHAR(8),        -- '02:30 PM'
    am_pm             VARCHAR(2),
    is_business_hour  BOOLEAN,           -- 08:00 a 17:59
    shift             VARCHAR(20)        -- 'Turno 1', 'Turno 2', 'Turno 3'
);

-- Dimensión Vehículo (SCD tipo 2: guarda el historial de cambios)
CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_key            INT IDENTITY(1,1) ORDER PRIMARY KEY,
    vehicle_id             INT NOT NULL,          -- ID en PostgreSQL
    license_plate          VARCHAR(20),
    vehicle_type           VARCHAR(50),
    capacity_kg            DECIMAL(10,2),
    fuel_type              VARCHAR(20),
    acquisition_date       DATE,
    age_months             INT,
    status                 VARCHAR(20),
    last_maintenance_date  DATE,
    valid_from             DATE,
    valid_to               DATE,                  -- 9999-12-31 = versión vigente
    is_current             BOOLEAN
);

-- Dimensión Conductor (SCD tipo 2)
CREATE OR REPLACE TABLE dim_driver (
    driver_key            INT IDENTITY(1,1) ORDER PRIMARY KEY,
    driver_id             INT NOT NULL,           -- ID en PostgreSQL
    employee_code         VARCHAR(20),
    full_name             VARCHAR(200),
    license_number        VARCHAR(50),
    license_expiry        DATE,
    phone                 VARCHAR(20),
    hire_date             DATE,
    experience_months     INT,
    status                VARCHAR(20),
    performance_category  VARCHAR(20),            -- 'Alto', 'Medio', 'Bajo'
    valid_from            DATE,
    valid_to              DATE,
    is_current            BOOLEAN
);

-- Dimensión Ruta
CREATE OR REPLACE TABLE dim_route (
    route_key                 INT IDENTITY(1,1) ORDER PRIMARY KEY,
    route_id                  INT NOT NULL,       -- ID en PostgreSQL
    route_code                VARCHAR(20),
    origin_city               VARCHAR(100),
    destination_city          VARCHAR(100),
    distance_km               DECIMAL(10,2),
    estimated_duration_hours  DECIMAL(5,2),
    toll_cost                 DECIMAL(10,2),
    difficulty_level          VARCHAR(20),        -- 'Fácil', 'Medio', 'Difícil'
    route_type                VARCHAR(20)         -- 'Urbana', 'Interurbana', 'Rural'
);

-- Dimensión Cliente (SCD tipo 1: se actualiza sobre el mismo registro)
CREATE OR REPLACE TABLE dim_customer (
    customer_key         INT IDENTITY(1,1) ORDER PRIMARY KEY,
    customer_name        VARCHAR(200) NOT NULL,   -- clave natural
    customer_type        VARCHAR(50),             -- 'Individual', 'Empresa', 'Gobierno'
    city                 VARCHAR(100),
    first_delivery_date  DATE,
    total_deliveries     INT,
    customer_category    VARCHAR(20)              -- 'Premium', 'Regular', 'Ocasional'
);


-- =====================================================
-- 3. TABLA DE HECHOS
-- =====================================================
-- Grano: una fila por entrega completada (delivery_status = 'delivered').
-- distance_km y fuel_consumed_liters son del viaje repartidos en partes
-- iguales entre sus entregas: así la suma por día, conductor o ruta da el
-- total real y no se multiplica por la cantidad de entregas.
CREATE OR REPLACE TABLE fact_deliveries (
    -- Claves
    delivery_key        INT IDENTITY(1,1) ORDER PRIMARY KEY,
    date_key            INT REFERENCES dim_date(date_key),       -- fecha de entrega
    scheduled_time_key  INT REFERENCES dim_time(time_key),
    delivered_time_key  INT REFERENCES dim_time(time_key),
    vehicle_key         INT REFERENCES dim_vehicle(vehicle_key),
    driver_key          INT REFERENCES dim_driver(driver_key),
    route_key           INT REFERENCES dim_route(route_key),
    customer_key        INT REFERENCES dim_customer(customer_key),

    -- Dimensiones degeneradas
    delivery_id         INT NOT NULL,
    trip_id             INT NOT NULL,
    tracking_number     VARCHAR(50),

    -- Métricas
    package_weight_kg     DECIMAL(10,2),
    distance_km           DECIMAL(10,2),
    fuel_consumed_liters  DECIMAL(10,2),
    delivery_time_minutes INT,             -- desde la salida del viaje hasta la entrega
    delay_minutes         INT,             -- minutos después de la hora programada (0 si llegó antes)

    -- Métricas calculadas
    deliveries_per_hour           DECIMAL(5,2),
    fuel_efficiency_km_per_liter  DECIMAL(5,2),
    cost_per_delivery             DECIMAL(10,2),
    revenue_per_delivery          DECIMAL(10,2),

    -- Indicadores
    is_on_time       BOOLEAN,              -- retraso de 30 minutos o menos
    is_damaged       BOOLEAN,              -- sin dato en el origen: FALSE
    has_signature    BOOLEAN,
    delivery_status  VARCHAR(20),

    -- Auditoría
    etl_batch_id   INT,
    etl_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================
-- 4. STAGING Y TOTALES DIARIOS
-- =====================================================

-- Staging: guarda cada entrega extraída tal como llegó, en formato JSON
-- (VARIANT). Sirve de respaldo y de auditoría de cada carga.
CREATE OR REPLACE TABLE staging_daily_load (
    raw_data        VARIANT,
    etl_batch_id    INT,
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Totales precalculados por día, para reportes rápidos (los llena el ETL)
CREATE OR REPLACE TABLE daily_totals (
    date_key             INT PRIMARY KEY,
    total_deliveries     INT,
    on_time_deliveries   INT,
    on_time_pct          DECIMAL(5,2),
    avg_delay_minutes    DECIMAL(10,2),
    total_weight_kg      DECIMAL(14,2),
    total_distance_km    DECIMAL(14,2),
    total_fuel_liters    DECIMAL(14,2),
    total_cost           DECIMAL(16,2),
    total_revenue        DECIMAL(16,2),
    active_drivers       INT,
    active_vehicles      INT,
    etl_batch_id         INT,
    updated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================
-- 5. CARGA DE dim_date Y dim_time
-- =====================================================

-- dim_date: 1.461 días (2024-01-01 a 2027-12-31). Cubre los datos
-- (oct-2024 a oct-2026) y deja margen para las cargas diarias futuras.
INSERT INTO dim_date
SELECT
    TO_NUMBER(TO_CHAR(f.d, 'YYYYMMDD'))                    AS date_key,
    f.d                                                    AS full_date,
    DAYOFWEEKISO(f.d)                                      AS day_of_week,
    DECODE(DAYOFWEEKISO(f.d), 1,'Lunes', 2,'Martes', 3,'Miércoles', 4,'Jueves',
                              5,'Viernes', 6,'Sábado', 7,'Domingo') AS day_name,
    DAY(f.d)                                               AS day_of_month,
    DAYOFYEAR(f.d)                                         AS day_of_year,
    WEEKISO(f.d)                                           AS week_of_year,
    MONTH(f.d)                                             AS month_num,
    DECODE(MONTH(f.d), 1,'Enero', 2,'Febrero', 3,'Marzo', 4,'Abril', 5,'Mayo',
                       6,'Junio', 7,'Julio', 8,'Agosto', 9,'Septiembre',
                       10,'Octubre', 11,'Noviembre', 12,'Diciembre') AS month_name,
    QUARTER(f.d)                                           AS quarter,
    YEAR(f.d)                                              AS year,
    DAYOFWEEKISO(f.d) IN (6, 7)                            AS is_weekend,
    h.fecha IS NOT NULL                                    AS is_holiday,
    h.nombre                                               AS holiday_name,
    QUARTER(f.d)                                           AS fiscal_quarter,
    YEAR(f.d)                                              AS fiscal_year
FROM (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2024-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))
) f
LEFT JOIN (
    -- Festivos de Colombia: fijos, trasladados al lunes (Ley Emiliani)
    -- y los que dependen de la Pascua
    SELECT column1::DATE AS fecha, column2 AS nombre
    FROM VALUES
        ('2024-01-01', 'Año Nuevo'),
        ('2024-01-08', 'Reyes Magos'),
        ('2024-03-25', 'San José'),
        ('2024-03-28', 'Jueves Santo'),
        ('2024-03-29', 'Viernes Santo'),
        ('2024-05-01', 'Día del Trabajo'),
        ('2024-05-13', 'Ascensión del Señor'),
        ('2024-06-03', 'Corpus Christi'),
        ('2024-06-10', 'Sagrado Corazón'),
        ('2024-07-01', 'San Pedro y San Pablo'),
        ('2024-07-20', 'Día de la Independencia'),
        ('2024-08-07', 'Batalla de Boyacá'),
        ('2024-08-19', 'Asunción de la Virgen'),
        ('2024-10-14', 'Día de la Raza'),
        ('2024-11-04', 'Todos los Santos'),
        ('2024-11-11', 'Independencia de Cartagena'),
        ('2024-12-08', 'Inmaculada Concepción'),
        ('2024-12-25', 'Navidad'),
        ('2025-01-01', 'Año Nuevo'),
        ('2025-01-06', 'Reyes Magos'),
        ('2025-03-24', 'San José'),
        ('2025-04-17', 'Jueves Santo'),
        ('2025-04-18', 'Viernes Santo'),
        ('2025-05-01', 'Día del Trabajo'),
        ('2025-06-02', 'Ascensión del Señor'),
        ('2025-06-23', 'Corpus Christi'),
        ('2025-06-30', 'Sagrado Corazón / San Pedro y San Pablo'),
        ('2025-07-20', 'Día de la Independencia'),
        ('2025-08-07', 'Batalla de Boyacá'),
        ('2025-08-18', 'Asunción de la Virgen'),
        ('2025-10-13', 'Día de la Raza'),
        ('2025-11-03', 'Todos los Santos'),
        ('2025-11-17', 'Independencia de Cartagena'),
        ('2025-12-08', 'Inmaculada Concepción'),
        ('2025-12-25', 'Navidad'),
        ('2026-01-01', 'Año Nuevo'),
        ('2026-01-12', 'Reyes Magos'),
        ('2026-03-23', 'San José'),
        ('2026-04-02', 'Jueves Santo'),
        ('2026-04-03', 'Viernes Santo'),
        ('2026-05-01', 'Día del Trabajo'),
        ('2026-05-18', 'Ascensión del Señor'),
        ('2026-06-08', 'Corpus Christi'),
        ('2026-06-15', 'Sagrado Corazón'),
        ('2026-06-29', 'San Pedro y San Pablo'),
        ('2026-07-20', 'Día de la Independencia'),
        ('2026-08-07', 'Batalla de Boyacá'),
        ('2026-08-17', 'Asunción de la Virgen'),
        ('2026-10-12', 'Día de la Raza'),
        ('2026-11-02', 'Todos los Santos'),
        ('2026-11-16', 'Independencia de Cartagena'),
        ('2026-12-08', 'Inmaculada Concepción'),
        ('2026-12-25', 'Navidad'),
        ('2027-01-01', 'Año Nuevo'),
        ('2027-01-11', 'Reyes Magos'),
        ('2027-03-22', 'San José'),
        ('2027-03-25', 'Jueves Santo'),
        ('2027-03-26', 'Viernes Santo'),
        ('2027-05-01', 'Día del Trabajo'),
        ('2027-05-10', 'Ascensión del Señor'),
        ('2027-05-31', 'Corpus Christi'),
        ('2027-06-07', 'Sagrado Corazón'),
        ('2027-07-05', 'San Pedro y San Pablo'),
        ('2027-07-20', 'Día de la Independencia'),
        ('2027-08-07', 'Batalla de Boyacá'),
        ('2027-08-16', 'Asunción de la Virgen'),
        ('2027-10-18', 'Día de la Raza'),
        ('2027-11-01', 'Todos los Santos'),
        ('2027-11-15', 'Independencia de Cartagena'),
        ('2027-12-08', 'Inmaculada Concepción'),
        ('2027-12-25', 'Navidad')
) h ON f.d = h.fecha;

-- dim_time: 1.440 minutos del día
INSERT INTO dim_time
SELECT
    t.h * 100 + t.m                                        AS time_key,
    t.h                                                    AS hour,
    t.m                                                    AS minute,
    0                                                      AS second,
    CASE WHEN t.h < 6  THEN 'Madrugada'
         WHEN t.h < 12 THEN 'Mañana'
         WHEN t.h < 18 THEN 'Tarde'
         ELSE 'Noche' END                                  AS time_of_day,
    LPAD(t.h::VARCHAR, 2, '0') || ':' || LPAD(t.m::VARCHAR, 2, '0') AS hour_24,
    LPAD(IFF(MOD(t.h, 12) = 0, 12, MOD(t.h, 12))::VARCHAR, 2, '0') || ':' ||
        LPAD(t.m::VARCHAR, 2, '0') || IFF(t.h < 12, ' AM', ' PM')   AS hour_12,
    IFF(t.h < 12, 'AM', 'PM')                              AS am_pm,
    t.h BETWEEN 8 AND 17                                   AS is_business_hour,
    CASE WHEN t.h BETWEEN 6 AND 13  THEN 'Turno 1'         -- 06:00 a 13:59
         WHEN t.h BETWEEN 14 AND 21 THEN 'Turno 2'         -- 14:00 a 21:59
         ELSE 'Turno 3' END                                AS shift  -- 22:00 a 05:59
FROM (
    SELECT FLOOR(n / 60)::INT AS h, MOD(n, 60)::INT AS m
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
          FROM TABLE(GENERATOR(ROWCOUNT => 1440)))
) t;


-- =====================================================
-- 6. VISTAS SEGURAS Y ROLES
-- =====================================================
-- SECURE VIEW: quien la consulta ve el resultado, pero no la definición
-- ni las tablas de origen.

-- Ventas: datos comerciales (clientes, pesos, ingresos). No incluye datos de
-- conductores, vehículos ni costos operativos, y excluye a los clientes de
-- tipo Gobierno. (Con los datos actuales no hay clientes de ese tipo; el
-- filtro queda preparado para cuando existan.)
CREATE OR REPLACE SECURE VIEW v_sales_deliveries AS
SELECT
    d.full_date,
    c.customer_name,
    c.customer_type,
    c.customer_category,
    c.city,
    f.package_weight_kg,
    f.delivery_status,
    f.is_on_time,
    f.revenue_per_delivery
FROM fact_deliveries f
JOIN dim_date d     ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_type <> 'Gobierno';

-- Operaciones: todo el detalle operativo
CREATE OR REPLACE SECURE VIEW v_operations_deliveries AS
SELECT
    d.full_date,
    t.hour_24            AS hora_programada,
    t.shift              AS turno,
    v.license_plate,
    v.vehicle_type,
    dr.full_name         AS conductor,
    r.route_code,
    r.origin_city,
    r.destination_city,
    c.customer_name,
    f.delivery_time_minutes,
    f.delay_minutes,
    f.is_on_time,
    f.distance_km,
    f.fuel_consumed_liters,
    f.cost_per_delivery,
    f.revenue_per_delivery
FROM fact_deliveries f
JOIN dim_date d      ON f.date_key = d.date_key
JOIN dim_time t      ON f.scheduled_time_key = t.time_key
JOIN dim_vehicle v   ON f.vehicle_key = v.vehicle_key
JOIN dim_driver dr   ON f.driver_key = dr.driver_key
JOIN dim_route r     ON f.route_key = r.route_key
JOIN dim_customer c  ON f.customer_key = c.customer_key;

-- Roles
CREATE ROLE IF NOT EXISTS SALES_ANALYST;
CREATE ROLE IF NOT EXISTS OPERATIONS_ANALYST;

-- Para consultar una vista, el rol necesita además usar el warehouse,
-- la base y el esquema
GRANT USAGE ON WAREHOUSE FLEETLOGIX_WH      TO ROLE SALES_ANALYST;
GRANT USAGE ON DATABASE  FLEETLOGIX_DW      TO ROLE SALES_ANALYST;
GRANT USAGE ON SCHEMA    FLEETLOGIX_DW.ANALYTICS TO ROLE SALES_ANALYST;
GRANT SELECT ON VIEW v_sales_deliveries     TO ROLE SALES_ANALYST;

GRANT USAGE ON WAREHOUSE FLEETLOGIX_WH      TO ROLE OPERATIONS_ANALYST;
GRANT USAGE ON DATABASE  FLEETLOGIX_DW      TO ROLE OPERATIONS_ANALYST;
GRANT USAGE ON SCHEMA    FLEETLOGIX_DW.ANALYTICS TO ROLE OPERATIONS_ANALYST;
GRANT SELECT ON VIEW v_operations_deliveries TO ROLE OPERATIONS_ANALYST;

-- Los roles se asignan al usuario actual para poder probarlos
SET usuario = '"' || CURRENT_USER() || '"';   -- entre comillas por si el nombre tiene símbolos
GRANT ROLE SALES_ANALYST      TO USER IDENTIFIER($usuario);
GRANT ROLE OPERATIONS_ANALYST TO USER IDENTIFIER($usuario);


-- =====================================================
-- 7. VERIFICACIÓN
-- =====================================================

-- Debe listar 9 tablas; la columna retention_time debe mostrar 30
SHOW TABLES IN SCHEMA FLEETLOGIX_DW.ANALYTICS;

-- Debe mostrar max_cluster_count = 3 y auto_resume = true
SHOW WAREHOUSES LIKE 'FLEETLOGIX_WH';

-- Deben dar 1461 y 1440
SELECT 'dim_date' AS tabla, COUNT(*) AS filas FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time;

-- Festivos por año: 18 en cada año (en 2025 hay 17 fechas, porque
-- Sagrado Corazón y San Pedro y San Pablo caen el mismo lunes, 30 de junio)
SELECT year, COUNT(*) AS festivos
FROM dim_date
WHERE is_holiday
GROUP BY year
ORDER BY year;

-- Turnos: 480 minutos cada uno
SELECT shift, COUNT(*) AS minutos
FROM dim_time
GROUP BY shift
ORDER BY shift;
