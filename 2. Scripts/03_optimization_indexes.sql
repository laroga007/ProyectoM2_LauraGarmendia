-- =====================================================
-- FLEETLOGIX - AVANCE 2: ÍNDICES DE OPTIMIZACIÓN
-- Motor: PostgreSQL 16 · Ejecutado en DBeaver
-- =====================================================
-- Método:
--   1. Medir cada query con EXPLAIN ANALYZE ANTES de crear índices.
--   2. Evaluar los 5 índices propuestos y crear el set final.
--   3. Actualizar estadísticas (VACUUM ANALYZE) y volver a medir DESPUÉS.
--   Criterio: cada medición se ejecutó 2 veces y se registró la segunda.
--
-- Evaluación de los índices propuestos:
--   * idx_trips_composite_joins       -> SE USA (Q9 ajustada). Se conserva.
--   * idx_maintenance_vehicle_cost    -> SE USA (Q9). Se conserva.
--   * idx_deliveries_scheduled_datetime (parcial, solo 'delivered')
--                                     -> la Q12 no puede usarlo porque no
--                                        filtra por estado. Se reemplaza.
--   * idx_drivers_status_license      -> tabla de 400 filas: PostgreSQL
--                                        prefiere leerla completa. Se descarta.
--   * idx_routes_metrics              -> tabla de 50 filas: no se usa. Se descarta.
-- Set final: 2 índices de los propuestos + 3 nuevos.
-- =====================================================


-- Índice 1 (nuevo): entregas por fecha programada -> Q8 y Q12
-- Índice cubriente: INCLUDE guarda dentro del índice las columnas que esas
-- queries también leen, así no necesitan abrir la tabla (Index Only Scan)
CREATE INDEX idx_deliveries_scheduled ON deliveries(scheduled_datetime)
    INCLUDE (delivered_datetime, delivery_status);

-- Índice 2 (nuevo): entregas por viaje -> JOIN trips-deliveries de Q4
-- Indexa la clave foránea deliveries.trip_id (PostgreSQL no lo hace solo)
CREATE INDEX idx_deliveries_trip_id ON deliveries(trip_id);

-- Índice 3 (nuevo): viajes por estado -> Q3
-- Permite contar por estado leyendo solo el índice (Index Only Scan)
CREATE INDEX idx_trips_status ON trips(status);

-- Índice 4 (propuesto): viajes completados por vehículo, conductor y ruta -> Q9
-- Índice parcial: solo guarda los viajes con status = 'completed'
CREATE INDEX idx_trips_composite_joins ON trips(vehicle_id, driver_id, route_id, departure_datetime)
    WHERE status = 'completed';

-- Índice 5 (propuesto): mantenimientos por vehículo con su costo -> Q9
CREATE INDEX idx_maintenance_vehicle_cost ON maintenance(vehicle_id, cost);


-- =====================================================
-- AJUSTE DE CONFIGURACIÓN
-- =====================================================
-- random_page_cost viene en 4 por defecto, pensado para discos rígidos. En un
-- disco SSD el acceso aleatorio es casi tan rápido como el secuencial; con 1.1
-- el planificador valora mejor los índices. Sin este ajuste, la Q4 no usaba
-- idx_deliveries_trip_id. Se aplica a las conexiones nuevas (reconectar).
ALTER DATABASE fleetlogix SET random_page_cost = 1.1;

-- Actualiza las estadísticas que usa el planificador
VACUUM ANALYZE;


-- =====================================================
-- VERIFICACIÓN
-- =====================================================
-- Debe listar 8 índices: los 3 del esquema y los 5 creados
SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
ORDER BY tablename;

-- Debe devolver 1.1
SHOW random_page_cost;


-- =====================================================
-- RESULTADOS (Execution Time, ms)
-- =====================================================
-- Query          Antes      Después   Mejora   Plan después
-- Q1              0,219      0,161     ruido   Seq Scan (tabla de 200 filas)
-- Q2              0,125      0,156     ruido   Seq Scan (tabla de 400 filas)
-- Q3             22,026     15,822      28%    Index Only Scan idx_trips_status
-- Q4             37,170     33,841       9%    Index Scan idx_deliveries_trip_id
-- Q5             36,743     37,707       -     Seq Scan (necesita todos los viajes)
-- Q8             75,841     43,264      43%    Index Only Scan idx_deliveries_scheduled
-- Q9 inicial    801,861    479,250      40%    más rápida, pero con totales sobrestimados
-- Q9 ajustada    38,274     17,481      54%    Index Only Scan (índices 4 y 5)
-- Q12            38,530     22,334      42%    Index Only Scan idx_deliveries_scheduled
--
-- Q9 inicial -> Q9 ajustada + índices: 801,861 -> 17,481 ms (98%)
-- Conclusión: un índice acelera una consulta, pero no cambia lo que calcula.
-- La mayor ganancia vino de ajustar la lógica de agregación de la Q9.
