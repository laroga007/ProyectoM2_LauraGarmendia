-- =====================================================
-- FLEETLOGIX - AVANCE 2: ANÁLISIS DE QUERIES
-- 8 queries seleccionadas de las 12 proporcionadas
-- Motor: PostgreSQL 16 · Ejecutadas en DBeaver
-- =====================================================
-- Selección (según consigna): las 3 básicas (Q1, Q2, Q3), Q4 y Q5,
-- una entre Q6/Q7/Q8 -> Q8, la Q9, y una entre Q10/Q11/Q12 -> Q12.
--
-- Criterio de elección:
--   * Q8: filtra la tabla más grande (deliveries, 400.000 filas) por fecha
--     y analiza un problema operativo concreto (retrasos).
--   * Q12: se eligió frente a la Q10. La Q10 busca identificar a los
--     conductores con mejor desempeño, pero en estos datos sintéticos los
--     viajes se asignan según disponibilidad y descanso del conductor, no
--     según su desempeño: las diferencias serían producto del azar. Además,
--     el consumo depende sobre todo del tipo de vehículo asignado. La Q12
--     analiza patrones de entregas por hora y día que sí están presentes en
--     los datos y sirven para planificar turnos y personal.
--
-- Nota: los filtros "últimos N días" se calculan desde CURRENT_DATE. Los
-- datos llegan hasta la fecha de generación (15/10/2026), por lo que una
-- ventana de 60 días tomada el 28/10 contiene unos 47 días de datos.
-- =====================================================


-- =====================================================
-- QUERIES BÁSICAS
-- =====================================================

-- Query 1: Contar vehículos por tipo
-- Problema de negocio: conocer la composición de la flota
-- Resultado: Camión Mediano 63, Van 61, Camión Grande 60, Motocicleta 16
SELECT
    vehicle_type,               -- tipo de vehículo
    COUNT(*) AS cantidad        -- cuántos vehículos hay de ese tipo
FROM vehicles
GROUP BY vehicle_type
ORDER BY cantidad DESC;


-- Query 2: Conductores con licencia próxima a vencer
-- Problema de negocio: prevenir problemas legales por licencias vencidas
-- Resultado: 23 conductores; 14 con la licencia YA vencida y 9 que vencen
-- en los próximos 30 días (la condición incluye ambos casos)
SELECT
    first_name,
    last_name,
    license_number,
    license_expiry
FROM drivers
WHERE license_expiry < CURRENT_DATE + INTERVAL '30 days'
ORDER BY license_expiry;

-- Q2 mejorada (propuesta): distingue licencias vencidas de licencias por vencer
SELECT
    first_name,
    last_name,
    license_number,
    license_expiry,
    CASE WHEN license_expiry < CURRENT_DATE
         THEN 'Vencida'
         ELSE 'Por vencer' END AS estado_licencia
FROM drivers
WHERE license_expiry < CURRENT_DATE + INTERVAL '30 days'
ORDER BY license_expiry;


-- Query 3: Total de viajes por estado
-- Problema de negocio: monitorear operaciones en curso
-- Resultado: 99.981 completados y 19 en curso (a la fecha de generación)
SELECT
    status,
    COUNT(*) AS total_viajes
FROM trips
GROUP BY status;


-- =====================================================
-- QUERIES INTERMEDIAS
-- =====================================================

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
-- Problema de negocio: identificar la demanda por ciudad para planificar recursos
-- Resultado: 5 ciudades, 6.936 viajes en total; Bogotá concentra la mayor demanda
SELECT
    r.destination_city,
    COUNT(DISTINCT t.trip_id) AS total_viajes,
    COUNT(d.delivery_id) AS total_entregas,
    SUM(d.package_weight_kg) AS peso_total_kg
FROM routes r
INNER JOIN trips t ON r.route_id = t.route_id
INNER JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY r.destination_city
ORDER BY total_entregas DESC;

-- Verificación Q4: el total debe coincidir con la suma de viajes por ciudad (6.936)
SELECT COUNT(*) AS total_viajes
FROM trips
WHERE departure_datetime >= CURRENT_DATE - INTERVAL '60 days';


-- Query 5: Conductores activos con cantidad de viajes completados
-- Problema de negocio: evaluar la carga de trabajo por conductor
-- Resultado: 377 conductores activos; de más de 350 viajes a menos de 10
SELECT
    d.driver_id,
    d.first_name || ' ' || d.last_name AS nombre_completo,
    d.license_expiry,
    COUNT(t.trip_id) AS viajes_totales,
    SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) AS viajes_completados
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id
WHERE d.status = 'active'
GROUP BY d.driver_id, d.first_name, d.last_name, d.license_expiry
HAVING COUNT(t.trip_id) > 0
ORDER BY viajes_completados DESC;

-- Verificación Q5: los 10 conductores activos con menos viajes y su antigüedad
-- Resultado: todos fueron contratados entre agosto y octubre de 2026 y tienen
-- la licencia vigente -> la diferencia se explica por antigüedad
SELECT
    d.first_name || ' ' || d.last_name AS nombre_completo,
    d.hire_date,
    d.license_expiry,
    COUNT(t.trip_id) AS viajes_totales
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id
WHERE d.status = 'active'
GROUP BY d.driver_id, d.first_name, d.last_name, d.hire_date, d.license_expiry
ORDER BY viajes_totales ASC
LIMIT 10;


-- Query 8: Análisis de entregas retrasadas por día de la semana
-- Problema de negocio: identificar patrones de retraso para mejorar la planificación
-- Resultado: entre 12% y 15% de entregas con más de 30 min de retraso todos los
-- días y unos 15-18 min de diferencia promedio: no hay un día crítico
SELECT
    TO_CHAR(d.scheduled_datetime, 'Day') AS dia_semana,
    EXTRACT(DOW FROM d.scheduled_datetime) AS num_dia,
    COUNT(*) AS total_entregas,
    COUNT(CASE
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes'
        THEN 1
    END) AS entregas_retrasadas,
    ROUND(100.0 * COUNT(CASE
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes'
        THEN 1
    END) / COUNT(*), 2) AS porcentaje_retrasos,
    AVG(EXTRACT(EPOCH FROM (d.delivered_datetime - d.scheduled_datetime)) / 60) AS minutos_promedio_diferencia
FROM deliveries d
WHERE d.delivery_status = 'delivered'
    AND d.scheduled_datetime >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY dia_semana, num_dia
ORDER BY num_dia;


-- =====================================================
-- QUERIES COMPLEJAS
-- =====================================================

-- Query 9 (VERSIÓN INICIAL): Costo de mantenimiento por kilómetro recorrido
-- Problema de negocio: evaluar el costo-beneficio de cada tipo de vehículo
-- Nota técnica: al unir trips y maintenance en el mismo JOIN sobre el mismo
-- vehículo se generan todas las combinaciones viaje x mantenimiento: cada costo
-- se suma tantas veces como viajes tiene el vehículo, y cada km tantas veces como
-- mantenimientos. Los totales resultan ~600 veces mayores que el valor real.
-- Se conserva como referencia; los resultados válidos son los de la versión ajustada (abajo).
WITH vehicle_metrics AS (
    SELECT
        v.vehicle_id,
        v.vehicle_type,
        v.license_plate,
        COUNT(DISTINCT t.trip_id) AS total_viajes,
        SUM(r.distance_km) AS km_totales,
        SUM(m.cost) AS costo_mantenimiento_total,
        COUNT(DISTINCT m.maintenance_id) AS cantidad_mantenimientos
    FROM vehicles v
    LEFT JOIN trips t ON v.vehicle_id = t.vehicle_id
    LEFT JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
    WHERE t.status = 'completed'
    GROUP BY v.vehicle_id, v.vehicle_type, v.license_plate
)
SELECT
    vehicle_type,
    COUNT(vehicle_id) AS cantidad_vehiculos,
    SUM(total_viajes) AS viajes_totales,
    SUM(km_totales) AS kilometros_totales,
    SUM(costo_mantenimiento_total) AS costo_total_mantenimiento,
    ROUND(SUM(costo_mantenimiento_total) / NULLIF(SUM(km_totales), 0), 2) AS costo_por_km,
    ROUND(AVG(costo_mantenimiento_total / NULLIF(cantidad_mantenimientos, 0)), 2) AS costo_promedio_por_mantenimiento
FROM vehicle_metrics
WHERE km_totales > 0 AND costo_mantenimiento_total > 0
GROUP BY vehicle_type
ORDER BY costo_por_km DESC;

-- Verificación Q9: costo real total de mantenimiento, sin JOINs
-- Resultado: 5.000 mantenimientos, $1.941.943.139,90 COP (~$388.000 promedio)
SELECT
    COUNT(*) AS cantidad_mantenimientos,
    SUM(cost) AS costo_total_real
FROM maintenance;

-- Query 9 (VERSIÓN AJUSTADA): viajes y mantenimientos se agregan por separado
-- (una fila por vehículo en cada CTE) y recién después se unen.
-- Resultado: Motocicleta ~48 COP/km (la más cara por km, la más barata por
-- servicio), Camión Grande ~34, Camión Mediano ~25, Van ~23 (la más eficiente)
WITH km_por_vehiculo AS (
    SELECT
        t.vehicle_id,
        COUNT(*) AS total_viajes,
        SUM(r.distance_km) AS km_totales
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    WHERE t.status = 'completed'
    GROUP BY t.vehicle_id
),
mant_por_vehiculo AS (
    SELECT
        vehicle_id,
        COUNT(*) AS cantidad_mantenimientos,
        SUM(cost) AS costo_mantenimiento_total
    FROM maintenance
    GROUP BY vehicle_id
)
SELECT
    v.vehicle_type,
    COUNT(*) AS cantidad_vehiculos,
    SUM(k.total_viajes) AS viajes_totales,
    SUM(k.km_totales) AS kilometros_totales,
    SUM(m.costo_mantenimiento_total) AS costo_total_mantenimiento,
    ROUND(SUM(m.costo_mantenimiento_total) / NULLIF(SUM(k.km_totales), 0), 2) AS costo_por_km,
    ROUND(SUM(m.costo_mantenimiento_total) / SUM(m.cantidad_mantenimientos), 2) AS costo_promedio_por_mantenimiento
FROM vehicles v
JOIN km_por_vehiculo k   ON v.vehicle_id = k.vehicle_id
JOIN mant_por_vehiculo m ON v.vehicle_id = m.vehicle_id
GROUP BY v.vehicle_type
ORDER BY costo_por_km DESC;


-- Query 12: Pivot de entregas por hora y día de la semana
-- Problema de negocio: optimizar horarios de operación y personal
-- Resultado: de martes a sábado las entregas se reparten de forma pareja; el
-- domingo concentra entregas en las primeras horas y decrece (viajes del sábado
-- que llegan a destino), y el lunes muestra el patrón inverso
WITH entregas_por_hora_dia AS (
    SELECT
        EXTRACT(DOW FROM scheduled_datetime) AS dia_semana,
        EXTRACT(HOUR FROM scheduled_datetime) AS hora,
        COUNT(*) AS cantidad_entregas
    FROM deliveries
    WHERE scheduled_datetime >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY EXTRACT(DOW FROM scheduled_datetime), EXTRACT(HOUR FROM scheduled_datetime)
)
SELECT
    hora,
    SUM(CASE WHEN dia_semana = 0 THEN cantidad_entregas ELSE 0 END) AS domingo,
    SUM(CASE WHEN dia_semana = 1 THEN cantidad_entregas ELSE 0 END) AS lunes,
    SUM(CASE WHEN dia_semana = 2 THEN cantidad_entregas ELSE 0 END) AS martes,
    SUM(CASE WHEN dia_semana = 3 THEN cantidad_entregas ELSE 0 END) AS miercoles,
    SUM(CASE WHEN dia_semana = 4 THEN cantidad_entregas ELSE 0 END) AS jueves,
    SUM(CASE WHEN dia_semana = 5 THEN cantidad_entregas ELSE 0 END) AS viernes,
    SUM(CASE WHEN dia_semana = 6 THEN cantidad_entregas ELSE 0 END) AS sabado,
    SUM(cantidad_entregas) AS total_semana
FROM entregas_por_hora_dia
WHERE hora BETWEEN 6 AND 22
GROUP BY hora
ORDER BY hora;
