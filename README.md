tabla trip - driver
Fechas espaciadas uniformemente (cada 10.5 minutos) cuando deberían tener picos

Combustible no varía por tipo de vehículo (todos usan 8-15 L/100km)

Estado del viaje depende de cuándo ejecuto el script (no es coherente)

No valida que el conductor esté disponible (podría tener dos viajes al mismo tiempo)

-- viajes cruzados
SELECT COUNT(*)
FROM trips t1
JOIN trips t2 
    ON t1.driver_id = t2.driver_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime;

-- conductores están involucrados - 373
SELECT COUNT(DISTINCT t1.driver_id) 
FROM trips t1
JOIN trips t2 
    ON t1.driver_id = t2.driver_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime;
-- resultados graves
SELECT 
    t1.driver_id,
    COUNT(*) AS conflictos
FROM trips t1
JOIN trips t2 
    ON t1.driver_id = t2.driver_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime
GROUP BY t1.driver_id
ORDER BY conflictos DESC
LIMIT 10;

-- viajes totales de trip
select count(*) from trips t 

-- Consulta para ver los viajes conflictivos
SELECT 
    t1.driver_id,
    t1.trip_id AS trip_1,
    t2.trip_id AS trip_2,
    t1.departure_datetime AS trip1_start,
    t1.arrival_datetime AS trip1_end,
    t2.departure_datetime AS trip2_start,
    t2.arrival_datetime AS trip2_end
FROM trips t1
JOIN trips t2 
    ON t1.driver_id = t2.driver_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime
ORDER BY t1.driver_id
LIMIT 100;

-- peores casos por conductor
SELECT 
    driver_id,
    COUNT(DISTINCT trip_id) AS viajes_con_conflicto
FROM (
    SELECT t1.driver_id, t1.trip_id
    FROM trips t1
    JOIN trips t2 
        ON t1.driver_id = t2.driver_id
        AND t1.trip_id < t2.trip_id
    WHERE 
        t1.arrival_datetime IS NOT NULL
        AND t2.arrival_datetime IS NOT NULL
        AND t1.departure_datetime < t2.arrival_datetime
        AND t2.departure_datetime < t1.arrival_datetime

    UNION

    SELECT t2.driver_id, t2.trip_id
    FROM trips t1
    JOIN trips t2 
        ON t1.driver_id = t2.driver_id
        AND t1.trip_id < t2.trip_id
    WHERE 
        t1.arrival_datetime IS NOT NULL
        AND t2.arrival_datetime IS NOT NULL
        AND t1.departure_datetime < t2.arrival_datetime
        AND t2.departure_datetime < t1.arrival_datetime
) sub
GROUP BY driver_id
ORDER BY viajes_con_conflicto DESC
LIMIT 10;

-- Detectar vehículos con viajes simultáneos - 29364
SELECT 
    t1.vehicle_id,
    t1.trip_id AS trip_1,
    t2.trip_id AS trip_2,
    t1.departure_datetime AS trip1_start,
    t1.arrival_datetime AS trip1_end,
    t2.departure_datetime AS trip2_start,
    t2.arrival_datetime AS trip2_end
FROM trips t1
JOIN trips t2 
    ON t1.vehicle_id = t2.vehicle_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime
ORDER BY t1.vehicle_id
LIMIT 100;

-- Solo contar conflictos - 
SELECT COUNT(*)
FROM trips t1
JOIN trips t2 
    ON t1.vehicle_id = t2.vehicle_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime;

-- Cuántos vehículos están afectados - 181
SELECT COUNT(DISTINCT t1.vehicle_id)
FROM trips t1
JOIN trips t2 
    ON t1.vehicle_id = t2.vehicle_id
    AND t1.trip_id < t2.trip_id
WHERE 
    t1.arrival_datetime IS NOT NULL
    AND t2.arrival_datetime IS NOT NULL
    AND t1.departure_datetime < t2.arrival_datetime
    AND t2.departure_datetime < t1.arrival_datetime;

--
