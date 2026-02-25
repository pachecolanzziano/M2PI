-- Conteo de registros por tabla ✅
SELECT 'vehicles' as tabla, COUNT(*) as cantidad FROM vehicles
UNION ALL
SELECT 'drivers', COUNT(*) FROM drivers
UNION ALL
SELECT 'routes', COUNT(*) FROM routes
UNION ALL
SELECT 'trips', COUNT(*) FROM trips
UNION ALL
SELECT 'deliveries', COUNT(*) FROM deliveries
UNION ALL
SELECT 'maintenance', COUNT(*) FROM maintenance
ORDER BY cantidad DESC;

-- Entregas_imposibles ✅ 
SELECT COUNT(*) as entregas_imposibles
FROM deliveries d
JOIN trips t ON d.trip_id = t.trip_id
WHERE d.delivered_datetime IS NOT NULL
AND d.delivered_datetime < t.departure_datetime;

-- Clasifica todos los viajes según su nivel de ocupación en porcentaje.✅
-- lo porcentajes obtenidos estan en el rango normal y optimo
SELECT *
FROM (
    SELECT 
        CASE 
            WHEN ocupacion < 30 THEN 'Baja (<30%)'
            WHEN ocupacion BETWEEN 30 AND 70 THEN 'Normal (30-70%)'
            WHEN ocupacion BETWEEN 70 AND 95 THEN 'Óptima (70-95%)'
            ELSE 'Sobre (>95%)'
        END as rango_ocupacion,
        COUNT(*) as cantidad_viajes,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as porcentaje
    FROM (
        SELECT 
            t.trip_id,
            (t.total_weight_kg / v.capacity_kg * 100) as ocupacion
        FROM trips t
        JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        WHERE v.capacity_kg > 0
    ) subconsulta
    GROUP BY 
        CASE 
            WHEN ocupacion < 30 THEN 'Baja (<30%)'
            WHEN ocupacion BETWEEN 30 AND 70 THEN 'Normal (30-70%)'
            WHEN ocupacion BETWEEN 70 AND 95 THEN 'Óptima (70-95%)'
            ELSE 'Sobre (>95%)'
        END
) resultado
ORDER BY 
    CASE rango_ocupacion
        WHEN 'Baja (<30%)' THEN 1
        WHEN 'Normal (30-70%)' THEN 2
        WHEN 'Óptima (70-95%)' THEN 3
        ELSE 4
    END;

