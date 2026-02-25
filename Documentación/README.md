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



-- generar la llave 
openssl genrsa -out snowflake_key 2048
-- escribirla 
openssl rsa -in snowflake_key -pubout -out snowflake_key.pub

-- verla la llave
cat snowflake_key.pub



librería que guarda tokens de autenticación de forma segura
pip install snowflake-connector-python[secure-local-storage]

--Generar nueva llave en formato PEM - git bash
openssl genrsa -out snowflake_key.pem 2048
openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub

--Convertir la llave PEM a DER 
openssl pkcs8 -topk8 -inform PEM -outform DER -in snowflake_key.pem -out snowflake_key.der -nocrypt

--Verificar que se creó
ls -la snowflake_key.der

-- sql - Actualizar Snowflake con la llave pública
ALTER USER LUISPACHECO90 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApeoWJfLFJlUftKTSmYlh
RU/0nKwagGDv1pac2dH70tPQmQQPuTv/mYDY5Y7drxHQkwcln/QCyab/I1eloZiU
H0fcfecvfPlCM29A3IjF1RbDjb/BeljsufB8s5bhJpiYX1BfJCm52trKjuCKSwYo
7DsZAXLrnIBs3xsDyEyZ98h+i5Lz7n8gqaGM3XczLTTT/u/3dTAlHHAtgQh2+phU
J7Zmc9Cv69NkNqhzALVXPVnx4ju5A4U6mfTSHSFYWcN9EKKUzEfeqeLcL39fkDAV
S7oh30KUuhlTInMaAAdwhWgSZpFJlxXodVsy5261gV/cAXM/g6xVBef/LTTZHA9t
QwIDAQAB';


customers = df[['customer_name']].drop_duplicates()
            for _, row in customers.iterrows():
                cursor.execute("""
                    MERGE INTO dim_customer c
                    USING (SELECT %s as customer_name) s
                    ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN
                        INSERT (customer_name, customer_type, city, first_delivery_date, 
                               total_deliveries, customer_category)
                        VALUES (%s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                """, (row['customer_name'], row['customer_name'], 
                     df[df['customer_name'] == row['customer_name']]['destination_city'].iloc[0]))
            
            # Actualizar dimensiones SCD Type 2 si hay cambios
            # (Ejemplo simplificado para dim_driver)
            cursor.execute("""
                UPDATE dim_driver 
                SET valid_to = CURRENT_DATE() - 1, is_current = FALSE
                WHERE driver_id IN (
                    SELECT DISTINCT driver_id 
                    FROM staging_daily_load
                ) AND is_current = TRUE
            """)