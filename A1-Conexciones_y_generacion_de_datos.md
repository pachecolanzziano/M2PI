# 🚀 Módulo 2 - Proyecto Integrador - Avance 1
### 👨‍💻 Presentado por:
## Luis Carlos Pacheco Lanzziano  
> Proyecto desarrollado como parte del Módulo 2 del programa de Ciencia de Datos, aplicando análisis, modelado y documentación técnica.
# Contenido

- [1 Analisis del modelo proporcionado](#1-analisis-del-modelo-proporcionado)
    - [1.1 Analisis del modelo proporcionado](#11-Analisis-del-modelo-proporcionado)
    - [1.2 Patrones de negocio implícitos](#12-patrones-de-negocio-implícitos)
    - [1.3 Diagrama entidad relacion](#13-diagrama-entidad-relacion)
    - [1.4 Posibles mejoras sin modificar la estructura base](#14-posibles-mejoras-sin-modificar-la-estructura-base)

---
## 1 Analisis del modelo proporcionado
| TABLA       | PROPÓSITO                         | ANALOGÍA MUNDO REAL              |
|-------------|----------------------------------|-----------------------------------|
| vehicles    | Activos fijos de la empresa (200 unidades) | Los camiones en el patio de la bodega |
| drivers     | Capital humano (400 empleados)   | Los choferes con su licencia y uniforme |
| routes      | Conocimiento de negocio (50 trayectos clave) | Mapas de entregas pre-aprobados |
| trips       | Eventos principales (100k viajes) | Cada salida de un vehículo |
| deliveries  | Corazón del negocio (400k entregas) | Cada caja/paquete que se entrega |
| maintenance | Salud de la flota (5k servicios) | Las visitas al taller mecánico |

## 1.1 Documentación de relaciones y constrainsts
### Relaciones (Foreign Keys)
| Tabla Origen | Columna    | → | Tabla Destino | Columna    | Tipo Relación | Descripción                                   |
| ------------ | ---------- | - | ------------- | ---------- | ------------- | --------------------------------------------- |
| trips        | vehicle_id | → | vehicles      | vehicle_id | Muchos a Uno  | Un vehículo puede tener muchos viajes         |
| trips        | driver_id  | → | drivers       | driver_id  | Muchos a Uno  | Un conductor puede tener muchos viajes        |
| trips        | route_id   | → | routes        | route_id   | Muchos a Uno  | Una ruta puede tener muchos viajes            |
| deliveries   | trip_id    | → | trips         | trip_id    | Muchos a Uno  | Un viaje puede tener muchas entregas          |
| maintenance  | vehicle_id | → | vehicles      | vehicle_id | Muchos a Uno  | Un vehículo puede tener muchos mantenimientos |

### Constraints de Unicidad
| Tabla      | Columna         | Tipo   | Propósito                   |
| ---------- | --------------- | ------ | --------------------------- |
| vehicles   | license_plate   | UNIQUE | Cada placa es única         |
| drivers    | employee_code   | UNIQUE | Código de empleado único    |
| drivers    | license_number  | UNIQUE | Número de licencia único    |
| routes     | route_code      | UNIQUE | Código de ruta único        |
| deliveries | tracking_number | UNIQUE | Número de seguimiento único |

### Constraints de Obligatoriedad (NOT NULL)
| Tabla       | Columnas NOT NULL                                    | Razón de Negocio                      |
| ----------- | ---------------------------------------------------- | ------------------------------------- |
| vehicles    | license_plate, vehicle_type                          | Todo vehículo debe tener placa y tipo |
| drivers     | employee_code, first_name, last_name, license_number | Datos básicos del conductor           |
| routes      | route_code, origin_city, destination_city            | Identificación de ruta                |
| trips       | departure_datetime                                   | Fecha de salida obligatoria           |
| deliveries  | tracking_number, customer_name, delivery_address     | Datos de entrega obligatorios         |
| maintenance | maintenance_date, maintenance_type                   | Fecha y tipo de mantenimiento         |

### Valores por defecto
| Tabla      | Columna             | Default         | Significado                  |
| ---------- | ------------------- | --------------- | ---------------------------- |
| vehicles   | status              | `'active'`      | Vehículo activo por defecto  |
| drivers    | status              | `'active'`      | Conductor activo por defecto |
| routes     | toll_cost           | `0`             | Por defecto sin peaje        |
| trips      | status              | `'in_progress'` | Viaje inicia "en progreso"   |
| deliveries | delivery_status     | `'pending'`     | Entrega inicia "pendiente"   |
| deliveries | recipient_signature | `FALSE`         | Sin firma por defecto        |

## 1.2 Patrones de negocio implícitos
### *Restricciones temporales del modelo de datos*
#### *Tabla - Restricciones por Categoría*

| Categoría | Descripción | Cantidad |
|-----------|-------------|----------|
| **Tipo A** | Contratación y Licencias (RRHH) | 3 |
| **Tipo B** | Ciclo de Vida del Vehículo | 3 |
| **Tipo C** | Secuencia del Viaje | 6 |
| **Tipo D** | Relación Viaje-Entregas | 5 |
| **Tipo E** | Mantenimiento de Flota | 4 |
| **Tipo F** | Integridad Operativa | 4 |
| **Tipo G** | Reglas con Clientes (SLA) | 2 |
| **TOTAL** | | **27** |

*Estas 27 restricciones garantizan la integridad temporal y operativa de los datos de FleetLogix.*
### Tipo A: Contratación y Licencias

1. `license_expiry > hire_date`
   No se puede contratar a un conductor con la licencia vencida.

2. `license_expiry > CURRENT_DATE` (para conductores activos)
   Los conductores activos deben mantener su licencia vigente en todo momento.

3. `hire_date <= CURRENT_DATE`
   No se pueden registrar contrataciones con fecha futura.

### Tipo B: Ciclo de Vida del Vehículo

4. `acquisition_date <= CURRENT_DATE`
   La fecha de adquisición del vehículo no puede ser futura.

5. `status = 'active' → acquisition_date <= CURRENT_DATE - 30`
   Un vehículo activo debe tener al menos 30 días de antigüedad en la empresa.

6. `trips.departure_datetime >= vehicle.acquisition_date`
   No se puede usar un vehículo antes de haber sido adquirido.

### Tipo C: Secuencia del Viaje

7. `trips.departure_datetime >= driver.hire_date`
   No se pueden asignar viajes antes de contratar al conductor.

8. `trips.arrival_datetime > trips.departure_datetime`
   La llegada siempre debe ser posterior a la salida.

9. `departure_datetime BETWEEN '00:00:00' AND '23:59:59'`
   La fecha de salida debe ser un timestamp válido (no nulo).

10. `arrival_datetime - departure_datetime <= 24 horas`
    Los viajes de última milla no deben exceder un día de duración.

11. `departure_datetime >= viajes_previos.departure` (mismo vehículo)
    Un vehículo no puede realizar dos viajes simultáneamente.

12. `departure_datetime >= viajes_previos.departure` (mismo conductor)
    Un conductor no puede estar asignado a dos viajes al mismo tiempo.

### Tipo D: Relación Viaje-Entregas

13. `deliveries.delivered_datetime >= deliveries.scheduled_datetime`
    La entrega real no puede ocurrir antes de la fecha programada.

14. `deliveries.delivered_datetime <= trips.arrival_datetime`
    Las entregas deben realizarse durante el viaje, no después de su finalización.

15. `deliveries.delivered_datetime >= trips.departure_datetime`
    No se puede entregar un paquete antes de que el viaje haya comenzado.

16. `deliveries.scheduled_datetime >= trips.departure_datetime`
    La programación de entregas debe ocurrir después del inicio del viaje.

17. `deliveries.delivered_datetime` debe ser cronológico dentro del viaje. 
    Las entregas de un mismo viaje deben ocurrir en orden secuencial (delivered_1 < delivered_2 < ... < delivered_n).

### Tipo E: Mantenimiento de Flota

18. `maintenance.maintenance_date <= maintenance.next_maintenance_date`
    La fecha del próximo mantenimiento debe ser posterior al actual.

19. `maintenance.next_maintenance_date - maintenance.maintenance_date BETWEEN 30 AND 180`
    El intervalo entre mantenimientos debe ser realista: entre 1 y 6 meses.

20. `maintenance.maintenance_date >= vehicle.acquisition_date`
    No se puede registrar mantenimiento antes de adquirir el vehículo.

21. Para vehículo activo: `MAX(maintenance_date) >= CURRENT_DATE - 90`
    Todo vehículo activo debe haber recibido mantenimiento en los últimos 3 meses.

### Tipo F: Integridad Operativa

22. `driver.status = 'active'` (en todos sus viajes)
    Un conductor inactivo no puede tener viajes asignados.

23. `vehicle.status = 'active'` (en todos sus viajes)
    Un vehículo inactivo o retirado no puede realizar viajes.

24. `Tiempo entre viajes del mismo conductor ≥ 30 minutos`
    El conductor debe tener un tiempo mínimo de descanso entre viajes para carga/descarga y prevención de fatiga.

25. `driver.license_expiry >= trips.departure_datetime`
    La licencia del conductor debe estar vigente al momento de cada viaje.

### Tipo G: Reglas de Negocio con Clientes (SLA)

26. `deliveries.delivered_datetime - deliveries.scheduled_datetime ≤ 4 horas`
    La tolerancia máxima de retraso en entregas es de 4 horas (política de calidad).

27. Para entregas comerciales: `scheduled_datetime` en días hábiles
    Las entregas a comercios deben programarse en días laborales (lunes a viernes).


## 1.3 Diagrama entidad relacion
<p align="center">
  <img src="https://i.postimg.cc/kGhjr7Jj/diagrama.png" alt="Diagrama del sistema" width="600">
</p>

## 1.4 Posibles mejoras sin modificar la estructura base
## Categorías de mejora

| Categoría | Enfoque | Impacto | Esfuerzo |
|-----------|---------|---------|----------|
| **Vistas Materializadas** | Pre-calcular consultas frecuentes | Alto | Bajo |
| **Índices Estratégicos** | Acelerar búsquedas comunes | Medio | Bajo |
| **Campos Calculados** | Derivar valor sin almacenar | Alto | Muy Bajo |
| **Restricciones CHECK** | Validar datos a nivel DB | Medio | Bajo |
| **Particionamiento** | Separar datos por fecha | Alto | Medio |
| **Auditoría y Trazabilidad** | Tracking de cambios | Medio | Bajo |
| **Documentación Extendida** | Metadatos enriquecidos | Alto | Muy Bajo |
| **Funciones de Negocio** | Lógica reutilizable | Alto | Medio |

---

## Vistas materializadas (Consultas Pre-calculadas)
### Implementación sin modificar tablas:

Índices estratégicos 
Análisis de Consultas Frecuentes:

```sql

-- Índice 1: Búsqueda de viajes por conductor y fecha

CREATE INDEX idx_trips_driver_date ON trips(driver_id, departure_datetime);

-- Beneficio: Acelera reports de productividad por conductor


-- Índice 2: Búsqueda de entregas por tracking

CREATE INDEX idx_deliveries_tracking_status ON deliveries(tracking_number, delivery_status);

-- Beneficio: Consultas de cliente "¿dónde está mi paquete?" en tiempo real


-- Índice 3: Historial de mantenimiento por vehículo

CREATE INDEX idx_maintenance_vehicle_date ON maintenance(vehicle_id, maintenance_date DESC);

-- Beneficio: Obtener último mantenimiento rápido


-- Índice 4: Búsqueda por estado y fecha (reportes)

CREATE INDEX idx_trips_status_date ON trips(status, departure_datetime);


-- Índice 5: Índice compuesto para entregas por viaje

CREATE INDEX idx_deliveries_trip_status ON deliveries(trip_id, delivery_status);


-- Índice Parcial: Solo viajes activos

CREATE INDEX idx_trips_active ON trips(departure_datetime) 
WHERE status = 'in_progress';
```

### Campos calculados

```sql

-- Agregar campos calculados a trips 

ALTER TABLE trips 
ADD COLUMN trip_duration_hours DECIMAL(10,2) 
GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (arrival_datetime - departure_datetime))/3600
) STORED;

ALTER TABLE trips
ADD COLUMN fuel_efficiency_kmpl DECIMAL(10,2)
GENERATED ALWAYS AS (
    CASE 
        WHEN fuel_consumed_liters > 0 
        THEN (SELECT distance_km FROM routes r WHERE r.route_id = trips.route_id) / fuel_consumed_liters
        ELSE NULL
    END
) STORED;


-- Campo calculado en deliveries

ALTER TABLE deliveries
ADD COLUMN delay_minutes INTEGER
GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (delivered_datetime - scheduled_datetime))/60
) STORED;


-- Campo calculado en drivers

ALTER TABLE drivers
ADD COLUMN years_of_service DECIMAL(5,2)
GENERATED ALWAYS AS (
    EXTRACT(YEAR FROM age(CURRENT_DATE, hire_date))
) STORED;
⚠️ NOTA: Si la versión no soporta GENERATED, usar vistas:

sql
CREATE VIEW vw_trips_enriched AS
SELECT 
    *,
    EXTRACT(EPOCH FROM (arrival_datetime - departure_datetime))/3600 AS trip_duration_hours,
    (SELECT distance_km FROM routes r WHERE r.route_id = trips.route_id) / NULLIF(fuel_consumed_liters, 0) AS fuel_efficiency_kmpl
FROM trips;
```
### Restricciones check

Garantizar calidad de datos
```sql

-- Check 1: Peso positivo

ALTER TABLE deliveries 
ADD CONSTRAINT chk_delivery_weight_positive 
CHECK (package_weight_kg > 0);


-- Check 2: Fechas lógicas en mantenimiento

ALTER TABLE maintenance
ADD CONSTRAINT chk_maintenance_dates 
CHECK (next_maintenance_date > maintenance_date);


-- Check 3: Combustible positivo

ALTER TABLE trips
ADD CONSTRAINT chk_fuel_positive 
CHECK (fuel_consumed_liters >= 0);


-- Check 4: Estado válido en entregas

ALTER TABLE deliveries
ADD CONSTRAINT chk_delivery_status_values 
CHECK (delivery_status IN ('pending', 'delivered', 'failed'));


-- Check 5: Capacidad no negativa

ALTER TABLE vehicles
ADD CONSTRAINT chk_capacity_positive 
CHECK (capacity_kg >= 0);


-- Check 6: Fechas de licencia lógicas

ALTER TABLE drivers
ADD CONSTRAINT chk_license_expiry_future 
CHECK (license_expiry > hire_date);
```
### Particionamiento 
```sql

-- Convertir trips a tabla particionada por fecha


-- Paso 1: Renombrar tabla original
ALTER TABLE trips RENAME TO trips_old;

-- Paso 2: Crear nueva tabla particionada (misma estructura)
CREATE TABLE trips (
    LIKE trips_old INCLUDING DEFAULTS INCLUDING CONSTRAINTS
) PARTITION BY RANGE (departure_datetime);

-- Paso 3: Crear particiones por mes
CREATE TABLE trips_2022_q1 PARTITION OF trips
FOR VALUES FROM ('2022-01-01') TO ('2022-04-01');

CREATE TABLE trips_2022_q2 PARTITION OF trips
FOR VALUES FROM ('2022-04-01') TO ('2022-07-01');

CREATE TABLE trips_2022_q3 PARTITION OF trips
FOR VALUES FROM ('2022-07-01') TO ('2022-10-01');

CREATE TABLE trips_2022_q4 PARTITION OF trips
FOR VALUES FROM ('2022-10-01') TO ('2023-01-01');

CREATE TABLE trips_2023_q1 PARTITION OF trips
FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');

-- ... continuar según necesidad

-- Paso 4: Migrar datos
INSERT INTO trips SELECT * FROM trips_old;

-- Paso 5: Crear índices en cada partición
CREATE INDEX ON trips_2022_q1 (driver_id);
CREATE INDEX ON trips_2022_q2 (driver_id);
-- ... etc

-- Paso 6: Recrear foreign keys
ALTER TABLE deliveries 
ADD CONSTRAINT fk_deliveries_trips 
FOREIGN KEY (trip_id) REFERENCES trips(trip_id);
```
### Auditoría y trazabilidad
Tracking de cambios
```sql

-- Tabla de auditoría (nueva, no modifica existentes)

CREATE TABLE audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation_type CHAR(1) CHECK (operation_type IN ('I', 'U', 'D')),
    record_id INTEGER NOT NULL,
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para búsqueda
CREATE INDEX idx_audit_table_record ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_timestamp ON audit_log(changed_at);


-- Función de trigger para trips

CREATE OR REPLACE FUNCTION audit_trips_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log(table_name, operation_type, record_id, new_values, changed_by)
        VALUES ('trips', 'I', NEW.trip_id, row_to_json(NEW), current_user);
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log(table_name, operation_type, record_id, old_values, new_values, changed_by)
        VALUES ('trips', 'U', NEW.trip_id, row_to_json(OLD), row_to_json(NEW), current_user);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log(table_name, operation_type, record_id, old_values, changed_by)
        VALUES ('trips', 'D', OLD.trip_id, row_to_json(OLD), current_user);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- Trigger en trips

CREATE TRIGGER trigger_audit_trips
AFTER INSERT OR UPDATE OR DELETE ON trips
FOR EACH ROW EXECUTE FUNCTION audit_trips_changes();

-- Repetir para otras tablas (deliveries, maintenance, etc.)
```
### Documentación extendida
Enriquecer metadatos
```sql

-- Comentarios detallados en tablas

COMMENT ON TABLE vehicles IS 
'Registro maestro de vehículos FleetLogix.
Campos clave:
- vehicle_id: Identificador único interno
- license_plate: Placa visible en el vehículo (formato país)
- vehicle_type: Truck_Large(30u), Truck_Medium(50u), Van(70u), Motorcycle(50u)
- capacity_kg: Capacidad máxima de carga en kg
- status: active(85%), maintenance(10%), retired(5%)
Política de actualización: Se actualiza cuando cambia estado o se da de baja';


-- Comentarios en columnas

COMMENT ON COLUMN vehicles.acquisition_date IS 
'Fecha de compra o incorporación a la flota.
Restricciones: 
- Debe ser <= CURRENT_DATE
- Vehículos activos deben tener al menos 30 días de antigüedad
- Influye en programación de mantenimiento y depreciación';

COMMENT ON COLUMN trips.status IS 
'Estado del viaje:
- in_progress: Viaje activo (no ha llegado)
- completed: Viaje finalizado con arrival_datetime registrado
- cancelled: Viaje cancelado (no usado en este dataset)
Regla: Si arrival_datetime IS NULL, status debe ser "in_progress"';

COMMENT ON COLUMN deliveries.delivery_status IS 
'Estado de la entrega:
- pending: Programada pero no entregada (3%)
- delivered: Exitosa (92-95%)
- failed: Fallida (5%) por: ausente(3-4%), dirección(2-3%), rechazo(0.5%), daño(0.2%)
Regla: Si delivered_datetime IS NOT NULL, status debe ser "delivered" o "failed"';


-- Documentación de relaciones

COMMENT ON CONSTRAINT trips_vehicle_id_fkey ON trips IS 
'Relación viaje-vehículo: Un vehículo puede tener muchos viajes.
Restricción temporal: departure_datetime >= vehicle.acquisition_date';

COMMENT ON CONSTRAINT trips_driver_id_fkey ON trips IS 
'Relación viaje-conductor: Un conductor puede tener muchos viajes.
Restricciones:
- departure_datetime >= driver.hire_date
- driver.license_expiry >= departure_datetime
- Mínimo 30 minutos entre viajes del mismo conductor';
```
### Funciones de negocio
Encapsular reglas de negocio compleja
```sql

-- Función: Calcular ocupación de un viaje

CREATE OR REPLACE FUNCTION fn_trip_occupancy(p_trip_id INTEGER)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    v_occupancy DECIMAL(5,2);
BEGIN
    SELECT 
        ROUND((t.total_weight_kg / v.capacity_kg * 100)::numeric, 2)
    INTO v_occupancy
    FROM trips t
    JOIN vehicles v ON t.vehicle_id = v.vehicle_id
    WHERE t.trip_id = p_trip_id;
    
    RETURN v_occupancy;
END;
$$ LANGUAGE plpgsql;

-- Uso: SELECT fn_trip_occupancy(12345);


-- Función: Próximo mantenimiento recomendado

CREATE OR REPLACE FUNCTION fn_next_maintenance_due(p_vehicle_id INTEGER)
RETURNS TABLE (
    days_overdue INTEGER,
    trips_since_maintenance INTEGER,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH last_maint AS (
        SELECT MAX(maintenance_date) as last_date
        FROM maintenance
        WHERE vehicle_id = p_vehicle_id
    ),
    trips_count AS (
        SELECT COUNT(*) as trip_count
        FROM trips
        WHERE vehicle_id = p_vehicle_id
        AND departure_datetime > (SELECT last_date FROM last_maint)
    )
    SELECT 
        EXTRACT(DAY FROM (CURRENT_DATE - (SELECT last_date FROM last_maint)))::INTEGER AS days_overdue,
        (SELECT trip_count FROM trips_count)::INTEGER AS trips_since_maintenance,
        CASE 
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - (SELECT last_date FROM last_maint))) > 90 
                OR (SELECT trip_count FROM trips_count) > 25 
            THEN 'URGENTE: Mantenimiento requerido'
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - (SELECT last_date FROM last_maint))) > 60 
                OR (SELECT trip_count FROM trips_count) > 20 
            THEN 'PRÓXIMO: Programar mantenimiento'
            ELSE 'OK: Dentro de parámetros'
        END AS recommendation;
END;
$$ LANGUAGE plpgsql;


-- Función: Disponibilidad de conductores

CREATE OR REPLACE FUNCTION fn_available_drivers(
    p_datetime TIMESTAMP,
    p_hours_window INTEGER DEFAULT 2
)
RETURNS TABLE (
    driver_id INTEGER,
    driver_name TEXT,
    last_trip_end TIMESTAMP,
    hours_available DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    WITH last_trip AS (
        SELECT 
            d.driver_id,
            d.first_name || ' ' || d.last_name AS driver_name,
            MAX(t.arrival_datetime) AS last_arrival
        FROM drivers d
        LEFT JOIN trips t ON d.driver_id = t.driver_id
        WHERE d.status = 'active'
        GROUP BY d.driver_id, d.first_name, d.last_name
    )
    SELECT 
        lt.driver_id,
        lt.driver_name,
        lt.last_arrival,
        ROUND(EXTRACT(EPOCH FROM (p_datetime - lt.last_arrival))/3600, 2) AS hours_available
    FROM last_trip lt
    WHERE 
        lt.last_arrival IS NULL -- Nuevos conductores sin viajes
        OR (
            lt.last_arrival <= p_datetime 
            AND EXTRACT(EPOCH FROM (p_datetime - lt.last_arrival))/3600 >= 0.5 -- 30 min descanso
        )
    ORDER BY hours_available DESC;
END;
$$ LANGUAGE plpgsql;
```
### Tablas de dominio
Agregar tablas de apoyo
```sql

-- Catálogo de tipos de mantenimiento

CREATE TABLE maintenance_types (
    type_code VARCHAR(20) PRIMARY KEY,
    type_name VARCHAR(100) NOT NULL,
    description TEXT,
    typical_interval_days INTEGER,
    typical_cost_range_min DECIMAL(10,2),
    typical_cost_range_max DECIMAL(10,2),
    is_preventive BOOLEAN DEFAULT TRUE
);

INSERT INTO maintenance_types VALUES
    ('OIL_CHANGE', 'Cambio de Aceite', 'Cambio de aceite y filtro', 90, 150, 300, TRUE),
    ('BRAKES', 'Sistema de Frenos', 'Pastillas, discos, líquido', 180, 400, 800, TRUE),
    ('TIRES', 'Neumáticos', 'Rotación, balanceo, cambio', 120, 200, 1200, TRUE),
    ('ENGINE', 'Motor', 'Diagnóstico y reparación mayor', 365, 1000, 5000, FALSE),
    ('TRANSMISSION', 'Transmisión', 'Mantenimiento de caja', 365, 800, 3000, FALSE);


-- Catálogo de códigos de falla en entregas

CREATE TABLE failure_reasons (
    reason_code VARCHAR(20) PRIMARY KEY,
    reason_name VARCHAR(100),
    typical_percentage DECIMAL(5,2),
    requires_contact BOOLEAN
);

INSERT INTO failure_reasons VALUES
    ('ABSENT', 'Cliente ausente', 3.5, TRUE),
    ('WRONG_ADDR', 'Dirección incorrecta', 2.2, TRUE),
    ('REJECTED', 'Paquete rechazado', 0.7, FALSE),
    ('DAMAGED', 'Paquete dañado', 0.3, FALSE),
    ('NO_ACCESS', 'Sin acceso al domicilio', 0.8, TRUE);


-- Vista enriquecida de mantenimiento con catálogo

CREATE VIEW vw_maintenance_detailed AS
SELECT 
    m.*,
    mt.type_name,
    mt.description AS type_description,
    CASE 
        WHEN m.cost BETWEEN mt.typical_cost_range_min AND mt.typical_cost_range_max THEN 'Normal'
        WHEN m.cost < mt.typical_cost_range_min THEN 'Bajo costo'
        ELSE 'Sobre costo'
    END AS cost_evaluation
FROM maintenance m
JOIN maintenance_types mt ON m.maintenance_type = mt.type_code;
```
### Propuesta de nuevas tablas
Tablas complementarias que agregan valor
```sql

-- Tabla 1: driver_performance_metrics (Métricas calculadas)

CREATE TABLE driver_performance_metrics (
    driver_id INTEGER PRIMARY KEY REFERENCES drivers(driver_id),
    evaluation_date DATE DEFAULT CURRENT_DATE,
    avg_delivery_success_30d DECIMAL(5,2),
    avg_trips_per_day_30d DECIMAL(5,2),
    avg_fuel_efficiency_30d DECIMAL(10,2),
    on_time_performance_30d DECIMAL(5,2),
    safety_score DECIMAL(3,1),
    performance_rating CHAR(1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Tabla 2: vehicle_lifecycle_events (Eventos de flota)

CREATE TABLE vehicle_lifecycle_events (
    event_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    event_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL, -- 'acquisition', 'maintenance', 'accident', 'retirement'
    odometer_km INTEGER,
    cost DECIMAL(10,2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Tabla 3: route_performance_history (Historial de rutas)

CREATE TABLE route_performance_history (
    route_id INTEGER REFERENCES routes(route_id),
    trip_id INTEGER REFERENCES trips(trip_id),
    actual_duration_hours DECIMAL(5,2),
    traffic_condition VARCHAR(20),
    weather_condition VARCHAR(20),
    time_of_day VARCHAR(10),
    PRIMARY KEY (route_id, trip_id)
);


-- Tabla 4: delivery_attempts (Intentos de entrega)

CREATE TABLE delivery_attempts (
    attempt_id SERIAL PRIMARY KEY,
    delivery_id INTEGER REFERENCES deliveries(delivery_id),
    attempt_datetime TIMESTAMP NOT NULL,
    success BOOLEAN DEFAULT FALSE,
    failure_reason VARCHAR(50),
    attempt_number INTEGER,
    notes TEXT
);
```

## Generación de datos sintéticos para poblar las 6 tablas (505 k+ registros totales)
## Desarrollar script Python robusto con Faker y pandas
### Conexión a la base de datos
Para manejar de forma segura la configuración de mi conexión a la base de datos, utilicé la librería `python-dotenv` junto con el módulo os de Python. Con `load_dotenv()` cargo automáticamente las variables de entorno definidas en un archivo .env, donde guardo información sensible como la contraseña de PostgreSQL. Luego, usando `os.getenv()` accedo a estos valores desde mi código, lo que me permite mantener las credenciales fuera del script principal y evitar exponer datos sensibles si llegara a compartir el código

### Actualización de distancias
Se cambiaron las distancias entre las rutas para hacer mas reales, segun los datos proporcionados por Google Maps

### Problema en la generación 
El error ocurría específicamente en la línea donde se asignaba el peso de cada paquete: 
`package_weight = weights[i]`
Este valor era de tipo numpy.float64, un formato que PostgreSQL no reconoce. La solución fue modificar esa línea para convertir explícitamente el valor con `float(weights[i])`, transformándolo a un número decimal estándar que la base de datos sí puede almacenar. Además, se completaron las consultas SQL que estaban como #TO DO# para obtener correctamente los datos de viajes, rutas y vehículos.

### Entregas_imposibles
PROBLEMA: Las entregas están ocurriendo ANTES de que el viaje comience. Esto es ilógico físicamente.

CAUSA: En el código, cuando se calcula:
`scheduled = departure + timedelta`
no se verifica que scheduled sea después de departure.

En el proceso de validación y corrección de los datos, identifiqué inconsistencias relacionadas con los tiempos de entrega y programación frente a los horarios reales de salida y llegada. Para solucionarlo, implementé validaciones lógicas en el código que garantizan coherencia temporal. Primero, corregí los casos donde la hora de entrega era anterior a la salida, ajustándola a 30 minutos después de la salida 
`if delivered < departure: delivered = departure + 30min`.
 También controlé los casos donde la entrega superaba la hora de llegada, estableciendo como límite cinco minutos antes de la llegada
 `if delivered > arrival: delivered = arrival - 5min`
 Además, detecté que el uso de `randint(-30,30)` permitía generar tiempos programados antes del horario previsto, por lo que lo modifiqué a `randint(0,30)` para evitar valores negativos. Finalmente, añadí una validación para asegurar que la hora programada nunca fuera anterior a la salida, ajustándola a 30 minutos posteriores en caso contrario
 `if scheduled < departure: scheduled = departure + 30min` 

## Población de tablas maestras
![Generacion de datos con faker](https://i.postimg.cc/28wM240f/Captura-de-pantalla-2026-02-17-165823.png
)
![Generacion de datos con faker](https://i.postimg.cc/Qxbw0pfG/Captura-de-pantalla-2026-02-17-165801.png)

## EXPLICACIÓN DEL MÉTODO generate_trips()
El método `generate_trips()` es el encargado de crear 100,000 viajes simulando 2 años de operación de FleetLogix. Cuando se ejecuta, lo primero que hace es consultar la base de datos para obtener tres listas:
- los vehículos activos con su capacidad
- los conductores activos
- las rutas disponibles con su distancia y duración estimada.

Esto asegura que cada viaje que se genere use recursos que realmente existen y están disponibles.

Luego, se establece una fecha inicial que comienza exactamente 2 años antes de la fecha actual al momento de correr el script con  `datetime.now() - timedelta(days=730)` y va avanzando esta fecha gradualmente. Entra en un bucle que se repite 100,000 veces, y en cada iteración:

Selecciona aleatoriamente un vehículo, un conductor y una ruta de las listas que se obtuvo

Para la hora de salida, se usa `np.random.choice()` con las probabilidades que da `_get_hourly_distribution()`, lo que hace que sea más probable que los viajes ocurran en horas pico (8am-12pm y 2pm-6pm)

La fecha de salida se construye combinando la fecha actual `current_date` con la hora elegida y minutos aleatorios, Se calcula la llegada multiplicando la duración estimada de la ruta por un factor aleatorio entre 0.8 y 1.3, simulando que unos viajes son más rápidos y otros más lentos de lo estimado

se estima el combustible consumido como la distancia multiplicada por un factor entre 0.08 y 0.15 (8 a 15 litros por cada 100 km)

El peso transportado con un porcentaje entre 40% y 90% de la capacidad del vehículo, esto ayuda a que el viaje sea rentable con una capacidad minima del 40% y que notenga sobrecarga con un porcentaje maximo del 90%

El estado del viaje se decide comparando la llegada con el momento actual: si ya pasó, es "completed"; si aún no ocurre, es "in_progress"

Después de cada viaje, avanza `current_date` aproximadamente 10.5 minutos `timedelta(minutes=int(1440 * 2 * 365 / count)`, lo que distribuye uniformemente los 100,000 viajes en los 2 años. Finalmente, se insertan todos los viajes en la base de datos en lotes de 1,000 registros usando `execute_batch`, lo que hace el proceso mucho más eficiente que insertar uno por uno.

El método auxiliar `_get_hourly_distribution()` simplemente define las probabilidades para cada hora del día. Lo que hace es crear un arreglo de 24 posiciones, una por cada hora, asignar una probabilidad base de 2% y luego aumentar los valores en las horas donde hay más actividad logística: 
- De 6am a 8pm subo a 6%
- De 8am a 12pm subo a 8% (pico mañana)
- De 2pm a 6pm subo a 7% (pico tarde).

Esto hace que cuando en generate_trips() elije una hora aleatoria, sea mucho más probable que salga un viaje a las 9am que a las 3am, reflejando la operación mas real.