"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple
import os
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
<<<<<<< HEAD

load_dotenv()
POSTGRES_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
=======
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'fleetlogix',
    'user': 'postgres',
    'password': 'your_password',
    'port': 5432
>>>>>>> parent of dcc33e5 (A3 -  Se configuro los datos de conexiones de PostgreSQL Y Snowflake)
}

SNOWFLAKE_CONFIG = {
    'user': 'LUISPACHECO90',
    'password': 'your_password',
    'account': 'your_account',
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS'
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
        
        # Cargar llave privada en formato DER
        try:
            with open('snowflake_key.der', 'rb') as key_file:
                self.private_key = key_file.read()
            logging.info(" Llave privada cargada correctamente")
        except FileNotFoundError:
            logging.error(" Archivo snowflake_key.der no encontrado")
            self.private_key = None
        
    
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            # Se agrega la llave
            SNOWFLAKE_CONFIG['private_key'] = self.private_key
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
        SELECT 
            -- Información de entregas
            d.delivery_id,
            d.trip_id,
            d.tracking_number,
            d.customer_name,
            d.delivery_address,
            d.package_weight_kg,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.delivery_status,
            d.recipient_signature,
            
            -- Información del viaje
            t.vehicle_id,
            t.driver_id,
            t.route_id,
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,
            
            -- Información de la ruta
            r.origin_city,
            r.destination_city,
            r.distance_km,
            r.toll_cost,
            
            -- Información del conductor (para experiencia)
            dr.license_expiry,
            dr.hire_date,
            
            -- Información del vehículo (para edad)
            v.acquisition_date,
            v.vehicle_type,
            v.capacity_kg,
            v.fuel_type

        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        JOIN drivers dr ON t.driver_id = dr.driver_id
        JOIN vehicles v ON t.vehicle_id = v.vehicle_id

       -- WHERE d.scheduled_datetime >= CURRENT_DATE - INTERVAL '7 day'
        AND d.scheduled_datetime < CURRENT_DATE
        AND d.delivery_status IN ('delivered', 'pending')

        ORDER BY d.scheduled_datetime;
        """
        
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)
            
            # Validaciones de calidad
            # No permitir tiempos negativos
            df = df[df['delivery_time_minutes'] >= 0]
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = None
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar dimensiones - VERSIÓN SNOWFLAKE"""
        logging.info(" Cargando dimensiones...")
        
        cursor = self.sf_conn.cursor()  # ← Cursor de Snowflake
        
        try:
            # ============================================
            # CLIENTES - Insertar de a uno (son ~400)
            # ============================================
            customers = df[['customer_name', 'destination_city']].drop_duplicates()
            clientes_nuevos = 0
            
            for _, row in customers.iterrows():
                # Primero verificar si existe
                cursor.execute("""
                    SELECT customer_key FROM DIM_CUSTOMER 
                    WHERE customer_name = %s
                """, (row['customer_name'],))
                
                existing = cursor.fetchone()
                
                if not existing:
                    # Generar nuevo customer_key (max + 1)
                    cursor.execute("SELECT COALESCE(MAX(customer_key), 0) + 1 FROM DIM_CUSTOMER")
                    new_key = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        INSERT INTO DIM_CUSTOMER (
                            customer_key, customer_name, customer_type, 
                            city, first_delivery_date, total_deliveries, customer_category
                        ) VALUES (%s, %s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                    """, (new_key, row['customer_name'], row['destination_city']))
                    clientes_nuevos += 1
            
            # ============================================
            # DRIVERS - Actualizar SCD Type 2
            # ============================================
            active_drivers = df[['driver_id']].drop_duplicates()
            
            for _, row in active_drivers.iterrows():
                cursor.execute("""
                    UPDATE DIM_DRIVER 
                    SET valid_to = CURRENT_DATE() - 1, is_current = FALSE
                    WHERE driver_id = %s
                    AND is_current = TRUE
                """, (row['driver_id'],))
            
            self.sf_conn.commit()
            logging.info(f" Dimensiones actualizadas - {clientes_nuevos} clientes nuevos, {len(active_drivers)} conductores activos")
            
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def load_facts(self, df: pd.DataFrame):
        """Cargar hechos en Snowflake - VERSIÓN CORREGIDA"""
        logging.info(" Cargando tabla de hechos...")
        
        cursor = self.sf_conn.cursor()  # ← Cursor de Snowflake
        
        try:
            # Preparar datos
            fact_data = []
            for _, row in df.iterrows():
                date_key = int(pd.to_datetime(row['scheduled_datetime']).strftime('%Y%m%d'))
                scheduled_time_key = pd.to_datetime(row['scheduled_datetime']).hour * 100
                delivered_time_key = pd.to_datetime(row['delivered_datetime']).hour * 100
                
                fact_data.append((
                    date_key, scheduled_time_key, delivered_time_key,
                    row['vehicle_id'], row['driver_id'], row['route_id'], 1,
                    row['delivery_id'], row['trip_id'], row['tracking_number'],
                    row['package_weight_kg'], row['distance_km'], row['fuel_consumed_liters'],
                    row['delivery_time_minutes'], row['delay_minutes'], row['deliveries_per_hour'],
                    row['fuel_efficiency_km_per_liter'], row['cost_per_delivery'], row['revenue_per_delivery'],
                    row['is_on_time'], False, row['recipient_signature'], row['delivery_status'],
                    self.batch_id
                ))
            
            # ============================================
            # INSERTAR POR LOTES (SIN usar mogrify)
            # ============================================
            BATCH_SIZE = 1000
            total = len(fact_data)
            
            for i in range(0, total, BATCH_SIZE):
                batch = fact_data[i:i+BATCH_SIZE]
                
                # Snowflake executemany SÍ funciona
                cursor.executemany("""
                    INSERT INTO fact_deliveries (
                        date_key, scheduled_time_key, delivered_time_key,
                        vehicle_key, driver_key, route_key, customer_key,
                        delivery_id, trip_id, tracking_number,
                        package_weight_kg, distance_km, fuel_consumed_liters,
                        delivery_time_minutes, delay_minutes, deliveries_per_hour,
                        fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                        is_on_time, is_damaged, has_signature, delivery_status,
                        etl_batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                
                if i % 10000 == 0:
                    logging.info(f"  Progreso: {i}/{total} registros")
            
            self.sf_conn.commit()
            self.metrics['records_loaded'] = total
            logging.info(f" Cargados {total} registros en fact_deliveries")
            
        except Exception as e:
            logging.error(f" Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes
            self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # 1️⃣ PRIMERO: Crear la tabla (si no existe)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    summary_date DATE,
                    batch_id INT,
                    total_deliveries INT,
                    total_delivered INT,
                    total_pending INT,
                    total_weight_kg DECIMAL(12,2),
                    total_revenue DECIMAL(12,2),
                    total_fuel_consumed DECIMAL(12,2),
                    avg_delivery_time_minutes DECIMAL(6,2),
                    avg_delay_minutes DECIMAL(6,2),
                    on_time_percentage DECIMAL(5,2),
                    damaged_percentage DECIMAL(5,2),
                    avg_vehicles_used INT,
                    deliveries_per_vehicle DECIMAL(6,2),
                    avg_deliveries_per_driver DECIMAL(6,2),
                    top_driver_id INT,
                    top_driver_deliveries INT,
                    busiest_route_id INT,
                    busiest_route_deliveries INT,
                    unique_customers INT,
                    repeat_customers INT,
                    etl_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
            """)
            
            # 2️⃣ DESPUÉS: Insertar los datos
            cursor.execute("""
                INSERT INTO daily_summary (
                    summary_date, batch_id,
                    total_deliveries, total_delivered, total_pending,
                    total_weight_kg, total_revenue, total_fuel_consumed,
                    avg_delivery_time_minutes, avg_delay_minutes,
                    on_time_percentage, damaged_percentage,
                    avg_vehicles_used, deliveries_per_vehicle,
                    avg_deliveries_per_driver,
                    top_driver_id, top_driver_deliveries,
                    busiest_route_id, busiest_route_deliveries,
                    unique_customers, repeat_customers
                )
                SELECT
                    CURRENT_DATE() - 1 AS summary_date,
                    %s AS batch_id,
                    COUNT(*) AS total_deliveries,
                    SUM(CASE WHEN delivery_status = 'delivered' THEN 1 ELSE 0 END) AS total_delivered,
                    SUM(CASE WHEN delivery_status = 'pending' THEN 1 ELSE 0 END) AS total_pending,
                    SUM(package_weight_kg) AS total_weight_kg,
                    SUM(revenue_per_delivery) AS total_revenue,
                    SUM(fuel_consumed_liters) AS total_fuel_consumed,
                    AVG(delivery_time_minutes) AS avg_delivery_time_minutes,
                    AVG(delay_minutes) AS avg_delay_minutes,
                    AVG(CASE WHEN is_on_time THEN 100 ELSE 0 END) AS on_time_percentage,
                    AVG(CASE WHEN is_damaged THEN 100 ELSE 0 END) AS damaged_percentage,
                    COUNT(DISTINCT vehicle_key) AS avg_vehicles_used,
                    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT vehicle_key), 0) AS deliveries_per_vehicle,
                    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT driver_key), 0) AS avg_deliveries_per_driver,
                    (SELECT driver_key FROM fact_deliveries 
                    WHERE date_key = (SELECT MAX(date_key) FROM DIM_DATE WHERE full_date = CURRENT_DATE() - 1)
                    GROUP BY driver_key ORDER BY COUNT(*) DESC LIMIT 1) AS top_driver_id,
                    (SELECT MAX(cnt) FROM (
                        SELECT COUNT(*) AS cnt
                        FROM fact_deliveries 
                        WHERE date_key = (SELECT MAX(date_key) FROM DIM_DATE WHERE full_date = CURRENT_DATE() - 1)
                        GROUP BY driver_key
                    )) AS top_driver_deliveries,
                    (SELECT route_key FROM fact_deliveries 
                    WHERE date_key = (SELECT MAX(date_key) FROM DIM_DATE WHERE full_date = CURRENT_DATE() - 1)
                    GROUP BY route_key ORDER BY COUNT(*) DESC LIMIT 1) AS busiest_route_id,
                    (SELECT MAX(cnt) FROM (
                        SELECT COUNT(*) AS cnt
                        FROM fact_deliveries 
                        WHERE date_key = (SELECT MAX(date_key) FROM DIM_DATE WHERE full_date = CURRENT_DATE() - 1)
                        GROUP BY route_key
                    )) AS busiest_route_deliveries,
                    COUNT(DISTINCT customer_key) AS unique_customers,
                    COUNT(DISTINCT CASE WHEN delivery_count > 1 THEN customer_key END) AS repeat_customers
                FROM (
                    SELECT 
                        *,
                        COUNT(*) OVER (PARTITION BY customer_key) AS delivery_count
                    FROM fact_deliveries
                    WHERE date_key = (SELECT MAX(date_key) FROM DIM_DATE WHERE full_date = CURRENT_DATE() - 1)
                )
                GROUP BY date_key
            """, (self.batch_id,))
        
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()