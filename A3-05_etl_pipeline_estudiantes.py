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
from snowflake.connector.pandas_tools import write_pandas


# Configuración de logging para rastrear la ejecución y errores en consola y archivo local
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Carga de variables de entorno desde el archivo .env para seguridad de credenciales
load_dotenv()
POSTGRES_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

class FleetLogixETL:
    def __init__(self):
        """ Inicializa el estado del ETL, contadores de métricas y carga de seguridad """
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
        
        # Cargar llave privada en formato DER para autenticación segura en Snowflake
        try:
            with open('snowflake_key.der', 'rb') as key_file:
                self.private_key = key_file.read()
            logging.info(" Llave privada cargada correctamente")
        except FileNotFoundError:
            logging.error(" Archivo snowflake_key.der no encontrado")
            self.private_key = None
    
    def connect_databases(self):
        """ Establece las conexiones físicas con los motores origen (Postgres) y destino (Snowflake) """
        try:
            # Conexión a la base de datos transaccional de PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Conexión al Data Warehouse (Destino) usando la llave privada cargada
            SNOWFLAKE_CONFIG['private_key'] = self.private_key
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
        """ Ejecuta la extracción masiva uniendo tablas de envíos, viajes, vehículos, conductores y rutas """
        logging.info(" Extrayendo datos de PostgreSQL...")
        query = """
        SELECT 
            d.delivery_id, d.tracking_number, d.customer_name, d.delivery_address, 
            d.package_weight_kg, d.scheduled_datetime, d.delivered_datetime, d.delivery_status,
            t.trip_id, t.fuel_consumed_liters, t.departure_datetime, t.arrival_datetime,
            v.vehicle_id, v.license_plate, v.vehicle_type, v.capacity_kg, v.fuel_type,
            dr.driver_id, dr.employee_code, (dr.first_name || ' ' || dr.last_name) AS full_name,
            r.route_id, r.route_code, r.origin_city, r.destination_city, r.distance_km, r.toll_cost
        FROM public.deliveries d
        JOIN public.trips t ON d.trip_id = t.trip_id
        JOIN public.vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN public.drivers dr ON t.driver_id = dr.driver_id
        JOIN public.routes r ON t.route_id = r.route_id;
        """
        try:
            # Carga el resultado de la query directamente en un DataFrame de Pandas
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Aplica limpieza, lógica de negocio y cálculos de rendimiento de forma vectorizada """

        logging.info(" Transformando datos (Pandas Vectorized)...")
        try:
            # Normalización de tipos de datos a fechas para cálculos temporales
            for col in ['scheduled_datetime', 'delivered_datetime', 'departure_datetime', 'arrival_datetime']:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # Cálculo de KPIs de tiempo de entrega y retrasos
            df['delivery_time_minutes'] = ((df['delivered_datetime'] - df['scheduled_datetime']).dt.total_seconds() / 60).round(2)
            df['delay_minutes'] = df['delivery_time_minutes'].clip(lower=0).fillna(0)
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Cálculo de duración de viajes para métricas de eficiencia
            df['trip_duration_hours'] = ((df['arrival_datetime'] - df['departure_datetime']).dt.total_seconds() / 3600).round(2)
            df.loc[df['trip_duration_hours'] <= 0, 'trip_duration_hours'] = np.nan

            # Agregación vectorizada para determinar cuántas entregas se hicieron por hora de viaje
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (df['deliveries_in_trip'] / df['trip_duration_hours']).fillna(0).round(2)
            
            # Cálculos financieros y operativos: eficiencia de combustible, costos y utilidades
            df['fuel_efficiency_km_per_liter'] = (df['distance_km'] / df['fuel_consumed_liters']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
            df['cost_per_delivery'] = ((df['fuel_consumed_liters'].fillna(0) * 5000 + df['toll_cost'].fillna(0)) / df['deliveries_in_trip']).round(2)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

            # Limpieza de valores extremos o erróneos en el peso del paquete
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)].copy()
            
            # Generación de llaves inteligentes (Smart Keys) para dimensiones de fecha y hora en el Warehouse
            df['date_key'] = df['scheduled_datetime'].dt.strftime('%Y%m%d').fillna(0).astype(int)
            df['scheduled_time_key'] = (df['scheduled_datetime'].dt.hour * 10000 + df['scheduled_datetime'].dt.minute * 100).fillna(0).astype(int)
            
            self.metrics['records_transformed'] = len(df)
            return df
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def load_dimensions(self, df: pd.DataFrame):
        """ Carga las tablas de dimensiones utilizando tablas temporales de STAGING y comandos MERGE """

        logging.info(" Sincronizando dimensiones (Bulk Write + Merge)...")
        cursor = self.sf_conn.cursor()
        try:
            # --- PROCESAMIENTO DE CLIENTES ---
            # Extrae clientes únicos para evitar procesar duplicados innecesarios
            cust_df = df[['customer_name', 'destination_city']].drop_duplicates().copy()
            cust_df.columns = ['CUSTOMER_NAME', 'CITY']
            cust_df['CUSTOMER_TYPE'] = 'Individual'
            cust_df['FIRST_DELIVERY_DATE'] = datetime.now().date()
            cust_df['TOTAL_DELIVERIES'] = 1
            cust_df['CUSTOMER_CATEGORY'] = 'Regular'

            # Sube el DataFrame a una tabla temporal en Snowflake optimizada para carga masiva
            write_pandas(self.sf_conn, cust_df, "STG_CUSTOMERS", auto_create_table=True, table_type="temp")
            # Actualiza o inserta registros de clientes usando la tabla de Staging
            cursor.execute("""
                MERGE INTO dim_customer c USING STG_CUSTOMERS s ON c.customer_name = s.CUSTOMER_NAME
                WHEN NOT MATCHED THEN INSERT (customer_key, customer_name, customer_type, city, first_delivery_date, total_deliveries, customer_category)
                VALUES (seq_customer_key.NEXTVAL, s.CUSTOMER_NAME, s.CUSTOMER_TYPE, s.CITY, s.FIRST_DELIVERY_DATE, s.TOTAL_DELIVERIES, s.CUSTOMER_CATEGORY);
            """)

            # --- PROCESAMIENTO DE VEHÍCULOS ---
            # Similar a clientes, deduce vehículos únicos y sincroniza con la dimensión correspondiente
            v_df = df[['vehicle_id', 'license_plate', 'vehicle_type', 'capacity_kg', 'fuel_type']].drop_duplicates().copy()
            v_df.columns = [c.upper() for c in v_df.columns]
            write_pandas(self.sf_conn, v_df, "STG_VEHICLES", auto_create_table=True, table_type="temp")
            cursor.execute("""
                MERGE INTO dim_vehicle v USING STG_VEHICLES s ON v.license_plate = s.LICENSE_PLATE
                WHEN NOT MATCHED THEN INSERT (vehicle_key, vehicle_id, license_plate, vehicle_type, capacity_kg, fuel_type, status, is_current, valid_from)
                VALUES (seq_vehicle_key.NEXTVAL, s.VEHICLE_ID, s.LICENSE_PLATE, s.VEHICLE_TYPE, s.CAPACITY_KG, s.FUEL_TYPE, 'active', TRUE, CURRENT_DATE());
            """)
            self.sf_conn.commit()
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

        

    def load_facts(self, df: pd.DataFrame):
        """ Prepara y carga la tabla de hechos utilizando mapeos en memoria y cargas por lotes (Chunks) """
        logging.info(f" Cargando {len(df)} hechos por lotes...")
        cursor = self.sf_conn.cursor()
        try:
            # Carga mapeos de nombres/IDs naturales a Surrogated Keys (SK) para evitar JOINs costosos durante la carga
            cursor.execute("SELECT customer_name, customer_key FROM dim_customer")
            c_map = dict(cursor.fetchall())
            cursor.execute("SELECT driver_id, driver_key FROM dim_driver WHERE is_current=True")
            d_map = dict(cursor.fetchall())
            cursor.execute("SELECT vehicle_id, vehicle_key FROM dim_vehicle WHERE is_current=True")
            v_map = dict(cursor.fetchall())
            
            # Transforma el DataFrame en una lista de tuplas lista para el comando executemany
            fact_data = []
            for r in df.to_dict('records'):
                fact_data.append((
                    r['date_key'], r['scheduled_time_key'], 
                    v_map.get(r['vehicle_id'], 1), d_map.get(r['driver_id'], 1), 
                    1, c_map.get(r['customer_name'], 1),
                    r['delivery_id'], r['trip_id'], r['tracking_number'],
                    r['package_weight_kg'], r['distance_km'], r['fuel_consumed_liters'],
                    r['delivery_time_minutes'], r['delay_minutes'],
                    r['deliveries_per_hour'], r['fuel_efficiency_km_per_liter'],
                    r['cost_per_delivery'], r['revenue_per_delivery'],
                    int(r['is_on_time']), r['delivery_status'], self.batch_id
                ))

            # Definición del SQL de inserción con el orden exacto de la tabla de hechos
            sql = """INSERT INTO fact_deliveries (
                        date_key, scheduled_time_key, vehicle_key, driver_key, route_key, customer_key, 
                        delivery_id, trip_id, tracking_number, package_weight_kg, distance_km, fuel_consumed_liters, 
                        delivery_time_minutes, delay_minutes, deliveries_per_hour, fuel_efficiency_km_per_liter, 
                        cost_per_delivery, revenue_per_delivery, is_on_time, delivery_status, etl_batch_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            # Envío de datos en lotes de 15,000 registros para optimizar el uso de red y memoria de Snowflake
            chunk_size = 15000
            for i in range(0, len(fact_data), chunk_size):
                cursor.executemany(sql, fact_data[i:i+chunk_size])
                logging.info(f" -> Lote OK: {i+len(fact_data[i:i+chunk_size])}/{len(fact_data)}")
            
            self.sf_conn.commit()
            self.metrics['records_loaded'] = len(fact_data)
        except Exception as e:
            logging.error(f" Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
        

    def _calculate_daily_totals(self):
        """ Genera agregaciones finales directamente en Snowflake para reportes de rendimiento diario """
        cursor = self.sf_conn.cursor()
        try:
            # Calcula promedios y sumas del lote actual y los inserta en la tabla de sumario
            cursor.execute("""
                INSERT INTO daily_performance_summary (batch_id, total_deliveries, avg_delay_minutes, total_revenue, fuel_efficiency_avg)
                SELECT %s, COUNT(*), AVG(delay_minutes), SUM(revenue_per_delivery), AVG(fuel_efficiency_km_per_liter)
                FROM fact_deliveries WHERE etl_batch_id = %s
            """, (self.batch_id, self.batch_id))
            self.sf_conn.commit()
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def setup_infrastructure(self):
        """ Garantiza que los objetos necesarios (secuencias y tablas de sumario) existan en el Warehouse """
        cursor = self.sf_conn.cursor()
        logging.info(" Verificando infraestructura de secuencias...")
        try:
            # Creación de secuencias para la generación automática de Surrogated Keys
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS seq_customer_key START = 1 INCREMENT = 1")
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS seq_driver_key START = 1 INCREMENT = 1")
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS seq_vehicle_key START = 1 INCREMENT = 1")
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS seq_route_key START = 1 INCREMENT = 1")
            # Creación de la tabla de sumario para KPIs si no ha sido creada previamente
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_performance_summary (
                    batch_id INT,
                    report_date DATE DEFAULT CURRENT_DATE(),
                    total_deliveries INT,
                    avg_delay_minutes DECIMAL(10,2),
                    total_revenue DECIMAL(15,2),
                    fuel_efficiency_avg DECIMAL(10,2)
                )
            """)
            self.sf_conn.commit()
        except Exception as e:
            logging.error(f" Error en infraestructura: {e}")
    
    def setup_time_dimension(self):
        """Genera la dimensión de tiempo directamente en Snowflake si está vacía"""
        cursor = self.sf_conn.cursor()
        logging.info(" Verificando dimensión de tiempo...")
        
        try:
            # Evita la regeneración de datos si la tabla ya contiene información
            cursor.execute("SELECT COUNT(*) FROM dim_time")
            if cursor.fetchone()[0] > 0:
                logging.info(" -> Dimensión de tiempo ya poblada.")
                return

            logging.info(" -> Generando 86,400 registros de tiempo (Bulk Generation)...")
            # Ejecución de lógica SQL para poblar la dimensión de tiempo con atributos descriptivos (hora, AM/PM, jornada)
            cursor.execute("""
                INSERT INTO dim_time (time_key, hour, minute, second, time_of_day, hour_24, am_pm, is_business_hour)
                SELECT 
                    (hour(time_val) * 10000 + minute(time_val) * 100 + second(time_val)) as time_key, -- Key predecible HHMMSS
                    hour(time_val),
                    minute(time_val),
                    second(time_val),
                    CASE 
                        WHEN hour(time_val) BETWEEN 0 AND 5 THEN 'Madrugada'
                        WHEN hour(time_val) BETWEEN 6 AND 11 THEN 'Mañana'
                        WHEN hour(time_val) BETWEEN 12 AND 17 THEN 'Tarde'
                        ELSE 'Noche'
                    END,
                    to_char(time_val, 'HH24:MI'),
                    to_char(time_val, 'AM'),
                    CASE WHEN hour(time_val) BETWEEN 8 AND 18 THEN TRUE ELSE FALSE END
                FROM (
                    SELECT timeadd(second, seq4(), '00:00:00'::time) as time_val
                    FROM table(generator(rowcount => 86400))
                )
            """)
            self.sf_conn.commit()
            logging.info(" -> Dimensión de tiempo creada exitosamente.")
        except Exception as e:
            logging.error(f" Error creando dimensión de tiempo: {e}")
    
    
    def run_etl(self):
        """ Método orquestador que ejecuta el flujo completo de vida del ETL: Conectar -> Setup -> E -> T -> L """
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        try:
            # Fase 1: Establecer conexiones
            if not self.connect_databases(): return
            
            # Fase 2: Garantizar que el entorno esté listo (DDL y Dimensiones estáticas)
            self.setup_time_dimension()
            self.setup_infrastructure()
            
            # Fase 3: Proceso de datos
            df = self.extract_daily_data()
            if not df.empty:
                df = self.transform_data(df)
                self.load_dimensions(df)
                self.load_facts(df)
                self._calculate_daily_totals()
            
            # Fase 4: Finalización y métricas
            logging.info(f" ETL Finalizado. Métricas: {self.metrics}")
            self.pg_conn.close()
            self.sf_conn.close()
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()

if __name__ == "__main__":
    etl = FleetLogixETL()
    etl.run_etl()