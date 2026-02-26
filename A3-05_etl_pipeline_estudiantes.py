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
            SNOWFLAKE_CONFIG['private_key'] = self.private_key
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
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
        df = pd.read_sql(query, self.pg_conn)
        self.metrics['records_extracted'] = len(df)
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(" Transformando datos (Pandas Vectorized)...")
        # Convertir fechas de golpe
        for col in ['scheduled_datetime', 'delivered_datetime', 'departure_datetime', 'arrival_datetime']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Cálculos masivos
        df['delivery_time_minutes'] = ((df['delivered_datetime'] - df['scheduled_datetime']).dt.total_seconds() / 60).round(2)
        df['delay_minutes'] = df['delivery_time_minutes'].clip(lower=0).fillna(0)
        df['is_on_time'] = df['delay_minutes'] <= 30
        
        df['trip_duration_hours'] = ((df['arrival_datetime'] - df['departure_datetime']).dt.total_seconds() / 3600).round(2)
        df.loc[df['trip_duration_hours'] <= 0, 'trip_duration_hours'] = np.nan

        # Entregas por hora
        deliveries_per_trip = df.groupby('trip_id').size()
        df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
        df['deliveries_per_hour'] = (df['deliveries_in_trip'] / df['trip_duration_hours']).fillna(0).round(2)
        
        # Eficiencia y Costos
        df['fuel_efficiency_km_per_liter'] = (df['distance_km'] / df['fuel_consumed_liters']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df['cost_per_delivery'] = ((df['fuel_consumed_liters'].fillna(0) * 5000 + df['toll_cost'].fillna(0)) / df['deliveries_in_trip']).round(2)
        df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

        # Filtros de calidad
        df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)].copy()
        
        # Keys para Snowflake
        df['date_key'] = df['scheduled_datetime'].dt.strftime('%Y%m%d').fillna(0).astype(int)
        df['scheduled_time_key'] = (df['scheduled_datetime'].dt.hour * 10000 + df['scheduled_datetime'].dt.minute * 100).fillna(0).astype(int)
        
        self.metrics['records_transformed'] = len(df)
        return df

    def load_dimensions(self, df: pd.DataFrame):
        logging.info(" Sincronizando dimensiones (Bulk Write + Merge)...")
        cursor = self.sf_conn.cursor()

        # --- CLIENTES ---
        cust_df = df[['customer_name', 'destination_city']].drop_duplicates().copy()
        cust_df.columns = ['CUSTOMER_NAME', 'CITY']
        cust_df['CUSTOMER_TYPE'] = 'Individual'
        cust_df['FIRST_DELIVERY_DATE'] = datetime.now().date()
        cust_df['TOTAL_DELIVERIES'] = 1
        cust_df['CUSTOMER_CATEGORY'] = 'Regular'

        write_pandas(self.sf_conn, cust_df, "STG_CUSTOMERS", auto_create_table=True, table_type="temp")
        cursor.execute("""
            MERGE INTO dim_customer c USING STG_CUSTOMERS s ON c.customer_name = s.CUSTOMER_NAME
            WHEN NOT MATCHED THEN INSERT (customer_key, customer_name, customer_type, city, first_delivery_date, total_deliveries, customer_category)
            VALUES (seq_customer_key.NEXTVAL, s.CUSTOMER_NAME, s.CUSTOMER_TYPE, s.CITY, s.FIRST_DELIVERY_DATE, s.TOTAL_DELIVERIES, s.CUSTOMER_CATEGORY);
        """)

        # --- VEHÍCULOS ---
        v_df = df[['vehicle_id', 'license_plate', 'vehicle_type', 'capacity_kg', 'fuel_type']].drop_duplicates().copy()
        v_df.columns = [c.upper() for c in v_df.columns]
        write_pandas(self.sf_conn, v_df, "STG_VEHICLES", auto_create_table=True, table_type="temp")
        cursor.execute("""
            MERGE INTO dim_vehicle v USING STG_VEHICLES s ON v.license_plate = s.LICENSE_PLATE
            WHEN NOT MATCHED THEN INSERT (vehicle_key, vehicle_id, license_plate, vehicle_type, capacity_kg, fuel_type, status, is_current, valid_from)
            VALUES (seq_vehicle_key.NEXTVAL, s.VEHICLE_ID, s.LICENSE_PLATE, s.VEHICLE_TYPE, s.CAPACITY_KG, s.FUEL_TYPE, 'active', TRUE, CURRENT_DATE());
        """)
        self.sf_conn.commit()

    def load_facts(self, df: pd.DataFrame):
        logging.info(f" Cargando {len(df)} hechos por lotes...")
        cursor = self.sf_conn.cursor()
        
        # Mapeos rápidos
        cursor.execute("SELECT customer_name, customer_key FROM dim_customer")
        c_map = dict(cursor.fetchall())
        cursor.execute("SELECT driver_id, driver_key FROM dim_driver WHERE is_current=True")
        d_map = dict(cursor.fetchall())
        cursor.execute("SELECT vehicle_id, vehicle_key FROM dim_vehicle WHERE is_current=True")
        v_map = dict(cursor.fetchall())

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

        # Asegúrate de que el SQL en load_facts tenga exactamente este orden de columnas:
        sql = """INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, vehicle_key, driver_key, route_key, customer_key, 
                    delivery_id, trip_id, tracking_number, package_weight_kg, distance_km, fuel_consumed_liters, 
                    delivery_time_minutes, delay_minutes, deliveries_per_hour, fuel_efficiency_km_per_liter, 
                    cost_per_delivery, revenue_per_delivery, is_on_time, delivery_status, etl_batch_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        chunk_size = 15000
        for i in range(0, len(fact_data), chunk_size):
            cursor.executemany(sql, fact_data[i:i+chunk_size])
            logging.info(f" -> Lote OK: {i+len(fact_data[i:i+chunk_size])}/{len(fact_data)}")
        
        self.sf_conn.commit()
        self.metrics['records_loaded'] = len(fact_data)

    def _calculate_daily_totals(self):
        cursor = self.sf_conn.cursor()
        cursor.execute("""
            INSERT INTO daily_performance_summary (batch_id, total_deliveries, avg_delay_minutes, total_revenue, fuel_efficiency_avg)
            SELECT %s, COUNT(*), AVG(delay_minutes), SUM(revenue_per_delivery), AVG(fuel_efficiency_km_per_liter)
            FROM fact_deliveries WHERE etl_batch_id = %s
        """, (self.batch_id, self.batch_id))
        self.sf_conn.commit()

    def run_etl(self):
        if not self.connect_databases(): return
        df = self.extract_daily_data()
        if not df.empty:
            df = self.transform_data(df)
            self.load_dimensions(df)
            self.load_facts(df)
            self._calculate_daily_totals()
        logging.info(f" ETL Finalizado. Métricas: {self.metrics}")
        self.pg_conn.close()
        self.sf_conn.close()

if __name__ == "__main__":
    etl = FleetLogixETL()
    etl.run_etl()