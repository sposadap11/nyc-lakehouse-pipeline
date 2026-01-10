import os

# Configuración de AWS S3 para Databricks (Unity Catalog)
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
RAW_DATA_PATH = f"s3://{BUCKET_NAME}/raw"
STORAGE_PREFIX = f"s3://{BUCKET_NAME}/data"

# Rutas de las capas del Data Lakehouse en S3
BRONZE_PATH = f"{STORAGE_PREFIX}/bronze"
SILVER_PATH = f"{STORAGE_PREFIX}/silver"
GOLD_PATH   = f"{STORAGE_PREFIX}/gold"

# Nombres de tablas para el Metastore (opcional según uso)
BRONZE_TABLE = "bronze_trips"
SILVER_TABLE = "silver_trips"
GOLD_TABLE   = "gold_kpis_daily"

# Ruta para el dataset de entrada original
DATASET_PATH = RAW_DATA_PATH

# Definición de columnas de negocio del dataset NYC FHVHV
PICKUP_COL   = "pickup_datetime"
DROPOFF_COL  = "dropoff_datetime"
DISTANCE_COL = "trip_miles"

# Componentes monetarios para calcular el ingreso (Revenue)
REVENUE_COLS = [
    "base_passenger_fare", 
    "tolls", 
    "tips", 
    "sales_tax", 
    "congestion_surcharge", 
    "airport_fee"
]
