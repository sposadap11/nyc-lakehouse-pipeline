import os

# Configuración de AWS S3
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
RAW_DATA_PATH = f"s3://{BUCKET_NAME}/raw"
STORAGE_PREFIX = f"s3://{BUCKET_NAME}/data"

# Rutas de las Capas Lakehouse en S3
BRONZE_PATH = f"{STORAGE_PREFIX}/bronze"
SILVER_PATH = f"{STORAGE_PREFIX}/silver"
GOLD_PATH   = f"{STORAGE_PREFIX}/gold"

# Nombres de las tablas (Metastore)
BRONZE_TABLE = "bronze_trips"
SILVER_TABLE = "silver_trips"
GOLD_TABLE   = "gold_kpis_daily"

# Ruta para el dataset de entrada
DATASET_PATH = RAW_DATA_PATH

# Configuración de columnas (Data de NYC FHVHV)
PICKUP_COL   = "pickup_datetime"
DROPOFF_COL  = "dropoff_datetime"
DISTANCE_COL = "trip_miles"

# Componentes monetarios
REVENUE_COLS = [
    "base_passenger_fare", 
    "tolls", 
    "tips", 
    "sales_tax", 
    "congestion_surcharge", 
    "airport_fee"
]
