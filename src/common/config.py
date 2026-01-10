# Configuración del Pipeline NYC Lakehouse
# Definición de rutas y parámetros globales

BUCKET_NAME = "datalake-nyc-viajes-sebastian"

# Rutas compatibles con Unity Catalog (s3://)
RAW_PATH    = f"s3://{BUCKET_NAME}/raw"
BRONZE_PATH = f"s3://{BUCKET_NAME}/data/bronze"
SILVER_PATH = f"s3://{BUCKET_NAME}/data/silver"
GOLD_PATH   = f"s3://{BUCKET_NAME}/data/gold"

# Parámetros de calidad
QUALITY_MIN_DURATION = 0
QUALITY_MIN_DISTANCE = 0
