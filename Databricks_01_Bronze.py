# Databricks notebook source
# MAGIC %md # 01 Ingesta Bronze - Por sposada
# MAGIC En este paso cargamos la data cruda desde S3 directamente a nuestra capa Bronze en formato Delta.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid

# Configuración básica (la dejo aquí para que el notebook sea independiente)
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
RAW_DATA_PATH = f"s3://{BUCKET_NAME}/raw"
BRONZE_PATH = f"s3://{BUCKET_NAME}/data/bronze"

def ingest_bronze(spark: SparkSession):
    print(f"Buscando archivos en: {RAW_DATA_PATH}...")
    
    # Leo los archivos parquet originales
    # Nota: Unity Catalog no deja usar input_file_name(), así que usamos _metadata para traer la ruta
    df_raw = spark.read.parquet(RAW_DATA_PATH)
    
    # Agrego metadata técnica para trazabilidad (cuándo se cargó y de qué archivo viene)
    df_bronze = df_raw.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.lit(str(uuid.uuid4())).alias("_load_id")
    )
    
    # Guardo en la capa Bronze usando Delta Lake
    # Uso overwrite para el test pero en producción podría ser append
    df_bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"Capa Bronze lista en: {BRONZE_PATH}")

# Arrancamos la ejecución
spark = SparkSession.builder.getOrCreate()
ingest_bronze(spark)

# COMMAND ----------
# Una pequeña muestra para validar que todo quedó bien
# MAGIC %sql SELECT * FROM delta.`$BRONZE_PATH` LIMIT 5
