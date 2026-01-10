# Databricks notebook source
# MAGIC %md # 01 Ingesta Bronze - Por sposada
# MAGIC Carga de data cruda desde S3 a la capa Bronze en formato Delta.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid

# Configuración de S3 (ajusta si es necesario)
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
RAW_DATA_PATH = f"s3a://{BUCKET_NAME}/raw"
BRONZE_PATH = f"s3a://{BUCKET_NAME}/data/bronze"

# Configuración de credenciales AWS para Serverless
# IMPORTANTE: Reemplaza estos valores con tus credenciales reales
AWS_ACCESS_KEY = "TU_ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "TU_SECRET_KEY_AQUI"

# Configurar Spark para acceder a S3
spark.conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def ingest_bronze(spark: SparkSession):
    print(f"Buscando archivos en: {RAW_DATA_PATH}...")
    
    # Leo los archivos parquet originales
    df_raw = spark.read.parquet(RAW_DATA_PATH)
    
    # Agrego metadata técnica para trazabilidad
    df_bronze = df_raw.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.input_file_name().alias("_source_file"),
        F.lit(str(uuid.uuid4())).alias("_load_id")
    )
    
    # Guardo en la capa Bronze usando Delta Lake
    df_bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"✅ Capa Bronze lista en: {BRONZE_PATH}")
    print(f"📊 Total de registros cargados: {df_bronze.count()}")

# Arrancamos la ejecución
spark = SparkSession.builder.getOrCreate()
ingest_bronze(spark)

# COMMAND ----------
# Validación rápida
print("Primeras 5 filas de Bronze:")
spark.read.format("delta").load(BRONZE_PATH).show(5)
