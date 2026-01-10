# Databricks notebook source
# 01 Ingesta Bronze
# Ingesta de datos usando Unity Catalog External Locations en Serverless.

from pyspark.sql import functions as F
import uuid

# Configuración de acceso a AWS
# En un entorno real usamos Unity Catalog (External Locations) 
# o secretos para no exponer llaves en el código.
access_key = dbutils.secrets.get("aws", "access_key")
secret_key = dbutils.secrets.get("aws", "secret_key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Definicion de rutas
bucket = "datalake-nyc-viajes-sebastian"
ruta_raw = f"s3://{bucket}/raw"
ruta_bronze = f"s3://{bucket}/data/bronze"

print(f"Leyendo datos crudos desde: {ruta_raw}")

# Lectura directa (Databricks usa la External Location configurada)
df_raw = spark.read.parquet(ruta_raw)

# Agregamos metadata de trazabilidad
df_bronze = df_raw.select(
    "*",
    F.current_timestamp().alias("fecha_ingesta"),
    F.col("_metadata.file_path").alias("archivo_origen"),
    F.lit(str(uuid.uuid4())).alias("id_lote")
)

# Guardamos en la capa Bronze
df_bronze.write.format("delta").mode("overwrite").save(ruta_bronze)

print(f"Capa Bronze cargada exitosamente. Total: {df_bronze.count()}")
display(df_bronze.limit(10))
