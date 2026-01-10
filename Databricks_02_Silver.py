# Databricks notebook source
# MAGIC %md # 02 Transformación Silver - Por sposada
# MAGIC Limpieza, calidad de datos y optimización con Z-Order.

# COMMAND ----------
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Configuración
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
BRONZE_PATH = f"s3a://{BUCKET_NAME}/data/bronze"
SILVER_PATH = f"s3a://{BUCKET_NAME}/data/silver"

# Credenciales AWS (las mismas que en Bronze)
AWS_ACCESS_KEY = "TU_ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "TU_SECRET_KEY_AQUI"

spark.conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Columnas clave
PICKUP_COL   = "pickup_datetime"
DROPOFF_COL  = "dropoff_datetime"
DISTANCE_COL = "trip_miles"

def transform_silver(spark: SparkSession):
    # Valido que Bronze exista
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        raise Exception("❌ Falta la capa Bronze. Corre el primer notebook!")

    print("Leyendo desde Bronze...")
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    
    # Normalización de tipos
    df_silver = df_bronze.select(
        "*",
        F.to_timestamp(PICKUP_COL).alias("pickup_ts"),
        F.to_timestamp(DROPOFF_COL).alias("dropoff_ts"),
        F.col(DISTANCE_COL).cast("double").alias("trip_distance")
    )
    
    # Columnas calculadas
    df_silver = df_silver.withColumn("trip_date", F.to_date("pickup_ts")) \
                         .withColumn("trip_duration_sec", 
                                     F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))
    
    # Reglas de Calidad
    df_silver = df_silver.filter(
        (F.col("pickup_ts").isNotNull()) &
        (F.col("trip_duration_sec") > 0) &
        (F.col("trip_distance") >= 0)
    )
    
    # ID único para idempotencia
    df_silver = df_silver.withColumn("trip_key", F.sha2(F.concat_ws("|", 
        F.col("hvfhs_license_num"), 
        F.col("dispatching_base_num"), 
        F.col("pickup_ts"), 
        F.col("PULocationID")
    ), 256))

    # MERGE para evitar duplicados
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        df_silver.write.format("delta").partitionBy("trip_date").save(SILVER_PATH)
        print("Primera carga de Silver completada")
    else:
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("target").merge(
            df_silver.alias("source"),
            "target.trip_key = source.trip_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print("MERGE completado")
    
    # Optimización Z-ORDER (la magia para performance)
    try:
        spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (trip_date)")
        print("✅ Optimización ZORDER completada")
    except Exception as e:
        print(f"⚠️ ZORDER no disponible: {e}")

    print(f"✅ Capa Silver actualizada en: {SILVER_PATH}")

# Ejecutamos
spark = SparkSession.builder.getOrCreate()
transform_silver(spark)

# COMMAND ----------
# Validación
print("Primeras 5 filas de Silver:")
spark.read.format("delta").load(SILVER_PATH).show(5)
