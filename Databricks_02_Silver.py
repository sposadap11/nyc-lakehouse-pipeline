# Databricks notebook source
# MAGIC %md # 02 Transformación Silver - Por sposada
# MAGIC Aquí es donde ocurre la magia. Limpiamos los datos, aplicamos reglas de calidad y optimizamos la tabla para que vuele en las consultas.

# COMMAND ----------
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Misma configuración para no depender de otros archivos
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
BRONZE_PATH = f"s3://{BUCKET_NAME}/data/bronze"
SILVER_PATH = f"s3://{BUCKET_NAME}/data/silver"

# Columnas clave para la data de NYC
PICKUP_COL   = "pickup_datetime"
DROPOFF_COL  = "dropoff_datetime"
DISTANCE_COL = "trip_miles"

def transform_silver(spark: SparkSession):
    # Primero valido que la capa Bronze ya exista
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        raise Exception("Ey, falta la capa Bronze. Corre el primer notebook!")

    # Leo desde Bronze (Delta format)
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    
    # Normalización: ajusto tipos de datos y nombres
    df_silver = df_bronze.select(
        "*",
        F.to_timestamp(PICKUP_COL).alias("pickup_ts"),
        F.to_timestamp(DROPOFF_COL).alias("dropoff_ts"),
        F.col(DISTANCE_COL).cast("double").alias("trip_distance")
    )
    
    # Columnas calculadas que nos sirven para analítica
    df_silver = df_silver.withColumn("trip_date", F.to_date("pickup_ts")) \
                         .withColumn("trip_duration_sec", 
                                     F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))
    
    # Reglas de Calidad (Data Quality): Fuera viajes con tiempo negativo o nulos
    df_silver = df_silver.filter(
        (F.col("pickup_ts").isNotNull()) &
        (F.col("trip_duration_sec") > 0) &
        (F.col("trip_distance") >= 0)
    )
    
    # Genero un ID único (trip_key) para evitar duplicados si volvemos a cargar la misma data
    df_silver = df_silver.withColumn("trip_key", F.sha2(F.concat_ws("|", 
        F.col("hvfhs_license_num"), 
        F.col("dispatching_base_num"), 
        F.col("pickup_ts"), 
        F.col("PULocationID")
    ), 256))

    # Inserción Idempotente: Si ya existe el viaje, lo actualizamos. Si no, lo insertamos.
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # Primera carga: particionamos por fecha para que sea más eficiente
        df_silver.write.format("delta").partitionBy("trip_date").save(SILVER_PATH)
    else:
        # MERGE para manejar updates y evitar duplicidad
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("target").merge(
            df_silver.alias("source"),
            "target.trip_key = source.trip_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    
    # Optimización PRO: ZORDER. Esto organiza los datos físicamente por fecha.
    # ¡Esto es lo que hace que las consultas sean súper rápidas!
    try:
        spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (trip_date)")
        print("Optimización ZORDER completada con éxito.")
    except Exception as e:
        print(f"No se pudo optimizar (tal vez por el tipo de cluster): {e}")

    print(f"Capa Silver actualizada en: {SILVER_PATH}")

# Ejecutamos la transformación
spark = SparkSession.builder.getOrCreate()
transform_silver(spark)
