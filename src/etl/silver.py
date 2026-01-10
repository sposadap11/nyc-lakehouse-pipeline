from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.common import config

def transform_silver(spark: SparkSession):
    # Validación básica de que la capa anterior existe
    if not DeltaTable.isDeltaTable(spark, config.BRONZE_PATH):
        raise Exception("Error: No se encontró la capa Bronze. Ejecuta la ingesta primero.")

    # Lectura de datos desde la capa Bronze
    df_bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    
    # Transformación y normalización de columnas de fecha y distancia
    df_silver = df_bronze.select(
        "*",
        F.to_timestamp(config.PICKUP_COL).alias("pickup_ts"),
        F.to_timestamp(config.DROPOFF_COL).alias("dropoff_ts"),
        F.col(config.DISTANCE_COL).cast("double").alias("trip_distance")
    )
    
    # Cálculo de duración y fecha del viaje para particionamiento
    df_silver = df_silver.withColumn("trip_date", F.to_date("pickup_ts")) \
                         .withColumn("trip_duration_sec", 
                                     F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))
    
    # Filtros de calidad de datos
    df_silver = df_silver.filter(
        (F.col("pickup_ts").isNotNull()) &
        (F.col("trip_duration_sec") > 0) &
        (F.col("trip_distance") >= 0)
    )
    
    # Generación de llave única para asegurar idempotencia (evitar duplicados)
    df_silver = df_silver.withColumn("trip_key", F.sha2(F.concat_ws("|", 
        F.col("hvfhs_license_num"), 
        F.col("dispatching_base_num"), 
        F.col("pickup_ts"), 
        F.col("PULocationID")
    ), 256))

    # Lógica de MERGE para mantener la integridad de la capa Silver
    if not DeltaTable.isDeltaTable(spark, config.SILVER_PATH):
        # Primera carga de la tabla
        df_silver.write.format("delta").partitionBy("trip_date").save(config.SILVER_PATH)
    else:
        # Actualización de registros existentes o inserción de nuevos (Upsert)
        silver_table = DeltaTable.forPath(spark, config.SILVER_PATH)
        silver_table.alias("target").merge(
            df_silver.alias("source"),
            "target.trip_key = source.trip_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    
    # Optimización física de los datos con Z-Order para acelerar consultas por fecha
    try:
        spark.sql(f"OPTIMIZE delta.`{config.SILVER_PATH}` ZORDER BY (trip_date)")
        print("Optimización ZORDER BY (trip_date) completada.")
    except Exception as e:
        print(f"Optimización saltada o no soportada en este entorno: {e}")

    print(f"Capa Silver actualizada exitosamente en {config.SILVER_PATH}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverTransformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    transform_silver(spark)
