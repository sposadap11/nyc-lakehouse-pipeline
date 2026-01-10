# Capa Silver: Transformación y Calidad
# Aquí limpiamos los datos, estandarizamos tipos y optimizamos el almacenamiento.

from pyspark.sql import functions as F
from src.common import config

def transform_silver(spark):
    print("Iniciando transformación Silver...")
    
    # Carga desde Bronze
    df = spark.read.format("delta").load(config.BRONZE_PATH)
    
    # Estandarización de fechas y cálculo de duración
    df_silver = df.select(
        "*",
        F.to_timestamp("pickup_datetime").alias("recogida_ts"),
        F.to_timestamp("dropoff_datetime").alias("entrega_ts")
    ).withColumn("fecha_viaje", F.to_date("recogida_ts")) \
     .withColumn("duracion_seg", F.unix_timestamp("entrega_ts") - F.unix_timestamp("recogida_ts"))
    
    # Reglas de Calidad de Datos
    df_silver = df_silver.filter(
        (F.col("recogida_ts").isNotNull()) & 
        (F.col("duracion_seg") > config.QUALITY_MIN_DURATION)
    )
    
    # Generación de llave única para evitar duplicados (SHA-256)
    df_silver = df_silver.withColumn("trip_key", F.sha2(
        F.concat_ws("|", "hvfhs_license_num", "recogida_ts", "PULocationID"), 256
    ))
    
    # Guardado particionado para mejorar el rendimiento de las consultas
    df_silver.write.format("delta") \
        .partitionBy("fecha_viaje") \
        .mode("overwrite") \
        .save(config.SILVER_PATH)
    
    # Optimización física: Z-Order cronológico
    spark.sql(f"OPTIMIZE delta.`{config.SILVER_PATH}` ZORDER BY (recogida_ts)")
    
    print(f"Silver listo y optimizado por Z-Order.")
