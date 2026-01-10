# Databricks notebook source
# MAGIC %md # 03 Capa Gold (KPIs) - Por sposada
# MAGIC Aquí calculamos los indicadores de negocio que los analistas van a usar. Total de viajes, ingresos estimados y promedios.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuración
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
SILVER_PATH = f"s3://{BUCKET_NAME}/data/silver"
GOLD_PATH   = f"s3://{BUCKET_NAME}/data/gold"

# Estas son las columnas que suman para el ingreso total
REVENUE_COLS = [
    "base_passenger_fare", "tolls", "tips", 
    "sales_tax", "congestion_surcharge", "airport_fee"
]

def calculate_gold(spark: SparkSession):
    # Leo la data ya limpia de Silver
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    # Calculo el Revenue total sumando todos los componentes (manejando nulos como 0)
    revenue_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in REVENUE_COLS])
    df_silver = df_silver.withColumn("trip_revenue", revenue_expr)
    
    # Agrupo por día para sacar los KPIs diarios
    df_gold = df_silver.groupBy("trip_date").agg(
        F.count("trip_key").alias("total_trips"),
        F.sum("trip_revenue").alias("total_revenue"),
        F.avg("trip_duration_sec").alias("avg_trip_duration_sec"),
        F.avg("trip_distance").alias("avg_trip_distance")
    )
    
    # Un KPI extra: promedio de viajes por hora (asumiendo día de 24h)
    df_gold = df_gold.withColumn("avg_trips_per_hour", F.col("total_trips") / 24)
    
    # Guardo el resultado final en la capa Gold
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    
    print(f"Indicadores Gold actualizados en: {GOLD_PATH}")
    # Muestro el resultado final para cerrar el proceso con éxito
    display(df_gold)

# Ejecución final
spark = SparkSession.builder.getOrCreate()
calculate_gold(spark)
