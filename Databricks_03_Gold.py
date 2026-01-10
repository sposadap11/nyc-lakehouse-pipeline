# Databricks notebook source
# MAGIC %md # 03 Capa Gold (KPIs) - Por sposada
# MAGIC Cálculo de indicadores de negocio diarios.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuración
BUCKET_NAME = "datalake-nyc-viajes-sebastian"
SILVER_PATH = f"s3a://{BUCKET_NAME}/data/silver"
GOLD_PATH   = f"s3a://{BUCKET_NAME}/data/gold"

# Credenciales AWS
AWS_ACCESS_KEY = "TU_ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "TU_SECRET_KEY_AQUI"

spark.conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Columnas de ingresos
REVENUE_COLS = [
    "base_passenger_fare", "tolls", "tips", 
    "sales_tax", "congestion_surcharge", "airport_fee"
]

def calculate_gold(spark: SparkSession):
    print("Leyendo desde Silver...")
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    # Cálculo de Revenue total
    revenue_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in REVENUE_COLS])
    df_silver = df_silver.withColumn("trip_revenue", revenue_expr)
    
    # Agregación diaria
    df_gold = df_silver.groupBy("trip_date").agg(
        F.count("trip_key").alias("total_trips"),
        F.sum("trip_revenue").alias("total_revenue"),
        F.avg("trip_duration_sec").alias("avg_trip_duration_sec"),
        F.avg("trip_distance").alias("avg_trip_distance")
    )
    
    # KPI extra: viajes por hora
    df_gold = df_gold.withColumn("avg_trips_per_hour", F.col("total_trips") / 24)
    
    # Guardado final
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    
    print(f"✅ Indicadores Gold actualizados en: {GOLD_PATH}")
    print("\n📊 Resumen de KPIs:")
    df_gold.orderBy("trip_date", ascending=False).show(10)

# Ejecución
spark = SparkSession.builder.getOrCreate()
calculate_gold(spark)

# COMMAND ----------
# Estadísticas finales
df_final = spark.read.format("delta").load(GOLD_PATH)
print(f"\n📈 Total de días procesados: {df_final.count()}")
print(f"💰 Ingreso total acumulado: ${df_final.agg(F.sum('total_revenue')).collect()[0][0]:,.2f}")
