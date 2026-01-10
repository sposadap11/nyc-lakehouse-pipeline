# Databricks notebook source
# 03 KPIs Gold
# Calculo de indicadores de negocio finales.

from pyspark.sql import functions as F

bucket = "datalake-nyc-viajes-sebastian"
ruta_silver = f"s3://{bucket}/data/silver"
ruta_gold = f"s3://{bucket}/data/gold"

# Lectura de los datos transformados
df = spark.read.format("delta").load(ruta_silver)

# Calculo de ingresos totales por viaje
cols_ingreso = ["base_passenger_fare", "tolls", "tips", "sales_tax", "congestion_surcharge", "airport_fee"]
df_kpi = df.withColumn("ingreso", sum([F.coalesce(F.col(c), F.lit(0)) for c in cols_ingreso]))

# Configuracion de acceso a S3
access_key = dbutils.secrets.get("aws", "access_key")
secret_key = dbutils.secrets.get("aws", "secret_key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Resumen diario
df_gold = df_kpi.groupBy("fecha_viaje").agg(
    F.count("trip_key").alias("total_viajes"),
    F.sum("ingreso").alias("ingreso_total"),
    F.avg("duracion_seg").alias("promedio_duracion_seg")
)

# Guardamos los KPIs
df_gold.write.format("delta").mode("overwrite").save(ruta_gold)

print("Proceso finalizado. KPIs generados exitosamente.")
display(df_gold.orderBy("fecha_viaje", ascending=False))
