# Databricks notebook source
# 02 Transformacion Silver
# Transformacion, # Mantenemos la configuracion de acceso
access_key = dbutils.secrets.get("aws", "access_key")
secret_key = dbutils.secrets.get("aws", "secret_key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

from pyspark.sql import functions as F

bucket = "datalake-nyc-viajes-sebastian"
ruta_bronze = f"s3://{bucket}/data/bronze"
ruta_silver = f"s3://{bucket}/data/silver"

# Cargamos los datos de Bronze
df = spark.read.format("delta").load(ruta_bronze)

# Limpieza y estandarizacion
df_silver = df.select(
    "*",
    F.to_timestamp("pickup_datetime").alias("recogida_ts"),
    F.to_timestamp("dropoff_datetime").alias("entrega_ts")
).withColumn("fecha_viaje", F.to_date("recogida_ts")) \
 .withColumn("duracion_seg", F.unix_timestamp("entrega_ts") - F.unix_timestamp("recogida_ts"))

# Filtro de calidad basico
df_silver = df_silver.filter((F.col("recogida_ts").isNotNull()) & (F.col("duracion_seg") > 0))

# Creacion de llave unica (Senior level)
df_silver = df_silver.withColumn("trip_key", F.sha2(F.concat_ws("|", "hvfhs_license_num", "recogida_ts", "PULocationID"), 256))

# Guardado particionado para performance
df_silver.write.format("delta").partitionBy("fecha_viaje").mode("overwrite").save(ruta_silver)

# Optimización Z-Order (Imprescindible para el test)
spark.sql(f"OPTIMIZE delta.`{ruta_silver}` ZORDER BY (fecha_viaje)")

print(f"Capa Silver terminada y optimizada en: {ruta_silver}")
display(df_silver.limit(10))
