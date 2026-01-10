# Capa Gold: Agregación de Negocio (KPIs)
# Resumen diario para analistas y toma de decisiones.

from pyspark.sql import functions as F
from src.common import config

def calculate_gold(spark):
    print("Calculando indicadores Gold...")
    
    # Carga desde Silver
    df = spark.read.format("delta").load(config.SILVER_PATH)
    
    # Definición de Revenue total (suma de componentes monetarios)
    columnas_ingreso = [
        "base_passenger_fare", "tolls", "tips", 
        "sales_tax", "congestion_surcharge", "airport_fee"
    ]
    total_revenue_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in columnas_ingreso])
    
    # Agregación diaria
    df_gold = df.withColumn("ingreso_viaje", total_revenue_expr) \
        .groupBy("fecha_viaje") \
        .agg(
            F.count("trip_key").alias("total_viajes"),
            F.sum("ingreso_viaje").alias("ingreso_total"),
            F.avg("duracion_seg").alias("promedio_duracion_seg")
        )
    
    # Guardado final de KPIs
    df_gold.write.format("delta") \
        .mode("overwrite") \
        .save(config.GOLD_PATH)
    
    print("Indicadores calculados exitosamente.")
