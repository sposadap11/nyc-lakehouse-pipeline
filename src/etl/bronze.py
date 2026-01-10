# Capa Bronze: Ingesta inicial
# Este módulo se encarga de mover los datos de la zona de aterrizaje (Landing/Raw) 
# a Delta Lake con metadata técnica para trazabilidad.

from pyspark.sql import functions as F
from src.common import config
import uuid

def ingest_bronze(spark):
    print(f"Leyendo datos crudos desde {config.RAW_PATH}...")
    
    # Lectura de los archivos parquet originales
    df_raw = spark.read.parquet(config.RAW_PATH)
    
    # Agregamos metadata técnica para auditoría
    # Nota: Usamos _metadata.file_path para compatibilidad total con Unity Catalog
    df_bronze = df_raw.select(
        "*",
        F.current_timestamp().alias("fecha_ingesta"),
        F.col("_metadata.file_path").alias("archivo_origen"),
        F.lit(str(uuid.uuid4())).alias("id_lote")
    )
    
    # Guardado en formato Delta optimizado (overwrite para pruebas)
    df_bronze.write.format("delta") \
        .mode("overwrite") \
        .save(config.BRONZE_PATH)
    
    print(f"Bronze listo. Registros cargados: {df_bronze.count()}")
