from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
from src.common import config

def ingest_bronze(spark: SparkSession):
    # Leo los datos raw desde la ruta configurada en config.py
    print(f"Iniciando ingesta desde: {config.DATASET_PATH}...")
    
    # Cargo los archivos parquet
    df_raw = spark.read.parquet(config.DATASET_PATH)
    
    # Agrego metadata técnica para trazabilidad
    # Uso _metadata.file_path para compatibilidad con Unity Catalog
    df_bronze = df_raw.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.lit(str(uuid.uuid4())).alias("_load_id")
    )
    
    # Guardo en la capa Bronze usando el formato Delta
    df_bronze.write.format("delta").mode("overwrite").save(config.BRONZE_PATH)
    print(f"Capa Bronze actualizada exitosamente en {config.BRONZE_PATH}")

if __name__ == "__main__":
    # Configuración básica de Spark para Delta
    spark = SparkSession.builder.appName("BronzeIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    ingest_bronze(spark)
