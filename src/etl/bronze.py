from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
from src.common import config

def ingest_bronze(spark: SparkSession):
    """
    Ingests raw parquet files into the Bronze layer (Delta).
    Adds technical metadata for traceability.
    """
    print(f"Reading records from {config.DATASET_PATH}...")
    
    # Read raw parquet files
    df_raw = spark.read.parquet(config.DATASET_PATH)
    
    # Add technical metadata
    df_bronze = df_raw.withColumn("_ingest_ts", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name()) \
                      .withColumn("_load_id", F.lit(str(uuid.uuid4())))
    
    # Write to Bronze layer (Overwrite for simplicity in this stage, or Append if incremental)
    df_bronze.write.format("delta").mode("overwrite").save(config.BRONZE_PATH)
    print(f"Bronze layer updated at {config.BRONZE_PATH}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    ingest_bronze(spark)
