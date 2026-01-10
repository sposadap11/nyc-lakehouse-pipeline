from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.common import config

def transform_silver(spark: SparkSession):
    """
    Standardizes data, applies quality rules, and performs idempotent MERGE into Silver.
    """
    if not DeltaTable.isDeltaTable(spark, config.BRONZE_PATH):
        raise Exception("Bronze table not found. Run Bronze ingestion first.")

    # Read from Bronze
    df_bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    
    # Normalization and Transformation
    df_silver = df_bronze.select(
        "*",
        F.to_timestamp(config.PICKUP_COL).alias("pickup_ts"),
        F.to_timestamp(config.DROPOFF_COL).alias("dropoff_ts"),
        F.col(config.DISTANCE_COL).cast("double").alias("trip_distance")
    )
    
    # Derived columns
    df_silver = df_silver.withColumn("trip_date", F.to_date("pickup_ts")) \
                         .withColumn("trip_duration_sec", 
                                     F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))
    
    # DQ Rules
    df_silver = df_silver.filter(
        (F.col("pickup_ts").isNotNull()) &
        (F.col("trip_duration_sec") > 0) &
        (F.col("trip_distance") >= 0)
    )
    
    # Generate technical ID (Idempotency Key)
    # Using stable business columns to detect duplicates across loads
    df_silver = df_silver.withColumn("trip_key", F.sha2(F.concat_ws("|", 
        F.col("hvfhs_license_num"), 
        F.col("dispatching_base_num"), 
        F.col("pickup_ts"), 
        F.col("PULocationID")
    ), 256))

    # Idempotent Write (MERGE)
    if not DeltaTable.isDeltaTable(spark, config.SILVER_PATH):
        df_silver.write.format("delta").partitionBy("trip_date").save(config.SILVER_PATH)
    else:
        silver_table = DeltaTable.forPath(spark, config.SILVER_PATH)
        silver_table.alias("target").merge(
            df_silver.alias("source"),
            "target.trip_key = source.trip_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    
    # Optimization: ZORDER (Runs if compatible environment)
    try:
        spark.sql(f"OPTIMIZE delta.`{config.SILVER_PATH}` ZORDER BY (trip_date)")
        print("Optimization ZORDER BY (trip_date) completed.")
    except Exception as e:
        print(f"Optimization skipped or not supported: {e}")

    print(f"Silver layer updated at {config.SILVER_PATH}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverTransformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    transform_silver(spark)
