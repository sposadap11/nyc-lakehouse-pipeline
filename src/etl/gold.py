from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common import config

def calculate_gold(spark: SparkSession):
    """
    Calculates daily KPIs and updates the Gold layer.
    """
    # Read from Silver
    df_silver = spark.read.format("delta").load(config.SILVER_PATH)
    
    # Calculate revenue (Coalesce nulls to 0)
    revenue_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in config.REVENUE_COLS])
    df_silver = df_silver.withColumn("trip_revenue", revenue_expr)
    
    # Grouping and KPI calculation
    df_gold = df_silver.groupBy("trip_date").agg(
        F.count("trip_key").alias("total_trips"),
        F.sum("trip_revenue").alias("total_revenue"),
        F.avg("trip_duration_sec").alias("avg_trip_duration_sec"),
        F.avg("trip_distance").alias("avg_trip_distance")
    )
    
    # Add KPI specialized: avg trips per hour (assuming 24h)
    df_gold = df_gold.withColumn("avg_trips_per_hour", F.col("total_trips") / 24)
    
    # Write to Gold (Overwrite by partition logic manually or standard overwrite)
    # For a tech test, overwrite of the target table is sufficient to show the final result
    df_gold.write.format("delta").mode("overwrite").save(config.GOLD_PATH)
    
    print(f"Gold layer KPIs updated at {config.GOLD_PATH}")
    df_gold.show(5)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldKPIs") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    calculate_gold(spark)
