from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, max

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/order_items"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/order_items"
# Initialization for SparkSession
spark = SparkSession.builder \
        .appName("Silver_Processing") \
        .master("local[*]") \
        .getOrCreate()

def standardize_data(df_items):
    return df_items.select(
        trim(col("order_id")).alias("order_id"),
        col("order_item_id").cast("int"),
        trim(col("product_id")).alias("product_id"),
        trim(col("seller_id")).alias("seller_id"),
        to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
        col("price").cast("double"),
        col("freight_value").cast("double"),
        col("updated_at")
    )

def get_latest_version(df_items, time_col):
    print(f"--- Deduplicating: Keeping latest version based on '{time_col}' ---")
    df_items_latest = df_items.groupBy("order_id", "order_item_id").agg(
        max(to_timestamp(col(time_col))).alias("latest_time")
    )
    df_deduped = df_items.alias("main") \
        .join(
            df_items_latest.alias("lookup"),
            (col("main.order_id") == col("lookup.order_id")) &
            (col("main.order_item_id") == col("lookup.order_item_id")) &
            (col(f"main.{time_col}") == col("lookup.latest_time")),
            "inner"
        ).select("main.*")
    return df_deduped

def apply_quality_check(df_items):
    print("--- Applying data quality rules ---")
    return df_items.dropna(subset=["price", "freight_value"])

if __name__ == "__main__":
    df_items = spark.read.parquet(BRONZE_LAYER)
    df_items_standard = standardize_data(df_items)
    df_items_unique = get_latest_version(df_items_standard, "updated_at")
    df_items_cleaned = apply_quality_check(df_items_unique)
    df_items_cleaned = df_items_cleaned.drop("updated_at")
    df_items_cleaned.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL!!!")
    spark.stop()
