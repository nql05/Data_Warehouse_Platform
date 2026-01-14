from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim, when, count, max

# Initialize SparkSession
spark = SparkSession.builder \
        .appName("Silver_Layer_Processing") \
        .master("local[*]") \
        .getOrCreate()

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/orders"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/orders"

def standardize_data(df_orders):
    print(" --- Data Standardization and Consistency --- ")
    return df_orders.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        lower(trim(col("order_status"))).alias("order_status"),
        to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
        to_timestamp(col("order_approved_at")).alias("order_approved_at"),
        to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
        to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
        to_timestamp(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
        col("updated_at")
    )

def get_latest_version(df, id_col, time_col):
    print(f" --- Deduplicating: Keeping latest '{id_col}' based on '{time_col}' ---")

    df_max_dates = df.groupBy(id_col).agg(
        max(to_timestamp(col(time_col))).alias("latest_time")
    )

    df_deduped = df.alias("main") \
        .join(
            df_max_dates.alias("lookup"),
            (col(f"main.{id_col}") == col(f"lookup.{id_col}")) &
            (col(f"main.{time_col}") == col("lookup.latest_time")),
            "inner"
        ).select("main.*")

    return df_deduped


def apply_quality_rules(df):
    print(" --- Applying business logic and quality rules ---")

    df_checked = df.withColumn(
        "is_valid_time",
        when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_customer_date") < col("order_purchase_timestamp")),
            False
        ).otherwise(True)
    )
    df_valid = df_checked.filter(col("is_valid_time") == True).drop("is_valid_time")
    # Handle Missing Data
    df_clean = df_valid.dropna(subset=["order_id", "customer_id"])
    return df_clean

if __name__ == "__main__":
    # --- PROCESSING Orders Table ---
    df_orders = spark.read.parquet(BRONZE_LAYER)
    df_standard = standardize_data(df_orders)
    df_unique = get_latest_version(df_standard, "order_id", "updated_at")
    df_clean = apply_quality_rules(df_unique)
    df_final = df_clean.drop("updated_at")
    df_final.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL")
    spark.stop()
