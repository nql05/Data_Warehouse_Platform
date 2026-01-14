from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, regexp_replace, to_timestamp, max

# Initialization for SparkSession
spark = SparkSession.builder \
        .appName("Silver_Processing") \
        .master("local[*]") \
        .getOrCreate()

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/order_payments"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/order_payments"

def standardize_data(df_order_payments):
    print("--- Standardize data ----")
    return df_order_payments.select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential").cast("int"),
        # Convert "credit_card" -> "Credit Card"
        initcap(regexp_replace(col("payment_type"), "_", " ")).alias("payment_type"),
        col("payment_installments").cast("int"),
        col("payment_value").cast("double"),
        col("updated_at")
    )
def get_latest_version(df_order_payments, time_col):
    print(f" --- Deduplicating: Keeping latest version based on '{time_col}' ---")

    df_latest = df_order_payments.groupBy("order_id", "payment_sequential").agg(
        max(to_timestamp(col(time_col))).alias("latest_time")
    )

    # 2. Join back using Explicit Aliases to find the full row
    df_deduped = df_order_payments.alias("main") \
        .join(
            df_latest.alias("lookup"),
            (col("main.order_id") == col("lookup.order_id")) &
            (col("main.payment_sequential") == col("lookup.payment_sequential")) &
            (col(f"main.{time_col}") == col("lookup.latest_time")),
            "inner"
        ).select("main.*")
    return df_deduped


def apply_quality_rules(df):
    print(" --- Applying data quality rules ---")
    df_clean = df.filter(col("payment_value") >= 0)
    df_clean = df_clean.dropDuplicates(["order_id", "payment_sequential"])
    return df_clean


if __name__ == "__main__":
    df_order_payments = spark.read.parquet(BRONZE_LAYER)
    df_standard = standardize_data(df_order_payments)
    df_unique = get_latest_version(df_standard, "updated_at")
    df_clean = apply_quality_rules(df_unique)
    df_final = df_clean.drop("updated_at")
    df_final.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL")
    spark.stop()

