from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, max

#Initialize SparkSession
spark = SparkSession.builder \
        .appName("Silver_Layer_Processing") \
        .master("local[*]") \
        .getOrCreate()

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/products"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/products"

# Data Standardization and Consistency
def standardize_data(df_products):
    print(" --- Data Standardization ---")
    return df_products.select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_category_name")).alias("category_name"),
        col("product_name_lenght").cast("int").alias("product_name_length"),
        col("product_description_lenght").cast("int").alias("product_description_length"),
        col("product_photos_qty").cast("int").alias("product_photos_qty"),
        col("product_weight_g").cast("int").alias("product_weight_g"),
        col("product_length_cm").cast("int").alias("product_length_cm"),
        col("product_height_cm").cast("int").alias("product_height_cm"),
        col("product_width_cm").cast("int").alias("product_width_cm"),
        col("updated_at")
    )

def get_latest_version(df, id_col, time_col):
    print(f"--- Deduplicating: Keeping latest '{id_col}' based on '{time_col}' ---")

    # 1. Find the latest timestamp for every Product ID
    df_max_dates = df.groupBy(id_col).agg(
        max(to_timestamp(col(time_col))).alias("latest_time")
    )

    # 2. Join back using Explicit Aliases
    df_deduped = df.alias("main") \
        .join(
            df_max_dates.alias("lookup"),
            (col(f"main.{id_col}") == col(f"lookup.{id_col}")) &
            (col(f"main.{time_col}") == col("lookup.latest_time")),
            "inner"
        ).select("main.*")

    return df_deduped

def apply_quality_rules(df):
    print(" --- Applying data quality rules (Handling NULLs) ---")
    fill_values = {
        "product_name_length": 0,
        "product_description_length": 0,
        "product_photos_qty": 0,
        "product_weight_g": 0,
        "product_length_cm": 0,
        "product_height_cm": 0,
        "product_width_cm": 0
    }
    return df.fillna(fill_values)

if __name__ == "__main__":
    df_products = spark.read.parquet(BRONZE_LAYER)
    df_standard = standardize_data(df_products)
    df_unique = get_latest_version(df_standard, "product_id", "updated_at")
    df_clean = apply_quality_rules(df_unique)
    df_final = df_clean.drop("updated_at")
    df_final.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL!!!")
    spark.stop()