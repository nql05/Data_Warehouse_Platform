from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, length, upper, to_timestamp, max

#Initialize SparkSession
spark = SparkSession.builder \
        .appName("Silver_Layer_Processing") \
        .master("local[*]") \
        .getOrCreate()

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/sellers"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/sellers"


def standardize_sellers(df):

    print(" --- Standardizing columns ---")
    return df.select(
        trim(col("seller_id")).alias("seller_id"),
        col("seller_zip_code_prefix").cast("string").alias("seller_zip_code_prefix"),
        initcap(trim(col("seller_city"))).alias("seller_city"),
        upper(trim(col("seller_state"))).alias("seller_state"),
        col("updated_at")  # Required for "Latest Version" logic
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
    print(" --- Applying data quality rules ---")
    df_checked = df.filter(length(col("seller_state")) == 2)
    df_clean = df_checked.dropna(subset=["seller_id"])
    return df_clean

if __name__ == "__main__":
    df_sellers = spark.read.parquet(BRONZE_LAYER)
    df_standard = standardize_sellers(df_sellers)
    df_unique = get_latest_version(df_standard, "seller_id", "updated_at")
    df_clean = apply_quality_rules(df_unique)
    df_final = df_clean.drop("updated_at")
    df_final.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL!!!")
    spark.stop()