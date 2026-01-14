from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace, trim, coalesce, lit, max, to_timestamp
spark = SparkSession.builder.appName("Silver_Reviews").getOrCreate()

# Paths
BRONZE_PATH = "hdfs://localhost:9000/datalake/bronze/order_reviews"
SILVER_PATH = "hdfs://localhost:9000/datalake/silver/order_reviews"

def standardize_data(df_reviews):
    print("--- Standardize data ---")
    return df_reviews.select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        col("review_score").cast("int"),
        # Clean Title: Replace newlines with space, Trim, Handle Nulls
        coalesce(
            trim(regexp_replace(col("review_comment_title"), "[\n\r]", " ")),
            lit("No Title")
        ).alias("review_comment_title"),
        # Clean Message: Replace newlines with space (Crucial for NLP/BI)
        coalesce(
            trim(regexp_replace(col("review_comment_message"), "[\n\r]", " ")),
            lit("No Comment")
        ).alias("review_comment_message"),
        to_timestamp(col("review_creation_date")).alias("review_creation_date"),
        to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
        col("updated_at")
)

def get_latest_version(df, id_col, time_col):
    """
    Keeps the latest review version based on updated_at.
    """
    print(f" --- Deduplicating: Keeping latest '{id_col}' based on '{time_col}' ---")
    # 1. Find the latest timestamp for every Review ID
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

if __name__ == "__main__":
    df_reviews = spark.read.parquet(BRONZE_PATH)
    df_standard = standardize_data(df_reviews)
    df_unique = get_latest_version(df_standard, "review_id", "updated_at")
    df_final = df_unique.drop("updated_at")
    df_final.write.mode("overwrite").parquet(SILVER_PATH)
    print("SUCCESSFUL!!!")
    spark.stop()
