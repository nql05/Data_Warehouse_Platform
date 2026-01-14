from dns.dnssec import to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, count, initcap, trim, length, max, to_timestamp

# Path
BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/customers"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/customers"

# Initialize SparkSession
spark = SparkSession.builder \
        .appName("Silver_Layer_Processing_Customer") \
        .master("local[*]") \
        .getOrCreate()

# Data Standardization and Consistency
def standardize_data(df_customers):
    print("Data Standardization and Consistency ...")
    return df_customers.select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_unique_id")).alias("customer_unique_id"),
        col("customer_zip_code_prefix").cast("string"),
        initcap(trim(col("customer_city"))).alias("customer_city"),
        upper(trim(col("customer_state"))).alias("customer_state"),
        col("updated_at")
    )

def get_latest_version (df_customers, id_col, time_col):
    print(f"Deduplicating: Keeping latest {id_col} based on {time_col} ...")
    # Aggregation based on customer_id to get latest updated customer
    df_latest = df_customers.groupBy(id_col).agg(max(to_timestamp(time_col)).alias("latest_updated"))
    df_deduped = df_customers.alias("main") \
        .join(
        df_latest.alias("lookup"),
        (col(f"main.{id_col}") == col(f"lookup.{id_col}")) &
        (col(f"main.{time_col}") == col("lookup.latest_updated")),
        "inner"
    ).select("main.*")  # Keep only columns from the main table
    return df_deduped

def apply_quality_rules(df_customers):
    print("Applying data quality rules ...")
    df_customers_cleaned = df_customers.filter(length(col("customer_state")) == 2)
    df_customers_cleaned = df_customers_cleaned.dropna(subset=["customer_id"])
    return df_customers_cleaned

if __name__ == "__main__":
    print(" --- PROCESSING CUSTOMER TABLE ---")
    # Read data from BRONZE_LAYER
    df_customers = spark.read.parquet(BRONZE_LAYER)

    # Transform
    df_customers_standard = standardize_data(df_customers)
    # Deduplicate
    df_customers_unique = get_latest_version(df_customers_standard, "customer_id", "updated_at")
    # Clean
    df_customers_clean = apply_quality_rules(df_customers_unique)
    # Write
    df_customers_clean = df_customers_clean.drop("updated_at")
    df_customers_clean.write.mode("overwrite").parquet(SILVER_LAYER)
    print("Successfully")
    spark.stop()
