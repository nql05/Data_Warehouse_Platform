from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# --- CONFIGURATION ---
# 1. JDBC Driver Path
jdbc_jar_path = "/home/hiveusr/hive-4.0.0/lib/postgresql-42.7.4.jar"

# 2. Database Connection Details
db_properties = {
    "user": "postgres",
    "password": "Linh@280405",
    "driver": "org.postgresql.Driver"
}
jdbc_url = "jdbc:postgresql://localhost:5432/ecommerce"

# 3. List of tables to ingest
tables_to_ingest = [
    "orders",
    "order_items",
    "customers",
    "products",
    "order_payments",
    "sellers",
    "geolocation",
    "order_reviews",
]

# --- INITIALIZE SPARK ---
print("Starting Spark Session ...")
spark = SparkSession.builder \
        .appName("IncrementalLoad_Postgres_To_HDFS") \
        .config("spark.jars", jdbc_jar_path) \
        .master("local[*]") \
        .getOrCreate()

def get_max_timestamp(hdfs_path):
    """
    Reads the existing Parquet data in HDFS to find the latest updated_at timestamp
    Returns None if the path doesn't exist (implying a Full load is needed)
    :param hdfs_path:
    :return:
    """
    try:
        df_existing = spark.read.parquet(hdfs_path)
        latest_update = df_existing.agg(max("updated_at")).collect()[0][0]
        return latest_update
    except Exception as e:
        # If the path doesn't exist or error reading, return None -> Trigger Full load
        return None

if __name__ == "__main__":
    # -- INGESTION LOOP ---
    print("Starting Incremental ingestion ...")
    for table in tables_to_ingest:
        #Write to BRONZE layer
        save_path = f"hdfs://localhost:9000/datalake/bronze/{table}"
        latest_updated = get_max_timestamp(save_path)

        if latest_updated:
            print(f"[{table}] Found existing data. Latest updated_at: {latest_updated}")
            print(f"[{table}] Performing INCREMENTAL load...")

            # Query only data NEWER than the last timestamp in HDFS bronze layer
            query = f"""
                    (SELECT * FROM public.{table} 
                     WHERE updated_at > '{latest_updated}') AS subq
                    """
        else:
            print(f"[{table}] No existing data found (or error reading).")
            print(f"[{table}] Performing FULL load...")
            query = f"public.{table}"  # Just read the whole table

        # Read from Source
        df_new = spark.read.jdbc(url=jdbc_url, table=query, properties = db_properties)
        if df_new.count() > 0:
            df_new.write.mode("append").parquet(save_path)
            print(f"[{table}] Appended {df_new.count()} new rows.")
        else:
            print(f"[{table}] No new data to ingest.")
    print("\nIngestion Complete")
    spark.stop()

