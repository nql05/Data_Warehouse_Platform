from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, trim, upper, avg, first

BRONZE_LAYER = "hdfs://localhost:9000/datalake/bronze/geolocation"
SILVER_LAYER = "hdfs://localhost:9000/datalake/silver/geolocation"

# Initialization for SparkSession
spark = SparkSession.builder \
        .appName("Silver_Layer_Processing_Geolocation") \
        .master("local[*]") \
        .getOrCreate()

def standardize_data(df_geo):
    print("--- Standardization data ---")
    return df_geo.select(
        col("geolocation_zip_code_prefix").cast("string").alias("zip_code"),
        col("geolocation_lat").cast("double").alias("raw_lat"),
        col("geolocation_lng").cast("double").alias("raw_lng"),
        initcap(trim(col("geolocation_city"))).alias("city"),
        upper(trim(col("geolocation_state"))).alias("state")
    )
def aggregate_zip_code(df_geo):
    print("--- Aggregating to unique Zip code ---")
    return df_geo.groupBy("zip_code").agg(
        avg("raw_lat").alias("lat"),
        avg("raw_lng").alias("lng"),
        first("city").alias("city"),
        first("state").alias("state")
    )

if __name__ == "__main__":
    df_geo = spark.read.parquet(BRONZE_LAYER)
    df_geo_standard = standardize_data(df_geo)
    df_geo_aggregate = aggregate_zip_code(df_geo_standard)
    df_geo_aggregate.write.mode("overwrite").parquet(SILVER_LAYER)
    print("SUCCESSFUL!!!")
    spark.stop()
