from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

SILVER_PATH = "hdfs://localhost:9000/datalake/silver"
GOLD_PATH = "hdfs://localhost:9000/datalake/gold"

def create_dim_customers(df_customers, df_geo):
    print("JOINING CUSTOMERS WITH GEOLOCATION")
    dim_customers_geo = df_customers.join(
        df_geo,
        df_customers.customer_zip_code_prefix == df_geo.zip_code,
        "left"
    )
    final_dim_customers = dim_customers_geo.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        col("customer_zip_code_prefix"),
        #Enrich fields
        col("lat").alias("latitude"),
        col("lng").alias("longtitude")
    )
    return final_dim_customers

if __name__ == "__main__":
    # Initialization of SparkSession
    spark = SparkSession.builder \
        .appName("Dim_Customer_Gold_Layer") \
        .master("local[*]") \
        .getOrCreate()
    df_customers = spark.read.parquet(f"{SILVER_PATH}/customers")
    df_geo = spark.read.parquet(f"{SILVER_PATH}/geolocation")
    df_dim = create_dim_customers(df_customers, df_geo)
    df_dim.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customers")
    print("SUCCESSFUL!!!")
    spark.stop()


