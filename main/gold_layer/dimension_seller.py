from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SILVER_PATH = "hdfs://localhost:9000/datalake/silver"
GOLD_PATH = "hdfs://localhost:9000/datalake/gold"

def create_dim_seller(df_sellers, df_geo):
    print("JOINING SELLERS WITH GEOLOCATION")
    dim_sellers_geo = df_sellers.join(
        df_geo,
        df_sellers.seller_zip_code_prefix == df_geo.zip_code,
        "left"
    )
    final_dim_sellers = dim_sellers_geo.select(
        col("seller_id"),
        col("seller_city"),
        col("seller_state"),
        col("seller_zip_code_prefix"),
        #Enrich fields
        col("lat").alias("latitude"),
        col("lng").alias("longtitude")
    )
    return final_dim_sellers

if __name__ == "__main__":
    # Initialization of SparkSession
    spark = SparkSession.builder \
        .appName("Dim_Customer_Gold_Layer") \
        .master("local[*]") \
        .getOrCreate()

    df_sellers = spark.read.parquet(f"{SILVER_PATH}/sellers")
    df_geo = spark.read.parquet(f"{SILVER_PATH}/geolocation")

    df_final = create_dim_seller(df_sellers, df_geo)
    df_final.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_sellers")
    print("SUCCESS!!!")
    spark.stop()