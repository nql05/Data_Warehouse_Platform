from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SILVER_PATH = "hdfs://localhost:9000/datalake/silver"
GOLD_PATH = "hdfs://localhost:9000/datalake/gold"

spark = SparkSession.builder \
    .appName("Dimension_Product_Gold_Layer") \
    .master("local[*]") \
    .getOrCreate()

dim_products = spark.read.parquet(f"{SILVER_PATH}/products").select(
    col("product_id"),
    col("category_name"),
    col("product_photos_qty"),
    col("product_weight_g"),
    col("product_length_cm"),
    col("product_height_cm"),
    col("product_width_cm")
)

dim_products.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_products")
print("SUCCESS")
spark.stop()
