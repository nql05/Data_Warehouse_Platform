import sys
sys.path.insert(0, "/home/nql/PycharmProjects/HadoopDataPipeline/src")

from pyspark.sql import SparkSession
from etl.bronze_layer.bronze_customers import CustomerBronzeETL
from etl.bronze_layer.bronze_geolocation import GeolocationBronzeETL
from etl.bronze_layer.bronze_order_items import OrderItemBronzeETL
from etl.bronze_layer.bronze_order_payments import OrderPaymentBronzeETL
from etl.bronze_layer.bronze_order_reviews import OrderReviewBronzeETL
from etl.bronze_layer.bronze_products import ProductBronzeETL
from etl.bronze_layer.bronze_sellers import SellerBronzeETL
from utils.db_connection import JDBCConnection, DBConfig

spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

source_jdbc = JDBCConnection(
    spark=spark,
    db_conn=DBConfig(
        host="localhost",
        port=5432,
        db="ecommerce",
        user="postgres",
        password="Linh@280405",
        db_type="postgresql"
    )
)

metadata_jdbc = JDBCConnection(
    spark=spark,
    db_conn=DBConfig(
        host="localhost",
        port=5432,
        db="metadata",
        user="postgres",
        password="Linh@280405",
        db_type="postgresql"
    )
)

BRONZE_ETL_CLASSES = [
    CustomerBronzeETL,
    GeolocationBronzeETL,
    OrderItemBronzeETL,
    OrderPaymentBronzeETL,
    OrderReviewBronzeETL,
    ProductBronzeETL,
    SellerBronzeETL,
]

for etl_class in BRONZE_ETL_CLASSES:
    etl = etl_class(spark, source_jdbc, metadata_jdbc)
    etl.run()