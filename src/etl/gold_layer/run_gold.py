import sys
sys.path.insert(0, "/home/nql/PycharmProjects/HadoopDataPipeline/src")

from pyspark.sql import SparkSession
from etl.gold_layer.seller_report import SellerReportGoldETL
from etl.gold_layer.customer_report import CustomerReportGoldETL
from utils.db_connection import JDBCConnection, DBConfig

spark = SparkSession.builder.appName("GoldLayer").enableHiveSupport().getOrCreate()

GOLD_ETL_CLASSES = [
    SellerReportGoldETL,
    CustomerReportGoldETL
]

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


for etl_class in GOLD_ETL_CLASSES:
    etl = etl_class(spark, source_jdbc, metadata_jdbc)
    etl.run()