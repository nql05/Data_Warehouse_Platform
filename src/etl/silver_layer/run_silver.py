import sys
sys.path.insert(0, "/home/nql/PycharmProjects/HadoopDataPipeline/src")

from pyspark.sql import SparkSession
from etl.silver_layer.dim_seller import DimSellerSilverETL
from etl.silver_layer.dim_customer import DimCustomerSilverETL
from etl.silver_layer.fact_sale import FactSaleSilverETL
from utils.db_connection import JDBCConnection, DBConfig

spark = SparkSession.builder.appName("SilverLayer").enableHiveSupport().getOrCreate()

SILVER_ETL_CLASSES = [
    DimSellerSilverETL,
    DimCustomerSilverETL,
    FactSaleSilverETL
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


for etl_class in SILVER_ETL_CLASSES:
    etl = etl_class(spark, source_jdbc, metadata_jdbc)
    etl.run()