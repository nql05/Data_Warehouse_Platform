import findspark
findspark.init("/home/sparkusr/spark-3.5.7")
from pyspark.sql import SparkSession
from etl.gold_layer.customer_report import CustomerReportGoldETL
from etl.gold_layer.seller_report import SellerReportGoldETL


def run_code(spark):
    customer_report_data = CustomerReportGoldETL(spark=spark)
    customer_report_data.run()

    seller_report_data = SellerReportGoldETL(spark=spark)
    seller_report_data.run()

if __name__ == "__main__":
    # Create a Spark Session
    spark = (SparkSession.builder. \
            appName("Ecommerce Data Pipeline") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)