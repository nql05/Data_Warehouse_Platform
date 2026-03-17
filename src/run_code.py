import findspark
findspark.init("/home/sparkusr/spark-3.5.7")
from pyspark.sql import SparkSession
from etl.gold_layer.customer_report import CustomerReportGoldETL
from etl.gold_layer.seller_report import SellerReportGoldETL
from utils.db_connection import JDBCConnection, DBConfig


def run_code(spark):
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

    # Gold Layer ETL tables (bronze and silver are run automatically upstream)
    gold_tables = [
        CustomerReportGoldETL(
            spark=spark,
            jdbc_conn=source_jdbc,
            metadata_jdbc_conn=metadata_jdbc
        ),
        SellerReportGoldETL(
            spark=spark,
            jdbc_conn=source_jdbc,
            metadata_jdbc_conn=metadata_jdbc
        ),
    ]

    for table in gold_tables:
        print(f"[INFO] Running Gold ETL for: {table.name}")
        table.run()
        print(f"[INFO] Completed Gold ETL for: {table.name}")


if __name__ == "__main__":

    # Create a Spark Session
    spark = (
        SparkSession.builder
        .appName("Ecommerce Data Pipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)

    input("Press Enter to exit and close Spark UI ...")
    spark.stop()