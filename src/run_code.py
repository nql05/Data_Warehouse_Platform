import findspark
findspark.init("/home/sparkusr/spark-3.5.7")
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import argparse
# from etl.gold_layer.customer_report import CustomerReportGoldETL
# from etl.gold_layer.seller_report import SellerReportGoldETL
from etl.bronze_layer.bronze_order_items import OrderItemBronzeETL

def parse_args():
    parser = argparse.ArgumentParser(description="Ecommerce Data Warehouse")
    parser.add_argument("--execution-date", type=str, required=False,
                        help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()


def get_execution_date(args):
    """Parse execution date or default to yesterday."""
    if args.execution_date:
        exec_date = datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    else:
        exec_date = (datetime.now() - timedelta(days=1)).date()

    # Split into year, month, day
    return {
        "execution_date": exec_date,
        "year": exec_date.year,
        "month": exec_date.month,
        "day": exec_date.day
    }

def run_code(spark, date_params):
    # customer_report_data = CustomerReportGoldETL(spark=spark, date_params=date_params)
    # customer_report_data.run()
    #
    # seller_report_data = SellerReportGoldETL(spark=spark, date_params=date_params)
    # seller_report_data.run()
    order_table = OrderItemBronzeETL(spark=spark, date_params=date_params)
    order_table.run()
    result = order_table.read()
    result.curr_data.limit(20).show()

if __name__ == "__main__":
    args = parse_args()
    date_params = get_execution_date(args)

    print(f"Running pipeline for: {date_params['execution_date']} "
          f"(year={date_params['year']}, month={date_params['month']}, day={date_params['day']})")

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
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark, date_params)