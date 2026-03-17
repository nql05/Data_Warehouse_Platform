from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.base_table import ETLDataset, TableETL, DataQualityException
from utils.db_connection import JDBCConnection
from utils.metadata_connection import MetadataConnection
from etl.silver_layer.dim_customer import DimCustomerSilverETL
from etl.silver_layer.fact_sale import FactSaleSilverETL


class CustomerReportGoldETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 jdbc_conn: JDBCConnection,
                 metadata_jdbc_conn: JDBCConnection,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "customer_report",
                 primary_keys: List[str] = ["order_id", "order_item_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/gold/customer_report",
                 data_format: str = "parquet",
                 database: str = "ecommerce",
                 partition_keys: List[str] = ["updated_date"]
                 ) -> None:
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys
        )
        self.jdbc_conn = jdbc_conn
        self.metadata_jdbc_conn = MetadataConnection(metadata_jdbc_conn)

    @property
    def metadata_conn(self):
        return self.metadata_jdbc_conn

    def extract_upstream(self) -> List[ETLDataset]:
        dim_customer_etl = DimCustomerSilverETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )
        dim_customer_etl.run()

        fact_sale_etl = FactSaleSilverETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )
        fact_sale_etl.run()

        return [
            dim_customer_etl.read(partition_values=None),
            fact_sale_etl.read(partition_values=None),
        ]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        dim_customer_data = upstream_datasets[0].curr_data
        fact_sale_data = upstream_datasets[1].curr_data

        dim_customer_data = dim_customer_data \
            .withColumnRenamed("customer_id", "dim_customer_id") \
            .withColumnRenamed("updated_date", "dim_updated_date")

        joined_data = fact_sale_data.join(
            dim_customer_data,
            fact_sale_data["customer_id"] == dim_customer_data["dim_customer_id"],
            "left"
        )

        customer_agg = joined_data.groupBy(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            fact_sale_data["updated_date"].alias("updated_date")
        ).agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders"),
            F.avg("review_score").alias("avg_review_score"),
            F.max("order_purchase_timestamp").alias("last_purchase_timestamp")
        )

        customer_report_data = customer_agg.withColumn(
            "avg_order_value",
            F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders")).otherwise(F.lit(0.0))
        )

        return ETLDataset(
            name=self.name,
            curr_data=customer_report_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
            records_pulled=customer_report_data.count()
        )

    def load(self, data: ETLDataset) -> None:
        (
            data.curr_data \
             .write \
             .format(data.data_format) \
             .mode("overwrite") \
             .partitionBy(data.partition_keys) \
             .option("path", data.storage_path) \
             .saveAsTable("report.customer_report")
         )

        # Read back and verify row count matches source
        if data.records_pulled is not None:
            written_count = (
                self.spark.read
                .format(data.data_format)
                .load(data.storage_path)
                .count()
            )
            if written_count != data.records_pulled:
                raise DataQualityException(
                    f"[{data.name}] Row count mismatch: "
                    f"extracted {data.records_pulled} rows but "
                    f"only {written_count} rows written to {data.storage_path}"
                )

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataset:
        if partition_values:
            partition_filter = " AND ".join(f"{k} = '{v}'" for k, v in partition_values.items())
        else:
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(updated_date)")
                .collect()[0][0]
            )
            partition_filter = f"updated_date = '{latest_partition}'"

        raw_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        customer_report_data = raw_data.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "total_orders",
        "canceled_orders",
        "total_revenue",
        "avg_order_value",
        "avg_review_score",
        "last_purchase_timestamp",
        "updated_date"
        )

        return ETLDataset(
            name=self.name,
            curr_data=customer_report_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )