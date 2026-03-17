from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.base_table import ETLDataset, TableETL
from utils.db_connection import JDBCConnection
from utils.metadata_connection import MetadataConnection
from etl.bronze_layer.bronze_orders import OrderBronzeETL
from etl.bronze_layer.bronze_order_items import OrderItemBronzeETL
from etl.bronze_layer.bronze_order_payments import OrderPaymentBronzeETL
from etl.bronze_layer.bronze_order_reviews import OrderReviewBronzeETL


class FactSaleSilverETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 jdbc_conn: JDBCConnection,
                 metadata_jdbc_conn: JDBCConnection,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "fact_sale",
                 primary_keys: List[str] = ["order_id", "order_item_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/silver/fact_sale",
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
        order_etl = OrderBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        order_item_etl = OrderItemBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        order_review_etl = OrderReviewBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        order_payment_etl = OrderPaymentBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        return [
            order_etl.read(partition_values=None),
            order_item_etl.read(partition_values=None),
            order_review_etl.read(partition_values=None),
            order_payment_etl.read(partition_values=None),
        ]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        order_data = upstream_datasets[0].curr_data
        order_item_data = upstream_datasets[1].curr_data
        order_review_data = upstream_datasets[2].curr_data
        order_payment_data = upstream_datasets[3].curr_data

        # Pre-select only needed columns to avoid ambiguity on metadata columns
        order_item_data = order_item_data.select(
            F.col("order_id"),
            F.col("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.col("price"),
            F.col("freight_value"),
        )

        order_review_data = order_review_data.select(
            F.col("order_id"),
            F.col("review_id"),
            F.col("review_score"),
            F.col("review_comment_title"),
            F.col("review_comment_message"),
            F.col("review_creation_date"),
            F.col("review_answer_timestamp"),
        )

        order_payment_data = order_payment_data.select(
            F.col("order_id"),
            F.col("payment_sequential"),
            F.col("payment_type"),
            F.col("payment_installments"),
            F.col("payment_value"),
        )

        fact_sale_data = (
            order_data
            .join(order_item_data, on="order_id", how="left")
            .join(order_review_data, on="order_id", how="left")
            .join(order_payment_data, on="order_id", how="left")
            .select(
                F.col("order_id"),
                F.col("order_item_id"),
                F.col("product_id"),
                F.col("seller_id"),
                order_data["customer_id"],
                order_data["order_status"],
                order_data["order_purchase_timestamp"],
                order_data["order_approved_at"],
                order_data["order_delivered_carrier_date"],
                order_data["order_delivered_customer_date"],
                order_data["order_estimated_delivery_date"],
                F.col("price"),
                F.col("freight_value"),
                (F.col("price") + F.col("freight_value")).alias("total_amount"),
                F.col("review_id"),
                F.col("review_score"),
                F.col("review_comment_title"),
                F.col("review_comment_message"),
                F.col("review_creation_date"),
                F.col("review_answer_timestamp"),
                F.col("payment_sequential"),
                F.col("payment_type"),
                F.col("payment_installments"),
                F.col("payment_value"),
                order_data["updated_date"].alias("updated_date")
            )
        )

        return ETLDataset(
            name=self.name,
            curr_data=fact_sale_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
            records_pulled=fact_sale_data.count()
        )

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataset:
        if partition_values:
            partition_filter = " AND ".join(
                f"{k} = '{v}'" for k, v in partition_values.items()
            )
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

        fact_sale_data = raw_data.select(
            F.col("order_id"),
            F.col("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.col("customer_id"),
            F.col("order_status"),
            F.col("order_purchase_timestamp"),
            F.col("order_approved_at"),
            F.col("order_delivered_carrier_date"),
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date"),
            F.col("price"),
            F.col("freight_value"),
            F.col("total_amount"),
            F.col("review_id"),
            F.col("review_score"),
            F.col("review_comment_title"),
            F.col("review_comment_message"),
            F.col("payment_sequential"),
            F.col("payment_type"),
            F.col("payment_installments"),
            F.col("payment_value"),
            F.col("updated_date")
        )

        return ETLDataset(
            name=self.name,
            curr_data=fact_sale_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )