from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from src.etl.bronze_layer.bronze_orders import OrderBronzeETL
from src.etl.bronze_layer.bronze_order_items import OrderItemBronzeETL
from src.etl.bronze_layer.bronze_order_payments import OrderPaymentBronzeETL
from src.etl.bronze_layer.bronze_order_reviews import OrderReviewBronzeETL
from src.utils.base_table import ETLDataset, TableETL


class FactSaleSilverETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = [
                     OrderBronzeETL,
                     OrderItemBronzeETL,
                     OrderReviewBronzeETL,
                     OrderPaymentBronzeETL
                 ],
                 name: str = "fact_sale",
                 primary_keys: List[str] = ["order_id", "order_item_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/silver/fact_sale",
                 data_format: str = "parquet",
                 database: str = "ecommerce",
                 partition_keys: List[str] = ["order_inserted_time"],
                 run_upstream: bool = True,
                 load_data: bool = True
                 ):
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )

    def extract_upstream(self) -> List[ETLDataset]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data
            )
            # Write bronze data into datalake/bronze/...
            if self.run_upstream:
                t1.run()
            # Save written data in DataFrame
            upstream_etl_datasets.append(t1.read(None))
        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        # DataFrame of customers table and geolocation table
        order_data = upstream_datasets[0].curr_data
        order_item_data = upstream_datasets[1].curr_data
        order_review_data = upstream_datasets[2].curr_data
        order_payment_data = upstream_datasets[3].curr_data
        current_timestamp = datetime.now()

        # Get columns with same name in both customer_data and geolocation
        # Rename these columns to avoid conflicts
        order_data = order_data.withColumnRenamed("inserted_time", "order_inserted_time")
        order_item_data = order_item_data.withColumnRenamed("inserted_time", "order_item_inserted_time")
        order_review_data = order_review_data.withColumnRenamed("inserted_time", "order_review_inserted_time")
        order_payment_data = order_payment_data.withColumnRenamed("inserted_time", "order_payment_inserted_time")
        # In my project, there are no columns with same name

        # Join all tables
        fact_sale_data = (
            order_data
            .join(order_item_data, on="order_id", how="left")
            .join(order_review_data, on="order_id", how="left")
            .join(order_payment_data, on="order_id", how="left")
        )

        transformed_data = fact_sale_data.withColumn("total_amount", col("price") + col("freight_value"))

        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )

        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]]) -> ETLDataset:
        partition_filter = ""

        selected_columns = [
            col("order_id"),
            col("order_item_id"),
            col("product_id"),
            col("seller_id"),
            col("customer_id"),
            col("order_purchase_timestamp"),
            col("price"),
            col("total_amount"),
            col("review_score"),
            col("order_inserted_time")
        ]

        data = self.transform_upstream(self.extract_upstream())
        if not self.load_data:
            return ETLDataset(
                name=self.name,
                curr_data=data.curr_data.select(selected_columns),
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        if partition_values:
            partition_filter = " AND ".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(order_inserted_time)")
                .collect()[0][0]
            )
            partition_filter = f"order_inserted_time = '{latest_partition}'"

        order_data = self.spark.read.format(self.data_format).load(self.storage_path).filter(partition_filter)
        order_data = order_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=order_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
        return etl_dataset


