from typing import Dict, List, Optional, Type
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.base_table import ETLDataset, TableETL
from utils.db_connection import get_upstream_table


class OrderItemBronzeETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "order_items",
                 primary_keys: List[str] = ["order_id", "order_item_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/bronze/order_items",
                 data_format: str = "parquet",
                 database: str = "ecommerce",
                 partition_keys: List[str] = ["year", "month", "day"],
                 run_upstream: bool = True,
                 load_data: bool = True,
                 date_params: Optional[Dict[str, str]] = None
                 ) -> None:
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
            date_params
        )

    def extract_upstream(self) -> List[ETLDataset]:
        table_name = "order_items"

        # Pass execution_date for incremental load
        # This will only fetch records where created_at matches the execution date
        order_data = get_upstream_table(
            table_name,
            self.spark,
            execution_date=self.execution_date,
            incremental_column="created_at"  # or "updated_at" depending on your needs
        )

        # Create ETLDataset instance
        order_dataset = ETLDataset(
            name=self.name,
            curr_data=order_data,  # DataFrame
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )

        return [order_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        user_data = upstream_datasets[0].curr_data

        # Add partition columns (year, month, day) using the base class method
        transformed_data = self.add_partition_columns(user_data)

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

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataset:
        """
        Read data from HDFS with optional partition filtering.

        Args:
            partition_values: Dict with partition keys like {"year": "2026", "month": "2", "day": "11"}
                             If None, will use date_params if available
        """
        # If no partition_values provided, use date_params
        if partition_values is None and self.date_params:
            partition_values = {
                "year": str(self.date_params.get("year")),
                "month": str(self.date_params.get("month")),
                "day": str(self.date_params.get("day"))
            }

        # If load_data is False, return transformed data without reading from HDFS
        if not self.load_data:
            data = self.transform_upstream(self.extract_upstream())
            return ETLDataset(
                name=self.name,
                curr_data=data.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys
            )

        # Read the user data from the HDFS
        user_data = self.spark.read.format(self.data_format).load(self.storage_path)

        # Apply partition filter if provided
        if partition_values:
            for key, value in partition_values.items():
                if value is not None:
                    user_data = user_data.filter(col(key) == value)

        user_data = user_data.select(
            col("order_id"),
            col("order_item_id"),
            col("product_id"),
            col("seller_id"),
            col("shipping_limit_date"),
            col("price"),
            col("freight_value"),
            col("year"),
            col("month"),
            col("day")
        )

        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=user_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )
        return etl_dataset