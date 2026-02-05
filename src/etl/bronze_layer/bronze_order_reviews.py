from datetime import datetime
from typing import Dict, List, Optional, Type
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.utils.base_table import ETLDataset, TableETL
from src.utils.db_connection import get_upstream_table


class OrderReviewBronzeETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "order_reviews",
                 primary_keys: List[str] = ["order_id", "review_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/bronze/order_reviews",
                 data_format: str = "parquet",
                 database: str = "ecommerce",
                 partition_keys: List[str] = ["inserted_time"],
                 run_upstream: bool = True,
                 load_data: bool = True
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
            load_data
        )

    def extract_upstream(self) -> List[ETLDataset]:
        table_name = "order_reviews"
        order_data = get_upstream_table(table_name, self.spark)

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
        current_timestamp = datetime.now()

        transformed_data = user_data.withColumn("inserted_time", lit(current_timestamp))

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
        data = self.transform_upstream(self.extract_upstream())
        if not self.load_data:
            return ETLDataset(
                name=self.name,
                curr_data=data.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys
            )
        if partition_values:
            partition_filter = " AND ".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.read.format(self.data_format).load(self.storage_path).selectExpr(
                    "max(inserted_time)").collect()[0][0]
            )
            partition_filter = f"inserted_time = '{latest_partition}'"

        # Read the user data from the HDFS
        user_data = self.spark.read.format(self.data_format).load(self.storage_path).filter(partition_filter)

        user_data = user_data.select(
            col("review_id"),
            col("order_id"),
            col("review_score"),
            col("review_comment_title"),
            col("review_comment_message"),
            col("review_creation_date"),
            col("review_answer_timestamp"),
            col("inserted_time")
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