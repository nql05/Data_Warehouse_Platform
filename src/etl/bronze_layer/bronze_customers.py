from datetime import datetime
from typing import Dict, List, Optional, Type
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.utils.base_table import ETLDataset, TableETL
from src.utils.db_connection import get_upstream_table

class CustomerBronzeETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "customers",
                 primary_keys: List[str] = ["customer_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/bronze/customers",
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
        """
        Extract customer data from ecommerce database and load it into a DataFrame
        :return:
        """
        table_name = "customers"
        customer_data = get_upstream_table(table_name, self.spark)

        # Create ETLDataset instance
        customer_dataset = ETLDataset(
            name=self.name,
            curr_data=customer_data, # DataFrame
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )

        return [customer_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        user_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Perform any necessary transformations on the user data
        transformed_data = user_data.withColumn("inserted_time", lit(current_timestamp))

        # Create a new ETLDataSet instance with the transformed data
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
        """
        Retrives customer data from HDFS storage based on partition filtering
        If 'load_data' is False, returns an ETLDataset with the current in-memory data without reading from storage
        If 'partition_values' is provided, it will build a filter string from the dictionary
        If no 'partition_values', find the latest 'inserted_time' partition and uses as the filter
        Use this when we want to load data as input for silver layer
        :param partition_values:
        :return:
        """
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
                self.spark.read.format(self.data_format).load(self.storage_path).selectExpr("max(inserted_time)").collect()[0][0]
            )
            partition_filter = f"inserted_time = '{latest_partition}'"

        # Read the user data from the HDFS
        user_data = (self.spark.read.format(self.data_format)
                     .load(self.storage_path)
                     .filter(partition_filter))

        user_data = user_data.select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_city"),
            col("customer_state"),
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




