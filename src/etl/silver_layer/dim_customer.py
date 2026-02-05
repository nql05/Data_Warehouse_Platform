from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from src.etl.bronze_layer.bronze_customers import CustomerBronzeETL
from src.etl.bronze_layer.bronze_geolocation import GeolocationBronzeETL
from src.utils.base_table import ETLDataset, TableETL


class DimCustomerSilverETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = [
                     CustomerBronzeETL,
                     GeolocationBronzeETL
                 ],
                 name: str = "dim_customer",
                 primary_keys: List[str] = ["customer_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/silver/dim_customer",
                 data_format: str = "parquet",
                 database: str = "ecommerce",
                 partition_keys: List[str] = ["customer_inserted_time"],
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
        customer_data = upstream_datasets[0].curr_data
        geolocation_data = upstream_datasets[1].curr_data

        # Get columns with same name in both customer_data and geolocation
        # Rename these columns to avoid conflicts
        customer_data = customer_data.withColumnRenamed("inserted_time", "customer_inserted_time")
        geolocation_data = geolocation_data.withColumnRenamed("inserted_time", "geolocation_inserted_time")

        # Perform the join based on foreign keys
        dim_customer_data = customer_data.join(geolocation_data,
                                               customer_data["customer_zip_code_prefix"] == geolocation_data["geolocation_zip_code_prefix"],
                                               "inner")

        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=dim_customer_data,
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
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
            col("customer_zip_code_prefix"),
            col("geolocation_lat"),
            col("geolocation_lng"),
            col("customer_inserted_time"),
            col("geolocation_inserted_time")
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
                .selectExpr("max(customer_inserted_time)")
                .collect()[0][0]
            )
            partition_filter = f"customer_inserted_time = '{latest_partition}'"

        dim_buyer_data = self.spark.read.format(self.data_format).load(self.storage_path).filter(partition_filter)
        dim_buyer_data = dim_buyer_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=dim_buyer_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
        return etl_dataset


