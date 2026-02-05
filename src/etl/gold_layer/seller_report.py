from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.etl.silver_layer.dim_seller import SellerBronzeETL, DimSellerSilverETL
from src.etl.silver_layer.fact_sale import FactSaleSilverETL
from src.utils.base_table import ETLDataset, TableETL

class SellerReportGoldETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 upstream_table_names: Optional[List[Type[TableETL]]] = [
                     DimSellerSilverETL,
                     FactSaleSilverETL
                 ],
                 name: str = "seller_report",
                 primary_keys: List[str] = ["seller_id"],
                 storage_path: str = "hdfs://localhost:9000/warehouse/seller_report",
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

    def load(self, data: ETLDataset):
        table_name = f"{self.database}.{self.name}"
        data.curr_data.write.mode("overwrite").format("parquet").partitionBy(self.partition_keys).saveAsTable(table_name)

    def extract_upstream(self) -> List[ETLDataset]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read(None))

        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        # DataFrame of customers table and geolocation table
        dim_seller_data = upstream_datasets[0].curr_data
        fact_sale_data = upstream_datasets[1].curr_data

        # Get columns with same name in both customer_data and geolocation
        # Rename these columns to avoid conflicts
        dim_seller_data = dim_seller_data.withColumnRenamed("seller_id", "dim_seller_id")

        # Perform the join based on foreign keys
        customer_report_data = fact_sale_data.join(dim_seller_data,
                                               fact_sale_data["seller_id"] == dim_seller_data[
                                                   "dim_seller_id"],
                                               "left")

        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=customer_report_data,
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
            col("price"),
            col("total_amount"),
            col("review_score"),
            col("seller_id"),
            col("seller_city"),
            col("seller_state"),
            col("geolocation_lat"),
            col("geolocation_lng"),
            col("order_inserted_time")
        ]

        data = self.transform_upstream(self.extract_upstream())

        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=data.curr_data.select(selected_columns),
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        # Read from Hive table instead of path
        table_name = f"{self.database}.{self.name}"

        if partition_values:
            partition_filter = " AND ".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.sql(f"SELECT max(order_inserted_time) FROM {table_name}")
                .collect()[0][0]
            )
            partition_filter = f"order_inserted_time = '{latest_partition}'"

        seller_report_data = self.spark.sql(
            f"SELECT * FROM {table_name} WHERE {partition_filter}"
        ).select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=seller_report_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
        return etl_dataset