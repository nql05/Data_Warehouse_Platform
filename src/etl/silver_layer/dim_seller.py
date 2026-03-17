from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.base_table import ETLDataset, TableETL
from utils.db_connection import JDBCConnection
from utils.metadata_connection import MetadataConnection
from etl.bronze_layer.bronze_sellers import SellerBronzeETL
from etl.bronze_layer.bronze_geolocation import GeolocationBronzeETL


class DimSellerSilverETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 jdbc_conn: JDBCConnection,
                 metadata_jdbc_conn: JDBCConnection,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "dim_seller",
                 primary_keys: List[str] = ["seller_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/silver/dim_seller",
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
        seller_etl = SellerBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        geolocation_etl = GeolocationBronzeETL(
            spark=self.spark,
            jdbc_conn=self.jdbc_conn,
            metadata_jdbc_conn=self.metadata_jdbc_conn.jdbc_conn
        )

        return [
            seller_etl.read(partition_values=None),
            geolocation_etl.read(partition_values=None)
        ]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        seller_data = upstream_datasets[0].curr_data
        geolocation_data = upstream_datasets[1].curr_data

        geolocation_data = geolocation_data.select(
            F.col("geolocation_zip_code_prefix"),
            F.col("geolocation_lat"),
            F.col("geolocation_lng"),
        )

        dim_seller_data = (
            seller_data.join(
                geolocation_data,
                seller_data["seller_zip_code_prefix"] == geolocation_data["geolocation_zip_code_prefix"],
                "inner"
            )
            .select(
                F.col("seller_id"),
                F.col("seller_zip_code_prefix"),
                F.col("seller_city"),
                F.col("seller_state"),
                F.col("geolocation_lat"),
                F.col("geolocation_lng"),
                seller_data["updated_date"].alias("updated_date")
            )
        )

        return ETLDataset(
            name=self.name,
            curr_data=dim_seller_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
            records_pulled=dim_seller_data.count()
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

        dim_seller_data = raw_data.select(
            F.col("seller_id"),
            F.col("seller_zip_code_prefix"),
            F.col("seller_city"),
            F.col("seller_state"),
            F.col("geolocation_lat"),
            F.col("geolocation_lng"),
            F.col("updated_date")
        )

        return ETLDataset(
            name=self.name,
            curr_data=dim_seller_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )