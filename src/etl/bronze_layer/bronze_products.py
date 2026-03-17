from typing import Dict, List, Optional, Type
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.base_table import ETLDataset, TableETL, DataQualityException
from utils.db_connection import JDBCConnection
from utils.metadata_connection import MetadataConnection


class ProductBronzeETL(TableETL):
    def __init__(self,
                 spark: SparkSession,
                 jdbc_conn: JDBCConnection,
                 metadata_jdbc_conn: JDBCConnection,
                 upstream_table_names: Optional[List[Type[TableETL]]] = None,
                 name: str = "products",
                 primary_keys: List[str] = ["product_id"],
                 storage_path: str = "hdfs://localhost:9000/datalake/bronze/products",
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
        table_name = "products"

        src_metadata = self.metadata_conn.get_source_metadata(table_name)
        pull_typ = src_metadata.pull_typ.upper()

        qualified_table = (
            f"{src_metadata.src_schema_name}.{src_metadata.src_tbname}"
            if src_metadata.src_schema_name
            else src_metadata.src_tbname
        )

        if pull_typ == "FULL":
            products_data = self.jdbc_conn.read_full_table(
                table_name=qualified_table,
                partition_column="updated_at",
                num_partitions=16
            )
        elif pull_typ == "INCREMENTAL":
            last_run = self.metadata_jdbc_conn.get_last_successful_run(self.name)
            products_data = self.jdbc_conn.read_incremental_table(
                table_name=qualified_table,
                last_updated="updated_at",
                last_run=last_run,
                partition_column=self.partition_keys[0],
                num_partitions=16
            )
        else:
            raise ValueError(
                f"[ProductBronzeETL] Unknown pull_type '{pull_typ}' "
                f"for source table '{table_name}'"
            )

        records_pulled = products_data.count()

        products_dataset = ETLDataset(
            name=self.name,
            curr_data=products_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
            records_pulled=records_pulled
        )

        return [products_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        products_data = upstream_datasets[0].curr_data
        records_pulled = upstream_datasets[0].records_pulled

        transformed_data = (
            products_data
            .withColumn("updated_date", F.to_date(F.col("updated_at")))
        )

        etl_dataset = ETLDataset(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
            records_pulled=records_pulled
        )
        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]]) -> ETLDataset:
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

        products_data = raw_data.select(
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_name_length"),
            F.col("product_description_length"),
            F.col("product_photos_qty"),
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm"),
            F.col("updated_date")
        )

        return ETLDataset(
            name=self.name,
            curr_data=products_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys
        )