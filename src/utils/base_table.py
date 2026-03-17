from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Type
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class InValidDataException(Exception):
    pass

class DataQualityException(Exception):
    pass

@dataclass
class ETLDataset:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    data_format: str
    database: str
    partition_keys: List[str]
    records_pulled: Optional[int] = None

class TableETL(ABC):
    def __init__(
        self,
        spark,
        upstream_table_names: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str]
    ) -> None:
        self.spark = spark
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys

    @property
    def metadata_conn(self):
        """
        Subclasses that want audit logging must override this property
        and return a MetadataConnection instance.
        """
        return None

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataset]:
        pass

    @abstractmethod
    def transform_upstream(
        self, upstream_datasets: List[ETLDataset]
    ) -> ETLDataset:
        pass

    def validate(self, data: ETLDataset) -> bool:
        return True

    def load(self, data: ETLDataset) -> None:
        (
            data.curr_data \
            .write \
            .format(data.data_format) \
            .mode("overwrite") \
            .partitionBy(data.partition_keys) \
            .save(data.storage_path)
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

    def run(self):
        """
        Tracks run_date, start_time, end_time and records_pulled in pipeline_logs
        """
        log_id = None

        if self.metadata_conn is not None:
            log_id = self.metadata_conn.start_pipeline_log(
                job_name=self.name,
                run_date=date.today()
            )
        try:
            transformed_data = self.transform_upstream(self.extract_upstream())
            if not self.validate(transformed_data):
                raise InValidDataException(
                    f"The {self.name} dataset did not pass validation, please check the metadata db for more information"
                )
            self.load(transformed_data)

            # Finish audit log on success
            if self.metadata_conn is not None and log_id is not None:
                records = transformed_data.records_pulled or 0
                self.metadata_conn.finish_pipeline_log(
                    log_id=log_id,
                    records_pulled=records
                )

        except Exception as e:
            # Finish audit log on failure with -1
            if self.metadata_conn is not None and log_id is not None:
                self.metadata_conn.finish_pipeline_log(
                    log_id=log_id,
                    records_pulled=-1
                )
            raise e

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]]) -> ETLDataset:
        pass