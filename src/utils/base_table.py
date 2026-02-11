from __future__ import annotations

import os.path
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Type
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class InValidDataException(Exception):
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

class TableETL(ABC):
    @abstractmethod
    def __init__(self,
                 spark,
                 upstream_table_names: Optional[List[Type[TableETL]]],
                 name: str,
                 primary_keys: List[str],
                 storage_path: str,
                 data_format: str,
                 database: str,
                 partition_keys: List[str],
                 run_upstream: bool = True,
                 load_data: bool = True,
                 date_params: Optional[Dict[str, str]] = None
                 ) -> None:

        self.spark = spark
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys
        self.run_upstream = run_upstream
        self.load_data = load_data
        self.date_params = date_params or {}

    @property
    def year(self):
        return self.date_params.get("year", "")

    @property
    def month(self):
        return self.date_params.get("month", "")

    @property
    def day(self):
        return self.date_params.get("day", "")

    @property
    def execution_date(self):
        return self.date_params.get("execution_date")

    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """Add year, month, day partition columns"""
        return (df
                .withColumn("year", F.lit(self.year))
                .withColumn("month", F.lit(self.month))
                .withColumn("day", F.lit(self.day))
                .withColumn("inserted_time", F.lit(self.execution_date))
                )

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataset]:
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        pass

    def validate(self, data: ETLDataset) -> bool:
        return True

    def load(self, data: ETLDataset):
        data.curr_data.write.format(data.data_format).mode("overwrite").partitionBy("year", "month", "day").save(data.storage_path)

    def run(self):
        transformed_data = self.transform_upstream(self.extract_upstream())
        if not self.validate(transformed_data):
            raise InValidDataException(
                f"The {self.name} dataset did not pass validation, please check the metadata db for more information"
            )
        if self.load_data:
            self.load(transformed_data)

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]]) -> ETLDataset:
        pass