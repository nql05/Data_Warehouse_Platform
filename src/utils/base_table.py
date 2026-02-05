from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Type
from pyspark.sql import DataFrame

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
                 load_data: bool = True
                 ) -> None:
        """

        :param spark:
        :param upstream_table_names:
        :param name:
        :param primary_keys:
        :param storage_path:
        :param data_format:
        :param database:
        :param partition_keys:
        :param run_upstream: Controls the ETL dependency chain execution. True -> run all upstream ETL tables first, False -> Skips upstream execution and assumes data is already available
        :param load_data: Controls whether to persist data to storage. True -> writes transformed data to HDFS and register in Hive, False -> Keeps data in memory only (DataFrame), doesn't save to storage
        """
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

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataset]:
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataset]) -> ETLDataset:
        pass

    def validate(self, data: ETLDataset) -> bool:
        return True

    def load(self, data: ETLDataset):
        data.curr_data.write.format(data.data_format).mode("overwrite").partitionBy(data.partition_keys).save(data.storage_path)

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