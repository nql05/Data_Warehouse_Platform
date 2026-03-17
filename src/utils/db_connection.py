import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

# Use a structured logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataPlatform.Ingestion")

JDBC_DRIVERS = {
    "postgresql": ("jdbc:postgresql", "org.postgresql.Driver"),
    "sqlserver":  ("jdbc:sqlserver",  "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    "mysql":      ("jdbc:mysql",      "com.mysql.cj.jdbc.Driver"),
}

@dataclass(frozen=True)
class DBConfig:
    db: str
    user: str
    password: str
    host: str
    port: str
    db_type: str

class DBConnection(ABC):
    @abstractmethod
    def read_full_table(self, table_name: str):
        pass

    @abstractmethod
    def read_incremental_table(self, table_name: str, incremental_column: str, last_value: str):
        pass

class JDBCConnection(DBConnection):
    def __init__(self, spark, db_conn: DBConfig):
        self.spark = spark
        self.db_conn = db_conn
    def read_full_table(self, table_name: str, partition_column: str = None, num_partitions: int = 10):
        logger.info(f"[Full_load] Reading full table: {table_name}")
        try:
            prefix, driver = JDBC_DRIVERS[self.db_conn.db_type]
            jdbc_url = f"{prefix}://{self.db_conn.host}:{self.db_conn.port}/{self.db_conn.db}"
            reader = (
                self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.db_conn.user)
                .option("password", self.db_conn.password)
                .option("driver", driver)
                .option("fetchsize", 10000)
            )

            if partition_column:
                bounds_query = (
                    f"(SELECT MIN({partition_column}), MAX({partition_column}) "
                    f"FROM {table_name}) AS bounds"
                )
                bounds_row = (
                    self.spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", bounds_query)
                    .option("user", self.db_conn.user)
                    .option("password", self.db_conn.password)
                    .option("driver", driver)
                    .load()
                    .collect()[0]
                )
                lower, upper = bounds_row[0], bounds_row[1]
                if lower is not None and upper is not None:
                    logger.info(
                        f"[Full_load] Partitioning '{table_name}' on '{partition_column}' "
                        f"[{lower}, {upper}] with {num_partitions} partitions"
                    )
                    reader = (
                        reader
                        .option("partitionColumn", partition_column)
                        .option("lowerBound", str(lower))
                        .option("upperBound", str(upper))
                        .option("numPartitions", num_partitions)
                    )
                else:
                    logger.warning(
                        f"[Full_load] No rows found for partition bounds on '{table_name}'. "
                        "Falling back to single-partition read."
                    )

            return reader.load()
        except Exception as e:
            logger.error(f"[Full_load] Failed on '{table_name}': {e}")
            raise

    def read_incremental_table(
        self,
        table_name: str,
        last_updated: str,
        last_run: str,
        partition_column: str = None,
        num_partitions: int = 10,
    ):
        logger.info(f"[IncrementalLoad] Reading '{table_name}' where {last_updated} > '{last_run}'")
        try:
            prefix, driver = JDBC_DRIVERS[self.db_conn.db_type]
            jdbc_url = f"{prefix}://{self.db_conn.host}:{self.db_conn.port}/{self.db_conn.db}"

            # Push the WHERE predicate down to the database so only the
            # incremental slice is transferred over the network.
            predicate_query = (
                f"(SELECT * FROM {table_name} "
                f"WHERE {last_updated} > '{last_run}') AS incremental"
            )

            reader = (
                self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", predicate_query)
                .option("user", self.db_conn.user)
                .option("password", self.db_conn.password)
                .option("driver", driver)
                .option("fetchsize", 10000)          # fetch 10 k rows per round-trip
            )

            if partition_column:
                # Fetch min/max only on the already-filtered slice so the
                # bounds are tight and partitions are evenly sized.
                bounds_query = (
                    f"(SELECT MIN({partition_column}), MAX({partition_column}) "
                    f"FROM {table_name} "
                    f"WHERE {last_updated} > '{last_run}') AS bounds"
                )
                bounds_row = (
                    self.spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", bounds_query)
                    .option("user", self.db_conn.user)
                    .option("password", self.db_conn.password)
                    .option("driver", driver)
                    .load()
                    .collect()[0]
                )
                lower, upper = bounds_row[0], bounds_row[1]

                if lower is not None and upper is not None:
                    logger.info(
                        f"[IncrementalLoad] Partitioning '{table_name}' on '{partition_column}' "
                        f"[{lower}, {upper}] with {num_partitions} partitions"
                    )
                    reader = (
                        reader
                        .option("partitionColumn", partition_column)
                        .option("lowerBound", str(lower))
                        .option("upperBound", str(upper))
                        .option("numPartitions", num_partitions)
                    )
                else:
                    logger.warning(
                        f"[IncrementalLoad] No rows found for partition bounds on '{table_name}'. "
                        "Falling back to single-partition read."
                    )

            return reader.load()
        except Exception as e:
            logger.error(f"[IncrementalLoad] Failed on '{table_name}': {e}")
            raise



