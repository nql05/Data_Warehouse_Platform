from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Optional
from pyspark.sql import DataFrame
from utils.db_connection import JDBCConnection, logger
import psycopg2

# ------------------------------------------------------------------ #
#  Typed dataclasses matching the actual DB schemas                   #
# ------------------------------------------------------------------ #

@dataclass(frozen=True)
class SourceMetadata:
    src_id: int
    src_typ: str                      # e.g. 'POSTGRES', 'MYSQL', 'SQL_SERVER'
    src_ip: str
    src_dbname: Optional[str]
    src_schema_name: Optional[str]
    src_tbname: Optional[str]
    pull_typ: str                        # 'FULL' or 'INCREMENTAL'

@dataclass(frozen=True)
class DestinationMetadata:
    dest_id: int
    dest_typ: str                 # e.g. 'HDFS', 'S3', 'BIGQUERY'
    dest_ip: Optional[str]
    dest_dbname: Optional[str]
    dest_schema_name: Optional[str]
    dest_tbname: Optional[str]

@dataclass(frozen=True)
class PipelineLog:
    log_id: int
    job_name: str
    run_date: str
    start_time: str
    end_time: Optional[str]
    records_pulled: int


# ------------------------------------------------------------------ #
#  MetadataConnection                                                 #
# ------------------------------------------------------------------ #

class MetadataConnection:
    """
    Wraps a JDBCConnection dedicated to the metadata PostgreSQL database.
    Provides typed accessors for source_metadata, destination_metadata, pipeline_logs.
    """

    SOURCE_METADATA_TABLE      = "source_metadata"
    DESTINATION_METADATA_TABLE = "destination_metadata"
    PIPELINE_LOGS_TABLE        = "pipeline_logs"

    def __init__(self, jdbc_conn: JDBCConnection):
        self.jdbc_conn = jdbc_conn

    def _get_psycopg2_conn(self):
        """Opens a raw psycopg2 connection using the JDBCConnection config."""
        return psycopg2.connect(
            host=self.jdbc_conn.db_conn.host,
            port=self.jdbc_conn.db_conn.port,
            dbname=self.jdbc_conn.db_conn.db,
            user=self.jdbc_conn.db_conn.user,
            password=self.jdbc_conn.db_conn.password
        )
    # ------------------------------------------------------------------ #
    #  Raw DataFrame accessors                                            #
    # ------------------------------------------------------------------ #

    def read_source_metadata(self) -> DataFrame:
        logger.info("[MetadataConnection] Reading source_metadata")
        return self.jdbc_conn.read_full_table(
            self.SOURCE_METADATA_TABLE,
            partition_column="src_id",
            num_partitions=8
        )

    def read_destination_metadata(self) -> DataFrame:
        logger.info("[MetadataConnection] Reading destination_metadata")
        return self.jdbc_conn.read_full_table(
            self.DESTINATION_METADATA_TABLE,
            partition_column="dest_id",
            num_partitions=16
        )

    def read_pipeline_logs(self) -> DataFrame:
        logger.info("[MetadataConnection] Reading pipeline_logs")
        return self.jdbc_conn.read_full_table(
            self.PIPELINE_LOGS_TABLE,
            partition_column="log_id",
            num_partitions=16
        )

    # ------------------------------------------------------------------ #
    #  Typed row accessors                                                #
    # ------------------------------------------------------------------ #

    def get_source_metadata(self, source_table_name: str) -> SourceMetadata:
        """Fetches the SourceMetadata row for a specific source_table_name.
        Raises ValueError if not found.
        """
        df = self.read_source_metadata()
        rows = df.filter(df["src_tbname"] == source_table_name).collect()

        if not rows:
            raise ValueError(f"[MetadataConnection] No source_metadata found for table: '{source_table_name}'")

        row = rows[0]
        return SourceMetadata(
            src_id=row["src_id"],
            src_typ=row["src_typ"],
            src_ip=row["src_ip"],
            src_dbname=row["src_dbname"],
            src_schema_name=row["src_schema_name"],
            src_tbname=row["src_tbname"],
            pull_typ=row["pull_typ"],
        )

    def get_destination_metadata(self, destination_name: str) -> DestinationMetadata:
        """
        Fetches the DestinationMetadata row for a specific destination_name.
        Raises ValueError if not found.
        """
        df = self.read_destination_metadata()
        rows = df.filter(df["destination_name"] == destination_name).collect()

        if not rows:
            raise ValueError(f"[MetadataConnection] No destination_metadata found for: '{destination_name}'")

        row = rows[0]
        return DestinationMetadata(
            destination_id=row["destination_id"],
            destination_name=row["destination_name"],
            destination_type=row["destination_type"],
            destination_ip=row["destination_ip"],
            destination_database_name=row["destination_database_name"],
            destination_schema_name=row["destination_schema_name"],
            destination_table_name=row["destination_table_name"],
        )

    def get_pipeline_logs(self, job_name: str) -> list[PipelineLog]:
        """
        Fetches all pipeline_logs rows for a specific job_name.
        Returns an empty list if none found.
        """
        df = self.read_pipeline_logs()
        rows = df.filter(df["job_name"] == job_name).collect()

        return [
            PipelineLog(
                log_id=row["log_id"],
                job_name=row["job_name"],
                run_date=str(row["run_date"]),
                start_time=str(row["start_time"]),
                end_time=str(row["end_time"]) if row["end_time"] else None,
                records_pulled=row["records_pulled"],
            )
            for row in rows
        ]

    # ------------------------------------------------------------------ #
    #  Pipeline log write methods                                        #
    # ------------------------------------------------------------------ #
    def start_pipeline_log(self, job_name: str, run_date: date) -> int:
        """
        INSERTs a new row into pipeline_logs with start_time = NOW().
        Returns the generated log_id to be used later in finish_pipeline_log.
        """
        sql = """
            INSERT INTO pipeline_logs (job_name, run_date, start_time)
            VALUES (%s, %s, NOW())
            RETURNING log_id;
        """
        with self._get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (job_name, run_date))
                log_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"[MetadataConnection] Started pipeline log: log_id={log_id}, job={job_name}, run_date={run_date}")
        return log_id

    def finish_pipeline_log(self, log_id: int, records_pulled: int) -> None:
        """
        UPDATEs the pipeline_logs row identified by log_id.
        Sets end_time = NOW() and writes the final records_pulled count.
        """
        sql = """
            UPDATE pipeline_logs
            SET end_time = NOW(),
                records_pulled = %s
            WHERE log_id = %s;
        """
        with self._get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (records_pulled, log_id))
            conn.commit()

        logger.info(f"[MetadataConnection] Finished pipeline log: log_id={log_id}, records_pulled={records_pulled}")

    def get_last_successful_run(self, job_name: str) -> Optional[str]:
        """
        Returns the last successful run_date for a given job as a string 'YYYY-MM-DD',
        or None if no successful run exists yet.
        """
        query = """
            SELECT MAX(run_date)
            FROM pipeline_logs
            WHERE job_name = %s
              AND records_pulled >= 0
              AND end_time IS NOT NULL
        """
        with self._get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (job_name,))
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])  # returns 'YYYY-MM-DD'
                return None

