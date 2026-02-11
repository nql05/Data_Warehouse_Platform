import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from typing import Optional

# Load environment variables from .env file
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")
load_dotenv(env_path)

def get_upstream_table(
    table_name: str,
    spark: SparkSession,
    execution_date: Optional[datetime] = None,
    incremental_column: str = "updated_at"
):
    """
    Get upstream table with incremental load support.

    Args:
        table_name: Name of the table to read
        spark: SparkSession
        execution_date: Date to filter data (reads data created/updated on this date)
        incremental_column: Column to use for incremental filtering (created_at or updated_at)

    Returns:
        DataFrame with filtered data
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DATABASE")
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    # If execution_date is provided, do incremental load
    if execution_date:
        # Convert execution_date to string format for SQL
        start_date = execution_date.strftime("%Y-%m-%d 00:00:00")
        end_date = (execution_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        # Build query to filter by date range
        # This captures all records created/updated on the execution date
        query = f"""
        (SELECT * FROM {table_name} 
         WHERE {incremental_column} >= '{start_date}' 
         AND {incremental_column} < '{end_date}') AS filtered_data
        """

        return spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    else:
        # Full load if no date provided
        return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# host = os.getenv("DB_HOST")
# port = os.getenv("DB_PORT")
# db = os.getenv("DATABASE")
# jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
# print(jdbc_url)
