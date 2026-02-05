import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from .env file
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")
load_dotenv(env_path)
def get_upstream_table(table_name: str, spark: SparkSession):
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DATABASE")
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# host = os.getenv("DB_HOST")
# port = os.getenv("DB_PORT")
# db = os.getenv("DATABASE")
# jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
# print(jdbc_url)
