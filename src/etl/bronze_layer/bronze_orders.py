from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

# 1. SETUP LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ParquetIngest")

# 2. INITIALIZE SPARK
spark = (SparkSession.builder
    .appName("Orders_Kafka_To_Parquet_Bronze")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate())

# 3. SCHEMA DEFINITION
payload_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("updated_at", TimestampType()),
])

# 4. READ STREAM
logger.info("Reading from Kafka...")
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_topic")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

# 5. TRANSFORMATION
parsed_stream = (raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json("json_str", payload_schema).alias("data"))
    .select("data.*")
    .withColumn("updated_date", F.to_date("updated_at"))
    .withWatermark("updated_at", "10 minutes")
    .dropDuplicates(["order_id", "updated_at"]))

# 6. WRITE STREAM (Modified for Parquet)
logger.info("Writing to Parquet destination...")
query = (parsed_stream.writeStream
    .format("parquet") # Changed from delta to parquet
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoint/bronze/orders")
    .partitionBy("updated_date")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start("hdfs://localhost:9000/datalake/bronze/orders"))

# 7. KEEP ALIVE
query.awaitTermination()