from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "pg-server.public.orders"

# HDFS Paths
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/logs/bronze/orders"
CHECKPOINT_PATH = "hdfs://localhost:9000/checkpoints/orders"


def get_spark_session():
    return SparkSession.builder \
        .appName("Stream_Orders_To_HDFS") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def get_schema():
    """
    Defines the Debezium JSON structure.
    Since you set 'schemas.enable': 'false', the JSON is flat:
    { "before": ..., "after": ..., "op": "c", "ts_ms": ... }
    """
    # 1. Your PostgreSQL 'orders' table columns
    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", LongType(), True),  # Debezium sends dates as epoch
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
        StructField("updated_at", LongType(), True)
    ])

    # 2. The Debezium Envelope
    envelope_schema = StructType([
        StructField("before", order_schema, True),
        StructField("after", order_schema, True),
        StructField("op", StringType(), True),  # Operation: c, u, d
        StructField("ts_ms", LongType(), True)  # Timestamp
    ])

    return envelope_schema


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"--- Listening to Kafka Topic: {TOPIC_NAME} ---")

    # 1. READ STREAM
    # Reads raw binary data from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. PARSE JSON
    # Convert binary 'value' to String -> Parse with Schema
    json_schema = get_schema()

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    )

    # 3. TRANSFORM (Flatten)
    # We extract the 'after' (new) data + metadata
    clean_stream = parsed_stream.select(
        col("data.after.order_id"),
        col("data.after.customer_id"),
        col("data.after.order_status"),
        col("data.after.price"),
        # Metadata is useful for debugging
        col("data.op").alias("operation"),
        col("data.ts_ms").alias("event_timestamp")
    )

    # 4. WRITE STREAM
    # Writes Parquet files to HDFS
    query = clean_stream.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="1 minute") \
        .start()

    print(f"Streaming started... Writing to {HDFS_OUTPUT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()