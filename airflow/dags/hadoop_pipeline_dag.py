from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# --- CONFIGURATION ---
PROJECT_PATH = "/home/nql/PycharmProjects/HadoopDataPipeline"
POSTGRES_JAR = "/home/hiveusr/hive-4.0.0/lib/postgresql-42.7.4.jar"
SPARK_SUBMIT = "/home/sparkusr/spark-3.5.7/bin/spark-submit"
SPARK_SUBMIT_CMD = (
    f"{SPARK_SUBMIT} "
    f"--master spark://192.168.1.39:7077 "
    f"--deploy-mode client "
    f"--executor-memory 2G "
    f"--executor-cores 4 "
    f"--total-executor-cores 16 "
    # -- Hive Integration
    # f"--conf spark.sql.catalogImplementation=hive "
    # f"--conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 "
    # f"--conf spark.sql.warehouse.dir=hdfs://localhost:9000/warehouse "
    # -- Partition Behaviour
    f"--conf spark.sql.sources.partitionOverwriteMode=dynamic "
    # -- Performance
    f"--conf spark.sql.files.maxPartitionBytes=8388608 " # 8MB
    f"--conf spark.sql.shuffle.partitions=16 "
    f"--conf spark.sql.adaptive.enabled=true "
    f"--conf spark.sql.adaptive.skewJoin.enabled=true "
    # -- Python
    f"--conf spark.pyspark.python=/home/nql/PycharmProjects/HadoopDataPipeline/.venv/bin/python3.12 "
    f"--conf spark.pyspark.driver.python=/home/nql/PycharmProjects/HadoopDataPipeline/.venv/bin/python3.12 "
    # -- History Server --
    f"--conf spark.eventLog.enabled=true "
    f"--conf spark.eventLog.dir=hdfs://localhost:9000/tmp/spark-history "
    f"--jars {POSTGRES_JAR} "
)

SPARK_ENV = {
    "SPARK_HOME": "/home/sparkusr/spark-3.5.7",
    "JAVA_HOME": "/usr/lib/jvm/java-11-amazon-corretto",
    "PATH": "/home/sparkusr/spark-3.5.7/bin:/usr/lib/jvm/java-11-amazon-corretto/bin:/usr/bin:/bin",
}

with DAG(
    dag_id="Data_Warehouse_Platform",
    start_date=datetime(2026, 3, 5),
    schedule=None,
    catchup=False,
    tags=["medallion", "spark"]
) as dag:

    bronze_layer = BashOperator(
        task_id="submit_bronze_layer",
        bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/src/etl/bronze_layer/run_bronze.py",
        env=SPARK_ENV,
        append_env=True,
    )

    # orders_stream = BashOperator(
    #     task_id="submit_orders_stream",
    #     bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/src/etl/bronze_layer/bronze_orders.py",
    #     env=SPARK_ENV,
    #     append_env=True,
    # )

    silver_layer = BashOperator(
        task_id="submit_silver_layer",
        bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/src/etl/silver_layer/run_silver.py",
        env=SPARK_ENV,
        append_env=True,
    )

    gold_layer = BashOperator(
        task_id="submit_gold_layer",
        bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/src/etl/gold_layer/run_gold.py",
        env=SPARK_ENV,
        append_env=True,
    )

    bronze_layer >> silver_layer >> gold_layer
    # [orders_stream, bronze_layer] >> silver_layer