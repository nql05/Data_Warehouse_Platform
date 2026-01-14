import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

# --- CONFIGURATION ---
PROJECT_PATH = "/home/nql/PycharmProjects/HadoopDataPipeline"

# !!! CRITICAL CHANGE !!!
# 1. Define where your local Spark is installed
# (Replace this path with the output of 'echo $SPARK_HOME' from your terminal)
SPARK_HOME = "/home/sparkusr/spark-3.5.7"

# 2. We use 'spark-submit' instead of 'python'
# This tool automatically finds pyspark, java, and hadoop configurations.

SPARK_SUBMIT_CMD = f"{SPARK_HOME}/bin/spark-submit --packages org.postgresql:postgresql:42.7.4"
SILVER_SCRIPTS = [
    "process_customers_silver_layer.py",
    "process_geolocation_silver_layer.py",
    "process_order_items_silver_layer.py",
    "process_order_payments_silver_layer.py",
    "process_order_reviews_silver_layer.py",
    "process_orders_silver_layer.py",
    "process_products_silver_layer.py",
    "process_sellers_silver_layer.py"
]

GOLD_DIM_SCRIPTS = ["dimension_customer.py", "dimension_product.py", "dimension_seller.py"]
GOLD_FACT_SCRIPTS = ["fact_sale.py"]

with DAG(
    dag_id="medallion_architecture_spark_submit",
    start_date=datetime(2026, 1, 12),
    schedule=None,
    catchup=False,
    tags=["hadoop", "spark"],
) as dag:

    # 1. BRONZE LAYER
    # Changed command to use SPARK_SUBMIT_CMD
    bronze_task = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/main/bronze_layer/bronze_batch_ingest.py",
    )

    # 2. SILVER LAYER
    with TaskGroup("silver_layer") as silver_group:
        for script in SILVER_SCRIPTS:
            task_id_clean = script.replace(".py", "").replace("_silver_layer", "")
            BashOperator(
                task_id=task_id_clean,
                bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/main/silver_layer/{script}",
            )

    # 3. GOLD LAYER
    with TaskGroup("gold_layer") as gold_group:
        dim_tasks = []
        for script in GOLD_DIM_SCRIPTS:
            task_id_clean = script.replace(".py", "")
            t = BashOperator(
                task_id=task_id_clean,
                bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/main/gold_layer/{script}",
            )
            dim_tasks.append(t)

        for script in GOLD_FACT_SCRIPTS:
            task_id_clean = script.replace(".py", "")
            fact_task = BashOperator(
                task_id=task_id_clean,
                bash_command=f"{SPARK_SUBMIT_CMD} {PROJECT_PATH}/main/gold_layer/{script}",
            )
            dim_tasks >> fact_task

    bronze_task >> silver_group >> gold_group