#!/bin/bash

#---CONFIGURATION---
MASTER_URL="spark://10.35.232.232:7077"
DEPLOY_MODE="client"

#Resource Allocation
EXEC_MEM="2G"
EXEC_CORES="4"
TOTAL_CORES="8"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "Starting $APP_NAME at $TIMESTAMP..."

# --- THE SPARK SUBMIT ---
spark-submit \
  --master $MASTER_URL \
  --deploy-mode $DEPLOY_MODE \
  --executor-memory $EXEC_MEM \
  --executor-cores $EXEC_CORES \
  --total-executor-cores $TOTAL_CORES \
  --conf "spark.sql.shuffle.partitions=5" \
  --packages "org.postgresql:postgresql:42.7.3" \
  --conf "spark.sql.warehouse.dir=hdfs://localhost:9000/warehouse" \
  --conf "spark.sql.catalogImplementation=hive" \
  --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
  src/etl/bronze_layer/run_bronze.py
