# Ecommerce Data Warehouse Platform

## 🚀 Project Overview
Engineered a scalable data platform using Spark and Hadoop to solve critical production latency issues. By implementing advanced Spark optimizations, I reduced pipeline execution time.

Also, I architected the end-to-end flow from on-prem ingestion to BigQuery, utilizing Airflow for orchestration and Apache Superset for real-time business intelligence

## 🏗️ Data Warehouse Architecture

<div style="text-align:center;">
  <img src="images/ecommerce_data_warehouse.drawio.png" alt="PostgreSQL Schema" style="display:block;margin:0 auto;max-width:80%;width:700px;height:auto;" />
</div>

## 🛠️ Engineering Solutions
### 1. Ingestion
* **Incremental Load**: I use a timestamp column named **updated_date** to track newly inserted or modified records since the next run.
  * This strategy cannot detect deleted rows. Therefore, I use **CDC** to track deleted rows from my data source.
* **Change Data Capture (CDC)**: I use **Debezium** with **Kafka connect** to capture row-level changes from Postgres Database, which is my data source.
### 2. Transformation
In transformation phase, I use Spark Optimization techniques to optimize my data pipelines. There are some problems that i have handled.
* **Data Skew**: In my project, when i ingest data from data source into bronze layer, i have observed that there was an executor which processes almost the data so i have configured **AQE** to solve my problem
```python
    --conf spark.sql.adaptive.enabled=true
    --conf spark.sql.adaptive.skewJoin.enabled=true
```
* **Partitioning:**
    * By default, Spark uses 128 MB for partition size. But my data is about 100 - 500 MB and i have 16 cores to process so that i should spit the data into 16 cores to process to increase parallelism
```python
    --conf spark.sql.files.maxPartitionBytes=8388608
```

* **Optimized Shuffle:** I have configured to controls the number of partitions created during **Wide transformations** (Joins, GroupBy, ...) so that it could increase parallelism
```python
    --conf spark.sql.shuffle.partitions=16
```

* **Resource Allocation Strategy**
  * In my cluster, there are 16 cores and 14 GB RAM. I leave out 1 core and 1 GB RAM for **Spark standalone**, 1 core and 1 GB RAM for **Application master**. I have 2 worker nodes and each executors should have 3 - 5 cores.
```python
    --deploy-mode client
    --executor-memory 2G
    --executor-cores 4
    --total-executor-cores 16
```

### 3. Storage
* I use **HDFS** from Apache Hadoop as Data Storage with Medallion Architecture
* I use **Google BigQuery** to query data from Gold Layer for reporting

### 4. Business Intelligence
* I use **Apache Superset** which is connected with **Google BigQuery** to use for reporting
## 📈 Impact
* Accuracy: Eliminated data duplications issues in financial reporting
* Performance: Reduce BigQuery scan costs by pre-filtering and optimizing SQL Lab queries.



