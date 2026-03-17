CREATE DATABASE metadata;

CREATE TABLE pipeline_config (
    id SERIAL PRIMARY KEY,

    -- Upstream (Source) Information
    upstream_type VARCHAR(50) NOT NULL,       -- e.g., 'SQL', 'NoSQL', 'API'
    upstream_name VARCHAR(100) NOT NULL,       -- e.g., 'Production_DB' or 'Shopify_API'
    upstream_table_name VARCHAR(150),          -- Table name or Endpoint path
    upstream_ip VARCHAR(45),                   -- Source IP/Host

    -- Downstream (Target) Information
    downstream_type VARCHAR(50) NOT NULL,     -- e.g., 'SQL', 'DataLake'
    downstream_name VARCHAR(100) NOT NULL,     -- e.g., 'Analytics_Warehouse'
    downstream_table_name VARCHAR(150) NOT NULL,
    downstream_ip VARCHAR(45),                 -- Target IP/Host

    -- Control Flag
    -- 0: Full Load (All), 1: Incremental Load, Maybe some other loading strategies
    pull_type SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE job_logs (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(150) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    execution_date TIMESTAMP,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    record_pull_counts INT DEFAULT 0
);