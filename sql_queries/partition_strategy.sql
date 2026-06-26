--CREATE 500K RECORDS
CREATE EXTERNAL TABLE IF NOT EXISTS practice.orders_raw_500k (
  order_id bigint,
  customer_id string,
  order_date string,
  product_category string,
  city string,
  order_amount double,
  order_status string,
  payment_status string,
  updated_at string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
LOCATION 's3://iceberg-data-lake-958165011713/raw/500k_records/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

--NO PARTITION
CREATE TABLE practice.orders_500k_iceberg_no_partition
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_500k_iceberg_no_partition',
  format = 'PARQUET'
)
AS
SELECT
  order_id,
  customer_id,
  CAST(order_date AS date) AS order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  CAST(updated_at AS timestamp) AS updated_at
FROM practice.orders_raw;

--PARTITION BY MONTH
CREATE TABLE practice.orders_500k_iceberg_partitionby_month
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_500k_iceberg_partitionby_month',
  format = 'PARQUET',
  partitioning = ARRAY['month(order_date)']
)
AS
SELECT
  order_id,
  customer_id,
  CAST(order_date AS date) AS order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  CAST(updated_at AS timestamp) AS updated_at
FROM practice.orders_raw;

-- RUN THIS, IT WILL FAIL BECAUSE OF ACTIVE PARTITIONS
CREATE TABLE practice.orders_iceberg_partitionby_month_city_customer
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_iceberg_month_city_customer_bucket/',
  format = 'PARQUET',
  partitioning = ARRAY[
    'month(order_date)',
    'city',
    'bucket(customer_id, 10)'
  ]
)
AS
SELECT
 order_id,
  customer_id,
  CAST(order_date AS date) AS order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  CAST(updated_at AS timestamp) AS updated_at
FROM practice.orders_raw_500k;

--BUCKET(N, ID)
CREATE TABLE practice.orders_500k_iceberg_partitionby_customer
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_500k_iceberg_partitionby_customer/',
  format = 'PARQUET',
  partitioning = ARRAY[
    'bucket(customer_id, 10)'
  ]
)
AS
SELECT
 order_id,
  customer_id,
  CAST(order_date AS date) AS order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  CAST(updated_at AS timestamp) AS updated_at
FROM practice.orders_raw_500k;

"""
  PERFORMANCE ANALYSIS (NOTE: DATA SCANNED)
"""
SELECT
  COUNT(*) AS jan_orders,
  SUM(order_amount) AS jan_revenue
FROM practice.orders_500k_iceberg_no_partition
WHERE order_date >= DATE '2026-01-01'
  AND order_date < DATE '2026-02-01';
  
SELECT
  COUNT(*) AS jan_orders,
  SUM(order_amount) AS jan_revenue
FROM practice.orders_500k_iceberg_partitionby_month
WHERE order_date >= DATE '2026-01-01'
  AND order_date < DATE '2026-02-01';
  
 SELECT
  COUNT(*) AS jan_orders,
  SUM(order_amount) AS jan_revenue
FROM practice.orders_500k_iceberg_partitionby_customer
WHERE order_date >= DATE '2026-01-01'
  AND order_date < DATE '2026-02-01'; 


"""
  PARTITION EVOLUTION
"""
ALTER TABLE practice.orders
ADD PARTITION FIELD month(order_date);