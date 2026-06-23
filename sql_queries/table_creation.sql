CREATE EXTERNAL TABLE practice.orders_raw (
  order_id bigint primary key,
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
LOCATION 's3://iceberg-data-lake-<aws account id>/raw/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);