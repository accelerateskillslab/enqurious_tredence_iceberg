CREATE TABLE practice.orders_iceberg_partitionby_month
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-<aws account id>/iceberg/orders_iceberg_partitionby_month',
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