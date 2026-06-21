"""
This SQL script creates a new table called `orders_merge_stage` in the `practice` db. 
"""
CREATE TABLE practice.orders_merge_incoming
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/iceberg/orders_merge_incoming/'
)
AS
SELECT *
FROM (
  VALUES
    (
      100002,
      'C0001',
      DATE '2026-02-15',
      'Electronics',
      'Kolkata',
      99999.00,
      'DELIVERED',
      'PAID',
      CAST(current_timestamp AS timestamp)
    ),
    (
      999999,
      'C9999',
      DATE '2026-03-20',
      'Fashion',
      'Mumbai',
      4500.00,
      'PLACED',
      'PAID',
      CAST(current_timestamp AS timestamp)
    )
)
AS t (
  order_id,
  customer_id,
  order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  updated_at
);

"""
    MERGE SQL
"""
MERGE INTO practice.orders_iceberg_partitionby_month target
USING practice.orders_merge_incoming source
ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
  customer_id = source.customer_id,
  order_date = source.order_date,
  product_category = source.product_category,
  city = source.city,
  order_amount = source.order_amount,
  order_status = source.order_status,
  payment_status = source.payment_status,
  updated_at = source.updated_at

WHEN NOT MATCHED THEN INSERT (
  order_id,
  customer_id,
  order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  updated_at
)
VALUES (
  source.order_id,
  source.customer_id,
  source.order_date,
  source.product_category,
  source.city,
  source.order_amount,
  source.order_status,
  source.payment_status,
  source.updated_at
);