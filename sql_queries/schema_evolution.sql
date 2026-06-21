"""
    ALTER DDL
"""
ALTER TABLE practice.orders_iceberg_partitionby_month
ADD COLUMNS (
  delivery_partner string,
  estimated_delivery_minutes int
);

"""
    CREATE TABLE HAVING NEW COLUMNS
"""
CREATE TABLE practice.orders_schema_evolution_stage
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/stage/orders_schema_evolution_stage/'
)
AS
SELECT *
FROM (
  VALUES
    (
      1001,
      'C0001',
      DATE '2026-02-15',
      'Electronics',
      'Kolkata',
      99999.00,
      'DELIVERED',
      'PAID',
      CAST(current_timestamp AS timestamp),
      'BlueDart',
      45
    ),
    (
      999998,
      'C9998',
      DATE '2026-03-22',
      'Home Decor',
      'Delhi',
      3200.00,
      'PLACED',
      'PAID',
      CAST(current_timestamp AS timestamp),
      'Delhivery',
      60
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
  updated_at,
  delivery_partner,
  estimated_delivery_minutes
);

"""
    MERGE SQL
"""

MERGE INTO practice.orders_iceberg_partitionby_month target
USING practice.orders_schema_evolution_stage source
ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
  customer_id = source.customer_id,
  order_date = source.order_date,
  product_category = source.product_category,
  city = source.city,
  order_amount = source.order_amount,
  order_status = source.order_status,
  payment_status = source.payment_status,
  updated_at = source.updated_at,
  delivery_partner = source.delivery_partner,
  estimated_delivery_minutes = source.estimated_delivery_minutes

WHEN NOT MATCHED THEN INSERT (
  order_id,
  customer_id,
  order_date,
  product_category,
  city,
  order_amount,
  order_status,
  payment_status,
  updated_at,
  delivery_partner,
  estimated_delivery_minutes
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
  source.updated_at,
  source.delivery_partner,
  source.estimated_delivery_minutes
);


"""
    VALIDATION
"""
SELECT
  order_id,
  order_status,
  payment_status,
  delivery_partner,
  estimated_delivery_minutes
FROM practice.orders_iceberg_partitionby_month
WHERE order_id IN (1001, 999998)
ORDER BY order_id;