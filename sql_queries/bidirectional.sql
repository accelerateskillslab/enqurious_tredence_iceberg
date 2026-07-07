--RUN THIS IN ATHENA TO CREATE TABLE
CREATE TABLE practice.orders_iceberg_bidir
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_iceberg_bidir/',
  format = 'PARQUET',
  partitioning = ARRAY['month(order_date)']
)
AS
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(customer_id AS VARCHAR) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(product_category AS VARCHAR) AS product_category,
  CAST(city AS VARCHAR) AS city,
  CAST(order_amount AS DECIMAL(12,2)) AS order_amount,
  CAST(order_status AS VARCHAR) AS order_status,
  CAST(payment_status AS VARCHAR) AS payment_status,
  CAST(updated_at AS TIMESTAMP) AS updated_at,
  'ICEBERG' AS source_system,
  1 AS sync_version
FROM practice.orders_500k_iceberg_partitionby_customer;


--RUN THIS IN ATHENA TO GENERATE SOME SOURCE DATA (SOURCE AS ICEBERG)
MERGE INTO practice.orders_iceberg_bidir target
USING (
  SELECT *
  FROM (
    VALUES
      (
        100001,
        'C0001',
        DATE '2026-02-15',
        'Electronics',
        'Kolkata',
        7777.00,
        'DELIVERED',
        'PAID',
        'ICEBERG',
        CAST(current_timestamp AS timestamp)
      ),
      (
        999996,
        'C9996',
        DATE '2026-03-28',
        'Fashion',
        'Mumbai',
        2500.00,
        'PLACED',
        'PAID',
        'ICEBERG',
        CAST(current_timestamp AS timestamp)
      )
  ) AS v (
    order_id,
    customer_id,
    order_date,
    product_category,
    city,
    order_amount,
    order_status,
    payment_status,
    source_system,
    updated_at
  )
) source
ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
  customer_id = source.customer_id,
  order_date = source.order_date,
  product_category = source.product_category,
  city = source.city,
  order_amount = source.order_amount,
  order_status = source.order_status,
  payment_status = source.payment_status,
  source_system = source.source_system,
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
  source_system,
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
  source.source_system,
  source.updated_at
);

--RUN THIS IN AURORA TO CREATE SOME CHANGES
UPDATE public.orders_sync
SET
    order_status = 'DELIVERED_BY_AURORA',
    payment_status = 'PAID',
    source_system = 'AURORA',
    sync_version = COALESCE(sync_version, 1) + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id IN (
    100002, 100003, 100004    
);

--RUN THIS IN AURORA TO CREATE SOME CHANGES
UPDATE public.inventory_sync
SET
    available_quantity = GREATEST(available_quantity - 20, 0),
    damaged_quantity = damaged_quantity + 20,
    movement_type = 'STOCK_ADJUSTED',
    inventory_status = CASE
        WHEN GREATEST(available_quantity - 20, 0) = 0 THEN 'OUT_OF_STOCK'
        WHEN GREATEST(available_quantity - 20, 0) < 20 THEN 'LOW_STOCK'
        ELSE 'IN_STOCK'
    END,
    source_system = 'AURORA',
    sync_version = COALESCE(sync_version, 1) + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE inventory_id IN (
    SELECT inventory_id
    FROM public.inventory_sync
    ORDER BY updated_at DESC
    LIMIT 5
);

