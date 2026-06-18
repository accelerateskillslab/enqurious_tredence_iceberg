########Check the existing data########
SELECT *
FROM practice.orders_iceberg_partitionby_month
WHERE order_id = 105000;

UPDATE practice.orders_iceberg_partitionby_month
SET
  city = 'Kolkata',
  updated_at = current_timestamp
WHERE order_id = 105000;

