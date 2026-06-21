INSERT INTO practice.orders_iceberg_partitionby_month
SELECT *
FROM practice.orders_iceberg_partitionby_month
FOR VERSION AS OF 1234567890123456789
WHERE order_id = 999999;

