"""
    This SQL script demonstrates the concept of time travel in Iceberg tables. 
    It creates a new table called `orders_time_travel` in the `practice` db
"""
##BY SNAPSHOT
SELECT
  order_id,
  customer_id,
  order_date,
  order_status,
  payment_status,
  order_amount,
  updated_at
FROM practice.orders_iceberg_partitionby_month
FOR VERSION AS OF 1234567890123456789
WHERE order_id IN (100002, 999999)
ORDER BY order_id;

##BY TIMESTAMP

SELECT
  order_id,
  customer_id,
  order_date,
  order_status,
  payment_status,
  order_amount,
  updated_at
FROM practice.orders_iceberg_partitionby_month
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-21 15:30:00'
WHERE order_id IN (100002, 999999)
ORDER BY order_id;