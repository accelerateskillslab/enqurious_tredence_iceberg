"""
    1) Find out Total Orders
    2) Find out Orders status distribution
    3) Find out Payment status distribution
    4) Capture the current snapshot history
"""

"""
    ALTER DDL
"""
ALTER TABLE practice.orders_iceberg_partitionby_month
ADD COLUMNS (
  delivery_partner string,
  coupon_code string
);

"""
    Create business corrections staging table, focus on the last column
"""

CREATE TABLE practice.orders_business_corrections_stage
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/stage/orders_business_corrections_stage/'
)
AS
SELECT *
FROM (
  VALUES
    (
      100010,
      'C0001',
      DATE '2026-02-15',
      'Electronics',
      'Kolkata',
      5200.00,
      'DELIVERED',
      'PAID',
      CAST('2026-06-24 10:00:00' AS timestamp),
      'BlueDart',
      'WELCOME10',
      'finance_team'
    ),
    (
      100010,
      'C0001',
      DATE '2026-02-15',
      'Electronics',
      'Kolkata',
      5000.00,
      'SHIPPED',
      'PAID',
      CAST('2026-06-24 09:30:00' AS timestamp),
      'Delhivery',
      'WELCOME10',
      'ops_team'
    ),
    (
      999997,
      'C9997',
      DATE '2026-03-25',
      'Fashion',
      'Mumbai',
      3200.00,
      'PLACED',
      'PAID',
      CAST('2026-06-24 10:15:00' AS timestamp),
      'Shadowfax',
      'NEWUSER',
      'ops_team'
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
  coupon_code,
  correction_source
);

"""
    CHECK DUPLICATES
"""
SELECT
  order_id,
  count(*) AS record_count
FROM practice.orders_business_corrections_stage
GROUP BY order_id
HAVING count(*) > 1;

"""
    CREATE CLEAN CORRECTIONS TABLE
"""
CREATE TABLE practice.orders_business_corrections_clean
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/stage/orders_business_corrections_clean/'
)
AS
WITH ranked_corrections AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY order_id
      ORDER BY
        CASE
          WHEN correction_source = 'finance_team' THEN 1
          WHEN correction_source = 'ops_team' THEN 2
          ELSE 3
        END,
        updated_at DESC
    ) AS rn
  FROM practice.orders_business_corrections_stage
)
SELECT
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
  coupon_code,
  correction_source
FROM ranked_corrections
WHERE rn = 1;

"""
    MERGE BUSINESS CORRECTIONS
"""
MERGE INTO practice.orders_iceberg_partitionby_month target
USING practice.orders_business_corrections_clean source
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
  coupon_code = source.coupon_code

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
  coupon_code
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
  source.coupon_code
);

"""
    Create fraud orders list
"""
CREATE TABLE practice.orders_fraud_delete_list
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/stage/orders_fraud_delete_list/'
)
AS
SELECT *
FROM (
  VALUES
    (999997, 'fraud_order'),
    (999999, 'test_order')
)
AS t (
  order_id,
  delete_reason
);

"""
    DELETE FRAUD ORDERS
"""
DELETE FROM practice.orders_iceberg_partitionby_month
WHERE order_id IN (
  SELECT order_id
  FROM practice.orders_fraud_delete_list
);

"""
    SIMULTATE BAD UPDATE
"""
UPDATE practice.orders_iceberg_partitionby_month
SET
  order_status = 'CANCELLED',
  updated_at = CAST(current_timestamp AS timestamp)
WHERE city = 'Kolkata';


"""
    SNAPSHOT COMPARISON
"""
WITH old_version AS (
  SELECT
    order_id,
    order_status AS old_order_status
  FROM practice.orders_iceberg_partitionby_month
  FOR VERSION AS OF <snapshot_before_bad_update>
  WHERE city = 'Kolkata'
),
current_version AS (
  SELECT
    order_id,
    order_status AS current_order_status
  FROM practice.orders_iceberg_partitionby_month
  WHERE city = 'Kolkata'
)
SELECT
  c.order_id,
  o.old_order_status,
  c.current_order_status
FROM current_version c
JOIN old_version o
  ON c.order_id = o.order_id
WHERE c.current_order_status <> o.old_order_status
ORDER BY c.order_id
LIMIT 20;

"""
    Create recovery staging table from old snapshot
"""
CREATE TABLE practice.orders_kolkata_recovery_stage
WITH (
  format = 'PARQUET',
  external_location = 's3://iceberg-data-lake-958165011713/stage/orders_kolkata_recovery_stage/'
)
AS
SELECT *
FROM practice.orders_iceberg_partitionby_month
FOR VERSION AS OF <snapshot_before_bad_update>
WHERE city = 'Kolkata';

"""
    MERGE RECOVERY DATA
"""
MERGE INTO practice.orders_iceberg_partitionby_month target
USING practice.orders_kolkata_recovery_stage source
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
  coupon_code = source.coupon_code;