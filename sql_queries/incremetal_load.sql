"""
    CREATE TARGET TABLE FOR INCREMENTAL LOAD
"""

CREATE TABLE practice.orders_iceberg_replica_target
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://iceberg-data-lake-958165011713/iceberg/orders_iceberg_replica_target/',
  format = 'PARQUET',
  partitioning = ARRAY['month(order_date)']
)
AS
WITH deduped_source AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY order_id
      ORDER BY updated_at DESC
    ) AS rn
  FROM practice.orders_iceberg_partitionby_month
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
  delivery_partner,
  coupon_code,

  -- original source table updated_at
  updated_at AS source_updated_timestamp,

  -- target load/update timestamp
  CAST(current_timestamp AS timestamp) AS updated_at

FROM deduped_source
WHERE rn = 1;

"""
    DO CHANGE IN SOURCE, you will find something weird
"""
MERGE INTO practice.orders_iceberg_partitionby_month target
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
        'BlueDart',
        'DAY4TEST',
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
        'Delhivery',
        'NEWORDER',
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
    delivery_partner,
    coupon_code,
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
  delivery_partner = source.delivery_partner,
  coupon_code = source.coupon_code,
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
  delivery_partner,
  coupon_code,
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
  source.delivery_partner,
  source.coupon_code,
  source.updated_at
);

"""
   FIND INCREMENTAL CHANGES HAVING DUPLICATES
"""
WITH old_snapshot AS (
  SELECT
    order_id,
    customer_id,
    order_date,
    product_category,
    city,
    order_amount,
    order_status,
    payment_status,
    delivery_partner,
    coupon_code,
    updated_at AS source_updated_timestamp
  FROM practice.orders_iceberg_partitionby_month
  FOR VERSION AS OF <old_snapshot_id>
),

new_snapshot AS (
  SELECT
    order_id,
    customer_id,
    order_date,
    product_category,
    city,
    order_amount,
    order_status,
    payment_status,
    delivery_partner,
    coupon_code,
    updated_at AS source_updated_timestamp
  FROM practice.orders_iceberg_partitionby_month
  FOR VERSION AS OF <new_snapshot_id>
)

SELECT
  CASE
    WHEN o.order_id IS NULL THEN 'INSERT'
    WHEN n.order_id IS NULL THEN 'DELETE'
    ELSE 'UPDATE'
  END AS change_type,

  COALESCE(n.order_id, o.order_id) AS order_id,

  n.customer_id,
  n.order_date,
  n.product_category,
  n.city,
  n.order_amount,
  n.order_status,
  n.payment_status,
  n.delivery_partner,
  n.coupon_code,
  n.source_updated_timestamp,

  CAST(current_timestamp AS timestamp) AS updated_at

FROM old_snapshot o
FULL OUTER JOIN new_snapshot n
  ON o.order_id = n.order_id

WHERE
  -- inserted in new snapshot
  o.order_id IS NULL

  -- deleted from new snapshot
  OR n.order_id IS NULL

  -- updated between snapshots
  OR COALESCE(o.customer_id, '') <> COALESCE(n.customer_id, '')
  OR o.order_date <> n.order_date
  OR COALESCE(o.product_category, '') <> COALESCE(n.product_category, '')
  OR COALESCE(o.city, '') <> COALESCE(n.city, '')
  OR o.order_amount <> n.order_amount
  OR COALESCE(o.order_status, '') <> COALESCE(n.order_status, '')
  OR COALESCE(o.payment_status, '') <> COALESCE(n.payment_status, '')
  OR COALESCE(o.delivery_partner, '') <> COALESCE(n.delivery_partner, '')
  OR COALESCE(o.coupon_code, '') <> COALESCE(n.coupon_code, '')
  OR o.source_updated_timestamp <> n.source_updated_timestamp;

"""
    DO CHANGE IN SOURCE
"""
MERGE INTO practice.orders_iceberg_partitionby_month target
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
        'BlueDart',
        'DAY4TEST',
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
        'Delhivery',
        'NEWORDER',
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
    delivery_partner,
    coupon_code,
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
  delivery_partner = source.delivery_partner,
  coupon_code = source.coupon_code,
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
  delivery_partner,
  coupon_code,
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
  source.delivery_partner,
  source.coupon_code,
  source.updated_at
);

"""
    CHECK DUPLICATES IN SOURCE
"""
SELECT
  order_id,
  count(*) AS cnt
FROM practice.orders_iceberg_partitionby_month
GROUP BY order_id
HAVING count(*) > 1
ORDER BY cnt DESC;

"""
    CHECK INCREMENTAL CHANGES HANDLING DUPLICATES
"""
WITH old_snapshot_raw AS (
  SELECT *
  FROM practice.orders_iceberg_partitionby_month
  FOR VERSION AS OF <OLD_SNAPSHOT_ID>
),

new_snapshot_raw AS (
  SELECT *
  FROM practice.orders_iceberg_partitionby_month
  FOR VERSION AS OF <NEW_SNAPSHOT_ID>
),

old_snapshot AS (
  SELECT *
  FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY order_id
        ORDER BY updated_at DESC
      ) AS rn
    FROM old_snapshot_raw
  )
  WHERE rn = 1
),

new_snapshot AS (
  SELECT *
  FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY order_id
        ORDER BY updated_at DESC
      ) AS rn
    FROM new_snapshot_raw
  )
  WHERE rn = 1
)

SELECT
  CASE
    WHEN o.order_id IS NULL THEN 'INSERT'
    WHEN n.order_id IS NULL THEN 'DELETE'
    ELSE 'UPDATE'
  END AS change_type,

  COALESCE(n.order_id, o.order_id) AS order_id,
  n.customer_id,
  n.order_date,
  n.product_category,
  n.city,
  n.order_amount,
  n.order_status,
  n.payment_status,
  n.delivery_partner,
  n.coupon_code,
  n.updated_at AS source_updated_timestamp,
  CAST(current_timestamp AS timestamp) AS updated_at

FROM old_snapshot o
FULL OUTER JOIN new_snapshot n
  ON o.order_id = n.order_id

WHERE
     o.order_id IS NULL
  OR n.order_id IS NULL
  OR COALESCE(o.customer_id, '') <> COALESCE(n.customer_id, '')
  OR o.order_date <> n.order_date
  OR COALESCE(o.product_category, '') <> COALESCE(n.product_category, '')
  OR COALESCE(o.city, '') <> COALESCE(n.city, '')
  OR o.order_amount <> n.order_amount
  OR COALESCE(o.order_status, '') <> COALESCE(n.order_status, '')
  OR COALESCE(o.payment_status, '') <> COALESCE(n.payment_status, '')
  OR COALESCE(o.delivery_partner, '') <> COALESCE(n.delivery_partner, '')
  OR COALESCE(o.coupon_code, '') <> COALESCE(n.coupon_code, '')
  OR o.updated_at <> n.updated_at;