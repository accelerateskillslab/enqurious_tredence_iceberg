import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Iceberg / Glue Catalog config
# -----------------------------
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-data-lake-958165011713/iceberg/")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

DATABASE = "practice"
SOURCE_TABLE = "orders_iceberg_partitionby_month"
TARGET_TABLE = "orders_iceberg_replica_target"
CONTROL_TABLE = "iceberg_incremental_control"

PIPELINE_NAME = "orders_source_to_replica_incremental"

SOURCE = f"glue_catalog.{DATABASE}.{SOURCE_TABLE}"
TARGET = f"glue_catalog.{DATABASE}.{TARGET_TABLE}"
CONTROL = f"glue_catalog.{DATABASE}.{CONTROL_TABLE}"

CONTROL_LOCATION = "s3://iceberg-data-lake-958165011713/iceberg/iceberg_incremental_control/"

should_process = True

# -----------------------------
# 1. Create control table
# -----------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL} (
  pipeline_name string,
  source_table string,
  target_table string,
  last_processed_snapshot_id string,
  last_processed_at timestamp,
  status string
)
USING iceberg
LOCATION '{CONTROL_LOCATION}'
""")

# -----------------------------
# 2. Get source snapshots
# -----------------------------
snapshots_df = spark.sql(f"""
SELECT
  committed_at,
  snapshot_id,
  operation
FROM {SOURCE}.snapshots
ORDER BY committed_at
""")

latest_snapshot_rows = (
    snapshots_df
    .orderBy(col("committed_at").desc())
    .limit(1)
    .collect()
)

if not latest_snapshot_rows:
    raise Exception("No snapshots found in source Iceberg table.")

new_snapshot_id = str(latest_snapshot_rows[0]["snapshot_id"])
print(f"Latest source snapshot: {new_snapshot_id}")

# -----------------------------
# 3. Get last processed snapshot from control table
# -----------------------------
control_rows = spark.sql(f"""
SELECT
  last_processed_snapshot_id,
  last_processed_at
FROM {CONTROL}
WHERE pipeline_name = '{PIPELINE_NAME}'
  AND status = 'COMPLETED'
ORDER BY last_processed_at DESC
LIMIT 1
""").collect()

# -----------------------------
# 4. Bootstrap control table on first run
# -----------------------------
if not control_rows:
    print("No control record found. Bootstrapping control table.")

    target_watermark = spark.sql(f"""
    SELECT max(updated_at) AS max_target_updated_at
    FROM {TARGET}
    """).collect()[0]["max_target_updated_at"]

    if target_watermark is None:
        raise Exception("Target table is empty. Create initial target CTAS before running incremental job.")

    baseline_snapshot_rows = (
        snapshots_df
        .filter(col("committed_at") <= target_watermark)
        .orderBy(col("committed_at").desc())
        .limit(1)
        .collect()
    )

    if not baseline_snapshot_rows:
        raise Exception("Could not find baseline source snapshot for target watermark.")

    old_snapshot_id = str(baseline_snapshot_rows[0]["snapshot_id"])

    spark.sql(f"""
    INSERT INTO {CONTROL}
    VALUES (
      '{PIPELINE_NAME}',
      '{SOURCE_TABLE}',
      '{TARGET_TABLE}',
      '{old_snapshot_id}',
      current_timestamp(),
      'COMPLETED'
    )
    """)

    print(f"Baseline snapshot saved in control table: {old_snapshot_id}")

else:
    old_snapshot_id = str(control_rows[0]["last_processed_snapshot_id"])

print(f"Old snapshot: {old_snapshot_id}")
print(f"New snapshot: {new_snapshot_id}")

# -----------------------------
# 5. No new source snapshot
# -----------------------------
if old_snapshot_id == new_snapshot_id:
    print("No new source snapshot to process. Target MERGE will be skipped.")
    should_process = False

# -----------------------------
# 6. Process only if new snapshot exists
# -----------------------------
if should_process:

    # -----------------------------
    # Read old and new snapshots
    # -----------------------------
    old_source_df = (
        spark.read
        .format("iceberg")
        .option("snapshot-id", old_snapshot_id)
        .load(SOURCE)
    )

    new_source_df = (
        spark.read
        .format("iceberg")
        .option("snapshot-id", new_snapshot_id)
        .load(SOURCE)
    )

    # -----------------------------
    # Deduplicate snapshots by order_id
    # -----------------------------
    dedup_window = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

    old_dedup_df = (
        old_source_df
        .withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    new_dedup_df = (
        new_source_df
        .withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    old_dedup_df.createOrReplaceTempView("old_snapshot")
    new_dedup_df.createOrReplaceTempView("new_snapshot")

    # -----------------------------
    # Detect INSERT / UPDATE / DELETE
    # -----------------------------
    incremental_changes_df = spark.sql("""
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

      current_timestamp() AS updated_at

    FROM old_snapshot o
    FULL OUTER JOIN new_snapshot n
      ON o.order_id = n.order_id

    WHERE
         o.order_id IS NULL
      OR n.order_id IS NULL
      OR NOT (o.customer_id <=> n.customer_id)
      OR NOT (o.order_date <=> n.order_date)
      OR NOT (o.product_category <=> n.product_category)
      OR NOT (o.city <=> n.city)
      OR NOT (o.order_amount <=> n.order_amount)
      OR NOT (o.order_status <=> n.order_status)
      OR NOT (o.payment_status <=> n.payment_status)
      OR NOT (o.delivery_partner <=> n.delivery_partner)
      OR NOT (o.coupon_code <=> n.coupon_code)
      OR NOT (o.updated_at <=> n.updated_at)
    """)

    incremental_changes_df.createOrReplaceTempView("incremental_changes")

    print("Snapshot-level incremental changes found:")
    incremental_changes_df.groupBy("change_type").count().show()

    # -----------------------------
    # Filter only changes that actually need target action
    # This prevents unnecessary target snapshots.
    # -----------------------------
    target_current_df = spark.sql(f"""
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
      source_updated_timestamp
    FROM {TARGET}
    """)

    target_current_df.createOrReplaceTempView("target_current")

    merge_changes_df = spark.sql("""
    SELECT
      ic.*
    FROM incremental_changes ic
    LEFT JOIN target_current t
      ON ic.order_id = t.order_id
    WHERE
         -- delete only if row exists in target
         (ic.change_type = 'DELETE' AND t.order_id IS NOT NULL)

      OR -- insert if row missing in target
         (ic.change_type IN ('INSERT', 'UPDATE') AND t.order_id IS NULL)

      OR -- update only if target values are actually different
         (
           ic.change_type IN ('UPDATE', 'INSERT')
           AND t.order_id IS NOT NULL
           AND (
                NOT (t.customer_id <=> ic.customer_id)
             OR NOT (t.order_date <=> ic.order_date)
             OR NOT (t.product_category <=> ic.product_category)
             OR NOT (t.city <=> ic.city)
             OR NOT (t.order_amount <=> ic.order_amount)
             OR NOT (t.order_status <=> ic.order_status)
             OR NOT (t.payment_status <=> ic.payment_status)
             OR NOT (t.delivery_partner <=> ic.delivery_partner)
             OR NOT (t.coupon_code <=> ic.coupon_code)
             OR NOT (t.source_updated_timestamp <=> ic.source_updated_timestamp)
           )
         )
    """)

    merge_changes_df.createOrReplaceTempView("merge_changes")

    merge_change_count = merge_changes_df.count()

    print(f"Actual target changes to apply: {merge_change_count}")

    # -----------------------------
    # 7. Skip target MERGE if no actual changes
    # -----------------------------
    if merge_change_count == 0:
        print("No actual target changes found. Skipping target MERGE.")

        spark.sql(f"""
        INSERT INTO {CONTROL}
        VALUES (
          '{PIPELINE_NAME}',
          '{SOURCE_TABLE}',
          '{TARGET_TABLE}',
          '{new_snapshot_id}',
          current_timestamp(),
          'COMPLETED'
        )
        """)

        print(f"Control table updated to snapshot {new_snapshot_id}. Target table unchanged.")

    else:
        print("Running MERGE into target table.")

        spark.sql(f"""
        MERGE INTO {TARGET} target
        USING merge_changes source
        ON target.order_id = source.order_id

        WHEN MATCHED AND source.change_type = 'DELETE' THEN DELETE

        WHEN MATCHED
          AND source.change_type IN ('UPDATE', 'INSERT')
        THEN UPDATE SET
          target.customer_id = source.customer_id,
          target.order_date = source.order_date,
          target.product_category = source.product_category,
          target.city = source.city,
          target.order_amount = source.order_amount,
          target.order_status = source.order_status,
          target.payment_status = source.payment_status,
          target.delivery_partner = source.delivery_partner,
          target.coupon_code = source.coupon_code,
          target.source_updated_timestamp = source.source_updated_timestamp,
          target.updated_at = source.updated_at

        WHEN NOT MATCHED
          AND source.change_type IN ('INSERT', 'UPDATE')
        THEN INSERT (
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
          source_updated_timestamp,
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
          source.source_updated_timestamp,
          source.updated_at
        )
        """)

        spark.sql(f"""
        INSERT INTO {CONTROL}
        VALUES (
          '{PIPELINE_NAME}',
          '{SOURCE_TABLE}',
          '{TARGET_TABLE}',
          '{new_snapshot_id}',
          current_timestamp(),
          'COMPLETED'
        )
        """)

        print(f"Incremental MERGE completed. Control table updated to snapshot {new_snapshot_id}.")

job.commit()