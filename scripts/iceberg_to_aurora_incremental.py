import sys
import uuid
import psycopg2
from psycopg2.extras import execute_values

from pyspark.context import SparkContext
from pyspark.sql.functions import col, row_number, max as spark_max
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# --------------------------------------------------
# Glue Job Arguments
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "AURORA_HOST",
        "AURORA_PORT",
        "AURORA_DB",
        "AURORA_USER",
        "AURORA_PASSWORD",
        "ICEBERG_TABLE",
        "PIPELINE_NAME"
    ]
)

AURORA_HOST = args["AURORA_HOST"]
AURORA_PORT = int(args["AURORA_PORT"])
AURORA_DB = args["AURORA_DB"]
AURORA_USER = args["AURORA_USER"]
AURORA_PASSWORD = args["AURORA_PASSWORD"]

ICEBERG_TABLE = args["ICEBERG_TABLE"]
PIPELINE_NAME = args["PIPELINE_NAME"]

RUN_ID = str(uuid.uuid4())


# --------------------------------------------------
# Glue / Spark Context
# --------------------------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# --------------------------------------------------
# Iceberg / Glue Catalog config
# --------------------------------------------------

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-data-lake-958165011713/iceberg/")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")


# --------------------------------------------------
# PostgreSQL Connection Helper
# --------------------------------------------------

def get_pg_connection():
    return psycopg2.connect(
        host=AURORA_HOST,
        port=AURORA_PORT,
        database=AURORA_DB,
        user=AURORA_USER,
        password=AURORA_PASSWORD
    )


# --------------------------------------------------
# Read Watermark
# --------------------------------------------------

def get_last_watermark():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT last_watermark
        FROM public.pipeline_watermark
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if row is None:
        return "1900-01-01 00:00:00"

    return row[0].strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------
# Update Watermark
# --------------------------------------------------

def update_watermark(new_watermark, status):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO public.pipeline_watermark (
            pipeline_name,
            last_watermark,
            last_run_id,
            last_status,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            last_watermark = EXCLUDED.last_watermark,
            last_run_id = EXCLUDED.last_run_id,
            last_status = EXCLUDED.last_status,
            updated_at = CURRENT_TIMESTAMP
    """, (
        PIPELINE_NAME,
        new_watermark,
        RUN_ID,
        status
    ))

    conn.commit()
    cur.close()
    conn.close()


# --------------------------------------------------
# Aurora Upsert Per Spark Partition
# --------------------------------------------------

def upsert_partition(rows_iterator):
    rows = list(rows_iterator)

    if not rows:
        return

    records = []

    for row in rows:
        row_dict = row.asDict()

        records.append((
            row_dict["order_id"],
            row_dict["customer_id"],
            row_dict["order_date"],
            row_dict["product_category"],
            row_dict["city"],
            row_dict["order_amount"],
            row_dict["order_status"],
            row_dict["payment_status"],
            row_dict["updated_at"],
            row_dict["source_system"],
            row_dict["sync_version"]
        ))

    conn = get_pg_connection()
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO public.orders_sync (
            order_id,
            customer_id,
            order_date,
            product_category,
            city,
            order_amount,
            order_status,
            payment_status,
            updated_at,
            source_system,
            sync_version
        )
        VALUES %s
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_date = EXCLUDED.order_date,
            product_category = EXCLUDED.product_category,
            city = EXCLUDED.city,
            order_amount = EXCLUDED.order_amount,
            order_status = EXCLUDED.order_status,
            payment_status = EXCLUDED.payment_status,
            updated_at = EXCLUDED.updated_at,
            source_system = EXCLUDED.source_system,
            sync_version = EXCLUDED.sync_version
        WHERE EXCLUDED.updated_at > orders_sync.updated_at
    """

    execute_values(
        cur,
        upsert_sql,
        records,
        page_size=1000
    )

    conn.commit()
    cur.close()
    conn.close()


# --------------------------------------------------
# Main Pipeline
# --------------------------------------------------

try:
    print("--------------------------------------------------")
    print(f"Pipeline Name: {PIPELINE_NAME}")
    print(f"Run ID: {RUN_ID}")
    print(f"Iceberg Table: {ICEBERG_TABLE}")
    print("--------------------------------------------------")

    last_watermark = get_last_watermark()
    print(f"Last Watermark: {last_watermark}")

    incremental_df = spark.sql(f"""
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
            source_system,
            sync_version
        FROM {ICEBERG_TABLE}
        WHERE updated_at > TIMESTAMP '{last_watermark}'
          AND source_system = 'ICEBERG'
    """)

    incremental_count = incremental_df.count()
    print(f"Incremental rows found: {incremental_count}")

    if incremental_count == 0:
        print("No new Iceberg records found. Pipeline completed safely.")
        update_watermark(last_watermark, "NO_DATA")
        job.commit()

    # --------------------------------------------------
    # Deduplicate by order_id, keep latest updated_at
    # --------------------------------------------------
    else:
        window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
    
        latest_df = (
            incremental_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )
    
        latest_count = latest_df.count()
        print(f"Rows after deduplication: {latest_count}")
    
        # --------------------------------------------------
        # New watermark
        # --------------------------------------------------
    
        max_watermark = (
            latest_df
            .select(spark_max("updated_at").alias("max_updated_at"))
            .collect()[0]["max_updated_at"]
        )
    
        print(f"New candidate watermark: {max_watermark}")
    
        # --------------------------------------------------
        # Reduce DB connections
        # --------------------------------------------------
    
        latest_df = latest_df.repartition(4)
    
        # --------------------------------------------------
        # Direct upsert into Aurora
        # --------------------------------------------------
    
        print("Starting Aurora upsert...")
    
        latest_df.foreachPartition(upsert_partition)
    
        print("Aurora upsert completed successfully.")
    
        # --------------------------------------------------
        # Update watermark only after successful write
        # --------------------------------------------------
    
        update_watermark(max_watermark, "SUCCESS")
    
        print("Watermark updated successfully.")
        print(f"Final Watermark: {max_watermark}")
        print("Pipeline completed successfully.")
    
        job.commit()


except Exception as e:
    print("Pipeline failed.")
    print(str(e))

    try:
        current_watermark = get_last_watermark()
        update_watermark(current_watermark, "FAILED")
    except Exception as wm_error:
        print("Failed to update failure status in watermark table.")
        print(str(wm_error))

    raise e