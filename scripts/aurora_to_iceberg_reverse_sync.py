import sys
import uuid
import psycopg2

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

AURORA_TABLE = "public.orders_sync"

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
# Main Pipeline
# --------------------------------------------------

try:
    print("--------------------------------------------------")
    print(f"Pipeline Name: {PIPELINE_NAME}")
    print(f"Run ID: {RUN_ID}")
    print(f"Aurora Table: {AURORA_TABLE}")
    print(f"Iceberg Table: {ICEBERG_TABLE}")
    print("--------------------------------------------------")

    last_watermark = get_last_watermark()
    print(f"Last Watermark: {last_watermark}")


    # --------------------------------------------------
    # Read changed Aurora records using JDBC
    # --------------------------------------------------

    jdbc_url = f"jdbc:postgresql://{AURORA_HOST}:{AURORA_PORT}/{AURORA_DB}"

    aurora_query = f"""
        (
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
            FROM {AURORA_TABLE}
            WHERE updated_at > TIMESTAMP '{last_watermark}'
              AND source_system = 'AURORA'
        ) AS aurora_incremental_orders
    """

    incremental_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", aurora_query)
        .option("user", AURORA_USER)
        .option("password", AURORA_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    incremental_count = incremental_df.count()
    print(f"Incremental Aurora rows found: {incremental_count}")

    if incremental_count == 0:
        print("No new Aurora records found. Pipeline completed safely.")
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
        # Create temp view for Iceberg MERGE
        # --------------------------------------------------
    
        latest_df.createOrReplaceTempView("aurora_changed_orders")
    
    
        # --------------------------------------------------
        # Merge Aurora changes into Iceberg
        # Latest updated_at wins
        # --------------------------------------------------
    
        print("Starting Iceberg MERGE...")
    
        spark.sql(f"""
            MERGE INTO {ICEBERG_TABLE} AS target
            USING aurora_changed_orders AS source
            ON target.order_id = source.order_id
    
            WHEN MATCHED AND source.updated_at > target.updated_at
            THEN UPDATE SET
                target.customer_id = source.customer_id,
                target.order_date = source.order_date,
                target.product_category = source.product_category,
                target.city = source.city,
                target.order_amount = source.order_amount,
                target.order_status = source.order_status,
                target.payment_status = source.payment_status,
                target.updated_at = source.updated_at,
                target.source_system = source.source_system,
                target.sync_version = source.sync_version
    
            WHEN NOT MATCHED
            THEN INSERT (
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
                source.source_system,
                source.sync_version
            )
        """)
    
        print("Iceberg MERGE completed successfully.")
    
    
        # --------------------------------------------------
        # Update watermark only after successful merge
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