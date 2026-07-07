--RUN THIS IN ATHENA, TO CREATE TABLE LOADED FROM S3 RAW INVENTORY DATA
CREATE EXTERNAL TABLE practice.inventory_raw (
  inventory_id string,
  product_id string,
  warehouse_id string,
  city string,
  movement_date string,
  product_category string,
  movement_type string,
  quantity string,
  available_quantity string,
  reserved_quantity string,
  damaged_quantity string,
  inventory_status string,
  updated_at string,
  source_system string,
  sync_version string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
LOCATION 's3://<S3_BUCKET_LOCATION>'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

--RUN THIS IN ATHENA, TO CREATE DIM TABLE LOADED FROM S3
CREATE EXTERNAL TABLE practice.warehouse_dim_raw (
  warehouse_id string,
  city string,
  region string,
  warehouse_type string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
LOCATION 's3://<S3_BUCKET_LOCATION>/warehouse_dim/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);

--RUN THIS IN ATHENA TO CREATE ICEBERG TABLE WITH PARTITIONS
CREATE TABLE practice.inventory_iceberg
WITH (
  table_type = 'ICEBERG',
  is_external = false,
  location = 's3://<S3_BUCKET_LOCATION>',
  format = 'PARQUET',
  partitioning = <PARTITION OF YOUR CHOICE>
)
AS
SELECT
  CAST(inventory_id AS BIGINT) AS inventory_id,
  CAST(product_id AS VARCHAR) AS product_id,
  CAST(warehouse_id AS VARCHAR) AS warehouse_id,
  CAST(city AS VARCHAR) AS city,
  CAST(movement_date AS DATE) AS movement_date,
  CAST(product_category AS VARCHAR) AS product_category,
  CAST(movement_type AS VARCHAR) AS movement_type,
  CAST(quantity AS INTEGER) AS quantity,
  CAST(available_quantity AS INTEGER) AS available_quantity,
  CAST(reserved_quantity AS INTEGER) AS reserved_quantity,
  CAST(damaged_quantity AS INTEGER) AS damaged_quantity,
  CAST(inventory_status AS VARCHAR) AS inventory_status,
  CAST(updated_at AS TIMESTAMP) AS updated_at,
  CAST(source_system AS VARCHAR) AS source_system,
  CAST(sync_version AS INTEGER) AS sync_version
FROM practice.inventory_raw;

--RUN THIS IN AURORA TO CREATE SYNC TABLE FOR INVENTORY DATA (CONTAINING REPLICA OF ICEBERG TABLE)
CREATE TABLE public.inventory_sync (
    inventory_id          BIGINT PRIMARY KEY,
    product_id            VARCHAR(100),
    warehouse_id          VARCHAR(100),
    city                  VARCHAR(100),
    movement_date         DATE,
    product_category      VARCHAR(100),
    movement_type         VARCHAR(100),
    quantity              INTEGER,
    available_quantity    INTEGER,
    reserved_quantity     INTEGER,
    damaged_quantity      INTEGER,
    inventory_status      VARCHAR(100),
    updated_at            TIMESTAMP,
    source_system         VARCHAR(50),
    sync_version          INTEGER
);

--RUN THIS IN AURORA TO CREATE WATERMARK TABLE FOR INVENTORY DATA
CREATE TABLE IF NOT EXISTS public.pipeline_watermark (
    pipeline_name       VARCHAR(200) PRIMARY KEY,
    last_watermark      TIMESTAMP NOT NULL,
    last_run_id         VARCHAR(200),
    last_status         VARCHAR(50),
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO public.pipeline_watermark (
    pipeline_name,
    last_watermark,
    last_run_id,
    last_status
)
VALUES
(
    'iceberg_to_aurora_inventory',
    TIMESTAMP '1900-01-01 00:00:00',
    'initial',
    'INITIALIZED'
),
(
    'aurora_to_iceberg_inventory',
    TIMESTAMP '1900-01-01 00:00:00',
    'initial',
    'INITIALIZED'
)
ON CONFLICT (pipeline_name)
DO NOTHING;

--INTRODUCE INCREMENTAL CHANGES in iceberg (just an example)
MERGE INTO practice.inventory_iceberg AS target
USING (
  SELECT
    inventory_id,
    product_id,
    warehouse_id,
    city,
    movement_date,
    product_category,
    'STOCK_ADJUSTED' AS movement_type,
    quantity,
    GREATEST(available_quantity - 20, 0) AS available_quantity,
    reserved_quantity,
    damaged_quantity + 20 AS damaged_quantity,
    CASE
      WHEN GREATEST(available_quantity - 20, 0) = 0 THEN 'OUT_OF_STOCK'
      WHEN GREATEST(available_quantity - 20, 0) < 20 THEN 'LOW_STOCK'
      ELSE 'IN_STOCK'
    END AS inventory_status,
    current_timestamp AS updated_at,
    'WAREHOUSE_APP' AS source_system,
    sync_version + 1 AS sync_version
  FROM practice.inventory_iceberg
  WHERE inventory_id IN (
    SELECT inventory_id
    FROM practice.inventory_iceberg
    ORDER BY inventory_id
    LIMIT 10
  )
) AS source
ON target.inventory_id = source.inventory_id

WHEN MATCHED
  AND (
    source.updated_at > target.updated_at
    OR (
      source.updated_at = target.updated_at
      AND source.sync_version > target.sync_version
    )
  )
THEN UPDATE SET
  product_id = source.product_id,
  warehouse_id = source.warehouse_id,
  city = source.city,
  movement_date = source.movement_date,
  product_category = source.product_category,
  movement_type = source.movement_type,
  quantity = source.quantity,
  available_quantity = source.available_quantity,
  reserved_quantity = source.reserved_quantity,
  damaged_quantity = source.damaged_quantity,
  inventory_status = source.inventory_status,
  updated_at = source.updated_at,
  source_system = source.source_system,
  sync_version = source.sync_version;

-- AURORA SIDE OPERATIONAL CORRECTION from WAREHOUSE TEAM (Just an example)
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
    source_system = 'WAREHOUSE_APP',
    sync_version = COALESCE(sync_version, 1) + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE inventory_id IN (
    SELECT inventory_id
    FROM public.inventory_sync
    ORDER BY updated_at DESC
    LIMIT 5
);
