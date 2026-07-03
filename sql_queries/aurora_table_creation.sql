--RUN THIS IN AURORA TO CREATE TABLES

CREATE TABLE IF NOT EXISTS public.orders_sync (
    order_id            BIGINT PRIMARY KEY,
    customer_id         VARCHAR(100),
    order_date          DATE,
    product_category    VARCHAR(100),
    city                VARCHAR(100),
    order_amount        NUMERIC(12,2),
    order_status        VARCHAR(100),
    payment_status      VARCHAR(100),
    updated_at          TIMESTAMP,
    source_system       VARCHAR(50),
    sync_version        INTEGER
);

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
VALUES (
    'iceberg_to_aurora_orders',
    TIMESTAMP '1900-01-01 00:00:00',
    'initial',
    'INITIALIZED'
)
ON CONFLICT (pipeline_name)
DO NOTHING;

INSERT INTO public.pipeline_watermark (
    pipeline_name,
    last_watermark,
    last_run_id,
    last_status
)
VALUES (
    'aurora_to_iceberg_orders',
    TIMESTAMP '1900-01-01 00:00:00',
    'initial',
    'INITIALIZED'
)
ON CONFLICT (pipeline_name)
DO NOTHING;