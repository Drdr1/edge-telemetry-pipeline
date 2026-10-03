CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw readings: one row per (device, metric, ts). Hypertable partitioned by time,
-- space-partitioned by device_id so hot devices don't contend on one chunk.
CREATE TABLE IF NOT EXISTS readings (
    ts          TIMESTAMPTZ NOT NULL,
    device_id   TEXT        NOT NULL,
    metric      TEXT        NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    source      TEXT        NOT NULL,            -- mqtt | modbus | open-data
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- idempotency key: the producer's event id; duplicates from at-least-once delivery are dropped
    event_id    UUID        NOT NULL
);
SELECT create_hypertable('readings', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS readings_event_id_ts ON readings (event_id, ts);
CREATE INDEX IF NOT EXISTS readings_device_metric_ts ON readings (device_id, metric, ts DESC);

-- Compress chunks older than 1 day (10-20x on telemetry), keep 30 days.
ALTER TABLE readings SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id,metric');
SELECT add_compression_policy('readings', INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('readings', INTERVAL '30 days', if_not_exists => TRUE);

-- 1-minute continuous aggregate: the real-time read model for dashboards / feature stores.
CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts) AS bucket,
       device_id, metric,
       avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
       count(*)   AS samples
FROM readings
GROUP BY bucket, device_id, metric
WITH NO DATA;
-- start_offset must cover how long an edge gateway can buffer offline: late rows older
-- than this are not picked up by the policy (backfill job / manual refresh needed).
SELECT add_continuous_aggregate_policy('readings_1m',
    start_offset => INTERVAL '6 hours', end_offset => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '10 seconds', if_not_exists => TRUE);

-- Hourly rollups owned by Dagster (batch layer).
CREATE TABLE IF NOT EXISTS readings_hourly (
    bucket      TIMESTAMPTZ NOT NULL,
    device_id   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    avg_value   DOUBLE PRECISION,
    p95_value   DOUBLE PRECISION,
    samples     BIGINT,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, device_id, metric)
);

-- Data-quality findings written by the Dagster asset check.
CREATE TABLE IF NOT EXISTS dq_findings (
    id          BIGSERIAL PRIMARY KEY,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_name  TEXT NOT NULL,
    device_id   TEXT,
    detail      JSONB NOT NULL
);

-- Energy cost per device-hour (Dagster: telemetry x open data).
CREATE TABLE IF NOT EXISTS plant_hourly_cost (
    bucket    TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    cost_eur  DOUBLE PRECISION,
    PRIMARY KEY (bucket, device_id)
);

-- Open-data batch source (hourly energy prices, aligned to the same time axis).
CREATE TABLE IF NOT EXISTS market_reference (
    ts        TIMESTAMPTZ NOT NULL,
    series    TEXT NOT NULL,
    value     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ts, series)
);
