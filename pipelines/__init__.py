"""Dagster code location: batch layer on top of the streaming hot path.

- readings_hourly       : hourly partitioned rollup (avg, p95) from the raw hypertable
- market_reference      : open-data ingest (day-ahead power prices), daily partitions
- plant_hourly_cost     : join of telemetry power_kw with price -> cost per device-hour
- telemetry_freshness   : asset check (stale devices, spikes) written to dq_findings
Schedules: hourly rollup at :05, daily open-data at 13:30 UTC (after day-ahead auction).
"""

import json
from datetime import UTC, datetime, timedelta

import psycopg
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    Backoff,
    DailyPartitionsDefinition,
    Definitions,
    HourlyPartitionsDefinition,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    asset_check,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from common.settings import settings
from pipelines.open_data import fetch_prices, parse_prices
from pipelines.quality import spikes, stale_devices

hourly = HourlyPartitionsDefinition(start_date="2026-01-01-00:00", timezone="UTC")
daily = DailyPartitionsDefinition(start_date="2026-01-01", timezone="UTC")
retry = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL)

PRICE_SERIES = "day_ahead_eur_mwh_de_lu"


def _conn() -> psycopg.Connection:
    return psycopg.connect(settings.database_url)


@asset(partitions_def=hourly, retry_policy=retry, group_name="telemetry")
def readings_hourly(context: AssetExecutionContext) -> MaterializeResult:
    """Idempotent: re-running a partition overwrites exactly that hour."""
    start, end = context.partition_time_window
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM readings_hourly WHERE bucket >= %s AND bucket < %s", (start, end))
        cur.execute(
            """
            INSERT INTO readings_hourly (bucket, device_id, metric, avg_value, p95_value, samples)
            SELECT time_bucket('1 hour', ts), device_id, metric,
                   avg(value),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY value),
                   count(*)
            FROM readings
            WHERE ts >= %s AND ts < %s
            GROUP BY 1, 2, 3
            """,
            (start, end),
        )
        n = cur.rowcount
    return MaterializeResult(metadata={"rows": n, "window": f"{start}..{end}"})


@asset(partitions_def=daily, retry_policy=retry, group_name="open_data")
def market_reference(context: AssetExecutionContext) -> MaterializeResult:
    start, end = context.partition_time_window
    payload = fetch_prices("DE-LU", start.date().isoformat(), end.date().isoformat())
    rows = parse_prices(payload, series=PRICE_SERIES)
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO market_reference (ts, series, value) VALUES (%s, %s, %s) "
            "ON CONFLICT (ts, series) DO UPDATE SET value = EXCLUDED.value",
            rows,
        )
    return MaterializeResult(metadata={"rows": len(rows)})


@asset(deps=[readings_hourly, market_reference], group_name="telemetry")
def plant_hourly_cost() -> MaterializeResult:
    """Energy cost per device-hour, last 48h: avg power_kw (= kWh over 1h) x EUR/MWh / 1000."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plant_hourly_cost (bucket, device_id, cost_eur)
            SELECT h.bucket, h.device_id, h.avg_value * m.value / 1000.0
            FROM readings_hourly h
            JOIN market_reference m ON m.ts = h.bucket AND m.series = %s
            WHERE h.metric = 'power_kw' AND h.bucket >= now() - interval '48 hours'
            ON CONFLICT (bucket, device_id) DO UPDATE SET cost_eur = EXCLUDED.cost_eur
            """,
            (PRICE_SERIES,),
        )
        n = cur.rowcount
    return MaterializeResult(metadata={"rows": n})


@asset_check(asset=readings_hourly, blocking=False)
def telemetry_freshness() -> AssetCheckResult:
    now = datetime.now(UTC)
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT device_id, max(ts) FROM readings WHERE ts > now() - interval '1 day' GROUP BY 1"
        )
        last_seen = dict(cur.fetchall())
        cur.execute(
            """
            WITH recent AS (
              SELECT device_id, metric, value, ts,
                     row_number() OVER (PARTITION BY device_id, metric ORDER BY ts DESC) rn
              FROM readings WHERE ts > now() - interval '15 minutes')
            SELECT r.device_id, r.metric, r.value, s.mean, s.std
            FROM recent r
            JOIN (SELECT device_id, metric, avg(value) mean, stddev_samp(value) std
                  FROM recent GROUP BY 1, 2) s USING (device_id, metric)
            WHERE r.rn = 1
            """
        )
        findings = stale_devices(last_seen, now, timedelta(minutes=2)) + spikes(cur.fetchall())
        cur.executemany(
            "INSERT INTO dq_findings (check_name, device_id, detail) VALUES (%s, %s, %s)",
            [(f.check, f.device_id, json.dumps(f.detail)) for f in findings],
        )
    stale = sum(f.check == "stale_device" for f in findings)
    return AssetCheckResult(
        passed=stale == 0,
        metadata={"devices": len(last_seen), "stale": stale, "spikes": len(findings) - stale},
    )


hourly_job = define_asset_job("hourly_rollup", selection=[readings_hourly], partitions_def=hourly)
daily_job = define_asset_job("daily_open_data", selection=[market_reference], partitions_def=daily)
cost_job = define_asset_job("cost_model", selection=[plant_hourly_cost])

defs = Definitions(
    assets=[readings_hourly, market_reference, plant_hourly_cost],
    asset_checks=[telemetry_freshness],
    jobs=[hourly_job, daily_job, cost_job],
    schedules=[
        build_schedule_from_partitioned_job(hourly_job, minute_of_hour=5),
        build_schedule_from_partitioned_job(daily_job, hour_of_day=13, minute_of_hour=30),
        ScheduleDefinition(job=cost_job, cron_schedule="15 * * * *"),
    ],
)
