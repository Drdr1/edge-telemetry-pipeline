"""Ingest benchmark for the stream consumer's DB write path.

Generates N readings, round-trips each through the wire format (bytes, as they
arrive from Kafka), and writes them with stream.consumer.write_batch in BATCH_SIZE
chunks. Reports sustained rows/s and per-batch latency percentiles, then replays the
first batch to show dedup cost. Usage: python -m scripts.bench_ingest 200000
"""

import statistics
import sys
import time
from datetime import UTC, datetime, timedelta

import psycopg

from common.events import Reading
from common.settings import settings
from stream.consumer import BATCH_SIZE, write_batch


def main(n: int) -> None:
    t0 = datetime.now(UTC) - timedelta(minutes=30)
    wire = [
        Reading(
            ts=t0 + timedelta(microseconds=i * 500),
            device_id=f"bench-{i % 200:03d}",
            metric=("temperature_c", "vibration_mm_s", "power_kw")[i % 3],
            value=float(i % 1000),
            source="mqtt",
        ).to_bytes()
        for i in range(n)
    ]
    lat = []
    inserted = 0
    with psycopg.connect(settings.database_url) as conn:
        start = time.perf_counter()
        first = None
        for off in range(0, n, BATCH_SIZE):
            rs = [Reading.from_bytes(b) for b in wire[off : off + BATCH_SIZE]]
            rows = [(r.ts, r.device_id, r.metric, r.value, r.source, str(r.event_id)) for r in rs]
            first = first or rows
            b0 = time.perf_counter()
            inserted += write_batch(conn, rows)
            lat.append((time.perf_counter() - b0) * 1000)
        elapsed = time.perf_counter() - start
        r0 = time.perf_counter()
        replayed = write_batch(conn, first)
        replay_ms = (time.perf_counter() - r0) * 1000
        with conn.cursor() as cur:
            cur.execute("DELETE FROM readings WHERE device_id LIKE 'bench-%'")
        conn.commit()
    q = statistics.quantiles(lat, n=100)
    print(f"rows={n} inserted={inserted} batches={len(lat)} batch_size={BATCH_SIZE}")
    print(f"throughput={n / elapsed:,.0f} rows/s (decode + validate + COPY + upsert)")
    print(f"batch latency ms: p50={q[49]:.1f} p95={q[94]:.1f} p99={q[98]:.1f}")
    print(f"replayed batch: inserted={replayed} in {replay_ms:.1f} ms (dedup)")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100_000)
