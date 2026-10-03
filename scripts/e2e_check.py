"""End-to-end assertion against the running docker compose stack.

Proves the full production path: edge (MQTT + MODBUS) -> bridge -> Kafka -> stream
consumer -> TimescaleDB -> continuous aggregate -> secured API. Exits non-zero on failure.
"""

import os
import sys
import time

import httpx
import psycopg

DB = os.environ.get("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
API = os.environ.get("API_URL", "http://localhost:8000")
KEY = os.environ.get("API_KEY", "dev-key-change-me")
TIMEOUT_S = int(os.environ.get("E2E_TIMEOUT_S", "180"))


def counts(conn) -> dict[str, int]:
    rows = conn.execute("SELECT source, count(*) FROM readings GROUP BY 1").fetchall()
    return dict(rows)


def main() -> int:
    deadline = time.monotonic() + TIMEOUT_S
    c: dict[str, int] = {}
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(DB, connect_timeout=3) as conn:
                c = counts(conn)
            if c.get("mqtt", 0) >= 2000 and c.get("modbus", 0) >= 10:
                break
        except psycopg.OperationalError:
            pass
        time.sleep(3)
    else:
        print(f"FAIL: rows did not arrive in {TIMEOUT_S}s: {c}")
        return 1
    print(f"OK readings via Kafka: {c}")

    # rate check: rows must keep flowing (consumer not stuck)
    with psycopg.connect(DB) as conn:
        a = sum(counts(conn).values())
        time.sleep(10)
        b = sum(counts(conn).values())
    rate = (b - a) / 10
    print(f"OK sustained ingest: {rate:,.0f} rows/s")
    if rate < 100:
        print("FAIL: ingest rate too low")
        return 1

    url = f"{API}/v1/latest?device_id=plc-001"
    if httpx.get(url, timeout=5).status_code != 401:
        print("FAIL: API served data without a key")
        return 1
    r = httpx.get(url, headers={"X-API-Key": KEY}, timeout=5)
    metrics = {x["metric"] for x in r.json()} if r.status_code == 200 else set()
    if metrics != {"flow_lpm", "pressure_bar", "rpm"}:
        print(f"FAIL: /v1/latest returned {r.status_code} {r.text[:200]}")
        return 1
    print(f"OK API: auth enforced, latest MODBUS metrics {sorted(metrics)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
