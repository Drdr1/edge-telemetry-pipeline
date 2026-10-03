# edge-telemetry-pipeline

[![ci](https://github.com/Drdr1/edge-telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Drdr1/edge-telemetry-pipeline/actions/workflows/ci.yml)

Reference implementation of a real-time telemetry platform: industrial edge devices
(MQTT sensors and a MODBUS/TCP PLC) streamed through Kafka into TimescaleDB, with a
Dagster batch layer, an open-data source, data-quality checks, and a secured read API.
One `docker compose up` brings it all up; GitHub Actions tests every layer.

```mermaid
flowchart LR
  subgraph Edge
    S[20 MQTT sensors<br/>10 Hz x 3 metrics] -->|MQTT QoS1| M[(Mosquitto)]
    PLC[MODBUS/TCP PLC] -->|FC3 poll 1 Hz| P[modbus_poller] -->|MQTT| M
  end
  M --> B[bridge<br/>validate, key by device]
  B -->|valid| K[(Kafka / Redpanda<br/>telemetry.readings x6)]
  B -->|invalid + reason| DLQ[(telemetry.readings.dlq)]
  K --> C1[stream consumer x2<br/>same group]
  C1 -->|COPY + idempotent upsert| T[(TimescaleDB<br/>hypertable)]
  T --> CA[1-min continuous aggregate]
  OD[Open data: day-ahead<br/>power prices] --> D
  T --> D[Dagster<br/>hourly rollup, cost model,<br/>DQ asset check]
  CA --> API[FastAPI<br/>API-key auth]
  D --> T
```

## Measured results

**Verified in GitHub Actions** (standard `ubuntu-latest` runner; numbers are in each run's job summary):

| What | Result |
|---|---|
| Full path, edge → MQTT → bridge → **Kafka** → consumer group → TimescaleDB | **588 rows/s sustained** (simulators produce 603/s; the remainder is in-flight batches), API returned live MODBUS metrics |
| DB write path benchmark, 100k rows | **~32,400 rows/s**, batch p50 47 ms / p95 90 ms / p99 130 ms |
| Replayed batch | **0 duplicate rows** |

**Local development run** on a 2-vCPU Linux container, with the database on the same box:

| What | Result |
|---|---|
| Edge leg (20 devices + PLC → Mosquitto → bridge validator) | **603 msg/s, 0 invalid** over a 10 s live window |
| Single consumer DB write path (decode + validate + COPY + dedup upsert) | **~26,600 rows/s**, batch p50 47 ms / p95 82 ms / p99 119 ms (2,000-row batches) |
| Replayed batch (crash after DB commit, before offset commit) | **0 duplicate rows**, dedup costs ~19 ms per batch |
| 90 s live run → DB | 54,270 rows, exactly 20 devices × 3 metrics × 10 Hz + 3 PLC registers × 1 Hz |
| Dagster hourly rollup, run twice on the same partition | 63 rows both times (idempotent); sample count matches raw exactly |
| API latency, 1-min series from the continuous aggregate | 18 ms |
| API security probes | no key → 401, wrong key → 401, SQL injection in path → 422, 30-day window → 422 |

The local run fed the database through `scripts/dev_mqtt_sink.py` (same validator, same
`write_batch`) because that environment could not pull broker images. The Kafka path is
covered by the `e2e-compose` CI job above, which fails the build unless rows arrive through
Kafka at a sustained rate and the API enforces auth.

## Design decisions

- **One event contract** (`common/events.py`). Every producer emits the same Pydantic
  `Reading`: timezone-aware UTC timestamps, finite values, bounded ids and snake_case
  metrics. Schema drift fails at the edge, not in the warehouse.
- **Ordering per device.** Kafka messages are keyed by `device_id`, so a device's readings
  stay ordered within one partition while the consumer group scales across partitions.
- **Exactly-once effect without exactly-once semantics.** Offsets are committed only after
  the DB transaction commits. Duplicates from at-least-once delivery are dropped by a
  unique `(event_id, ts)` index. This is tested against a real TimescaleDB.
- **Dead-letter, never drop.** Malformed payloads, or a topic that disagrees with its
  payload, go to `telemetry.readings.dlq` with the error attached.
- **Hot path and batch path are separate.** The continuous aggregate (refreshed every
  10 s) serves real-time reads. Dagster owns the heavier, partitioned, re-runnable work
  (p95 rollups, joins with open data, data quality).
- **Late data is handled explicitly.** The aggregate's `start_offset` is set to cover
  how long a gateway can buffer offline. Anything older needs a backfill (an
  idempotent Dagster partition re-run).
- **Storage lifecycle.** Hourly chunks, compression after 1 day (segmented by device
  and metric), 30-day raw retention. Rollups are kept long term.
- **Edge resilience.** The MODBUS poller reconnects with exponential backoff (1 → 30 s),
  and register maps and scaling are declared in one table.
- **API security.** Keys are compared in constant time and the API fails closed when
  no keys are configured. All SQL is parameterised. Ids, metrics, limits and time
  windows are bounded and validated.

## Run it

```bash
docker compose up -d --build
python -m scripts.e2e_check            # asserts edge -> Kafka -> DB -> API
open http://localhost:3000             # Dagster UI: assets, schedules, checks
curl -H "X-API-Key: dev-key-change-me" "http://localhost:8000/v1/latest?device_id=plc-001"
```

Tests:

```bash
pip install -e ".[dev]"
pytest -m "not integration"                       # unit tests, no services needed
TEST_DATABASE_URL=postgresql://telemetry:telemetry@localhost:5432/telemetry pytest -m integration
python -m scripts.bench_ingest 200000             # DB write-path benchmark
```

## Layout

```
edge/        mqtt_simulator, modbus_server (PLC stand-in), modbus_poller
bridge/      MQTT -> Kafka with validation + DLQ
stream/      Kafka -> TimescaleDB consumer (batched COPY, idempotent)
pipelines/   Dagster assets, schedules, asset check; open_data + quality (pure fns)
api/         FastAPI read API
sql/         schema: hypertable, compression, retention, continuous aggregate
scripts/     bench_ingest, e2e_check, dev_mqtt_sink
tests/       unit tests + DB integration tests
```

## Path to production on GCP

| Local | Production |
|---|---|
| docker compose | GKE (Autopilot or Standard), one Deployment per component, HPA on consumer lag |
| Redpanda | Managed Service for Apache Kafka (same client code), or Pub/Sub via a thin adapter |
| Mosquitto | EMQX / HiveMQ cluster with per-device mTLS (Google retired Cloud IoT Core in 2023, so the MQTT broker is self-managed or partner-run) |
| TimescaleDB container | Timescale on GKE with PVC + backups, or Timescale Cloud. ClickHouse is an option for very wide analytical scans |
| `dagster dev` | Dagster OSS on GKE (Helm) with Postgres run storage, or Dagster+ |
| API keys in env | Secret Manager + Workload Identity, keys rotated, behind a GCLB with Cloud Armor |
| — | Terraform for all of the above, GitHub Actions → Artifact Registry → GKE with OIDC (no long-lived keys) |
| — | Prometheus/Grafana: consumer lag, end-to-end latency (event ts → ingested_at), DLQ rate, stale devices |
