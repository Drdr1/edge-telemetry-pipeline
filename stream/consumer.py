"""Kafka -> TimescaleDB stream consumer.

Exactly-once *effect* without exactly-once semantics: batches are written with
COPY into the hypertable, duplicates are rejected by the (event_id, ts) unique index,
and offsets are committed only after the DB transaction commits. A crash between
commit and offset commit replays the batch, which is then de-duplicated.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import Consumer, KafkaError

from common.events import KAFKA_TOPIC, Reading
from common.settings import settings

log = logging.getLogger("stream")

BATCH_SIZE = 2000
BATCH_MAX_WAIT_S = 0.5


@dataclass
class Batch:
    rows: list[tuple] = field(default_factory=list)
    started: float = field(default_factory=time.monotonic)

    def add(self, r: Reading) -> None:
        self.rows.append((r.ts, r.device_id, r.metric, r.value, r.source, str(r.event_id)))

    def ready(self) -> bool:
        return len(self.rows) >= BATCH_SIZE or (
            self.rows and time.monotonic() - self.started > BATCH_MAX_WAIT_S
        )


def write_batch(conn: psycopg.Connection, rows: list[tuple]) -> int:
    """Insert rows idempotently; returns number actually inserted (dupes excluded)."""
    if not rows:
        return 0
    with conn.cursor() as cur, conn.transaction():
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _in (LIKE readings INCLUDING DEFAULTS) "
            "ON COMMIT DELETE ROWS"
        )
        with cur.copy("COPY _in (ts, device_id, metric, value, source, event_id) FROM STDIN") as cp:
            for row in rows:
                cp.write_row(row)
        cur.execute(
            "INSERT INTO readings (ts, device_id, metric, value, source, event_id) "
            "SELECT ts, device_id, metric, value, source, event_id FROM _in "
            "ON CONFLICT (event_id, ts) DO NOTHING"
        )
        return cur.rowcount


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "timescale-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit after the DB commit
            "max.poll.interval.ms": 300000,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    running = True

    def _stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    with psycopg.connect(settings.database_url, autocommit=False) as conn:
        batch = Batch()
        total = 0
        while running:
            msg = consumer.poll(0.1)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", msg.error())
                    continue
                try:
                    batch.add(Reading.from_bytes(msg.value()))
                except Exception as e:  # noqa: BLE001 - bad record already validated upstream; log & skip
                    log.error(
                        "skipping unparseable record at %s:%d: %s", msg.partition(), msg.offset(), e
                    )
            if batch.ready():
                t0 = time.perf_counter()
                inserted = write_batch(conn, batch.rows)
                consumer.commit(asynchronous=False)
                total += inserted
                log.info(
                    "wrote %d/%d rows in %.0fms (total=%d)",
                    inserted,
                    len(batch.rows),
                    (time.perf_counter() - t0) * 1000,
                    total,
                )
                batch = Batch()
        if batch.rows:
            write_batch(conn, batch.rows)
            consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    main()
