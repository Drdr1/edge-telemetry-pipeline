"""Runs against a real TimescaleDB (docker compose or the CI service container)."""

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

psycopg = pytest.importorskip("psycopg")

from stream.consumer import write_batch  # noqa: E402

pytestmark = pytest.mark.integration
DB = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def conn():
    if not DB:
        pytest.skip("TEST_DATABASE_URL not set")
    with psycopg.connect(DB) as c:
        yield c
        with c.cursor() as cur:
            cur.execute("DELETE FROM readings WHERE device_id LIKE 'it-%'")
        c.commit()


def _rows(n, device="it-dev"):
    t0 = datetime.now(UTC) - timedelta(minutes=1)
    return [
        (t0 + timedelta(milliseconds=i), device, "power_kw", float(i), "mqtt", str(uuid.uuid4()))
        for i in range(n)
    ]


def test_write_batch_inserts_all(conn):
    assert write_batch(conn, _rows(500)) == 500


def test_replayed_batch_is_deduplicated(conn):
    rows = _rows(200, "it-replay")
    assert write_batch(conn, rows) == 200
    # simulate crash after DB commit but before Kafka offset commit -> same batch redelivered
    assert write_batch(conn, rows) == 0
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM readings WHERE device_id = 'it-replay'")
        assert cur.fetchone()[0] == 200


def test_partial_overlap_inserts_only_new(conn):
    rows = _rows(100, "it-overlap")
    write_batch(conn, rows[:60])
    assert write_batch(conn, rows[40:]) == 40
