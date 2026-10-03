"""DEV ONLY: MQTT -> TimescaleDB without Kafka.

Uses the same validator as the bridge and the same idempotent write_batch as the
stream consumer, so the DB/Dagster/API layers can be exercised on a laptop without a
broker. Production path is always edge -> MQTT -> bridge -> Kafka -> stream.consumer.
"""

import logging
import queue
import sys
import time

import paho.mqtt.client as mqtt
import psycopg

from bridge.mqtt_to_kafka import validate
from common.settings import settings
from stream.consumer import BATCH_MAX_WAIT_S, write_batch

log = logging.getLogger("dev_sink")


def main(seconds: float) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    q: queue.Queue = queue.Queue()

    def on_msg(_c, _u, m):
        try:
            r = validate(m.topic, m.payload)
        except Exception:  # noqa: BLE001
            return
        q.put((r.ts, r.device_id, r.metric, r.value, r.source, str(r.event_id)))

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dev-sink")
    c.on_message = on_msg
    c.connect(settings.mqtt_host, settings.mqtt_port)
    c.subscribe("telemetry/#", qos=1)
    c.loop_start()
    total = 0
    deadline = time.monotonic() + seconds
    with psycopg.connect(settings.database_url) as conn:
        while time.monotonic() < deadline:
            time.sleep(BATCH_MAX_WAIT_S)
            rows = []
            while not q.empty():
                rows.append(q.get_nowait())
            total += write_batch(conn, rows)
    c.loop_stop()
    log.info("wrote %d rows in %.0fs", total, seconds)


if __name__ == "__main__":
    main(float(sys.argv[1]) if len(sys.argv) > 1 else 60)
