"""MQTT -> Kafka bridge.

Subscribes to telemetry/#, validates each payload against the Reading contract,
and produces to Kafka keyed by device_id (ordering per device). Malformed messages
go to a dead-letter topic with the reason, never silently dropped.
"""

from __future__ import annotations

import logging
import signal
import threading

import orjson
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

from common.events import KAFKA_DLQ_TOPIC, KAFKA_TOPIC, Reading, parse_topic
from common.settings import settings

log = logging.getLogger("bridge")


def validate(topic: str, payload: bytes) -> Reading:
    """Topic and payload must agree; payload is authoritative for ts/value."""
    _site, device_id, metric = parse_topic(topic)
    r = Reading.from_bytes(payload)
    if r.device_id != device_id or r.metric != metric:
        raise ValueError(f"topic/payload mismatch: {topic} vs {r.device_id}/{r.metric}")
    return r


class Bridge:
    def __init__(self, producer: Producer) -> None:
        self.producer = producer
        self.forwarded = 0
        self.dead_lettered = 0

    def handle(self, topic: str, payload: bytes) -> None:
        try:
            r = validate(topic, payload)
        except Exception as e:  # noqa: BLE001
            self.dead_lettered += 1
            self.producer.produce(
                KAFKA_DLQ_TOPIC,
                value=orjson.dumps(
                    {"topic": topic, "payload": payload.decode(errors="replace"), "error": str(e)}
                ),
            )
            return
        self.producer.produce(
            KAFKA_TOPIC, key=r.partition_key, value=r.to_bytes(), on_delivery=self._ack
        )
        self.forwarded += 1
        self.producer.poll(0)

    def _ack(self, err, msg) -> None:
        if err is not None:
            log.error("delivery failed: %s", err)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
        }
    )
    bridge = Bridge(producer)
    stop = threading.Event()

    def on_message(_c, _u, msg: mqtt.MQTTMessage) -> None:
        bridge.handle(msg.topic, msg.payload)

    def on_connect(c, _u, _f, rc, _p=None) -> None:
        log.info("mqtt connected rc=%s; subscribing", rc)
        c.subscribe("telemetry/#", qos=1)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    client.loop_start()

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.wait(10):
        log.info("forwarded=%d dead_lettered=%d", bridge.forwarded, bridge.dead_lettered)
    client.loop_stop()
    producer.flush(10)


if __name__ == "__main__":
    main()
