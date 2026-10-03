from datetime import UTC, datetime

import orjson
import pytest

from bridge.mqtt_to_kafka import Bridge, validate
from common.events import KAFKA_DLQ_TOPIC, KAFKA_TOPIC, Reading

R = Reading(
    ts=datetime(2026, 10, 1, tzinfo=UTC),
    device_id="dev-007",
    metric="power_kw",
    value=1.0,
    source="mqtt",
)
GOOD_TOPIC = "telemetry/plant-a/dev-007/power_kw"


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    def produce(self, topic, value, key=None, on_delivery=None):
        self.sent.append((topic, key, value))

    def poll(self, _t):
        return 0


def test_validate_accepts_matching_topic():
    assert validate(GOOD_TOPIC, R.to_bytes()) == R


def test_validate_rejects_topic_payload_mismatch():
    with pytest.raises(ValueError, match="mismatch"):
        validate("telemetry/plant-a/dev-999/power_kw", R.to_bytes())


def test_good_message_forwarded_keyed_by_device():
    p = FakeProducer()
    b = Bridge(p)
    b.handle(GOOD_TOPIC, R.to_bytes())
    assert b.forwarded == 1 and b.dead_lettered == 0
    topic, key, value = p.sent[0]
    assert topic == KAFKA_TOPIC and key == b"dev-007"
    assert Reading.from_bytes(value) == R


@pytest.mark.parametrize("payload", [b"not json", b"{}", orjson.dumps({"value": "hot"})])
def test_bad_message_goes_to_dlq_with_reason(payload):
    p = FakeProducer()
    b = Bridge(p)
    b.handle(GOOD_TOPIC, payload)
    assert b.forwarded == 0 and b.dead_lettered == 1
    topic, _key, value = p.sent[0]
    assert topic == KAFKA_DLQ_TOPIC
    body = orjson.loads(value)
    assert body["topic"] == GOOD_TOPIC and body["error"]
