from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from common.events import Reading, parse_topic, topic_for

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _r(**kw):
    base = dict(ts=NOW, device_id="dev-001", metric="temperature_c", value=45.2, source="mqtt")
    base.update(kw)
    return Reading(**base)


def test_roundtrip_bytes_preserves_event():
    r = _r()
    assert Reading.from_bytes(r.to_bytes()) == r


def test_timestamps_normalised_to_utc():
    cairo = timezone(timedelta(hours=3))
    r = _r(ts=datetime(2026, 10, 1, 15, 0, tzinfo=cairo))
    assert r.ts == NOW


@pytest.mark.parametrize(
    "bad",
    [
        {"ts": datetime(2026, 10, 1, 12, 0)},  # naive
        {"value": float("nan")},
        {"value": float("inf")},
        {"device_id": "dev 001"},  # space
        {"device_id": "dev/001"},  # would break topic routing
        {"metric": "Temperature"},  # must be snake_case
        {"source": "carrier-pigeon"},
    ],
)
def test_rejects_invalid(bad):
    with pytest.raises(ValidationError):
        _r(**bad)


def test_partition_key_is_device():
    assert _r().partition_key == b"dev-001"


def test_topic_roundtrip():
    t = topic_for("plant-a", "dev-001", "power_kw")
    assert t == "telemetry/plant-a/dev-001/power_kw"
    assert parse_topic(t) == ("plant-a", "dev-001", "power_kw")


@pytest.mark.parametrize(
    "topic", ["telemetry/plant-a/dev-001", "other/a/b/c", "telemetry//dev/m", "telemetry/a/b/c/d"]
)
def test_parse_topic_rejects_malformed(topic):
    with pytest.raises(ValueError):
        parse_topic(topic)
