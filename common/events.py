"""Canonical telemetry event contract shared by every stage of the pipeline.

Every producer (MQTT devices, MODBUS poller, open-data loaders) must emit this shape;
every consumer validates against it. Schema drift fails fast at the edge, not in the DB.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

import orjson
from pydantic import BaseModel, Field, field_validator

Source = Literal["mqtt", "modbus", "open-data"]

# MQTT topic layout: telemetry/<site>/<device_id>/<metric>
TOPIC_PREFIX = "telemetry"
KAFKA_TOPIC = "telemetry.readings"
KAFKA_DLQ_TOPIC = "telemetry.readings.dlq"


class Reading(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    ts: datetime
    device_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_\-.]+$")
    metric: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    value: float
    source: Source

    @field_validator("ts")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("ts must be timezone-aware")
        return v.astimezone(UTC)

    @field_validator("value")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("value must be finite")
        return v

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_bytes(cls, raw: bytes) -> Reading:
        return cls.model_validate(orjson.loads(raw))

    @property
    def partition_key(self) -> bytes:
        # Key by device so a device's readings stay ordered within one partition.
        return self.device_id.encode()


def topic_for(site: str, device_id: str, metric: str) -> str:
    return f"{TOPIC_PREFIX}/{site}/{device_id}/{metric}"


def parse_topic(topic: str) -> tuple[str, str, str]:
    """Return (site, device_id, metric) or raise ValueError on a malformed topic."""
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != TOPIC_PREFIX or not all(parts[1:]):
        raise ValueError(f"malformed topic: {topic!r}")
    return parts[1], parts[2], parts[3]
