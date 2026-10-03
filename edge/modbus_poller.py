"""Polls a MODBUS/TCP device and republishes registers as canonical MQTT readings.

This is the usual edge pattern: legacy fieldbus -> MQTT -> broker. Register map and
scaling live in one place so adding a PLC is a config change, not a code change.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

from common.events import Reading, topic_for
from common.settings import settings

log = logging.getLogger("edge.modbus_poller")

# address -> (metric, scale)
REGISTER_MAP: dict[int, tuple[str, float]] = {
    0: ("flow_lpm", 0.01),
    1: ("pressure_bar", 0.01),
    2: ("rpm", 1.0),
}
DEVICE_ID = "plc-001"


def decode_registers(regs: list[int], at: datetime) -> list[Reading]:
    """Pure function: raw register values -> canonical readings (unit-tested)."""
    out = []
    for addr, (metric, scale) in REGISTER_MAP.items():
        if addr < len(regs):
            out.append(
                Reading(
                    ts=at,
                    device_id=DEVICE_ID,
                    metric=metric,
                    value=regs[addr] * scale,
                    source="modbus",
                )
            )
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    mq = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="modbus-poller")
    mq.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    mq.loop_start()
    mb = ModbusTcpClient(settings.modbus_host, port=settings.modbus_port)
    backoff = 1.0
    while True:
        try:
            if not mb.connected and not mb.connect():
                raise ConnectionError("modbus connect failed")
            rr = mb.read_holding_registers(0, count=len(REGISTER_MAP))
            if rr.isError():
                raise OSError(str(rr))
            for r in decode_registers(rr.registers, datetime.now(UTC)):
                mq.publish(topic_for(settings.site, r.device_id, r.metric), r.to_bytes(), qos=1)
            backoff = 1.0
            time.sleep(1.0)
        except Exception as e:  # noqa: BLE001 - poller must survive flaky PLCs
            log.warning("poll failed: %s; retry in %.0fs", e, backoff)
            mb.close()
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()
