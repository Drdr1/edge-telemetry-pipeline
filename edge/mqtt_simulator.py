"""Simulates N edge devices publishing sensor readings over MQTT at a fixed rate.

Stand-in for real gateways. Each device drifts around a baseline with noise and
occasionally emits a spike so the data-quality checks have something to catch.
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import UTC, datetime

import paho.mqtt.client as mqtt

from common.events import Reading, topic_for
from common.settings import settings

log = logging.getLogger("edge.sim")

# metric -> (baseline, noise). All are physically non-negative.
METRICS = {"temperature_c": (45.0, 0.3), "vibration_mm_s": (2.5, 0.05), "power_kw": (120.0, 2.0)}
REVERSION = 0.05  # pull toward baseline each tick (Ornstein-Uhlenbeck), so values don't drift


def make_reading(
    device_id: str, metric: str, state: dict[str, float], rng: random.Random
) -> Reading:
    baseline, noise = METRICS[metric]
    prev = state.get(metric, baseline)
    state[metric] = max(0.0, prev + REVERSION * (baseline - prev) + rng.gauss(0, noise))
    value = state[metric]
    if rng.random() < 0.002:  # rare spike
        value *= 3
    return Reading(
        ts=datetime.now(UTC), device_id=device_id, metric=metric, value=value, source="mqtt"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    n = int(os.environ.get("DEVICE_COUNT", "5"))
    hz = float(os.environ.get("HZ", "2"))
    rng = random.Random(42)
    devices = [f"dev-{i:03d}" for i in range(n)]
    states: dict[str, dict[str, float]] = {d: {} for d in devices}

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-sim")
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    client.loop_start()
    log.info("publishing %d devices x %d metrics at %.1f Hz", n, len(METRICS), hz)
    try:
        while True:
            t0 = time.monotonic()
            for d in devices:
                for m in METRICS:
                    r = make_reading(d, m, states[d], rng)
                    client.publish(topic_for(settings.site, d, m), r.to_bytes(), qos=1)
            time.sleep(max(0.0, 1.0 / hz - (time.monotonic() - t0)))
    finally:
        client.loop_stop()


if __name__ == "__main__":
    main()
