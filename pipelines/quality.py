"""Pure data-quality rules, kept free of Dagster/DB so they are trivially testable."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class Finding:
    check: str
    device_id: str | None
    detail: dict


def stale_devices(
    last_seen: dict[str, datetime], now: datetime, max_silence: timedelta
) -> list[Finding]:
    """A device that hasn't reported within max_silence is flagged."""
    return [
        Finding(
            "stale_device",
            d,
            {"last_seen": ts.isoformat(), "silence_s": (now - ts).total_seconds()},
        )
        for d, ts in sorted(last_seen.items())
        if now - ts > max_silence
    ]


def spikes(
    rows: list[tuple[str, str, float, float, float]], z_threshold: float = 4.0
) -> list[Finding]:
    """rows: (device_id, metric, latest_value, mean, stddev). Flags |z| > threshold."""
    out = []
    for device_id, metric, value, mean, std in rows:
        if std and std > 0:
            z = (value - mean) / std
            if abs(z) > z_threshold:
                out.append(
                    Finding(
                        "spike", device_id, {"metric": metric, "value": value, "z": round(z, 2)}
                    )
                )
    return out
