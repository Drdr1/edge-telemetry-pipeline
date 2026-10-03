"""Open-data source: hourly day-ahead electricity prices from Energy-Charts (Fraunhofer ISE).

Public API, no key. Used as an external reference series aligned with plant
telemetry (e.g. power_kw x price = cost per hour). Parsing is a pure function,
unit-tested against a fixture in the API's documented response shape
(license_info, unix_seconds, price, unit, deprecated) with synthetic values.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

ENERGY_CHARTS_URL = "https://api.energy-charts.info/price"


def parse_prices(payload: dict, series: str) -> list[tuple[datetime, str, float]]:
    ts = payload.get("unix_seconds") or []
    prices = payload.get("price") or []
    if len(ts) != len(prices):
        raise ValueError(f"length mismatch: {len(ts)} timestamps vs {len(prices)} prices")
    rows = []
    for t, p in zip(ts, prices, strict=True):
        if p is None:
            continue  # gaps are expected; don't invent values
        rows.append((datetime.fromtimestamp(int(t), tz=UTC), series, float(p)))
    return rows


def fetch_prices(bidding_zone: str, start: str, end: str, timeout: float = 20.0) -> dict:
    r = httpx.get(
        ENERGY_CHARTS_URL,
        params={"bzn": bidding_zone, "start": start, "end": end},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()
