import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipelines.open_data import parse_prices

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "energy_charts_price.json").read_text())


def test_parse_recorded_payload():
    rows = parse_prices(FIXTURE, "s")
    assert rows[0] == (datetime(2026, 10, 1, 0, 0, tzinfo=UTC), "s", 92.31)
    assert all(r[1] == "s" for r in rows)


def test_gaps_are_skipped_not_filled():
    rows = parse_prices(FIXTURE, "s")
    assert len(rows) == len(FIXTURE["price"]) - FIXTURE["price"].count(None)


def test_length_mismatch_raises():
    with pytest.raises(ValueError, match="mismatch"):
        parse_prices({"unix_seconds": [1, 2], "price": [1.0]}, "s")


def test_empty_payload_is_empty():
    assert parse_prices({}, "s") == []
