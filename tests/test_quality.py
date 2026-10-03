from datetime import UTC, datetime, timedelta

from pipelines.quality import spikes, stale_devices

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def test_stale_devices_flags_only_silent_ones():
    last = {"a": NOW - timedelta(seconds=30), "b": NOW - timedelta(minutes=5)}
    f = stale_devices(last, NOW, timedelta(minutes=2))
    assert [x.device_id for x in f] == ["b"]
    assert f[0].detail["silence_s"] == 300


def test_spike_detection_uses_z_score():
    rows = [
        ("a", "power_kw", 120.0, 120.0, 2.0),  # z=0
        ("b", "power_kw", 360.0, 120.0, 2.0),  # z=120
        ("c", "power_kw", 50.0, 50.0, 0.0),  # zero std ignored, not div-by-zero
        ("d", "power_kw", 50.0, 50.0, None),  # single sample -> NULL stddev
    ]
    f = spikes(rows)
    assert [x.device_id for x in f] == ["b"]
    assert f[0].detail["z"] == 120.0
