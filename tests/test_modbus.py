from datetime import UTC, datetime

from edge.modbus_poller import decode_registers

AT = datetime(2026, 10, 1, tzinfo=UTC)


def test_registers_scaled_and_named():
    out = {r.metric: r.value for r in decode_registers([125000, 35012, 1480], AT)}
    assert out == {"flow_lpm": 1250.0, "pressure_bar": 350.12, "rpm": 1480.0}


def test_all_readings_tagged_modbus_and_device():
    rs = decode_registers([1, 2, 3], AT)
    assert {r.source for r in rs} == {"modbus"}
    assert {r.device_id for r in rs} == {"plc-001"}
    assert all(r.ts == AT for r in rs)


def test_short_read_only_decodes_available_registers():
    assert [r.metric for r in decode_registers([100], AT)] == ["flow_lpm"]
