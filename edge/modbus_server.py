"""Minimal MODBUS/TCP slave that behaves like a PLC. Holding registers (protocol
addresses 0-2), refreshed every second, as unsigned 16-bit ints:
  HR0 flow_lpm x100 | HR1 pressure_bar x100 | HR2 rpm x1
The scaling matches edge.modbus_poller.REGISTER_MAP."""

from __future__ import annotations

import asyncio
import logging
import random

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.datastore import ModbusDeviceContext, ModbusSequentialDataBlock, ModbusServerContext
from pymodbus.server import StartAsyncTcpServer

from common.settings import settings

log = logging.getLogger("edge.modbus_server")


async def _process(port: int) -> None:
    """The 'physical process': writes fresh values into the PLC over MODBUS (FC16),
    exactly like a field controller would. Going through the protocol instead of
    poking the datastore keeps this independent of pymodbus datastore internals."""
    rng = random.Random(7)
    flow, pressure, rpm = 125.0, 3.5, 1480.0  # L/min, bar, rpm
    client = AsyncModbusTcpClient("127.0.0.1", port=port)
    while not client.connected:
        await client.connect()
        await asyncio.sleep(0.2)
    while True:
        flow = max(0.0, flow + rng.gauss(0, 0.5))
        pressure = max(0.0, pressure + rng.gauss(0, 0.02))
        rpm = max(0.0, rpm + rng.gauss(0, 3))
        regs = [min(round(v), 0xFFFF) for v in (flow * 100, pressure * 100, rpm)]  # 16-bit
        await client.write_registers(0, regs)
        await asyncio.sleep(1)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # Device contexts map protocol address N to block address N+1, so the block starts at 1.
    block = ModbusSequentialDataBlock(1, [0] * 10)
    context = ModbusServerContext(devices=ModbusDeviceContext(hr=block), single=True)
    asyncio.create_task(_process(settings.modbus_port))
    log.info("MODBUS/TCP slave on 0.0.0.0:%d", settings.modbus_port)
    await StartAsyncTcpServer(context=context, address=("0.0.0.0", settings.modbus_port))


if __name__ == "__main__":
    asyncio.run(main())
