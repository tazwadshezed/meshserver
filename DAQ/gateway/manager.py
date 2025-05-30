"""
Async GatewayManager that launches:
- Gateway TCP server (emulator input)
- UDP autodiscovery responder

Integrated with asyncio.Queue and DAQProcess.
"""

import asyncio
from DAQ.gateway.server import start_gateway_servers
from DAQ.util.logger import make_logger
from DAQ.util.config import load_config

logger = make_logger("GatewayManager")
cfg = load_config()

class GatewayManager:
    def __init__(self, host: str, port: int, recv_queue: asyncio.Queue):
        self.host = host
        self.port = port
        self.recv_queue = recv_queue

        self.tcp_server = None
        self.udp_transport = None

    async def start(self):
        logger.info("Starting GatewayManager...")
        self.tcp_server, self.udp_transport = await start_gateway_servers(self.recv_queue)
        logger.info("GatewayManager started.")

    async def stop(self):
        logger.info("Stopping GatewayManager...")

        if self.tcp_server:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()
            self.tcp_server = None
            logger.info("TCP server closed.")

        if self.udp_transport:
            self.udp_transport.close()
            self.udp_transport = None
            logger.info("UDP transport closed.")

    def send_all(self, message):
        logger.debug("send_all() called but not implemented in async GatewayManager")
