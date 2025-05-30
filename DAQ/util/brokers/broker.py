import asyncio
from nats.aio.client import Client as NATS
from DAQ.util.logger import make_logger
from DAQ.util.config import load_config

logger = make_logger("LocalNATSBroker")

class LocalNATSPubSub:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.nc = NATS()
        self.connected = False
        self.server = load_config()["nats"]["server"]

    async def connect(self):
        if not self.connected:
            await self.nc.connect(servers=[self.server])
            self.connected = True
            logger.info(f"[LocalNATSBroker] Connected to NATS at {self.server}")

    async def publish(self, subject, payload: bytes):
        if not self.connected:
            await self.connect()
        await self.nc.publish(subject, payload)
        logger.debug(f"[LocalNATSBroker] Published to {subject}: {len(payload)} bytes")

    async def subscribe(self, subject, callback):
        if not self.connected:
            await self.connect()
        await self.nc.subscribe(subject, cb=callback)
        logger.info(f"[LocalNATSBroker] Subscribed to {subject}")

    async def close(self):
        if self.connected:
            await self.nc.close()
            self.connected = False
            logger.info("[LocalNATSBroker] Closed NATS connection")

# Singleton instance
local_nats_broker = LocalNATSPubSub()
