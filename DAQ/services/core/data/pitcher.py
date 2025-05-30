import asyncio
import queue
from nats.aio.client import Client as NATS
from DAQ.util.handlers.common import IHandler
from DAQ.util.logger import make_logger
from DAQ.util.config import get_topic, load_config

cfg = load_config()
logger = make_logger("Pitcher")

external_server = cfg["nats"]["external_publish_server"]
external_topic = get_topic("external_mesh")


class Pitcher(IHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # your custom init logic here (if any)

        self.logger = make_logger(self.__class__.__name__)
        self.ext_nats = NATS()
        self.connected = False
        self.throttle_delay = cfg.get("daq", {}).get("throttle_delay", 0.01)
        self.subject = external_topic

    async def connect(self):
        if not self.connected:
            await self.ext_nats.connect(servers=[external_server])
            self.connected = True
            self.logger.info(f"[Pitcher] Connected to external NATS at {external_server}")

    async def publish(self, payload: bytes):
        await self.ext_nats.publish(self.subject, payload)
        self.logger.info(f"[Pitcher] Published {len(payload)} bytes to: {self.subject}")

    def worker(self, data_queue, processed_queue):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def mainloop():
            await self.connect()
            while self._check_living():
                try:
                    data = data_queue.get(timeout=1)
                    await self.publish(data)
                    await asyncio.sleep(self.throttle_delay)
                except queue.Empty:
                    await asyncio.sleep(0.05)
                except Exception as e:
                    self.logger.exception(f"[Pitcher] Publish failed: {e}")

        try:
            loop.run_until_complete(mainloop())
        finally:
            try:
                loop.run_until_complete(self.ext_nats.close())
            except Exception:
                self.logger.warning("[Pitcher] Failed to close NATS connection")
            self.connected = False
            loop.close()
