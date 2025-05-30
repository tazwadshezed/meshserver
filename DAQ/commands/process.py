import asyncio
from bson import BSON
from DAQ.util.handlers import IHandler
from DAQ.util.logger import make_logger
from DAQ.util.config import get_topic
from DAQ.services.core.broker.nats_manager import nats_manager

class NATSCommands(IHandler):
    def __init__(self):
        super().__init__()
        self.logger = make_logger(self.__class__.__name__)
        self.command_topic = get_topic("command")
        self.response_topic = get_topic("response")

    async def worker(self, data_queue, processed_queue):
        await nats_manager.connect()

        async def handle_command(msg):
            try:
                payload = BSON(msg.data).decode()
                self.logger.info(f"[NATSCommands] Received command: {payload}")
                processed_queue.put([True, payload, False])
            except Exception as e:
                self.logger.error(f"[NATSCommands] Decode error: {e}")

        await nats_manager.nats.subscribe(self.command_topic, cb=handle_command)
        self.logger.info(f"[NATSCommands] Subscribed to: {self.command_topic}")

        while self._check_living():
            await asyncio.sleep(1)

            if not data_queue.empty():
                try:
                    payload = data_queue.get()
                    await nats_manager.nats.publish(self.response_topic, BSON.encode(payload))
                    self.logger.info(f"[NATSCommands] Published response to: {self.response_topic}")
                except Exception as e:
                    self.logger.warning(f"[NATSCommands] Publish failed: {e}")

        self.logger.info("[NATSCommands] Exiting...")
        await nats_manager.disconnect()
