import asyncio
import threading
import subprocess
import time
from nats.aio.client import Client as NATS
from DAQ.util.config import load_config
from DAQ.util.logger import make_logger


class NATSManager:
    """
    Centralized NATS connection manager usable by both meshserver and dataserver.

    Features:
    - Background async connection thread (start/stop lifecycle)
    - Supports explicit async connect/disconnect
    - publish/subscribe methods for easy messaging
    - Optional embedded NATS server launch (meshserver only)
    """

    def __init__(self):
        self.logger = make_logger(self.__class__.__name__)
        self.config = load_config()
        self.server = self.config["nats"].get("server", "nats://localhost:4222")

        # Internal state
        self.nats = NATS()
        self.connected = False
        self._stopping = False
        self._thread = None
        self._loop = None

    def set_server(self, url: str):
        """
        Override default NATS server at runtime.
        """
        self.logger.info(f"[NATSManager] Server override: {url}")
        self.server = url

    async def _connect_forever(self):
        """
        Background loop that maintains persistent NATS connection.
        """
        while not self._stopping:
            if not self.connected:
                try:
                    await self.nats.connect(
                        servers=[self.server],
                        reconnect_time_wait=2,
                        max_reconnect_attempts=60,
                    )
                    self.connected = True
                    self.logger.info(f"[NATSManager] Connected to NATS at {self.server}")
                except Exception as e:
                    self.logger.warning(f"[NATSManager] Connect failed: {e}. Retrying in 2s...")
                    await asyncio.sleep(2)
            else:
                await asyncio.sleep(5)

    def start(self):
        """
        Start the background thread and event loop for persistent connections.
        """
        if self._thread and self._thread.is_alive():
            self.logger.warning("[NATSManager] Already running.")
            return

        self._stopping = False

        def runner():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.create_task(self._connect_forever())
            self._loop.run_forever()

        self._thread = threading.Thread(target=runner, name="NATSBackgroundLoop", daemon=True)
        self._thread.start()
        self.logger.info("[NATSManager] Event loop started in background thread.")

    def stop(self):
        """
        Stop background loop and disconnect NATS.
        """
        self.logger.info("[NATSManager] Stopping...")
        self._stopping = True

        if self.connected:
            try:
                asyncio.run_coroutine_threadsafe(self.nats.close(), self._loop).result(timeout=5)
                self.logger.info("[NATSManager] Disconnected from NATS.")
            except Exception as e:
                self.logger.error(f"[NATSManager] Error during disconnect: {e}")
            self.connected = False

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

        time.sleep(0.1)

    async def connect(self):
        """
        Explicit async connection (useful if not using start/stop background thread).
        """
        if not self.connected:
            try:
                await self.nats.connect(servers=[self.server])
                self.connected = True
                self.logger.info(f"[NATSManager] Explicitly connected to NATS at {self.server}")
            except Exception as e:
                self.logger.error(f"[NATSManager] Explicit connection failed: {e}")
                raise

    async def disconnect(self):
        """
        Explicit async disconnect.
        """
        if self.connected:
            await self.nats.close()
            self.connected = False
            self.logger.info("[NATSManager] Explicitly disconnected from NATS.")

    async def publish(self, subject: str, payload: bytes):
        """
        Publish to a NATS subject.
        """
        if not self.connected:
            await self.connect()
        await self.nats.publish(subject, payload)
        self.logger.debug(f"[NATSManager] Published {len(payload)} bytes to '{subject}'.")

    async def subscribe(self, subject: str, callback):
        """
        Subscribe to a NATS subject.
        """
        if not self.connected:
            await self.connect()
        await self.nats.subscribe(subject, cb=callback)
        self.logger.info(f"[NATSManager] Subscribed to '{subject}'.")

    def launch_servers(self):
        """
        Launch embedded NATS servers (ports 4222 & 5222).
        Intended for meshserver use only.
        """
        self.logger.info("[NATSManager] Launching local NATS servers (4222 & 5222)...")
        subprocess.Popen(["nats-server", "-p", "4222"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.Popen(["nats-server", "-p", "5222"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# Singleton instance
nats_manager = NATSManager()
