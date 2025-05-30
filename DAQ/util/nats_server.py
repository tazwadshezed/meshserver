import asyncio
import logging

logger = logging.getLogger("NATSManager")

async def start_nats_servers():
    await _start_nats_server(4222, "internal")
    await _start_nats_server(5222, "external")

async def _start_nats_server(port, label):
    logger.info(f"[rundaq] Starting {label} NATS server ({port})...")
    cmd = ["nats-server", "-p", str(port)]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    logger.debug(f"[rundaq] Spawned {label} NATS server with PID {process.pid}")
