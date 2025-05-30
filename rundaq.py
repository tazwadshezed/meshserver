#!/usr/bin/env python3
"""
rundaq.py - Launches the DAQ system:
- Starts internal and external NATS servers
- Launches DAQProcess
"""

import asyncio
import subprocess
import logging
import os
from DAQ.util.logger import make_logger
from DAQ.lib.process import DAQProcess

logger = make_logger("rundaq")

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

async def start_nats_servers():
    logger.info("[NATSManager] Launching local NATS servers (4222 + 5222)...")
    subprocess.Popen(["nats-server", "-p", "4222"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.Popen(["nats-server", "-p", "5222"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await asyncio.sleep(1)

async def run_daq():
    logger.info(f"[rundaq] Running with PID={os.getpid()} UID={os.getuid()} CWD={os.getcwd()}")
    await start_nats_servers()

    logger.info("[rundaq] Launching DAQProcess...")
    daq = DAQProcess()
    await daq.run()

def main():
    setup_logging()
    try:
        asyncio.run(run_daq())
    except KeyboardInterrupt:
        logger.info("[rundaq] KeyboardInterrupt caught. Exiting.")

if __name__ == "__main__":
    main()
