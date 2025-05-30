"""
Modernized Gateway server using asyncio:
- TCP server for emulator connections (MI protocol)
- UDP autodiscovery responder (MARCO → POLO)

Config-driven from config.yaml:
  gateway:
    comm_host: "127.0.0.1"
    comm_port: 59990
    ad_host: "0.0.0.0"
    ad_listen_port: 59991
    ad_respond_port: 59992
"""

import asyncio
from DAQ.util.logger import make_logger
from DAQ.util.config import load_config
from DAQ.util.utctime import utcepochnow
from DAQ.util.hex import _h
from DAQ.commands.protocol import Message

logger = make_logger("gateway")
cfg = load_config()

comm_host = cfg["gateway"]["comm_host"]
comm_port = cfg["gateway"]["comm_port"]
ad_listen_port = cfg["gateway"]["ad_listen_port"]
ad_respond_port = cfg["gateway"]["ad_respond_port"]

# TCP Handler (MI protocol)
async def handle_tcp_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, recv_queue: asyncio.Queue):
    addr = writer.get_extra_info("peername")
    logger.info(f"[TCP] Connection from {addr[0]}:{addr[1]}")

    try:
        while True:
            header = await reader.readexactly(2)
            if header != b"MI":
                logger.warning(f"[TCP] Invalid header from {addr}: {header}")
                continue

            length_byte = await reader.readexactly(1)
            length = length_byte[0]

            raw_payload = await reader.readexactly(length)
            timestamp = utcepochnow()

            logger.debug(f"[TCP] MI:{length}:{_h(raw_payload)}")

            try:
                msg = Message.from_raw(Message.MESH_INDICATION, length, raw_payload, timestamp)
                logger.debug(f"[TCP] Parsed message with {len(msg.commands)} command(s)")
            except Exception:
                logger.exception("[TCP] Failed to parse message")
                continue

            payload = ("emulator", Message.MESH_INDICATION, length, raw_payload, timestamp)
            await recv_queue.put(payload)

    except asyncio.IncompleteReadError:
        logger.info(f"[TCP] Disconnected: {addr[0]}:{addr[1]}")
    except Exception:
        logger.exception("[TCP] Exception in handler")
    finally:
        writer.close()
        await writer.wait_closed()


# UDP Autodiscovery Handler (MARCO → POLO)
class AutodiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        logger.debug(f"MARCO {addr} → POLO?")
        if data.strip() == b"MARCO":
            try:
                self.transport.sendto(b"POLO", (addr[0], ad_respond_port))
                logger.debug(f"POLO sent to {addr[0]}:{ad_respond_port}")
            except Exception:
                logger.exception("Failed to send POLO response")


# Server launcher
async def start_gateway_servers(recv_queue: asyncio.Queue):
    # Start TCP server
    tcp_server = await asyncio.start_server(
        lambda r, w: handle_tcp_connection(r, w, recv_queue),
        host=comm_host,
        port=comm_port
    )
    logger.info(f"[TCP] Gateway TCP server listening on {comm_host}:{comm_port}")

    # Start UDP autodiscovery
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: AutodiscoveryProtocol(),
        local_addr=(comm_host, ad_listen_port)
    )
    logger.info(f"[UDP] Autodiscovery listening on {comm_host}:{ad_listen_port} → responds on {ad_respond_port}")

    return tcp_server, transport
