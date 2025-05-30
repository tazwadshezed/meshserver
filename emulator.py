import asyncio
import random
import socket
from DAQ.commands.protocol import Message, DataIndication
from DAQ.util.utctime import utcepochnow
from DAQ.util.config import load_config
from DAQ.mesh.simulator import MonitorSimulator
from DAQ.util.faults import get_fault, reset_fault  # ✅ Fault injection support

cfg = load_config()

comm_host = cfg['gateway']['comm_host']
comm_port = cfg['gateway']['comm_port']
ad_listen_port = cfg['gateway']['ad_listen_port']
ad_respond_port = cfg['gateway']['ad_respond_port']
ad_host = cfg['gateway']['ad_host']

panel_delay = cfg['emulator'].get('panel_delay', 0.25)
cycle_delay = cfg['emulator'].get('cycle_delay', 0.5)

PANEL_MACS = [
    "fa:29:eb:6d:87:01",
    "fa:29:eb:6d:87:02",
    "fa:29:eb:6d:87:03",
    "fa:29:eb:6d:87:04",
]

STATUS_PROFILES = {
    "red":    {"Vi": 0.0,   "Ii": 100.0},
    "yellow": {"Vi": 100.0, "Ii": 0.0},
    "grey":   {"Vi": 0.0,   "Ii": 0.0},
    "orange": {"Vi": 10.0,  "Ii": 10.0},
    "green":  {"Vi": 30.0,  "Ii": 5.0},
}
STATUS_KEYS = list(STATUS_PROFILES.keys())


def generate_profile(macaddr):
    mac = macaddr.lower()
    fault = get_fault(mac)  # ✅ Check if a fault is active

    if fault == "short_circuit":
        Vi = 0.0
        Ii = random.uniform(91.0, 100.0)
    elif fault == "open_circuit":
        Vi = random.uniform(96.0, 100.0)
        Ii = 0.0
    elif fault == "low_voltage":
        Vi = random.uniform(18.0, 24.0)
        Ii = random.uniform(6.0, 7.5)
    elif fault == "dead_panel":
        Vi, Ii = 0.0, 0.0
    else:
        # Normal
        Vi = random.uniform(38.0, 40.0)
        Ii = random.uniform(7.0, 8.0)

    Pi = round(Vi * Ii, 2)
    return {
        "voltage": round(Vi, 2),
        "current": round(Ii, 2),
        "power": Pi,
        "status": fault
    }


class AsyncEmulator:
    def __init__(self):
        self.sim = MonitorSimulator()
        self.start_time = utcepochnow()
        self.reader = None
        self.writer = None

    async def find_siteserver(self):
        loop = asyncio.get_running_loop()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.setblocking(False)

        try:
            s.bind(('', ad_respond_port))
        except Exception as e:
            print(f"[EMULATOR] Failed to bind UDP: {e}")
            return None

        marco = b"MARCO"
        target = ('<broadcast>', ad_listen_port)

        while True:
            print(f"[EMULATOR] Broadcasting MARCO...")
            try:
                await loop.sock_sendto(s, marco, target)
                data, addr = await asyncio.wait_for(loop.sock_recvfrom(s, 1024), timeout=2.0)
                if data.strip() == b"POLO":
                    print(f"[EMULATOR] Received POLO from {addr}")
                    s.close()
                    return addr[0]
            except asyncio.TimeoutError:
                print("[EMULATOR] No POLO yet, retrying...")
            except Exception as e:
                print(f"[EMULATOR] Error: {e}")

    async def connect(self, host):
        print(f"[EMULATOR] Connecting to {host}:{comm_port}...")
        self.reader, self.writer = await asyncio.open_connection(host, comm_port)
        print(f"[EMULATOR] Connected.")

    async def send_status_message(self, macaddr):
        profile = generate_profile(macaddr)
        Vi = profile["voltage"]
        Ii = profile["current"]
        Pi = profile["power"]
        timestamp = utcepochnow() - self.start_time

        msg = Message()
        try:
            if isinstance(macaddr, bytes):
                macaddr = macaddr.decode("utf-8")
            elif not isinstance(macaddr, str):
                macaddr = str(macaddr)
            mac_clean = macaddr.replace(":", "").strip().lower()
            if len(mac_clean) != 12:
                print(f"[ERROR] MAC {mac_clean} is not 6 bytes")
                return
            msg.set_addr(mac_clean)
        except Exception as e:
            print(f"[ERROR] Invalid MAC '{macaddr}': {e}")
            return

        msg.source_hopcount = random.randint(1, 10)
        msg.source_queue_length = 0
        msg.dtype = Message.TYPE_PLM

        cmd = DataIndication()
        cmd.add_data(timestamp, Vi, Vi, Ii, Ii, Pi, Pi)
        msg.add_command(cmd)

        payload = msg.decompile()

        print(f"[DEBUG] Decompile length: {len(payload)}")
        print(f"[DEBUG] Decompile hex: {payload.hex()}")

        if len(payload) > 255:
            print(f"[SKIP] Payload too large: {len(payload)} bytes")
            return

        message = b"MI" + bytes([len(payload)]) + payload
        print(f"[DEBUG] Final message hex: {message.hex()}")

        self.writer.write(message)
        await self.writer.drain()

        print(f"[EMULATOR] Sent: MAC={mac_clean}  V={Vi:.2f} I={Ii:.2f} P={Pi:.2f}")

    async def run(self):
        siteserver_host = await self.find_siteserver()
        if not siteserver_host:
            print("[EMULATOR] Siteserver not found. Exiting.")
            return

        await self.connect(siteserver_host)

        try:
            while True:
                for mac in PANEL_MACS:
                    await self.send_status_message(mac)
                    await asyncio.sleep(panel_delay)
                await asyncio.sleep(cycle_delay)
        except asyncio.CancelledError:
            print("[EMULATOR] Cancelled.")
        except Exception as e:
            print(f"[EMULATOR] Exception: {e}")
        finally:
            print("[EMULATOR] Shutting down...")
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()


if __name__ == "__main__":
    async def main():
        emulator = AsyncEmulator()
        await emulator.run()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[EMULATOR] Interrupted.")
