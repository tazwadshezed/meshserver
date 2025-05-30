import os
import random
import shutil
import asyncio
from datetime import datetime, time as dtime, timedelta, timezone, UTC
from bson import BSON
from DAQ.commands.protocol import Message, DataIndication
from DAQ.commands.strategy import CMD_FUNCS, MeshCommands
from DAQ.util.handlers.common import BSONHandler, CompressionHandler, IHandler, HandlerManager
from DAQ.services.core.data.pitcher import Pitcher
from DAQ.services.core.collector.collector import DeviceCollector
from DAQ.util.config import load_config
from DAQ.util.hex import _h
from DAQ.util.logger import make_logger
from DAQ.util.process.base import ProcessBase
from DAQ.gateway.manager import GatewayManager

cfg = load_config()
logger = make_logger("DAQProcess")

CMD_HANDLERS = {}

def handles(cmd_class):
    def decorator(fn):
        CMD_HANDLERS.setdefault(fn.__name__, []).append(cmd_class)
        return fn
    return decorator

def cleanup_temp_files():
    for path in ["/tmp", "/dev/shm"]:
        for f in os.listdir(path):
            full = os.path.join(path, f)
            try:
                if os.path.isfile(full) or os.path.islink(full):
                    os.remove(full)
                elif os.path.isdir(full) and f.startswith("pymp-"):
                    shutil.rmtree(full, ignore_errors=True)
            except Exception:
                pass

def sunrise_today():
    today = datetime.now(UTC).date()
    return datetime.combine(today, dtime(6, 0), tzinfo=UTC)

class DAQProcess(ProcessBase, MeshCommands):
    MAX_REQUEST_ID = 65535

    def __init__(self):
        super().__init__()
        self.logger = make_logger(self.__class__.__name__)
        self._request_id = random.randrange(0, self.MAX_REQUEST_ID)
        self._make_map()
        self.sunrise = sunrise_today()
        self.requests = {}
        self.last_device_data = {}

        self.recv_queue = asyncio.Queue()

        self.gateway_manager = GatewayManager(cfg['gateway']['comm_host'], cfg['gateway']['comm_port'], self.recv_queue)

        # Handler chain: BSON → Compression → Pitcher
        self.pitcher = Pitcher(IHandler.GENERIC)
        self.compression = CompressionHandler(IHandler.COMPILER)
        self.bson_handler = BSONHandler(IHandler.COMPILER)

        self.data_handler = self.bson_handler(self.compression(self.pitcher))
        self.handler_manager = HandlerManager()
        self.handler_manager.add_handler(self.data_handler)

        self.collector = DeviceCollector()
        self.collector_manager = HandlerManager()
        self.collector_manager.add_handler(self.collector)

        self.throttle_delay = cfg.get("daq", {}).get("throttle_delay", 0.01)
        self.backpressure_threshold = cfg.get("daq", {}).get("backpressure_qsize", 10)

        try:
            self.compression.set('batch_on', cfg.get("daq", {}).get("compression", {}).get("batch_on", 4))
            self.compression.set('batch_at', cfg.get("daq", {}).get("compression", {}).get("batch_at", 0.5))
        except Exception as e:
            self.logger.exception("Failed to configure handlers")

        try:
            devices_cfg = cfg['devices']['all']
            if isinstance(devices_cfg, (list, dict)):
                self.collector.set('devices', devices_cfg)
        except Exception as e:
            self.logger.warning("Could not set devices: %s", e)

        try:
            self.collector.set('convert_irradiance', cfg['devices']['convert_irradiance'])
        except Exception as e:
            self.logger.warning("Could not set irradiance conversion: %s", e)

    def _make_map(self):
        self.CMD_MAPPER = {name: getattr(self, name) for name in CMD_FUNCS if hasattr(self, name)}

    @property
    def request_id(self):
        self._request_id = (self._request_id + 1) % self.MAX_REQUEST_ID
        return self._request_id

    async def start(self):
        self.logger.info("DAQProcess starting gateway and handlers")
        await self.gateway_manager.start()
        self.data_handler.start(subhandlers=True)
        self.collector.start(subhandlers=True)

    async def stop(self):
        self.logger.info("DAQProcess stopping...")
        try:
            self.data_handler.stop(subhandlers=True)
        except Exception:
            self.logger.exception("data_handler stop failed")
        try:
            self.collector.stop(subhandlers=True)
        except Exception:
            self.logger.exception("collector stop failed")
        try:
            await self.gateway_manager.stop()
        except Exception:
            self.logger.exception("gateway_manager stop failed")
        cleanup_temp_files()

    async def run(self):
        await self.start()
        try:
            self.logger.info("DAQProcess entering async run loop...")
            while True:
                payload = await self.recv_queue.get()
                await self.process_gateway_indication(payload)
        except asyncio.CancelledError:
            self.logger.info("DAQProcess cancelled.")
        finally:
            await self.stop()

    async def process_gateway_indication(self, payload):
        if isinstance(payload, dict):
            self.data_handler.data_queue.put(payload)
            return

        try:
            gwid, msg_type, length, raw, received_on = payload
        except Exception as e:
            self.logger.warning(f"Malformed gateway payload: {payload} ({e})")
            return

        if msg_type == Message.MESH_INDICATION:
            try:
                msg = Message.from_raw(msg_type, length, raw, received_on)
            except Exception:
                self.logger.critical("Unable to parse MESH_INDICATION: [%s,%s,%s]" % (msg_type, length, _h(raw)))
                return
            for command in msg.commands:
                self.command_response(command, gwid)

        elif msg_type == Message.COMMAND_REQUEST:
            cmd_req = BSON(raw).decode()
            self.dispatch_command_request(cmd_req, gwid=gwid)

    def command_response(self, cmd, gwid=None):
        response = cmd.response()
        self.dispatch_command_handlers(cmd, response)

    def dispatch_command_request(self, cmd_req, gwid=None):
        func_name = cmd_req.get("func")
        args = cmd_req.get("args", {}) or {}
        func = self.CMD_MAPPER.get(func_name)
        if not func:
            self.logger.warning(f"[COMMAND] Unknown command: {func_name}")
            return {"status": False, "msg": "Unknown command"}
        try:
            return func(**args)
        except Exception as e:
            self.logger.exception(f"[COMMAND] Error executing {func_name}")
            return {"status": False, "msg": f"Error: {str(e)}"}

    def dispatch_command_handlers(self, cmd, response):
        handle_pass = True
        for handler_name, cmd_classes in CMD_HANDLERS.items():
            for klass in cmd_classes:
                if isinstance(cmd, klass):
                    func = getattr(self, handler_name, None)
                    if func:
                        try:
                            handle_pass = handle_pass and func(cmd, response)
                        except Exception as e:
                            self.logger.error(f"[{handler_name}] ERROR: {e}", exc_info=True)
        return handle_pass

    def from_seconds_since_sunrise(self, seconds):
        return self.sunrise + timedelta(seconds=seconds)

    def to_seconds_since_sunrise(self, dt):
        return min(int((dt - self.sunrise).total_seconds()), 0xFFFE)

    @handles(DataIndication)
    def handle_data_report(self, cmd, response):
        if 'reg_stat' not in response or 'op_stat' not in response:
            return False

        for data in response['data']:
            freezetime = self.from_seconds_since_sunrise(data['timestamp'])

            payload = dict(
                type=response['type'],
                macaddr=response['macaddr'],
                freezetime=freezetime,
                localtime=datetime.now(timezone.utc),
                reg_stat=response['reg_stat'],
                op_stat=response['op_stat'],
                Vi=data['Vi'],
                Vo=data['Vo'],
                Ii=data['Ii'],
                Io=data['Io'],
                Pi=data['Pi'],
                Po=data['Po']
            )
            self.data_handler.data_queue.put(payload)
            self.last_device_data[payload['type']] = payload

        return True
