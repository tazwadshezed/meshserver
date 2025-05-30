import functools
import re
import ephem
from DAQ.util.config import load_config
from DAQ.commands.protocol import Message

# Load configuration
config = load_config()

class CommandValidationError(Exception):
    pass

CMD_FUNCS = []
PACKET_DELAY = 0.60  # Slight delay to prevent CPU overuse

def command(func):
    @functools.wraps(func)
    def _command(*args, **kwargs):
        return func(*args, **kwargs)
    CMD_FUNCS.append(func.__name__)
    return _command

def call(value, func):
    return func(value)

def call_inlist(value, matches):
    return value, value in matches, f"required to be one of {matches}"

def call_isinstance(value, istype):
    return value, isinstance(value, istype), f"not an instance of {istype}"

def call_list(value, func):
    if not isinstance(value, list):
        return value, False, "required to be a list"

    status, new_values = None, []
    errmsg = ""

    for v in value:
        newv, valid, errmsg = func(v)
        new_values.append(newv)
        status = valid if status is None else status and valid

    return new_values, status, f"an item in the list is {errmsg}"

def call_couldbe_list(value, istype):
    if not isinstance(value, list):
        value = [value]

    status, new_values = None, []
    errmsg = ""

    for v in value:
        newv, valid, errmsg = call_isinstance(v, istype)
        new_values.append(newv)
        status = valid if status is None else status and valid

    return new_values, status, f"an item in the list is {errmsg}"

def validate(arg, istype, cmp=call_isinstance, required=False):
    def _validate(func):
        @functools.wraps(func)
        def __validate(*args, **kwargs):
            if required and arg not in kwargs:
                raise CommandValidationError(f"argument `{arg}` is required")

            if arg in kwargs:
                value, valid, errmsg = cmp(kwargs[arg], istype)
                if not valid:
                    raise CommandValidationError(f"argument `{arg}` is {errmsg}")
                kwargs[arg] = value

            return func(*args, **kwargs)
        return __validate
    return _validate

def is_hex(value):
    if value is None:
        return value, False, "not a valid hex value"
    if isinstance(value, (int, float)):
        value = str(int(value))
    if len(value) % 2 != 0:
        value = value.zfill(len(value) + 1)

    re_is_hex = re.compile(r'^[0-9a-fA-F]*$')
    return value, bool(re_is_hex.match(value)), "not a valid hex value"

def is_macaddr(macaddr):
    if macaddr is None:
        return macaddr, True, ""

    if isinstance(macaddr, (tuple, list)):
        if len(macaddr) != 1:
            return macaddr, False, "please provide only one mac address"
        macaddr = macaddr[0]

    if isinstance(macaddr, (int, float)):
        macaddr = str(int(macaddr))

    macaddr = macaddr.zfill(16)
    re_is_hex = re.compile(r'^[0-9a-fA-F]*$')
    return macaddr, bool(re_is_hex.match(macaddr)), "not a valid MAC address"

class MeshCommands:
    """Mixin for sending mesh network commands. Assumes host has `send()` and `dispatch_command_response()` methods."""
    send: callable
    dispatch_command_response: callable

    def basic_message(self, request_id, macaddr=None, rreq=None, commands=None, dtype=None):
        if commands is None:
            commands = []
        if dtype is None:
            dtype = Message.TYPE_RES

        msg = Message()
        msg.dtype = dtype
        msg.set_addr(macaddr)
        msg.request_id = request_id
        msg.mesh_ctrl.rreq = rreq if rreq is not None else not msg.is_broadcast()
        msg.mesh_ctrl.priority = True

        for cmd in commands:
            msg.add_command(cmd)

        return msg

    def reload_time(self):
        """Initialize ephemeris time-tracking."""
        self.obs = ephem.Observer()
        self.obs.lat = str(config.get("ephem", {}).get("lat", "0"))
        self.obs.long = str(config.get("ephem", {}).get("lon", "0"))
        # self.sun = ephem.Sun()

        self.sleep_time = config.get("ephem", {}).get("sleep_no_sun", 3600)
        self.utc_offset = round(float(self.obs.long) / 15.0)

        # self.is_sun_up()

        # if hasattr(self, "request_id"):
        #     self.mcp_reset(self.request_id)

    # def is_sun_up(self):
    #     from datetime import datetime
    #     self.obs.date = datetime.utcnow()
    #
    #     try:
    #         next_rise = self.obs.next_rising(self.sun)
    #         next_set = self.obs.next_setting(self.sun)
    #         if next_rise < next_set:
    #             self.logger.info("Sun is up.")
    #             return True
    #         else:
    #             self.logger.info("Sun is down.")
    #             return False
    #     except Exception:
    #         self.logger.warning("Could not determine sun state.")
    #         return False

    # def mcp_reset(self, request_id, macaddr=None):
    #     msg = self.basic_message(request_id, macaddr)
    #     msg.add_command(MCPReset())
    #     self.send(msg)
    #
    #     if macaddr is None:
    #         self.dispatch_command_response(request_id, None)
