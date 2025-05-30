# DAQ/commands/protocol.py
import struct
from DAQ.util.hex import _u, _h

command_mapper = {}


def safe_int16(val):
    return max(-32768, min(32767, int(val)))

def parse_commands(msg, payload):
    if not payload:
        return []
    cmd_byte = payload[0:1].hex().upper()  # First byte is command ID
    body = payload[1:]
    cmd_class = command_mapper.get(cmd_byte, RawResponse)
    cmd_instance = cmd_class(header=msg, raw=body)
    cmd_instance.parse(body)
    return [cmd_instance]


class CommandMeta(type):
    def __new__(cls, name, bases, attrs):
        newclass = super().__new__(cls, name, bases, attrs)
        cmd = attrs.get('CMD', None)
        if cmd and name not in ['CommandMeta', 'CommandBase']:
            command_mapper[cmd] = newclass
        return newclass


class CommandBase(metaclass=CommandMeta):
    CMD = 'FF'

    def __init__(self, header=None, raw=b''):
        self.header = header
        self.raw = raw
        self._init()

    def _init(self): pass

    def parse(self, raw=b''):
        if raw:
            self.raw = raw
        return self.raw

    def decompile(self):
        return self.to_string()

    def to_string(self):
        return self._wrap()

    def _wrap(self, raw=b''):
        cmd_byte = bytes.fromhex(self.CMD)
        body = cmd_byte + raw
        return bytes([len(body)]) + body

    def response(self):
        return {
            "status": not self.header.mesh_ctrl.fail,
            "macaddr": self.header.addr,
            "source_hopcount": self.header.source_hopcount,
            "source_queue_length": self.header.source_queue_length
        }

class RawResponse(CommandBase):
    CMD = '00'

    def _init(self):
        self.raw = b''

    def parse(self, raw):
        self.raw = _h(raw)

    def to_string(self):
        return self._wrap(_u(self.raw))

    def response(self):
        res = super().response()
        res["raw"] = self.raw
        return res


class DataIndication(CommandBase):
    CMD = 'DD'

    def _init(self):
        self.data = []
        self._parsed = False
        self.op_stat = 0
        self.reg_stat = 0
        print(f"[DEBUG] DataIndication registered as: {command_mapper.get('DD')}")

    def parse(self, raw=None):
        if raw is not None:
            self.raw = raw

        self._parsed = True
        try:
            if len(self.raw) < 4:
                raise ValueError("Payload too short to contain op_stat and reg_stat")

            self.op_stat, self.reg_stat = struct.unpack(">HH", self.raw[:4])
            data_raw = self.raw[4:]

            sample_size = 14  # 7 values × 2 bytes each
            if len(data_raw) % sample_size != 0:
                print(f"[WARN] Raw payload length {len(data_raw)} not a multiple of {sample_size}")

            for i in range(0, len(data_raw), sample_size):
                chunk = data_raw[i:i + sample_size]
                if len(chunk) < sample_size:
                    continue

                timestamp, Vi, Vo, Ii, Io, Pi, Po = struct.unpack(">Hhhhhhh", chunk)
                self.data.append({
                    'timestamp': timestamp,
                    'Vi': Vi / 100.0,  # ✅ fixed (was /256.0)
                    'Vo': Vo / 100.0,
                    'Ii': Ii / 100.0,
                    'Io': Io / 100.0,
                    'Pi': Pi / 100.0,
                    'Po': Po / 100.0
                })
        except Exception as e:
            print(f"[DataIndication] parse() error: {e}")

    def decompile(self):
        raw = bytearray()
        raw += struct.pack(">H", getattr(self, "op_stat", 0))
        raw += struct.pack(">H", getattr(self, "reg_stat", 0))

        for point in self.data:
            raw += struct.pack(">H", int(point['timestamp']))              # 2 bytes
            raw += struct.pack(">h", int(point['Vi'] * 100))              # 2 bytes
            raw += struct.pack(">h", int(point['Vo'] * 100))              # 2 bytes
            raw += struct.pack(">h", int(point['Ii'] * 100))              # 2 bytes
            raw += struct.pack(">h", int(point['Io'] * 100))              # 2 bytes
            raw += struct.pack(">h", safe_int16(point.get('Pi', 0) * 100)) # 2 bytes
            raw += struct.pack(">h", safe_int16(point.get('Po', 0) * 100)) # 2 bytes

        return self._wrap(raw)

    def response(self):
        return {
            'type': 'mon',
            'macaddr': self.header.addr if self.header else "unknown",
            'op_stat': self.op_stat,
            'reg_stat': self.reg_stat,
            'data': sorted(self.data, key=lambda x: x['timestamp'])
        }

    def add_data(self, timestamp, Vi, Vo, Ii, Io, Pi, Po):
        """Call this to insert one data record (e.g., from emulator)"""
        self.data.append({
            'timestamp': int(timestamp),
            'Vi': round(Vi, 2),
            'Vo': round(Vo, 2),
            'Ii': round(Ii, 2),
            'Io': round(Io, 2),
            'Pi': round(Pi, 2),
            'Po': round(Po, 2)
        })



class MeshCtrl:
    ATYPE = 0b10000000
    SUPER = 0b01000000
    RREQ  = 0b00100000
    FAIL  = 0b00010000
    PRIOR = 0b00001000
    TBD1  = 0b00000100
    VER   = 0b00000011

    def __init__(self, ctrl=0):
        self.ctrl = ctrl
        self.atype = bool(ctrl & self.ATYPE)
        self.super = bool(ctrl & self.SUPER)
        self.rreq  = bool(ctrl & self.RREQ)
        self.fail  = bool(ctrl & self.FAIL)
        self.prior = bool(ctrl & self.PRIOR)
        self.tbd1  = bool(ctrl & self.TBD1)
        self.version = ctrl & self.VER

    def __int__(self):
        return self.ctrl

class Message:
    MESH_INDICATION = 'MI'
    MESH_INDICATION_FULL = 'MF'
    MESH_MESSAGE = 'MM'
    MESH_UNICAST = 'MU'
    GW_REGISTRATION = 'GI'
    GW_DEREGISTRATION = 'GR'
    DIAG_REGISTRATION = 'DI'
    DIAGT_REGISTRATION = 'DT'
    DIAG_DEREGISTRATION = 'DR'
    COMMAND_REQUEST = 'CR'
    MAC_REGISTRATION = 'MR'

    LEN_MESH_CTRL = 1
    LEN_ADDR = 8
    LEN_REQ_ID = 2
    LEN_SHC = 1
    LEN_SQL = 1
    LEN_HC = 1
    LEN_QL = 1
    LEN_TYPE = 1
    LEN_PART = 1

    # defines the chunks of the clarity header, used for parsing.
    LEN_ORDER = (LEN_MESH_CTRL, LEN_ADDR, LEN_REQ_ID,
                 LEN_SHC, LEN_SQL, LEN_HC, LEN_QL, LEN_TYPE, LEN_PART)

    TYPE_RES = 0  # reserved bit
    TYPE_SPG = 1
    TYPE_PLM = 2
    TYPE_PLO = 3
    TYPE_JXM = 4

    def __init__(self):
        self.mesh_ctrl = MeshCtrl()
        self.addr = 'FF' * self.LEN_ADDR
        self.request_id = 0
        self.source_hopcount = 0
        self.source_queue_length = 0
        self.hopcount = 0
        self.queue_length = 0
        self._reserved = 0
        self.dtype = Message.TYPE_RES
        self.partnum = 1
        self.numparts = 1
        self.payload = b""
        self.commands = []
        self.raw = b""
        self.received_on = None

    def set_addr(self, macaddr=None):
        self.addr = macaddr.zfill(self.LEN_ADDR * 2) if macaddr else 'F' * (self.LEN_ADDR * 2)

    def add_command(self, cmd):
        cmd.header = self
        self.commands.append(cmd)

    def responses(self):
        return [cmd.response() for cmd in self.commands]

    def decompile(self):
        raw = bytearray()
        raw += struct.pack("B", int(self.mesh_ctrl))
        raw += bytes.fromhex(self.addr)[::-1]
        raw += struct.pack(">H", self.request_id)
        raw += struct.pack("B", self.source_hopcount)
        raw += struct.pack("B", self.source_queue_length)
        raw += struct.pack("B", self.hopcount)
        raw += struct.pack("B", self.queue_length)
        raw += struct.pack("B", (self._reserved << 4) | self.dtype)
        raw += struct.pack("B", ((self.partnum - 1) << 4) | (self.numparts - 1))
        for cmd in self.commands:
            raw += cmd.decompile()
        return bytes(raw)

    @classmethod
    def compile(cls, payload: bytes):
        msg = cls()
        tokens, remaining = cls.tokenize_string(payload, cls.LEN_ORDER)
        msg.mesh_ctrl = MeshCtrl(tokens[0][0])
        msg.addr = _h(tokens[1][::-1])
        msg.request_id = int.from_bytes(tokens[2], 'big')
        msg.source_hopcount = tokens[3][0]
        msg.source_queue_length = tokens[4][0]
        msg.hopcount = tokens[5][0]
        msg.queue_length = tokens[6][0]
        dt = tokens[7][0]
        msg._reserved = dt >> 4
        msg.dtype = dt & 0x0F
        parts = tokens[8][0]
        msg.partnum = (parts >> 4) + 1
        msg.numparts = (parts & 0x0F) + 1
        msg.payload = remaining

        try:
            msg.commands = parse_commands(msg, remaining)
        except Exception:
            import traceback
            traceback.print_exc()
            msg.commands = []

        return msg


    @staticmethod
    def tokenize_string(raw, chunks):
        tokens, i = [], 0
        for l in chunks:
            tokens.append(raw[i:i + l])
            i += l
        return tokens, raw[i:]

    @staticmethod
    def from_raw(message_type, length, raw, received_on):
        msg = Message()
        msg.raw = raw
        msg.received_on = received_on

        tokens, payload = Message.tokenize_string(raw, Message.LEN_ORDER)

        msg.mesh_ctrl = MeshCtrl(tokens[0][0])
        msg.addr = _h(tokens[1][::-1])
        msg.request_id = int.from_bytes(tokens[2], 'big')
        msg.source_hopcount = tokens[3][0]
        msg.source_queue_length = tokens[4][0]
        msg.hopcount = tokens[5][0]
        msg.queue_length = tokens[6][0]
        dt = tokens[7][0]
        msg._reserved = dt >> 4
        msg.dtype = dt & 0x0F
        parts = tokens[8][0]
        msg.partnum = (parts >> 4) + 1
        msg.numparts = (parts & 0x0F) + 1

        msg.payload = payload
        msg.commands = []

        try:
            i = 0
            while i < len(payload):
                cmd_len = payload[i]
                if i + 1 + cmd_len > len(payload):
                    raise ValueError("Malformed payload: cmd_len exceeds bounds")

                cmd_data = payload[i + 1: i + 1 + cmd_len]
                cmd_byte = cmd_data[0:1].hex().upper()

                cmd_class = command_mapper.get(cmd_byte, RawResponse)
                cmd_instance = cmd_class(header=msg, raw=cmd_data[1:])
                cmd_instance.parse(cmd_data[1:])
                msg.commands.append(cmd_instance)

                i += 1 + cmd_len

        except Exception:
            import traceback
            traceback.print_exc()
            msg.commands = []

        return msg

    def __repr__(self):
        return f"<Message [{', '.join(repr(c) for c in self.commands)}]>"

print(command_mapper.get('DD'))  # should print <class '...DataIndication'>

