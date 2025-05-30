from collections import defaultdict
from DAQ.util.hex import uint32_to_sint32, int_to_float, _u
from DAQ.devices import IConnector
from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.pdu import ExceptionResponse

EXCEPTIONS = {
    1: 'ILLEGAL FUNCTION',
    2: 'ILLEGAL DATA ADDRESS',
    3: 'ILLEGAL DATA VALUE',
    4: 'SLAVE DEVICE FAILURE',
    6: 'SLAVE DEVICE BUSY',
    10: 'GATEWAY PATH UNAVAILABLE',
    11: 'GATEWAY TARGET DEVICE FAILED TO RESPOND',
}

FORMAT_ASCII    = 0
FORMAT_SINT16   = 1
FORMAT_UINT16   = 2
FORMAT_SINT32   = 3
FORMAT_UINT32   = 4
FORMAT_FLOAT    = 5
FORMAT_BITMAP   = 6

def get_modbus_client(transport_type, **kwargs):
    if transport_type == 'tcp':
        client = ModbusTcpClient(host=kwargs['host'],
                                 port=kwargs.get('port'))

    elif transport_type == 'serial':
        method = kwargs['method']
        del kwargs['method']
        client = ModbusSerialClient(method=method, **kwargs)

    elif transport_type in ['ascii', 'rtu', 'binary']:
        client = ModbusSerialClient(method=transport_type, **kwargs)

    return client

class GenericModbusConnector(IConnector):

    def read_uint16(self, registers, mapper):
        value = registers[0]
        return value

    def read_uint32(self, registers, mapper):
        value = registers[0] << 16
        value += registers[1]
        return value

    def read_sint16(self, registers, mapper):
        return uint32_to_sint32(self.read_uint16(registers, mapper))

    def read_sint32(self, registers, mapper):
        return uint32_to_sint32(self.read_uint32(registers, mapper))

    def read_float(self, registers, mapper):
        return int_to_float(self.read_uint32(registers, mapper))

    def read_ascii(self, registers, mapper):
        value = ''.join([hex(x)[2:] for x in registers])
        value = _u(value).strip()
        return value

    def read_bitmap(self, registers, mapper):
        bits = bin(registers[0])[2:].rjust(16, '0')

        bitvalues = defaultdict(str)

        for i,mask in enumerate(mapper['bitmap']):
            if mask == '-':
                continue

            key = mapper['bitmap_map'][mask]

            bitvalues[key] += bits[i]

        value = {}

        for key in bitvalues:
            value[key] = int(bitvalues[key], 2)

        return value

    def write(self, address, value):
        self.client.write_register(address-1, value)

    def read(self, address, r):
        return self.client.read_holding_registers(address-1, r)

    def read_from_map(self, var):
        mapper = self.ADDRESS_MAP[var]

        reader = getattr(self.client, mapper['execute'])

        start = mapper['address'][0] - 1
        count = mapper['address'][1] - start

        rr = reader(start, count)
        valid = True

        if isinstance(rr, ExceptionResponse):
            value = EXCEPTIONS[rr.exception_code]
            valid = False
        elif isinstance(mapper['format'], str) \
        and callable(getattr(self, mapper['format'])):
            value = getattr(self, mapper['format'])(rr.registers, mapper)
        elif mapper['format'] == FORMAT_SINT32:
            value = self.read_sint32(rr.registers, mapper)
        elif mapper['format'] == FORMAT_UINT16:
            value = self.read_uint16(rr.registers, mapper)
        elif mapper['format'] == FORMAT_FLOAT:
            value = self.read_float(rr.registers, mapper)
        elif mapper['format'] == FORMAT_ASCII:
            value = self.read_ascii(rr.registers, mapper)
        elif mapper['format'] == FORMAT_BITMAP:
            value = self.read_bitmap(rr.registers, mapper)

        return value, valid

    def register_names(self):
        return list(self.ADDRESS_MAP.keys())

    def read_all(self, register_names=None):
        data = {}

        if register_names is None:
            register_names = self.register_names()

        for var in register_names:
            assert var in self.ADDRESS_MAP

            value, is_valid = self.read_from_map(var)

            if is_valid:
                data[var] = value
            else:
                data[var] = None

        return data

class SharkModbusConnector(GenericModbusConnector):
    __identifiers__ = ['SHARK 100', 'SHARK 100T']
    __dtype__ = 'acm'

    DEFAULT_PASSWORD = 5555

    ADDRESS_MAP = {
        'name': {
            'execute': 'read_holding_registers',
            'address': (1,8),
            'format': FORMAT_ASCII,
        },
        'serial_number': {
            'execute': 'read_holding_registers',
            'address': (9,16),
            'format': FORMAT_ASCII,
        },
        'firmware_version': {
            'execute': 'read_holding_registers',
            'address': (18,19),
            'format': FORMAT_ASCII,
        },
        'meter_type': {
            'execute': 'read_holding_registers',
            'address': (17,17),
            'format': FORMAT_BITMAP,
            'bitmap': '-------t-----vvv',
            'bitmap_map': {
                't': 'transducer_model',
                'v': 'v_switch',
            }
        },
        'meter_configuration': {
            'execute': 'read_holding_registers',
            'address': (21,21),
            'format': FORMAT_BITMAP,
            'bitmap': '----------ffffff',
            'bitmap_map': {
                'f': 'calibration_frequency',
            }
        },
        'user_settings': {
            'execute': 'read_holding_registers',
            'address': (30015,30015),
            'format': FORMAT_BITMAP,
            'bitmap': '---g--nnsrp--wf-',
            'bitmap_map': {
                'g': 'alternate_bargraph',
                'n': 'num_phases',
                's': 'scroll',
                'r': 'password_reset',
                'p': 'password_required',
                'w': 'pwr_dir',
                'f': 'flip_power_factor',
            }
        },
        'power_energy_formats': {
            'execute': 'read_holding_registers',
            'address': (30006,30006),
            'format': FORMAT_BITMAP,
            'bitmap': 'pppp--nn-eee-ddd',
            'bitmap_map': {
                'p': 'power_scale',
                'n': 'num_energy_digits',
                'e': 'energy_scale',
                'd': 'energy_digits_after_decimal',
            }
        },
        'ACPo': {
            'execute': 'read_holding_registers',
            'address': (1018,1019),
            'format': 'read_power',
        },
        'ACVARs': {
            'execute': 'read_holding_registers',
            'address': (1020,1021),
            'format': FORMAT_FLOAT,
        },
        'ACVAs': {
            'execute': 'read_holding_registers',
            'address': (1022,1023),
            'format': FORMAT_FLOAT,
        },
        'ACPwrFactor': {
            'execute': 'read_holding_registers',
            'address': (1024,1025),
            'format': FORMAT_FLOAT,
        },
        'ACFreq': {
            'execute': 'read_holding_registers',
            'address': (1026,1027),
            'format': FORMAT_FLOAT,
        },
        'ACV_A_N': {
            'execute': 'read_holding_registers',
            'address': (1000,1001),
            'format': FORMAT_FLOAT,
        },
        'ACV_B_N': {
            'execute': 'read_holding_registers',
            'address': (1002,1003),
            'format': FORMAT_FLOAT,
        },
        'ACV_C_N': {
            'execute': 'read_holding_registers',
            'address': (1004,1005),
            'format': FORMAT_FLOAT,
        },
        'ACV_A_B': {
            'execute': 'read_holding_registers',
            'address': (1006,1007),
            'format': FORMAT_FLOAT,
        },
        'ACV_B_C': {
            'execute': 'read_holding_registers',
            'address': (1008,1009),
            'format': FORMAT_FLOAT,
        },
        'ACV_C_A': {
            'execute': 'read_holding_registers',
            'address': (1010,1011),
            'format': FORMAT_FLOAT,
        },
        'ACI_A': {
            'execute': 'read_holding_registers',
            'address': (1012,1013),
            'format': FORMAT_FLOAT,
        },
        'ACI_B': {
            'execute': 'read_holding_registers',
            'address': (1014,1015),
            'format': FORMAT_FLOAT,
        },
        'ACI_C': {
            'execute': 'read_holding_registers',
            'address': (1016,1017),
            'format': FORMAT_FLOAT,
        },
        'ACEo_received': {
            'execute': 'read_holding_registers',
            'address': (1100,1101),
            'format': 'read_energy',
        },
        'ACEo_delivered': {
            'execute': 'read_holding_registers',
            'address': (1102,1103),
            'format': 'read_energy',
        },
        'ACEo_net': {
            'execute': 'read_holding_registers',
            'address': (1104,1105),
            'format': 'read_energy',
        },
        'ACEo_total': {
            'execute': 'read_holding_registers',
            'address': (1106,1107),
            'format': 'read_energy',
        },
        'ACVAR_positive': {
            'execute': 'read_holding_registers',
            'address': (1108,1109),
            'format': 'read_energy',
        },
        'ACVAR_negative': {
            'execute': 'read_holding_registers',
            'address': (1110,1111),
            'format': 'read_energy',
        },
        'ACVAR_net': {
            'execute': 'read_holding_registers',
            'address': (1112,1113),
            'format': 'read_energy',
        },
        'ACVAR_total': {
            'execute': 'read_holding_registers',
            'address': (1114,1115),
            'format': 'read_energy',
        },
        'ACVA_total': {
            'execute': 'read_holding_registers',
            'address': (1116,1117),
            'format': 'read_energy',
        },
    }

    FORCE_METER_RESTART = 25000
    RESET_ENERGY_ACCUMULATORS = 40100

    def __init__(self, host, port, password=None, client=None):
        self.host = host
        self.port = port
        self.verified = False

        if password is None:
            self.password = SharkModbusConnector.DEFAULT_PASSWORD
        else:
            self.password = password

        if client is None:
            self.client = get_modbus_client('tcp', host=self.host, port=self.port)
        else:
            self.client = client

        super(SharkModbusConnector, self).__init__()

    def verify(self):
        name, valid = self.read_from_map('name')

        if not valid \
        or not 'SHARK 100' in name.upper():
            self.is_valid = False
            return False

        is_valid = True

        self.name = name
        self.serial_number, valid = self.read_from_map('serial_number')
        is_valid = is_valid and valid

        self.firmware_version, valid = self.read_from_map('firmware_version')
        is_valid = is_valid and valid

        self.power_energy_formats, valid = self.read_from_map('power_energy_formats')
        is_valid = is_valid and valid

        self.verified = is_valid

    def close(self):
        self.verified = False
        self.client.close()

    def read_data(self):
        data = self.read_all(register_names=('serial_number',
                                             'ACPo', 'ACVARs', 'ACVAs', 'ACPwrFactor', 'ACFreq',
                                             'ACV_A_N', 'ACV_B_N', 'ACV_C_N', 'ACV_A_B',
                                             'ACV_B_C', 'ACV_C_A', 'ACI_A', 'ACI_B', 'ACI_C',
                                             'ACEo_received', 'ACEo_delivered', 'ACEo_net',
                                             'ACEo_total', 'ACVAR_positive', 'ACVAR_negative',
                                             'ACVAR_net', 'ACVAR_total', 'ACVA_total'))

        return data

    def read_power(self, registers, mapper):
        assert self.verified

        value = self.read_float(registers, mapper)

        #: Shark Guys say power scale is only for the display
        #if self.power_energy_formats['power_scale'] == 8:
        #    pass
        #else:
        #    value = value * (10**self.power_energy_formats['power_scale'])

        return value

    def read_energy(self, registers, mapper):
        assert self.verified

        value = self.read_sint32(registers, mapper)
        value = value / (10**self.power_energy_formats['energy_digits_after_decimal'])
        value = value * (10**self.power_energy_formats['energy_scale'])

        return value

    def force_restart(self):
        self.write(SharkModbusConnector.FORCE_METER_RESTART, self.password)

    def reset_energy_accumulators(self):
        self.write(SharkModbusConnector.RESET_ENERGY_ACCUMULATORS, self.password)

    START_PROGRAM_UPDATE_MODE = 22000
    STOP_PROGRAM_UPDATE_MODE = 22001
    CALC_CHECKSUM = 22002
    CHECKSUM = 22003
    CT_MULT_DENOM = 30000
    CT_NUMERATOR = 30001

    def start_program_mode(self):
        self.write(SharkModbusConnector.START_PROGRAM_UPDATE_MODE, self.password)

    def stop_program_mode(self):
        self.write(SharkModbusConnector.STOP_PROGRAM_UPDATE_MODE, self.password)

    def write_numerator(self, value):
        self.write(SharkModbusConnector.CT_NUMERATOR, value)

    def ram_checksum(self):
        return self.read(SharkModbusConnector.CALC_CHECKSUM,1).registers[0]

    def eeprom_checksum(self, value=None):
        if value:
            self.write(SharkModbusConnector.CHECKSUM, value)
        else:
            return self.read(SharkModbusConnector.CHECKSUM,1).registers[0]

    def get_multiplier_denominator(self):
        return bin(self.read(SharkModbusConnector.CT_MULT_DENOM,1).registers[0])

    def get_numerator(self):
        return self.read(SharkModbusConnector.CT_NUMERATOR,1).registers[0]

class SatconPowerGateInverterConnector(GenericModbusConnector):
    __identifiers__ = ['SATCON POWERGATE INVERTER']
    __dtype__ = 'inv'

    BUAD_RATE = 9600
    PARITY = None
    DATA_BITS = 8
    STOP_BITS = 1
    TRANSPORT_TYPE = 'rtu'

    ADDRESS_MAP = {
        'ACPi': {
            'execute': 'read_holding_registers',
            'address': (30083, 30083),
            'format': 'read_scale',
            'format_scaling': 1/4096
        },
        'Vi': {
            'execute': 'read_holding_registers',
            'address': (30085, 30085),
            'format': 'read_scale',
            'format_scaling': 327/4096
        },
        'Ii': {
            'execute': 'read_holding_registers',
            'address': (30086, 30086),
            'format': 'read_scale',
            'format_scaling': 1/4096,
        },
        'ACPo': {
            'execute': 'read_holding_registers',
            'address': (30088, 30088),
            'format': 'read_kw',
            'format_scaling': 1/4096,
        },
        'ACEo': {
            'execute': 'read_holding_registers',
            'address': (30099,30100),
            'format': 'read_kwh',
            'format_scaling': 0.01/1
        }
    }

    def __init__(self, transport_type, **kwargs):
        if transport_type == 'serial':
            transport_type = SatconPowerGateInverterConnector.TRANSPORT_TYPE

        self.client = get_modbus_client(transport_type, **kwargs)

        super(SatconPowerGateInverterConnector, self).__init__()

    def read_scale(self, registers, mapper):
        value = self.read_sint16(registers, mapper)

        value = value * mapper['format_scaling']

    def read_kw(self, registers, mapper):
        value = self.read_scale(registers, mapper)

        #: convert kilowatt hours to watt hours
        value = value * (10**3)

        return value

    def read_kwh(self, registers, mapper):
        #: flip the registers, register 0 is low bit, register 1 is high bit
        value = self.read_uint32([registers[1], registers[0]], mapper)

        value = value * mapper['format_scaling']

class SatconPowerGatePlusSolsticeInverterConnector(object):#SatconPowerGateInverterConnector):
    __identifiers__ = ['SATCON POWERGATE PLUS INVERTER', 'SATCON SOLSTICE INVERTER']
    __dtype__ = 'inv'

    BUAD_RATE = 9600
    PARITY = None
    DATA_BITS = 8
    STOP_BITS = 1
    TRANSPORT_TYPE = 'rtu'
