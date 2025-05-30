

import os
import pprint

from bson import BSON, InvalidBSON
from DAQ.util.config import local_config

try:
    from DAQ.util.logger import make_logger
    logger = make_logger('PARAMETERS')
except:
    logger = None

def open_parameter_file():
    try:
        cfg = local_config('/etc/DAQ',  'daq.cfg')

        cfg['PARAMETERS_FILE'] = cfg.get('PARAMETERS_FILE',
                                         'etc/DAQ/parameters.table')

        param_file = cfg['PARAMETERS_FILE']

    except:

        param_file = 'etc/DAQ/parameters.table'

        try_local = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 param_file)

        if os.path.exists(try_local):
            param_file = try_local

    return param_file

def read_map():
    param_file = open_parameter_file()

    if not os.path.exists(param_file):
        return dict()
    else:
        f = open(param_file, 'rb')
        timestamp = f.readline()
        data = f.read()
        f.close()

        raw = BSON(data).decode()

        map = {}

        for k,v in raw.items():
            map[int(k, 16)] = v

        return map

def save_map(timestamp, parameters, param_file = None,):
    if param_file is None:
        param_file = open_parameter_file()

    update = False

    if not os.path.exists(param_file):
        update = True
    else:
        f = open(param_file, 'rb')
        try:
            existing_timestamp = int(f.readline().strip())
        except:
            existing_timestamp = 0
        f.close()

        if existing_timestamp < timestamp:
            update = True

    if update:
        if logger:
            logger.info("Saving updated parameter table with timestamp of %s" % (timestamp))
            logger.info("Paramter Table:")
            logger.info(pprint.pformat(parameters))

        f = open(param_file, 'wb')
        f.write("%d\n" % (timestamp))
        f.write(BSON.encode(parameters))
        f.close()

    return update

class FirmwareImage():

    def __init__(self):
        self.timestamp       = None
        self.version         = None
        self.device_type     = None
        self.firmware_type   = None
        self.program         = None
        self.image_size      = None
        self.image_checksum  = None
        self.parameters      = None

    @staticmethod
    def parse_section(lines, section_name, _raw=False, _strip=False):
        try:
            start_index = lines.index('SECTION %s' % section_name)
            end_index = lines[start_index:].index('END SECTION') + start_index

            # cut out the section
            data = lines[start_index+1:end_index]

            if _strip:
                data = [x.strip() for x in data
                        if x.strip()]

            if _raw:
                return data

            return ''.join(data)
        except ValueError:
            return None

    def is_valid_image(self):
        return True # FIXME: should perform actual sanity checks

    def parse_firmware_image_file(self, text):
        if os.path.exists(text):
            text = open(text, 'r').read()

        text = text.replace('\r\n', '\n')
        lines = text.split('\n')

        self.timestamp = int(FirmwareImage.parse_section(lines, 'TIMESTAMP', _strip=True))
        self.version = FirmwareImage.parse_section(lines, 'VERSION', _strip=True)
        self.device_type = FirmwareImage.parse_section(lines, 'DEVICE_TYPE', _strip=True)
        self.firmware_type = FirmwareImage.parse_section(lines, 'FIRMWARE_TYPE', _strip=True)
        self.program = FirmwareImage.parse_section(lines, 'PROGRAM', _strip=True)
        self.image_size = FirmwareImage.parse_section(lines, 'IMAGE_SIZE', _strip=True)
        self.image_checksum = FirmwareImage.parse_section(lines, 'IMAGE_CHECKSUM', _strip=True)
        parameters = FirmwareImage.parse_section(lines, 'TABLE', _raw=True)
        self.parameters = '\n'.join(parameters)

    def parse_parameters(self, daq_format=False):
        params = {}

        for i,param in enumerate(self.parameters.split('\n')):
            tokens = param.strip().split(' ')

            address = tokens[0]
            default_value = tokens[1]
            data_type = tokens[2]
            parser = tokens[3]
            label = ' '.join(tokens[4:]).strip('"')

            if parser == 'integer':
                default_value = int(default_value)
            elif parser == 'float':
                default_value = float(default_value)

            params[label] = dict(address=address,
                                 default_value=default_value,
                                 data_type=data_type,
                                 parser=parser)

        if daq_format:
            relabled = dict()

            for label, map in params.items():
                relabled[map['address']] = dict(label=label,
                                                default=map['default_value'],
                                                parser=map['parser'],
                                                data_type=map['data_type'])

            params = relabled

        return params
