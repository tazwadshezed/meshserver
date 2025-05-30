
import datetime
import re

from DAQ.util.utctime import datetime_to_epoch
from numpy.compat import unicode

re_is_hex_word = re.compile(r'^([0-9a-fA-F]*$)')
re_is_number = re.compile(r'^([0-9])*\.?([0-9])*?$')

def format_mac(mac_addr, separator=':'):
    tokens = []

    # 0 padding
    if len(mac_addr) % 2 == 1:
        mac_addr = '0' + mac_addr

    for i in range(0, len(mac_addr), 2):
        offset = i + 2

        tokens.append(mac_addr[i:offset].upper())

    return separator.join(tokens)

def unicode_dict_to_str_dict(d):
    new_d = dict()

    for k,v in d.items():
        if isinstance(k, unicode):
            k = str(k)
        if isinstance(v, unicode):
            v = str(v)
        if isinstance(v, dict):
            v = unicode_dict_to_str_dict(v)

        new_d[k] = v

    return new_d

def dict_datetime_to_timestamps(d):
    for k,v in d.items():
        if isinstance(v, datetime.datetime):
            v = datetime_to_epoch(v)
        elif isinstance(v, dict):
            v = dict_datetime_to_timestamps(v)

        d[k] = v

    return d

def apply_to_dict(d, func):
    for k,v in d.items():
        if isinstance(v, dict):
            v = apply_to_dict(v, func)
        else:
            k,v = func(k,v)
        d[k] = v

    return v

def auto_coerce(value):
    value = str(value).strip()

    capped = value.capitalize()

    if capped == 'True':
        value = True
    elif capped == 'False':
        value = False
    else:
        try:
            value = int(value)
        except:
            try:
                value = float(value)
            except:
                pass

    return value
