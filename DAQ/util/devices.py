
#: Maps source device types into a device type for cube storage
#: Mainly, for example, monitor and monitors both go into panel data
DTYPE_MAPPER = {
    'mon': 'mon',
    'pnl': 'mon',
    'env': 'env',
    'acm': 'acm',
    'inv': 'inv',
    'alert': 'alert'
}

PANEL_LEVEL = 'mon'
STRING_LEVEL = 'str'
INVERTER_LEVEL = 'inv'
ARRAY_LEVEL = 'array'

MONITOR = 'mon'
ENVIRONMENTAL = 'env'
ACMETER = 'acm'
INVERTER = 'inv'

ROLLUP_MAPPER = {
    MONITOR: {
        'map_fields': ['macaddr', 'graph_key'],
        'calc_fields': ['Vi', 'Vo', 'Ii', 'Io'],
        'accum_fields': [],
    },
    ENVIRONMENTAL: {
        'calc_fields': ['ambient_temperature', 'panel_temperature',
                        'internal_temperature', 'irradiance'],
        'map_fields': [],
        'accum_fields': [],
    },
    INVERTER: {
        'map_fields': ['serial_number'],
        'calc_fields': ['eff', 'ACFreq',
                        'ACV', 'ACI',
                        'Vi', 'Ii', 'Pi',
                        'ACEo', 'ACPo']
    },
    ACMETER: {
        'map_fields': ['serial_number'],
        'calc_fields': ['ACPo', 'ACVARs', 'ACVAs', 'ACPwrFactor', 'ACFreq',
                        'ACV_A_N', 'ACV_B_N', 'ACV_C_N', 'ACV_A_B',
                        'ACV_B_C', 'ACV_C_A', 'ACI_A', 'ACI_B', 'ACI_C',],
        'accum_fields': ['ACEo_received', 'ACEo_delivered', 'ACEo_net',
                         'ACEo_total', 'ACVAR_positive', 'ACVAR_negative',
                         'ACVAR_net', 'ACVAR_total', 'ACVA_total'],
    }
}

def validate_opt(record):
    return -2.0 <= record['Ii'] <= 15.0 \
    and    -2.0 <= record['Io'] <= 15.0 \
    and     -10.0 <= record['Vi'] <= 1000.0 \
    and     -10.0 <= record['Vo'] <= 1000.0

_validate_mapper = {PANEL_LEVEL: validate_opt}

def validate(record):
    dtype = DTYPE_MAPPER.get(record['type'], record['type'])

    if dtype in _validate_mapper:
        return _validate_mapper[dtype](record)

    return True
