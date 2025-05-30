import functools
import re
from DAQ.util.utctime import utcepochnow

CMD_FUNCS = []
OFFSET_CUTOFF = 86400

class CommandValidationError(Exception):
    pass

def cmdreq_validate(rqst):
    rqst.setdefault('project_id', None)
    rqst.setdefault('func', None)
    rqst.setdefault('funcname', None)
    rqst.setdefault('classname', None)
    rqst.setdefault('args', {})
    rqst.setdefault('ttl', None)
    rqst.setdefault('routing_id', None)
    rqst.setdefault('status', 'pending')
    rqst.setdefault('received_on', utcepochnow())
    rqst.setdefault('completed_on', None)
    rqst.setdefault('dispatched_on', None)

    if rqst['funcname'] is None \
    and rqst['func']:
        if '.' in rqst['func']:
            rqst['classname'], rqst['funcname'] = rqst['func'].split('.')
        else:
            rqst['funcname'] = rqst['func']

    #: if ttl is less than a day, then
    #: the ttl is an offset from current time
    if rqst['ttl'] is not None \
    and rqst['ttl'] < OFFSET_CUTOFF:
        rqst['ttl'] = utcepochnow() + rqst['ttl']

    return rqst

def cmdreq_is_valid(rqst):
    return rqst['func'] is None

def cmdreq_is_timeout(rqst, timestamp=None):
    if timestamp is None:
        timestamp = utcepochnow()
    return rqst['ttl'] is not None \
           and rqst['ttl'] < timestamp

def cmdreq_is_done(rqst):
    return rqst['status'] == 'done'

def cmdreq_can_clean(rqst):
    return cmdreq_is_done(rqst) \
        or cmdreq_is_timeout(rqst)

def command(func):
    @functools.wraps(func)
    def _command(*args, **kwargs):
        return func(*args, **kwargs)

    CMD_FUNCS.append(func.__name__)

    return _command

def call(value, func):
    return func(value)

def call_inlist(value, matches):
    return value, \
           value in matches, \
           "required to be one of %s" % (str(matches))

def call_isinstance(value, istype):
    return value, \
           isinstance(value, istype), \
           "not an instance of %s" % (str(istype))

def call_list(value, func):
    if not isinstance(value, list):
        return value, \
               False, \
               "required to be a list"

    status = None
    errmsg = ""

    new_values = []

    for i,v in enumerate(value):
        newv, valid, errmsg = func(v)

        new_values.append(newv)

        if status is None:
            status = valid
        else:
            status = status and valid

    return new_values, \
           status, \
           "an item in the list is %s" % (errmsg)

def call_couldbe_list(value, istype):
    if not isinstance(value, list):
        value = [value]

    status = None
    errmsg = ""

    new_values = []

    for i,v in enumerate(value):
        newv, valid, errmsg = call_isinstance(v, istype)

        new_values.append(newv)

        if status is None:
            status = valid
        else:
            status = status and valid

    return new_values, \
           status, \
           "an item in the list is %s" % (errmsg)

def validate(arg, istype, cmp=call_isinstance, required=False):
    def _validate(func):
        @functools.wraps(func)
        def __validate(*args, **kwargs):
            if required and \
            not kwargs.has_key(arg):
                raise CommandValidationError("argument ``%s`` is required" % (arg))

            if kwargs.has_key(arg):
                value, valid, errmsg = cmp(kwargs[arg], istype)

                if not valid:
                    raise CommandValidationError("argument ``%s`` is %s" % (arg,errmsg))

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
        value = value.zfill(len(value)+1)

    re_is_hex = re.compile(r'^([0-9a-fA-F]*$)')

    return value, \
           re_is_hex.match(value) is not None, \
           "not a valid hex value"

def is_macaddr(macaddr):
    if macaddr is None:
        return macaddr, True, ""

    if isinstance(macaddr, (tuple, list)):
        if len(macaddr) > 1:
            return macaddr, False, "please provide only one mac address"
        elif len(macaddr) == 1:
            macaddr = macaddr[0]
        else:
            return macaddr, False, "please provide a mac address"

    if isinstance(macaddr, (int, float)):
        macaddr = str(int(macaddr))

    macaddr = macaddr.zfill(16)

    re_is_hex = re.compile(r'^([0-9a-fA-F]*$)')

    return macaddr, \
           re_is_hex.match(macaddr) is not None, \
           "not a valid mac address"

re_is_ip = re.compile(r'((2[0-5]|1[0-9]|[0-9])?[0-9]\.){3}((2[0-5]|1[0-9]|[0-9])?[0-9])', re.I)

def is_config_key(key):
    key, valid, errmsg = call_isinstance(key, str)

    if valid:
        if key.count('.') != 1:
            errmsg = 'must be defined in the format of <CATEGORY.KEY>'
            valid = False

    return key, valid, errmsg

def is_ip_addr(url):
    matchobj = re_is_ip.match(url)

    if matchobj:
        ip = matchobj.group()
        valid = True
        errmsg = ''
    else:
        ip = None
        valid = False
        errmsg = 'not a valid ip address'

    return ip, valid, errmsg
