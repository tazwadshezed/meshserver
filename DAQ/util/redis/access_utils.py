from DAQ.util.redis.exceptions import *
from redis.asyncio import Redis

MANAGER_SLOT = 0
MIN_SLOT = 1
MAX_SLOT = 158
TESTING_SLOT = 159

devtypes = {
    "SA": "Site Array",
    "I": "Inverter",
    "R": "Recombiner",
    "C": "Combiner",
    "S": "String",
    "P": "Panel",
    "SLE": "SPT String Level Equalizer",
    "SPM": "SPT Panel Monitor",
    "SPO": "SPT Panel Monitor",
    "S1W": "String 1 Wire",
    "ACM": "AC Meter",
    "SGW": "SPT Gateway",
    "SSS": "SPT Site Server",
    "SSC": "Site Server Computer",
    "ESI": "Env Sensor Interface",
    "ABT": "Ambient Temperature Sensor",
    "CET": "Cell Temperature Sensor",
    "IRR": "Irradiance Sensor",
}

monitor_devtypes = ["SPM", "SPO", "SLE"]

devtype_names = {
    "sitearray": "SA",
    "inverter": "I",
    "recombiner": "R",
    "combiner": "C",
    "string": "S",
    "panel": "P",
    "equalizer": "SLE",
    "monitor": "SPM",
    "one wire": "S1W",
    "AC meter": "ACM",
    "gateway": "SGW",
    "site server": "SSS",
    "site server computer": "SSC",
    "env sensor interface": "ESI",
    "ambient temp": "ABT",
    "cell temp": "CET",
    "irradiance": "IRR",
}

def panel_phrase(ulabel: str, use_lower: bool = False) -> str:
    """Generate a phrase for a panel."""
    string_name, panel_name = ulabel.split("|")
    if use_lower:
        return f"panel {panel_name} in string {string_name}"
    return f"Panel {panel_name} in String {string_name}"


def phrase(label: str, use_lower: bool = False) -> str:
    """Generate a human-readable phrase."""
    letter, number = label.split(":")
    devtype = devtypes.get(letter)
    if use_lower:
        devtype = devtype.lower()
    return f"{devtype} {number}"

async def get_redis_client(db=0):
    """
    Create an async Redis client connected to the specified database.
    """
    return Redis(host="localhost", port=6379, db=db)

async def has_sitearray_id(client: Redis):
    keys = await client.keys(pattern="SA-*")
    if len(keys) == 1:
        return True
    elif len(keys) > 1:
        raise MultipleGraphsLoadedException(client)
    return False

async def get_sitearray_id(client: Redis):
    keys = await client.keys(pattern="SA-*")
    if len(keys) < 1:
        raise GraphNotLoadedException(client)
    elif len(keys) > 1:
        raise MultipleGraphsLoadedException(client)
    return keys[0].decode()

async def _get_an_id(prefix, client: Redis):
    sitearray_id = await get_sitearray_id(client)
    return sitearray_id.replace("SA-", prefix)

async def get_zone_id(client: Redis):
    return await _get_an_id("Z-", client)

async def get_devdict_id(client: Redis):
    return await _get_an_id("DEV-", client)

async def get_histdict_id(client: Redis):
    return await _get_an_id("HIST-", client)

async def get_busnrule_id(client: Redis):
    return await _get_an_id("BUSN-", client)

async def get_portfolio_data_id(client: Redis):
    return await _get_an_id("PORT-", client)

async def get_redis_sites():
    client = await get_redis_client()
    keys = {}
    for key in await client.keys():
        keys[key.decode()] = await client.get(key)
    await client.close()
    return keys



async def get_props(device_id, client: Redis, include_devtype=False):
    result = await client.hgetall(device_id)
    if include_devtype and "id" in result:
        result["devtype"] = devtypes[result["id"].decode()]
    return {k.decode(): v.decode() for k, v in result.items()}

async def set_props(device_id, propdict, client: Redis):
    await client.hset(device_id, mapping=propdict)

async def get_prop(device_id, propname, client: Redis):
    return await client.hget(device_id, propname)

async def set_prop(device_id, propname, value, client: Redis):
    await client.hset(device_id, propname, value)

async def get_named_props(device_id, propname_array, client: Redis):
    return [await client.hget(device_id, propname) for propname in propname_array]

async def select_node(nodes, propname, value, client: Redis):
    for node in nodes:
        props = await get_props(node, client)
        if propname in props and (value is None or props[propname] == value):
            return node
    return None

async def dict_from_nodes(nodes, client: Redis, include_devtype=False):
    result = {}
    dict_stack = [result]
    for node in nodes:
        if node == "{":
            dict_stack.append({})
        elif node == "}":
            if len(dict_stack[-1]) < 1:
                dict_stack.pop()
            elif len(dict_stack) > 1:
                d = dict_stack.pop()
                dict_stack[-1].setdefault("inputs", []).append(d.copy())
        else:
            d = dict_stack[-1]
            props = await get_props(node, client, include_devtype)
            d.update(props)
    return result
