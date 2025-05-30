import re
from typing import List, Any, Optional, Dict

from DAQ.util.redis.access_utils import MAX_SLOT
from DAQ.util.redis.access_utils import MIN_SLOT
from DAQ.util.redis.access_utils import get_prop
from DAQ.util.redis.access_utils import get_props
from DAQ.util.redis.access_utils import get_redis_client
from DAQ.util.redis.access_utils import get_sitearray_id
from DAQ.util.redis.access_utils import set_prop
from DAQ.util.redis.exceptions import *
from redis import Redis


def natsort(l: List[str]) -> List[str]:
    """
    Sort the given list in the way that humans expect.
    """
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    l.sort(key=alphanum_key)
    return l

class GraphNode:
    """
    A node within an acyclic graph with twinned edges and named properties.
    """

    def __init__(self, node_id: str, client: Redis, **kwargs: Any):
        self.nodes = []
        if node_id is None:
            raise ValueError("Node ID cannot be None.")
        self.id = node_id
        self.client = client
        self.props = {}
        self.cin_key = f"in:{self.id}"
        self.props["id"] = self.id
        self.props.update(kwargs)

    async def set_prop(self, propname: str, value: Any) -> None:
        """Set a property for this node."""
        try:
            await self.client.hset(self.id, propname, value)
        except RedisException as e:
            raise Exception(f"Error setting property: {str(e)}")

    @staticmethod
    def get_prop(device_id: str, propname: str, client: Redis) -> Optional[str]:
        """Get a specific property of a device node."""
        try:
            return client.hget(device_id, propname)
        except RedisException:
            return None

    async def hook_into(self, parent: "GraphNode") -> None:
        """
        Attach this node to a parent node.
        """
        self.props["parent"] = parent.id
        await set_prop(parent.id, "inputs", self.id, client=self.client)

    async def inputs(self, show_levels: bool = False) -> List[str]:
        """
        Return all input devices in recursive order.
        """
        await self._collect_inputs(self.id, show_levels=show_levels)
        return self.nodes

    async def _collect_inputs(self, key: str, show_levels: bool = False) -> None:
        """
        Recursively collect all input devices for a node.
        """
        self.nodes.append(key)
        inputs = await self.client.lrange(f"in:{key}", 0, -1)
        if show_levels:
            self.nodes.append("{")
        for inp in inputs:
            await self._collect_inputs(inp, show_levels=show_levels)
        if show_levels:
            self.nodes.append("}")

    async def properties(self) -> Dict[str, Any]:
        """
        Return the properties of this node as a dictionary.
        """
        return await get_props(self.id, self.client)

    async def parents(self) -> List[str]:
        """
        Return all parent devices in recursive order.
        """
        self.nodes = []
        await self._collect_parents(self.id)
        return self.nodes

    async def _collect_parents(self, key: str) -> None:
        """
        Recursively collect all parent devices for a node.
        """
        self.nodes.append(key)
        parent = await get_prop(key, "parent", self.client)
        if parent:
            await self._collect_parents(parent)

class DeviceNode(GraphNode):
    """
    Extends GraphNode to include device-specific behaviors and relationships.
    """

    async def inverters(self) -> List[str]:
        return await self.matching_inputs(["I"])

    async def combiners(self) -> List[str]:
        return await self.matching_inputs(["C"])

    async def strings(self) -> List[str]:
        return await self.matching_inputs(["S"])

    async def panels(self) -> List[str]:
        return await self.matching_inputs(["P"])

    async def matching_inputs(self, matches: List[str]) -> List[str]:
        """
        Get all inputs but only return matching node types.
        """
        result = []
        inputs = await self.inputs()
        for inp in inputs:
            if "-" in inp:
                nodetype, _ = inp.split("-")
                if nodetype in matches:
                    result.append(inp)
        return result

class GraphManager:
    """
    Manages Redis-based site arrays, including their saving and loading.
    """

    def __init__(self, sitename: Optional[str] = None):
        self.client = None
        self.sitename = sitename

    async def set_current_site_array(self, sitename: str, db: int) -> None:
        """
        Set the current site array and initialize the Redis client.
        """
        self.client = await get_redis_client(db=db)
        self.sitename = sitename

    async def is_loaded(self, sitename: str) -> bool:
        """
        Check if a site is loaded in Redis.
        """
        keys = await self.client.keys(f"sitedata:{sitename}")
        return len(keys) > 0

    async def current_sitearray_id(self) -> Optional[str]:
        """
        Return the site array ID of the currently selected site.
        """
        return await get_sitearray_id(self.client)

    async def current_sitearray(self) -> Optional[DeviceNode]:
        """
        Return the current site array node as a DeviceNode.
        """
        sa_id = await self.current_sitearray_id()
        if sa_id:
            return DeviceNode(sa_id, client=self.client)
        return None

    @staticmethod
    async def clear_all_slots() -> None:
        """
        Clear all Redis slots.
        """
        for db in range(MIN_SLOT, MAX_SLOT + 1):
            client = await get_redis_client(db=db)
            await client.flushdb()

    async def current_keys(self) -> List[str]:
        """
        Return all keys in the current Redis database.
        """
        return await self.client.keys()

    async def setup(self, sitename: str, slot: int) -> None:
        """
        Set up a new site array in Redis.
        """
        self.client = await get_redis_client(db=slot)
        self.sitename = sitename

    async def unset(self) -> None:
        """
        Unset the current site array.
        """
        self.client = None
        self.sitename = None
