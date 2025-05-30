"""
Exceptions related to Redis and SiteData in general.
"""
class RedisException( Exception ):
    pass

class GraphNotLoadedException( Exception ):
    """
    Exception to raise if no graph is loaded
        when expected in the given redis slot.
    """
    def __init__(self, dbindex):
        self.dbindex = dbindex

    def __repr__(self):
        return "Graph is not loaded in slot %d" % self.dbindex

class MultipleGraphsLoadedException( Exception ):
    """
    Exception to raise if more than 1 root node is loaded
        in the given redis slot.
    """
    def __init__(self, dbindex):
        self.dbindex = dbindex

    def __repr__(self):
        return "Multiple Graph root nodes detected in slot %d" % self.dbindex

class MissingListException( Exception ):
    """
    Exception to raise if an attempt was made to add an item to a
        named list when a list with that name does not exist yet.
    """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "A list named %s does not exist for the add_to_list request." % self.name

class MissingDictException( Exception ):
    """
    Exception to raise if an attempt was made to add an item to a
        named list when a list with that name does not exist yet.
    """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "A dict named %s does not exist for the add_to_dict request." % self.name

class BadSlotForDeviceNodeException( Exception ):
    """
    Exception to raise if an attempt was made to create or locate a
    GraphNode in the MANAGER_SLOT.
    """
    def __repr__(self):
        return "A GraphNode was initialized in the MANGER SLOT."

class NoAvailableSlotException( Exception ):
    """
    Exception to raise if an attempt was made to return a slot
    from redis, but no empty slot was available.
    """
    def __repr__(self):
        return "No empty slot is available in redis."

class BadIdForDeviceNodeException( Exception ):
    """
    Exception to raise if an attempt was made to create a
    GraphNode with a null ID.
    """
    def __repr__(self):
        return "A GraphNode was initialized with a null ID."

