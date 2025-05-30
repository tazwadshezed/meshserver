
class Singleton(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

class LocalBackingStore(Singleton):
    """
    Implements a single global dictionary to be used as
    a key-value store. This is useful for a local in-memory
    cache for a process.
    """
    def __init__(self):
        self._data = {}

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __getattr__(self, key):
        return getattr(self._data, key)

    def clear(self):
        self._data = {}

class RedisBackingStore(object):
    """
    Implements a connection of backing store to redis.
    """
    def __init__(self, mgr):
        self.mgr = mgr

    def __getitem__(self, key):
        return self.mgr.rdb[key]

    def __setitem__(self, key, value):
        self.mgr.rdb[key] = value

    def get(self, key):
        return self.mgr.rdb.get(key)

class LocalCache(object):
    """
    Implements simple cache-switching.

    Will look up keys based on the "local" backing store,
    if they do not exists in the local store, then it will
    perform a lookup in the remote backing store and assign
    that returned value to the local store.

    Able to sync a local backing store with remote backing store
    """
    def __init__(self, local, backing_store=None):
        self.local = local
        self.backing_store = backing_store

    def __setitem__(self, key, value):
        #print "SETL", key, '->', value
        self.local[key] = value

    def clear_local(self):
        self.local.clear()

    def get(self, key):
        """
        If the cache is stored localy then return it,
        otherwise attempt to acquire the value from the
        backing store.
        """
        if self.local.has_key(key):
            #print "RETL", key,"->", self.local[key]
            return self.local[key]
        else:
            self.local[key] = self.backing_store.get(key)
            #print "RETR", key,"->",self.local[key]
            return self.local[key]

    def sync(self):
        for key,value in self.local.iteritems():
            #print "SYNC", key, "->", value
            self.backing_store[key] = value

if __name__ == '__main__':
    print("Testing singleton?")

    cache = LocalCache(RedisBackingStore(1))
    print(id(cache), id(cache.backing_store))

    cache2 = LocalCache(RedisBackingStore(2))
    print(id(cache2), id(cache2.backing_store))
