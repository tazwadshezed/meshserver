
class NotImplementedException(Exception): pass

connectors = {}

class ConnectorMeta(type):
    def __new__(cls, name, bases, attrs):
        newclass = type.__new__(cls, name, bases, attrs)

        if '__dtype__' in attrs \
        and '__identifiers__' in attrs:
            for identifier in newclass.__identifiers__:
                connectors[identifier] = newclass

        return newclass

class IConnector(object, metaclass=ConnectorMeta):
    def verify(self):
        raise NotImplementedException()

    def close(self):
        raise NotImplementedException()

    def read_data(self):
        raise NotImplementedException()
