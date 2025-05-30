import urllib.request, urllib.error, urllib.parse
import re

from DAQ.devices import IConnector

class CBWConnector(IConnector):
    __identifiers__ = ['CBW']
    __dtype__ = 'env'

    re_sensors = re.compile(r'<an1state>(?P<irradiance>.*)</an1state>.*'
                            r'<sensor1temp>(?P<ambient_temperature>.*)</sensor1temp>.*'
                            r'<sensor2temp>(?P<panel_temperature>.*)</sensor2temp>.*'
                            r'<sensor3temp>(?P<internal_temperature>.*)</sensor3temp>')

    def __init__(self, url='localhost'):
        self.verified = None
        self.url = url

    def verify(self):
        try:
            xml = urllib.request.urlopen(self.url, timeout=1).read()
            self.verified = True
        except urllib.error.URLError:
            self.verified = False

    def close(self):
        pass

    def read_data(self):
        try:
            xml = urllib.request.urlopen(self.url, timeout=1).read()
        except urllib.error.URLError:
            return None

        search = CBWConnector.re_sensors.search(xml)

        if search is not None:
            groupdict = search.groupdict()

            for k in groupdict:
                if groupdict[k] == 'x.x':
                    groupdict[k] = None
                else:
                    try:
                        groupdict[k] = float(groupdict[k])
                    except ValueError:
                        groupdict[k] = None

            return groupdict

        return None
