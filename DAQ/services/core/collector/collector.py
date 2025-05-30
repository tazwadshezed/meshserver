import multiprocessing
import json
import math
from DAQ.util.handlers.common import IHandler
from DAQ.util.utctime import utcnow
from DAQ.devices import connectors
class DeviceCollector(IHandler):

    def __init__(self, *args, **kwargs):
        super(DeviceCollector, self).__init__(*args, **kwargs)

        self.do_collecting = multiprocessing.Event()

    def wake(self):
        self.do_collecting.set()

    def sleep(self):
        self.do_collecting.clear()

    def worker(self, data_queue, processed_queue):
        devices_value = self.get('devices')
        device_meta = json.loads(devices_value) if isinstance(devices_value, (str, bytes)) else devices_value

        convert_irradiance = self.get('convert_irradiance', False)

        device_connectors = []

        for device in device_meta:
            connector = connectors[device['identifier']]

            device_connector = connector(*device['args'], **device['kwargs'])
            device_connector.logger = self.logger

            device_connectors.append(device_connector)

        while self._check_living():

            if self.do_collecting.is_set():

                for device in device_connectors:
                    try:
                        device.verify()
                    except:
                        device.verified = False

                    self.logger.info("Verifying %s -- %s" % (device.__class__.__name__, getattr(device, 'verified')))

                for device in device_connectors:
                    if not device.verified:
                        continue

                    self.logger.info("Reading %s" % (device.__class__.__name__))
                    data = device.read_data()

                    if data is not None:

                        if convert_irradiance \
                        and 'irradiance' in data \
                        and data['irradiance'] is not None \
                        and not math.isnan(data['irradiance']):
                            data['irradiance'] = data['irradiance'] * 1000.0

                        data['freezetime'] = utcnow()
                        data['type'] = device.__dtype__

                        # for k in data:
                        #     self.logger.info('%s = %s' % (k, data[k]))

                        self.processed_queue.put(data)



        for device in device_connectors:
            device.close()
