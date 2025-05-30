import json
import socket
import time
import uuid

from DAQ.util.config import load_config  # ✅ Use our unified load_config
import math
import random
import numpy

from DAQ.util.stats import frange

ln = lambda val: math.log(abs(val))
rand = lambda: float(random.randrange(0, 5)) / 100 + random.choice([.965, 1])

# ✅ Load Config
config = load_config()


class IrradianceSimulator():

    def __init__(self,
                 mean=0,
                 stdev=.01,
                 irradiance=400,
                 num_points=150):
        self.population = numpy.random.normal(mean, stdev, num_points)
        a = list(self.population[num_points / 2:])
        b = list(self.population[:num_points / 2])
        a = [abs(x) + 1 for x in a]
        a.sort(reverse=True)
        b = [1 - abs(x) for x in b]
        b.sort(reverse=True)
        # b = [-x for x in b]
        self.bell = list(a + b)
        self.factors = [x for x in self.bell]
        self.belli = 0
        self.num_points = num_points

        self.irradiance = irradiance

    def fluctuate(self, factor_modifier=None):
        factor = self.bell[self.belli]

        if callable(factor_modifier):
            factor = factor_modifier(self, factor)

        self.factors[self.belli] = factor

        self.irradiance *= factor

        self.belli += 1

        if self.belli == len(self.bell):
            self.belli = 0

class MonitorSimulator:
    """Simulates monitor data and sends it to the emulator."""

    def __init__(self, Voc=40.0, Vmp=32.0, Isc=6.0, Imp=5.0,
                 mean=0, stdev=.01, num_points=150):

        self.mean = mean
        self.stdev = stdev
        self.num_points = num_points

        self.population = numpy.random.normal(mean, stdev, num_points)
        a = list(self.population[num_points // 2:])
        b = list(self.population[:num_points // 2])
        a = [abs(x) + 1 for x in a]
        a.sort(reverse=True)
        b = [1 - abs(x) for x in b]
        b.sort(reverse=True)
        # b = [-x for x in b]
        self.bell = list(a + b)
        self.factors = [x for x in self.bell]
        self.belli = 0

        self.Voc = Voc
        self.Vmp = Vmp
        self.Isc = Isc
        self.Imp = Imp

        self.maxIsc = self.Isc * 2
        self.minIsc = self.Isc / 2
        self.maxVoc = self.Voc * 2
        self.minVoc = self.Voc / 2

        self._Voc = Voc
        self._Vmp = Vmp
        self._Isc = Isc
        self._Imp = Imp

        self._calc()

        self.fluctuate()
        self.belli = 0

        self.host = config["network"]["host"]  # ✅ Use proper config keys
        self.port = config["network"]["emulator_port"]
        self.report_interval = config["devices"]["report_interval"]

    def __repr__(self):
        return "<MonitorSimulator I:%f V:%f P:%f Isc:%f Voc:%f>" % (self.I, self.V, self.P,
                                                                      self.Isc, self.Voc)

    def __str__(self):
        return self.__repr__()

    def reset(self):
        self.Voc = self._Voc
        self.Vmp = self._Vmp
        self.Isc = self._Isc
        self.Imp = self._Imp
        self.belli = 0
        self.fluctuate()
        self.belli = 0

    def _do_input_calculations(self):
        Voc = self.Voc
        Vmp = self.Vmp
        Imp = self.Imp
        Isc = self.Isc

        Rs = (Voc - Vmp) / Imp

        a = (Vmp * (1 + (Rs * Isc / Voc)) + Rs * (Imp - Isc)) / Voc

        N = ln(2 - math.pow(2, a)) / ln(Imp / Isc)

        self.Rs = Rs
        self.a = a
        self.N = N

    def _calc(self):
        self._do_input_calculations()

        vals = []

        for I in frange(0, self.Isc, .1):
            V = math.pow(I / self.Isc, self.N)
            V = self.Voc * ln(2 - V)
            V = (V / ln(2)) - self.Rs * (I - self.Isc)
            V = V / 1 + (self.Rs * self.Isc / self.Voc)

            vals.append((I, V))

        best_index = 0
        best_value = float('-inf')
        for i, val in enumerate(vals):
            if val[0] * val[1] > best_value:
                best_value = val[0] * val[1]
                best_index = i

        self.I, self.V = vals[best_index]

        self.P = self.I * self.V

    def irradiance(self, factor):
        # if self.Isc < self.minIsc:
        #    factor = abs(factor + 1)
        # if self.Isc > self.maxIsc:
        #    factor = abs(factor - .956)

        self.Isc = self.Isc * factor
        self.Imp = self.Isc * .85  # Imp runs about 85-87% of Isc

        self._calc()

    def temperature(self, factor):
        # if self.Voc < self.minVoc:
        #    factor = abs(factor + 1)
        # if self.Voc > self.maxVoc:
        #    factor = abs(factor - .956)

        self.Voc = self.Voc * factor
        self.Vmp = self.Voc * .80  # Vmp runs about 80% of Voc

        self._calc()

    def fluctuate(self, factor_modifier=None):
        factor = self.bell[self.belli]

        if callable(factor_modifier):
            factor = factor_modifier(self, factor)

        # print factor, self.bell[self.belli], self.belli
        self.factors[self.belli] = factor
        self.irradiance(factor)

        if random.random() < .10:
            self.temperature(factor)
        # if random.random() < .5:
        #    self.irradiance(factor)
        # else:
        #    self.temperature(factor)

        self.belli += 1

        if self.belli == len(self.bell):
            self.belli = 0

        # if random.random() < .15:
        #    self.temperature(self.bell[self.belli])

    def as_hex_tuples(self):
        return math.modf(self.I), math.modf(self.V)

    def generate_data(self):
        """Generate fake monitor data."""
        return {
            "monitor_id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "voltage": round(30 + (5 * time.time() % 2), 2),  # Simulated fluctuation
            "current": round(5 + (2 * time.time() % 1.5), 2),
            "power": round(150 + (10 * time.time() % 3), 2),
            "temperature": round(25 + (3 * time.time() % 1.2), 2)
        }

    def send_data(self):
        """Send monitor data to the emulator."""
        try:
            with socket.create_connection((self.host, self.port), timeout=5) as emulator_socket:
                data = self.generate_data()
                json_data = json.dumps(data)
                emulator_socket.sendall(json_data.encode("utf-8"))
                print(f"\033[96m[SIMULATOR]\033[0m Sent Data: {json_data}")
        except Exception as e:
            print(f"\033[91m[SIMULATOR ERROR]\033[0m Failed to send data: {e}")

    def run(self):
        """Continuously sends data at a regular interval."""
        print(f"\033[94m[SIMULATOR]\033[0m Running with {self.report_interval}s interval...")
        while True:
            self.send_data()
            time.sleep(self.report_interval)


if __name__ == "__main__":
    simulator = MonitorSimulator()
    simulator.run()
