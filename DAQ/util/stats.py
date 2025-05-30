
import math
from collections import deque
from scipy.stats import stats


def frange(start, stop=None, increment=1.0, significant=5):
    if stop is None:
        stop, start = start, 0.0
    else:
        start = float(start)

    count = int((stop - start) / increment) + 1

    return (round(start + n*increment, significant) for n in range(count))


def stdev_distance(x, mean, stdev, absolute=False):
    def _calc(z):
        try:
            a = (z - mean) / stdev
        except ZeroDivisionError:
            a = 0

        if absolute:
            a = abs(a)

        return a

    if isinstance(x, (list, tuple)):
        xx = [_calc(i) for i in x]
    else:
        xx = _calc(x)

    return xx

def percentage_outlier_variance(x, max_stdev_distance):
    def _calc(z):
        try:
            a = (z * 100) / max_stdev_distance
        except ZeroDivisionError:
            a = 0

        return a

    if isinstance(x, (list, tuple)):
        xx = [_calc(i) for i in x]
    else:
        xx = _calc(x)

    return xx

def is_outlier(x, mean, stdev):
    y = abs(stdev_distance(x, mean, stdev))
    if y > 3:
        return True
    else:
        return False

def meanstdv(the_list):
    if the_list:
        return stats.mean(the_list), stats.stdev(the_list)
    else:
        return None, None

def color_scale(x, MAX_VALUE=255):
    return math.floor(255 * x / MAX_VALUE)

def clamp(x, min=0, max=255):
    if x < min:
        return min
    if x > max:
        return max
    return x

class LowPassFloat(object):
    """
    Is a float, but whenever data is added it will perform the lowpass division
    based on frequency. This is an IIR low pass filter.
    """
    def __init__(self, initial=0.0, frequency=7):
        self.stable_at = int(frequency * 3.125)
        self.value = float(initial)
        self.last_addition = 0.0
        self.iterations = 0
        self.frequency = frequency

    def __add__(self, other):
        self.last_addition = other

        if self.value == 0.0:
            self.value = other

        sample = other * (1.0 / self.frequency)

        history = self.value - (self.value * (1.0 / self.frequency))

        self.value = sample + history

        self.iterations += 1

        return self

    def __radd__(self, other):
        return self + other

    def __repr__(self):
        return "%f(%s)" % (self.value, self.stable)

    @property
    def stable_value(self):
        if self.stable:
            return self.value
        else:
            return self.last_addition

    @property
    def stable(self):
        return self.iterations > self.stable_at

class LowPassFloatFIR(LowPassFloat):
    """
    Is a float, but uses an FIR.
    """
    def __init__(self, initial=0.0, frequency=7):
        super(LowPassFloatFIR, self).__init__(initial=initial,
                                              frequency=frequency)

        self.history = deque([initial], maxlen=frequency)
        self.sum = 0.0

    def __add__(self, other):
        self.last_addition = other

        self.sum += other

        if len(self.history) == self.history.maxlen:
            self.sum -= self.history[-1]

        self.history.append(other)

        try:
            self.value = float(self.sum) / float(len(self.history))
        except ZeroDivisionError:
            self.value = other

        self.iterations += 1

        return self

def lowpass(data, frequency=7):
    history = None
    sample = None

    stable_at = int(frequency * 3.125)

    lpd = []

    for x in data:
        sample = x * (1.0 / frequency)

        if history is not None:
            history = (lpd[-1] - (lpd[-1] * (1.0 / frequency)))
        else:
            history = sample

        lpd.append(sample + history)

    return lpd, stable_at

if __name__ == '__main__':
    data = [0.2311676799,0.0511337387,0.0569817923,0.0522142806,0.0501614968
,0.1017957202,0.057431164,0.1163331463,0.1173983702,0.1182465463,0.115993563
,0.1176563388,0.1183827993,0.1139630031,0.113157452,0.1151064667,0.1154522412
,0.1147124645,0.1195526289,0.1152356397,0.1181241129,0.1172102466,0.1170281032
,0.1163934575,0.1133850426,0.1110008666,0.0987461012,0.1159410465,0.118473295
,0.1122705036,0.1176808346,0.1127894753,0.1106104464,0.116079469,0.1173410313
,0.1150922135,0.1153395864,0.1166896905,0.1134874462,0.1158454356,0.1153310407
,0.1163355413,0.1180549067,0.1158386358,0.1176547324,0.115709138,0.1126426464
,0.1156566917,0.117281036,0.1126315982,0.1167114044,0.1163657853,0.1159634782
,0.1167496392,0.1192985411,0.1188754321,0.1135226239,0.1170788688,0.1164138858
,0.1165777846,0.1274740615,0.1295332722,0.1296577997,0.1313884483,0.139814237
,0.1286213211,0.1286305738,0.1279978843,0.1280012271,0.128841552,0.1286613561
,0.1342421075,0.129351267,0.1273452672,0.1297619725,0.1293174765]

    new = lowpass(data)


