

import copy
import math
import time
from collections import defaultdict
from DAQ.util.devices import PANEL_LEVEL
from DAQ.util.devices import ROLLUP_MAPPER
from DAQ.util.stats import LowPassFloat
from DAQ.util.time_rounding import round_to_nearest_second
from DAQ.util.utctime import utcepochnow
from DAQ.util.utctime import utcnow


THRESHHOLD_CURRENT_PERCENTAGE = .035

class PeriodManager():
    def __init__(self, type, cache):
        self.type = type
        self.cache = cache
        self.periods = {}

    def clear(self):
        for period in self.periods.values():
            period.clear()
        self.periods = {}

    def process_expirations(self):
        now = utcnow()

        for period in self.periods.values():
            period.expire_all_before(now)

    def remove_interval(self, interval):
        try:
            del self.periods[interval]
        except KeyError:
            pass

    def get_interval_values(self):
        return self.periods.keys()

    def get_interval_periods(self):
        return self.periods.values()

    def get_intervals(self):
        return self.periods.items()

    def add_interval(self, interval, overlap=None):
        self.periods[interval] = RollupPeriod(self.type, interval, self.cache,
                                              overlap=overlap)

    def append(self, record):
        for period in self.periods.values():
            #: A period makes a copy of the record, no need to copy here.
            period.append(record)

    def expire_all_old(self):
        for period in self.periods.values():
            period.expire_timeslot(period.get_oldest_not_updated())

    def expire_all_before(self, timeslot):
        for period in self.periods.values():
            period.expire_all_before(timeslot)

class RollupPeriod():
    def __init__(self, type, interval, cache, overlap=None):
        self.calc_fields = ROLLUP_MAPPER[type]['calc_fields']
        self.map_fields = ROLLUP_MAPPER[type]['map_fields']
        self.accum_fields = ROLLUP_MAPPER[type]['accum_fields']
        #: at the moment, an accumulated field can look at
        #: row.field_max for the accumulated value for that
        #: 5 minute period. This may change at some point
        self.calc_fields.extend(self.accum_fields)

        self.type = type
        self.cache = cache

        self.last_update = time.time()

        self.overlap = overlap
        self.set_interval(interval)

    def clear(self):
        for record in self.get_all_records():
            record.clear()
        self.timeslots = {}
        self.timeslot_updates = {}
        self.timeslots_string_current = {}
        self.last_freezetime = {}
        self.interval = None

    def set_interval(self, interval):
        self.interval = interval
        self.expire = interval * 2 # reset filter if a whole rollup interval is missed

        self.last_freezetime = defaultdict(lambda: None)
        self.timeslot_updates = {}
        self.timeslots = {}
        self.timeslots_string_current = defaultdict(dict)

    def expire_all_before(self, timeslot):
        for ts in [_ts for _ts in self.timeslots if _ts <= timeslot]:
            self.expire_timeslot(ts)

    def get_oldest(self):
        oldest = None

        for timeslot in self.timeslots.keys():
            if timeslot < oldest or oldest is None:
                oldest = timeslot

        return oldest

    def get_oldest_not_updated(self):
        now = utcepochnow()

        non_updating_timeslots = [ts for ts,upd in self.timeslot_updates.items()
                                   if now - upd > self.expire]

        non_updating_timeslots.sort()

        if len(non_updating_timeslots):
            return non_updating_timeslots[0]
        else:
            return None

    def get_all_records(self):
        for timeslot in sorted(self.timeslots.keys()):
            for record in self.timeslots[timeslot].values():
                yield record

        raise StopIteration

    def get_records(self, timeslot):
        try:
            return self.timeslots[timeslot].values()
        except KeyError:
            return None

    def expire_timeslot(self, timeslot):
        try:
            del self.timeslots[timeslot]
        except KeyError:
            pass

        try:
            del self.timeslot_updates[timeslot]
        except KeyError:
            pass

    def append(self, rrecord):
        rounded_times = list()

        rounded_times.append(round_to_nearest_second(rrecord['freezetime'],
                                                     to_nearest=self.interval,
                                                     rounding_func=math.ceil))

#        if self.overlap is not None:
#            overlap_freezetime = rrecord['freezetime'] + self.overlap
#
#            rounded_time_overlap = round_to_nearest_second(overlap_freezetime,
#                                                           to_nearest=self.interval,
#                                                           rounding_func=math.ceil)
#
#            if rounded_time_overlap != rounded_times[0]:
#                rounded_times.append(rounded_time_overlap)

        for rounded_time in rounded_times:
            #: Copy the record for modifications later
            record = copy.copy(rrecord)

            record['freezetime'] = rounded_time
            record['rollup_interval'] = self.interval

            key_record = '|'.join([str(record[k]) for k in self.map_fields])

            self.timeslot_updates[rounded_time] = utcepochnow()

            if not self.timeslots.has_key(rounded_time):
                self.timeslots[rounded_time] = {}

            if not self.timeslots[rounded_time].has_key(key_record):
                self.timeslots[rounded_time][key_record] = StatsRecord(self, record)
            else:
                self.timeslots[rounded_time][key_record].append(record)

class StatsRecord():
    def __init__(self, period, record):
        self.period = period

        self.record = record

        self.calc_cols = []

        for field in self.period.calc_fields:
            if field not in record.keys():
                continue

            key_record = '|'.join([str(self.record[k])
                                   for k in self.period.map_fields])
            key_record += '|%s' % (field)

            col = StatsCol if field not in self.period.accum_fields else AccumedStatsCol
            self.calc_cols.append(col(field, record=self, key_record=key_record))
            # self.calc_cols[-1].append(record[field])

        if self.period.type == PANEL_LEVEL:
            ts = self.period.timeslots_string_current[self.record['freezetime']]

            if not ts.has_key(self.record['id_string']):
                key_record = self.record['id_string']

                ts[self.record['id_string']] = {'Io': StatsCol('Io', record=self, key_record=key_record+'|Io')}

            # ts_string = ts[self.record['id_string']]

            # if self.record['Io'] != 0.0:
            #     ts_string['Io'].append(self.record['Io'])
        self.append(self.record)

        for field in self.period.calc_fields:
            if field in record:
                del self.record[field]

        if self.period.type == PANEL_LEVEL:
            del self.record['Pi']
            del self.record['Po']

        # self.append(self.record)

    def clear(self):
        self.period = None
        self.record = None

    def to_dict(self):
        for field in self.calc_cols:
            self.record.update(field.to_dict())

        # calculate energy in joules
        if self.period.type == PANEL_LEVEL:
            ts = self.period.timeslots_string_current[self.record['freezetime']]
            ts_string = ts[self.record['id_string']]

            #: Only update string cube if the current data is similar.
            if not (self.record['Io_mean'] == 0.0 \
                    or math.isnan(self.record['Io_mean'])):
                ts_string['Io'].calc()

                if self.record['Io_mean'] / ts_string['Io']['mean'] > THRESHHOLD_CURRENT_PERCENTAGE:
                    self.record.update(ts_string['Io'].to_dict(calc=False))

            try:
                self.record['Pi_mean'] = self.record['Ii_mean'] * self.record['Vi_mean']
            except TypeError:
                self.record['Pi_mean'] = float('nan')

            try:
                self.record['Po_mean'] = self.record['Io_mean'] * self.record['Vo_mean']
            except TypeError:
                self.record['Po_mean'] = float('nan')

            try:
                self.record['Eo'] = self.record['Po_mean'] * self.period.interval / 3600
            except TypeError:
                self.record['Eo'] = float('nan')

        return self.record

    def append(self, record):
        for field in self.calc_cols:
            field.append(record[field.name])

        if self.period.type == PANEL_LEVEL:
            ts = self.period.timeslots_string_current[record['freezetime']]
            ts_string = ts[record['id_string']]

            if record['Io'] != 0.0:
                if ts_string['Io'].last_data:
                    if record['Io'] / ts_string['Io'].last_data > THRESHHOLD_CURRENT_PERCENTAGE:
                        ts_string['Io'].append(record['Io'])
                else:
                    ts_string['Io'].append(record['Io'])

            self.record['op_stat'] &= record['op_stat']
            self.record['reg_stat'] &= record['reg_stat']

class StatsCol():
    def __init__(self, name, store_array=False, do_filter=True, record=None, key_record=None):
        self.name = name
        self.store_array = store_array
        self.do_filter = do_filter
        self.record = record
        self.key_record = key_record

        if self.do_filter:
            if self.record:
                self.key_lpf = 'rollupcache:%s:lpf' % (self.key_record)
                self.key_ts = 'rollupcache:%s:ts' % (self.key_record)

                lpf = self.record.period.cache.get(self.key_lpf)
                ts = self.record.period.cache.get(self.key_ts)

                if lpf and ts:
                    ts = int(ts)

                    #: Reset LPF if the last known data is way old.
                    #: otherwise use the cached value.
                    #: conversely, if processing old data then the
                    #: lpf should be reset.
                    if abs(self.record.record['freezetime'] - ts) >= self.record.period.expire:
                        initial = 0.0
#                        print self.key_lpf, "Resetting LPF due to old records", self.record.record['freezetime'], ts
                    else:
#                        print self.key_lpf, "Loading value", float(self.record.period.cache[self.key_lpf])
                        initial = float(lpf)

                        #: just in case the cache has nan, discard that
                        #: nan and replace with 0 since nans are viral.
                        if math.isnan(initial):
                            initial = 0.0

                            #: Reset the cache so that it no longer contains nan
                            self.record.period.cache[self.key_lpf] = str(0.0)
                else:
#                    print self.key_lpf, 'no LPF cache available for loading'
                    initial = 0.0
            else:
                initial = 0.0

            self.filtered_data = LowPassFloat(frequency=4, initial=initial)
        else:
            initial = None

        self.sum = None
        self.sum2 = None
        self.count = None
        self.min = None
        self.max = None
        self.mean = None
        self.rms = None
        self.stdev = None

        self.last_data = initial

        self.array = []
        self.identifiers = []

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except:
            return None

    def recalc(self):
        assert len(self.array), "Must have elements in the array to calculate from"

        self.clear()

        self.append(self.array)

    def clear(self, array=False):
        self.sum = None
        self.sum2 = None
        self.count = None
        self.min = None
        self.max = None
        self.mean = None
        self.rms = None
        self.stdev = None

        if array:
            self.array = []
            self.identifiers = []

    def append(self, data, store_array=True, identifier=None):
        if data is None:
            return

        if isinstance(data, (tuple, list)):
            for x in data:
                self.append(x)
            return

        try:
            data = float(data)
        except TypeError:
            return

        #: Don't corrupt valid data that we do have with a nan
        if math.isnan(data):
            return

        #: DO NOT include invalid data reports in the
        #: count of records
        if self.count is None:
            self.count = 1
        else:
            self.count += 1

        if self.do_filter:
            #: if open circuit arrives, set to open circuit immediately.
            if data != 0.0:
                self.filtered_data += data

                data = self.filtered_data.value
            else:
                data = 0.0
                self.filtered_data.value = data

            self.record.period.cache[self.key_lpf] = str(data)
            self.record.period.cache[self.key_ts] = str(self.record.record['freezetime'])

#            print self.key_lpf, str(self.record.record['freezetime']), str(data)

        self.last_data = data

        if self.store_array and store_array:
            self.array.append(data)
            self.identifiers.append(identifier)

        if self.sum is not None:
            self.sum += data
            self.sum2 += data * data

            if data < self.min:
                self.min = data

            if data > self.max:
                self.max = data
        else:
            self.sum = data
            self.sum2 = data * data
            self.min = data
            self.max = data

    def calc(self):

        #: Some reasonable defaults for a small number of
        #: reports
        if self.count == 0:
            self.mean = None
            self.stdev = None
            self.rms = None
        elif self.count == 1:
            self.mean = self.sum
            self.rms = self.sum
            self.stdev = 0.0
        else:
            try:
                self.mean = self.sum / self.count
            except ZeroDivisionError: #: with the code added above this won't happen
                self.mean = 0.0
            except TypeError:
                self.mean = None

            try:
                self.stdev = math.sqrt( (self.sum2 / self.count)
                                       -(self.mean * self.mean))
            except ValueError:
                self.stdev = 0.0
            except TypeError:
                self.stdev = None

            try:
                self.rms = math.sqrt(self.sum2 / self.count)
            except ValueError:
                self.rms = 0.0
            except TypeError:
                self.rms = None

        if self.sum is None:
            self.sum = float('nan')
        if self.sum2 is None:
            self.sum2 = float('nan')
        if self.min is None:
            self.min = float('nan')
        if self.max is None:
            self.max = float('nan')
        if self.mean is None:
            self.mean = float('nan')
        if self.rms is None:
            self.rms = float('nan')
        if self.stdev is None:
            self.stdev = float('nan')
        if self.count is None:
            self.count = 0

    def to_dict(self, calc=True):
        if calc:
            self.calc()

        data = {
            self.name + '_sum': self.sum,
            self.name + '_sum2': self.sum2,
            self.name + '_count': self.count,
            self.name + '_min': self.min,
            self.name + '_max': self.max,
            self.name + '_mean': self.mean,
            self.name + '_rms': self.rms,
            self.name + '_stdev': self.stdev,
        }

        return data

class AccumedStatsCol():
    def __init__(self, name, store_array=False, record=None, key_record=None):
        self.name = name
        self.store_array = store_array
        self.record = record
        self.key_record = key_record

        self.count = None
        self.min = None
        self.max = None
        self.diff = None

        self.array = []
        self.identifiers = []

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except:
            return None

    def recalc(self):
        assert len(self.array), "Must have elements in the array to calculate from"

        self.clear()

        self.append(self.array)

    def clear(self, array=False):
        self.count = None
        self.min = None
        self.max = None
        self.diff = None

        if array:
            self.array = []
            self.identifiers = []

    def append(self, data, store_array=True, identifier=None):
        if data is None:
            return

        if isinstance(data, (tuple, list)):
            for x in data:
                self.append(x)

        #: Include invalid data reports in the
        #: count of records
        if self.count is None:
            self.count = 1
        else:
            self.count += 1

        try:
            data = float(data)
        except TypeError:
            return

        #: Don't corrupt valid data that we do have with a nan
        if math.isnan(data):
            return

        if self.store_array and store_array:
            self.array.append(data)
            self.identifiers.append(identifier)

        if self.min is not None:
            if data < self.min:
                self.min = data
            if data > self.max:
                self.max = data

            #: In case the number rolls over?
            if self.min < (self.max/2):
                self.diff = self.min
            else:
                self.diff = self.max - self.min
        else:
            self.min = data
            self.max = data
            self.diff = 0.0

    def calc(self):
        if self.min is None:
            self.min = float('nan')
        if self.max is None:
            self.max = float('nan')
        if self.diff is None:
            self.diff = float('nan')
        if self.count is None:
            self.count = 0

    def to_dict(self):
        self.calc()

        data = {
            self.name + '_count': self.count,
            self.name + '_min': self.min,
            self.name + '_max': self.max,
            self.name + '_diff': self.diff,
        }

        return data
