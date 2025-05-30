
import calendar
import datetime

import pytz
from numpy.compat import unicode

"""

  +-------------------------+-------------------------+-------------------------+
  | From                    | To                      | Use                     |
  +=========================+=========================+=========================+
  | seconds since the epoch | :class:`struct_time` in | :func:`gmtime`          |
  |                         | UTC                     |                         |
  +-------------------------+-------------------------+-------------------------+
  | seconds since the epoch | :class:`struct_time` in | :func:`localtime`       |
  |                         | local time              |                         |
  +-------------------------+-------------------------+-------------------------+
  | :class:`struct_time` in | seconds since the epoch | :func:`calendar.timegm` |
  | UTC                     |                         |                         |
  +-------------------------+-------------------------+-------------------------+
  | :class:`struct_time` in | seconds since the epoch | :func:`mktime`          |
  | local time              |                         |                         |
  +-------------------------+-------------------------+-------------------------+

"""



try:
    import ephem
except:
    ephem = None

def _get_ephem(date, lat, long):
    assert isinstance(lat, str)
    assert isinstance(long, str)

    # if isinstance(date, (datetime.date, datetime.datetime)):
    #     date = date.strftime('%Y/%m/%d')
    if isinstance(date, unicode):
        date = str(date)

    if isinstance(date, str):
        date = date.replace('-', '/')

    sun = ephem.Sun()
    here = ephem.Observer()
    here.lat = lat
    here.long = long
    here.date = date

    return here, sun

def solarnoon(date, lat, long):
    """
    Get the solar noon for a give day at latitude and longitude. Northern is -.
    So for Austin Texas USA...

    lat = "30.30"
    long = "-97.70"

    Date can be a string in y/m/d format or a datetime.date object.
    """
    here, sun = _get_ephem(date, lat, long)

    solarnoon = here.next_transit(sun).datetime()

    return add_utc_locale(solarnoon)

def sunrise(date, lat, long):
    """
    Get the sunrise for a give day at latitude and longitude. Northern is -.
    So for Austin Texas USA...

    lat = "30.30"
    long = "-97.70"

    Date can be a string in y/m/d format or a datetime.date object.
    """
    here, sun = _get_ephem(date, lat, long)

    rising = here.next_rising(sun).datetime()

    return add_utc_locale(rising)

def sunset(date, lat, long):
    """
    Get the sunrise for a give day at latitude and longitude. Northern is -.
    So for Austin Texas USA...

    lat = "30.30"
    long = "-97.70"

    Date can be a string in y/m/d format or a datetime.date object.
    """
    here, sun = _get_ephem(date, lat, long)

    setting = here.next_setting(sun).datetime()

    return add_utc_locale(setting)

def is_sun_up(date, lat, long):
    """
    Get whether the sun is up or not.

    Date should be a datetime.datetime object or a string in format
    y/m/d h:m:s and be UTC.
    """    
    here, sun = _get_ephem(date, lat, long)

    rising = here.previous_rising(sun)
    setting = here.previous_setting(sun)

    return rising > setting

def day_duration(date, lat, long):
    here, sun = _get_ephem(date, lat, long)

    rising = here.next_rising(sun)
    here.date = rising
    setting = here.next_setting(sun)

    return (setting.datetime() - rising.datetime())

def utcnow():
    """:return: ``datetime.datetime`` object in UTC"""
    return datetime.datetime.now(pytz.utc)

def utcepochnow():
    """:return: an epoch timestamp in UTC"""
    return float(calendar.timegm(utcnow().timetuple()))

def datetime_to_epoch(dt):
    """
    Converts a UTC aware datetime.datetime object to its associated epoch

    :return: an epoch timestamp in UTC
    """
    assert isinstance(dt, datetime.datetime)

    if dt.tzinfo:
        if dt.tzinfo != pytz.utc:
            dt = dt.astimezone(pytz.utc)
    else:
        dt = add_utc_locale(dt)

    return float(calendar.timegm(dt.timetuple()))

def epoch_to_datetime(epoch):
    """
    Converts a UTC epoch into a datetime.datetime UTC aware object

    :return: ``datetime.datetime`` object in UTC
    """
    rdt = datetime.datetime.utcfromtimestamp(epoch)
    rdt = pytz.utc.localize(rdt)

    return rdt

def add_utc_locale(dt):
    dt = pytz.utc.localize(dt)

    return dt

def apply_offset(dt, offset):
    dt = dt.replace(tzinfo=None)
    dt = dt + datetime.timedelta(hours=offset)
    return dt




















