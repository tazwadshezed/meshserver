

import datetime

from DAQ.util.utctime import datetime_to_epoch
from DAQ.util.utctime import epoch_to_datetime


def round_to_nearest_second(dt, to_nearest=1.0, rounding_func=round):

    if isinstance(dt, datetime.datetime):
        _dt_conversion = True

        epoch = datetime_to_epoch(dt)

    elif isinstance(dt, (int, float)):
        _dt_conversion = False

        epoch = dt

    else:
        raise TypeError("Invalid type %s. Must be datetime or epoch timestamp (int, float)." % (dt))

    rounded = rounding_func(epoch / float(to_nearest)) * float(to_nearest)

    if _dt_conversion:
        rdt = epoch_to_datetime(rounded)
    else:
        # its rounding to the nearest second anyways... no need for subseconds.
        rdt = int(rounded)

    return rdt

def round_to_nearest_minute(dt, to_nearest=1, rounding_func=round):
    """
    Rounds a datetime object to the nearest minute interval.

    :param dt: DateTime object. Date you want to round
    :param to_nearest: integer, representing the nearest minute to round.

    >>> now
    datetime.datetime(2010, 12, 16, 21, 16, 36, 404869)
    >>> round_to_nearest_minute(now, 5)
    datetime.datetime(2010, 12, 16, 21, 15)
    >>> round_to_nearest_minute(now, 60)
    datetime.datetime(2010, 12, 16, 21, 0)
    >>> round_to_nearest_minute(now, 15)
    datetime.datetime(2010, 12, 16, 21, 15)
    >>> round_to_nearest_minute(now, 10)
    datetime.datetime(2010, 12, 16, 21, 20)
    >>> round_to_nearest_minute(now, 3)
    datetime.datetime(2010, 12, 16, 21, 18)
    >>> round_to_nearest_minute(now, 2 * 60)
    datetime.datetime(2010, 12, 16, 22, 0)
    >>> round_to_nearest_minute(now, 1 * 60)
    datetime.datetime(2010, 12, 16, 21, 0)
    """
    return round_to_nearest_second(dt, to_nearest=to_nearest * 60,
                                   rounding_func=rounding_func)

def round_to_nearest_hour(dt, to_nearest=1, rounding_func=round):
    """
    Rounds a datetime object to the nearest hour interval

    (proxy function)

    :param dt: DateTime object. Date you want to round
    :param to_nearest: integer, representing the nearest minute to round
    """
    return round_to_nearest_minute(dt, to_nearest=to_nearest * 60,
                                   rounding_func=rounding_func)


def timesince(dt, default=None):
    """
    From Flask snippets (public domain)
    http://flask.pocoo.org/snippets/33/

    Returns string representing "time since" e.g.
    3 days ago, 5 hours ago etc.

    :param dt: Datetime.datetime object
    :param default: Default text to display for when something just happened
    """

    if default is None:
        default = "just now"

    if isinstance(dt, datetime.datetime):
        now = datetime.datetime.now()
        diff = now - dt
    else:
        diff = dt

    periods = (
        (diff.days / 365, "year", "years"),
        (diff.days / 30, "month", "months"),
        (diff.days / 7, "week", "weeks"),
        (diff.days, "day", "days"),
        (diff.seconds / 3600, "hour", "hours"),
        (diff.seconds / 60, "minute", "minutes"),
        (diff.seconds, "second", "seconds"),
    )

    for period, singular, plural in periods:

        if not period:
            continue

        singular = u"%%(num)d %s" % singular
        plural = u"%%(num)d %s" % plural

        if period == 1:
            return singular % dict(num=period)
        else:
            return plural % dict(num=period)

    return default
