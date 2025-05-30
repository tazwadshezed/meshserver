"""
Cross-platform (posix/nt) API for flock-style file locking.

Author: Jonathan Feinberg <jdf@pobox.com>,
        Lowell Alleman <lalleman@mfps.com>
Updated for Python 3 Compatibility.
"""
import os

__all__ = [
    "lock",
    "unlock",
    "LOCK_EX",
    "LOCK_SH",
    "LOCK_NB",
    "LockException",
]

from util import portalocker


class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1

if os.name == 'posix':
    import fcntl
    LOCK_EX = fcntl.LOCK_EX
    LOCK_SH = fcntl.LOCK_SH
    LOCK_NB = fcntl.LOCK_NB
else:
    raise RuntimeError("PortaLocker only defined for nt and posix platforms")

if os.name == 'posix':
    def lock(file, flags):
        try:
            fcntl.flock(file.fileno(), flags)
        except OSError as e:
            if e.errno == 11:  # Resource temporarily unavailable
                raise LockException(LockException.LOCK_FAILED, str(e))
            else:
                raise

    def unlock(file):
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)

if __name__ == '__main__':
    import sys
    from time import localtime
    from time import strftime
    from time import time


    log = open('log.txt', "a+")
    portalocker.lock(log, portalocker.LOCK_EX)

    timestamp = strftime("%m/%d/%Y %H:%M:%S\n", localtime(time()))
    log.write(timestamp)

    print("Wrote lines. Hit enter to release lock.")
    sys.stdin.readline()

    log.close()
