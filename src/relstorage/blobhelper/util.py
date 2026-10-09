##############################################################################
#
# Copyright (c) 2009,2019 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import time

import zc.lockfile

logger = logging.getLogger(__name__)

def lock_blob(path, retries=6000):
    lockfilename = os.path.join(os.path.dirname(path), '.lock')
    n = 0
    while 1:
        try:
            return zc.lockfile.LockFile(lockfilename)
        except zc.lockfile.LockError:
            n += 1
            if n > retries:
                raise
            time.sleep(0.01)

def close_lock(lock, path):
    """
    Close a blob cache lock, tolerating unlock races.

    On Windows, the byte-range lock region is process-wide, so under
    heavy multi-threaded contention unlocking can intermittently fail
    with ``LockError("Couldn't unlock ...")`` when another lock holder
    in the same process has already released the region. The data
    operation guarded by the lock has already completed at that point,
    so failing the whole operation would be wrong.

    Worse, ``SimpleLockFile.close()`` never closes its file object
    when the unlock raises, leaking the descriptor (and keeping the
    file open for other processes on Windows). So on an unlock failure
    we still forcibly close the underlying file object — closing it
    also releases any lock the handle may still hold — log the anomaly
    and carry on.
    """
    try:
        lock.close()
    except zc.lockfile.LockError:
        # pylint:disable-next=protected-access
        fp = getattr(lock, '_fp', None)
        if fp is not None:
            try:
                fp.close()
            except OSError:
                pass
            try:
                # pylint:disable-next=protected-access
                lock._fp = None
            except AttributeError:
                pass
        logger.warning(
            "Failed to unlock blob cache lock for %s; "
            "continuing with the file object closed.",
            path,
        )
