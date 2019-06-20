##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import division
from __future__ import print_function

import logging
import struct
import threading
import time

from ZODB.utils import p64
from ZODB.utils import z64

log = logging.getLogger(__name__)


class ZEOTracer(object):
    # Knows how to write ZEO trace files.

    def __init__(self, trace_file):
        self._trace_file = trace_file
        self._lock = threading.Lock()

        # closure variables are faster than self/global dict lookups
        # (going off example in ZEO code; in one test locally this gets us a
        # ~15% improvement)
        _now = time.time
        _pack = struct.Struct(">iiH8s8s").pack
        _trace_file_write = trace_file.write
        _p64 = p64
        _z64 = z64
        _int = int
        _len = len

        def trace(code, oid_int=0, tid_int=0, end_tid_int=0, dlen=0, now=None):
            # This method was originally part of ZEO.cache.ClientCache. The below
            # comment is verbatim:
            # The code argument is two hex digits; bits 0 and 7 must be zero.
            # The first hex digit shows the operation, the second the outcome.
            # ...
            # Note: when tracing is disabled, this method is hidden by a dummy.
            encoded = (dlen << 8) + code
            tid = _p64(tid_int) if tid_int else _z64
            end_tid = _p64(end_tid_int) if end_tid_int else _z64
            oid = b'' if not oid_int else _p64(oid_int)

            now = now or _now()
            try:
                _trace_file_write(
                    _pack(
                        _int(now), encoded, _len(oid), tid, end_tid) + oid,
                )
            except: # pragma: no cover
                log.exception("Problem writing trace info for %r at tid %r and end tid %r",
                              oid, tid, end_tid)
                raise

        self._trace = trace

    def trace(self, code, oid_int=0, tid_int=0, end_tid_int=0, dlen=0):
        with self._lock:
            self._trace(code, oid_int, tid_int, end_tid_int, dlen)

    def trace_store_current(self, tid_int, state_oid_iter):
        # As a locking optimization, we accept this in bulk
        # Theoretically this could be any iterable, but
        # we only work with the one defined in storage_cache.
        with self._lock:
            now = time.time()
            for startpos, endpos, oid_int, _prev_tid_int in state_oid_iter.items():
                self._trace(0x52, oid_int, tid_int, dlen=endpos - startpos, now=now)

    def close(self):
        self._trace_file.close()
        del self._trace
