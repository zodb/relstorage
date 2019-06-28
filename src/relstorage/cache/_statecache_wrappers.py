##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""
Wrappers for IStateCache.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope import interface

from relstorage.cache.interfaces import IStateCache

@interface.implementer(IStateCache)
class MultiStateCache(object):
    """
    A proxy that encapsulates the handling of a local and a global
    cache.

    This lets us write loop-free code in all cases in StorageCache,
    but still use both local and global caches.

    For the case that there's just a local cache, which is common and
    semi-recommended, we can just use that object directly.
    """

    __slots__ = ('l', 'g')

    def __init__(self, lcache, gcache):
        self.l = lcache
        self.g = gcache

    def close(self):
        if self.l is not None:
            self.l.close()
            self.g.close()
            self.l = None
            self.g = None

    def flush_all(self):
        self.l.flush_all()
        self.g.flush_all()

    def __getitem__(self, key):
        result = self.l[key]
        if not result:
            result = self.g[key]
            if result:
                self.l[key] = result
        return result

    def __setitem__(self, key, value):
        self.l[key] = value
        self.g[key] = value

    def __delitem__(self, key):
        del self.l[key]
        del self.g[key]

    def __call__(self, oid, tid1, tid2):
        result = self.l(oid, tid1, tid2)
        if not result:
            result = self.g(oid, tid1, tid2)
            if result:
                self.l[(oid, tid1)] = result
        return result

    def set_all_for_tid(self, tid_int, state_oid_iter):
        # If the transaction was very large, it may have spilled
        # over to disk, in which case we will need to read it
        # again (but the second time it's probably at least in the OS's
        # cache). Unless it was very, very large, it probably all winds
        # up in our local cache; we could avoid the second disk read by
        # making a note of (oid, tid) pairs and then pulling them back out
        # of the local cache. (Indeed, simply calling list() to materialize
        # the iterator is probably usually sufficient, except for those very, very
        # large cases.)
        self.l.set_all_for_tid(tid_int, state_oid_iter)
        self.g.set_all_for_tid(tid_int, state_oid_iter)

    # Unlike everything else, checkpoints are on the global
    # client first and then the local one.

    def get_checkpoints(self):
        return self.g.get_checkpoints() or self.l.get_checkpoints()

    def store_checkpoints(self, c0, c1):
        # TODO: Is there really much value in storing the checkpoints
        # globally (as opposed to just on a single process)?
        self.g.store_checkpoints(c0, c1)
        return self.l.store_checkpoints(c0, c1)

    def updating_delta_map(self, deltas):
        return self.l.updating_delta_map(deltas)

@interface.implementer(IStateCache)
class TracingStateCache(object):
    """
    An ``IStateCache`` implementation that handles tracing.`
    """
    __slots__ = ('cache', 'tracer', '_trace', '_trace_store_current')

    def __init__(self, cache, tracer):
        self.cache = cache
        self.tracer = tracer
        self._trace = tracer.trace
        self._trace_store_current = tracer.trace_store_current

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def __getitem__(self, key):
        oid_int, tid_int = key
        cache_data = self.cache[key]
        if cache_data:
            # Note that we trace all cache hits, not just the local cache hit.
            # This makes the simulation less useful, but the stats might still have
            # value to people trying different tuning options manually.
            self._trace(0x22, oid_int, tid_int, dlen=len(cache_data[0]))
        else:
            self._trace(0x20, oid_int)
        return cache_data

    def __setitem__(self, key, value):
        oid_int, _ = key
        state, tid_int = value
        # Record this as a store into the cache, ZEO does.
        self._trace(0x52, oid_int, tid_int, dlen=len(state) if state else 0)
        self.cache[key] = value

    def __call__(self, oid_int, tid1, tid2):
        response = self.cache(oid_int, tid1, tid2)
        if response:
            state, actual_tid = response
            self._trace(0x22, oid_int, actual_tid, dlen=len(state))
        else:
            self._trace(0x20, oid_int)
        return response

    def __delitem__(self, key):
        # TODO: Figure out an event for an explicit invalidation
        pass

    def set_all_for_tid(self, tid_int, state_oid_iter):
        self._trace_store_current(tid_int, state_oid_iter)
        self.cache.set_all_for_tid(tid_int, state_oid_iter)

    def updating_delta_map(self, deltas):
        return _MapWrapper(self.cache.updating_delta_map(deltas),
                           self._trace)


class _MapWrapper(object):
    # Every setitem generates 0x1c invalidate (hit, saving non-current)
    __slots__ = ('data', 'get', 'trace')
    def __init__(self, data, trace):
        self.get = data.get
        self.trace = trace
        self.data = data

    def __setitem__(self, oid_int, tid_int):
        self.trace(0x1C, oid_int, tid_int)
        self.data[oid_int] = tid_int
