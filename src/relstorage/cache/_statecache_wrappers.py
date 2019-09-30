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

    def release(self):
        if self.l is not None:
            self.l.release()
            self.g.release()
            self.l = None
            self.g = None

    def new_instance(self):
        return type(self)(self.l.new_instance(), self.g.new_instance())

    def flush_all(self):
        self.l.flush_all()
        self.g.flush_all()

    def __contains__(self, key):
        # This has race conditions, but that's ok.
        return key in self.l or key in self.g

    def __getitem__(self, key):
        result = self.l[key]
        if not result:
            result = self.g[key]
            if result:
                self.l[key] = result
        return result

    def get(self, key, peek=False):
        result = self.l.get(key, peek)
        if not result:
            result = self.g.get(key, peek)
            if result:
                self.l[key] = result
        return result

    def __setitem__(self, key, value):
        self.l[key] = value
        self.g[key] = value

    def __delitem__(self, key):
        del self.l[key]
        del self.g[key]

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

    def invalidate_all(self, oids):
        self.l.invalidate_all(oids)
        self.g.invalidate_all(oids)


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

    def new_instance(self):
        return type(self)(self.cache.new_instance(), self.tracer)

    def release(self):
        if self.cache is not None:
            self.cache.release()
            self.cache = None
            # But keep self.tracer because it has to be closed.
            self._trace = None
            self._trace_store_current = None

    def close(self):
        tracer = self.tracer
        self.release()
        if tracer is not None:
            tracer.close()
        self.tracer = None

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def __getitem__(self, key, peek=False):
        oid_int, tid_int = key
        cache_data = self.cache.get(key, peek)
        if cache_data:
            # Note that we trace all cache hits, not just the local cache hit.
            # This makes the simulation less useful, but the stats might still have
            # value to people trying different tuning options manually.
            self._trace(0x22, oid_int, tid_int, dlen=len(cache_data[0]))
        else:
            self._trace(0x20, oid_int)
        return cache_data

    get = __getitem__

    def __setitem__(self, key, value):
        oid_int, _ = key
        state, tid_int = value
        # Record this as a store into the cache, ZEO does.
        self._trace(0x52, oid_int, tid_int, dlen=len(state) if state else 0)
        self.cache[key] = value

    def __delitem__(self, key):
        # TODO: Figure out an event for an explicit invalidation
        del self.cache[key]

    def set_all_for_tid(self, tid_int, state_oid_iter):
        self._trace_store_current(tid_int, state_oid_iter)
        self.cache.set_all_for_tid(tid_int, state_oid_iter)



class _MapWrapper(object): # pragma: no cover
    # This was used for updating the old "checkpoint" style maps
    # but we don't use those anymore. Naively dropping this into
    # mvcc._vacuum() around obsolete_bucket produced unreadable
    # trace files --- but that's probably an artifact of our
    # artifical test, or the artificial way we implemented
    # __delitem__

    # Every setitem generates 0x1c invalidate (hit, saving non-current)
    __slots__ = ('data', 'get', 'trace')
    def __init__(self, data, trace):
        self.get = data.get
        self.trace = trace
        self.data = data

    def __setitem__(self, oid_int, tid_int):
        self.trace(0x1C, oid_int, tid_int)
        self.data[oid_int] = tid_int
