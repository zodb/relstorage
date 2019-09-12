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

# pylint:disable=too-many-lines

import logging
import os
import threading

from persistent.timestamp import TimeStamp
from ZODB.POSException import ReadConflictError
from ZODB.utils import p64
from zope import interface


from relstorage.autotemp import AutoTemporaryFile
from relstorage._compat import OID_OBJECT_MAP_TYPE
from relstorage._compat import OID_SET_TYPE as OIDSet
from relstorage._compat import iteroiditems
from relstorage._compat import IN_TESTRUNNER
from relstorage._util import bytes8_to_int64
from relstorage._mvcc import DetachableMVCCDatabaseViewer

from relstorage.cache import persistence
from relstorage.cache.interfaces import IStorageCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import CacheConsistencyError
from relstorage.cache.local_client import LocalClient
from relstorage.cache.memcache_client import MemcacheStateCache
from relstorage.cache.trace import ZEOTracer
from relstorage.cache._statecache_wrappers import MultiStateCache
from relstorage.cache._statecache_wrappers import TracingStateCache
from relstorage.cache.mvcc import MVCCDatabaseCoordinator

logger = log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    size = limit = 0
    def __len__(self):
        return 0
    def __call__(self):
        raise NotImplementedError
    close = reset_stats = release = unregister = lambda self, *args: None
    stats = lambda s: {}
    new_instance = lambda s: s
_UsedAfterRelease = _UsedAfterRelease()


@interface.implementer(IStorageCache, IPersistentCache)
class StorageCache(DetachableMVCCDatabaseViewer):
    """RelStorage integration with memcached or similar.

    Holds a list of memcache clients in order from most local to
    most global.  The first is a LocalClient, which stores the cache
    in the Python process, but shares the cache between threads.
    """
    # pylint:disable=too-many-instance-attributes,too-many-public-methods

    __slots__ = (
        'adapter',
        'options',
        'keep_history',
        'prefix',
        'polling_state',
        'local_client',
        'cache',
        'object_index',

        # Things used during commit
        'temp_objects',
        'store_temp',
        'read_temp',
    )


    if IN_TESTRUNNER:
        class MVCCInternalConsistencyError(Exception):
            "This can never be raised or caught."
    else:
        MVCCInternalConsistencyError = AssertionError

    def __init__(self, adapter, options, prefix, _parent=None):
        super(StorageCache, self).__init__()
        self.adapter = adapter
        self.options = options
        self.keep_history = options.keep_history
        self.prefix = prefix or ''
        # queue is a _TemporaryStorage used during commit
        self.temp_objects = None # type: _TemporaryStorage
        # store_temp and read_temp are methods copied from the queue while
        # we are committing.
        self.store_temp = None
        self.read_temp = None

        if _parent is None:
            # I must be the master!

            # This is shared between all instances of a cache in a tree,
            # including the master, so that they can share information about
            # polling.
            self.polling_state = MVCCDatabaseCoordinator(self.options)
            self.local_client = LocalClient(options, self.prefix)


            shared_cache = MemcacheStateCache.from_options(options, self.prefix)
            if shared_cache is not None:
                self.cache = MultiStateCache(self.local_client, shared_cache)
            else:
                self.cache = self.local_client

            tracefile = persistence.trace_file(options, self.prefix)
            if tracefile:
                tracer = ZEOTracer(tracefile)
                tracer.trace(0x00)
                self.cache = TracingStateCache(self.cache, tracer)
        else:
            self.polling_state = _parent.polling_state # type: MVCCDatabaseCoordinator
            self.local_client = _parent.local_client.new_instance()
            self.cache = _parent.cache.new_instance()

        # Once we have registered with the MVCCDatabaseCoordinator,
        # we cannot make any changes to our own mvcc state without
        # letting it know about them. In particular, that means we must
        # not just assign to this object (except under careful circumstances
        # where we're sure to be single threaded.)
        # This object can be None
        self.object_index = None

        # It is also important not to register with the coordinator until
        # we are fully initialized; we could be constructing a new_instance
        # in a separate thread while polling is going on in other threads.
        # We can get strange AttributeError if a partially constructed instance
        # is exposed.
        self.polling_state.register(self)

        if _parent is None:
            self.restore()


    @property
    def current_tid(self):
        # testing
        return self.highest_visible_tid

    # XXX: Note that our __bool__ and __len__ are NOT consistent
    def __bool__(self):
        return True
    __nonzero__ = __bool__

    def __len__(self):
        return len(self.local_client)

    @property
    def size(self):
        return self.local_client.size

    @property
    def limit(self):
        return self.local_client.limit

    def stats(self):
        """
        Return stats. This is a debugging aid only. The format is undefined and intended
        for human inspection only.
        """
        stats = self.local_client.stats()
        stats['local_index_stats'] = self.object_index.stats() if self.object_index else None
        stats['global_index_stats'] = self.polling_state.stats()
        return stats

    def __repr__(self):
        return "<%s at 0x%x hvt=%s bytes=%d len=%d state=%r>" % (
            self.__class__.__name__,
            id(self),
            self.highest_visible_tid,
            self.size,
            len(self),
            self.polling_state,
        )

    def reset_stats(self):
        self.local_client.reset_stats()

    def new_instance(self, before=None): # pylint:disable=method-hidden,unused-argument
        """
        Return a copy of this instance sharing the same local client
        and having the most current view of the database as collected
        by any instance.

        If *before* is given, the new cache will use a distinct
        :class:`MVCCDatabaseCoordinator`  so that
        its usage pattern does not interfere.
        """
        klass = type(self) if before is None else _BeforeStorageCache
        cache = klass(self.adapter, self.options, self.prefix,
                      _parent=self)
        return cache

    def release(self):
        """
        Release resources held by this instance.

        This does not corrupt shared state, and must be called
        on each instance that's not the root.

        This is usually memcache connections if they're in use.
        """
        self.cache.release()
        # Release our clients. If we had a non-shared local cache,
        # this will also allow it to release any memory it's holding.
        self.local_client = self.cache = _UsedAfterRelease
        self.polling_state.unregister(self)
        self.polling_state = _UsedAfterRelease
        self.object_index = None
        self.highest_visible_tid = None

    def close(self, **save_args):
        """
        Release resources held by this instance, and
        save any persistent data necessary.

        This is only called on the root. If there are still instances
        that haven't been released, they'll be broken.
        """
        # grab things that will be reset in release()
        cache = self.cache
        polling_state = self.polling_state

        # Go ahead and release our polling_state now, in case
        # it helps to vacuum for save.
        self.polling_state.unregister(self)
        self.save(**save_args)
        self.release()
        cache.close()
        polling_state.close()

    def save(self, **save_args):
        """
        Store any persistent client data.
        """
        if self.options.cache_local_dir and len(self) > 0: # pylint:disable=len-as-condition
            # (our __bool__ is not consistent with our len)
            stats = self.local_client.stats()
            if stats['hits'] or stats['sets']:
                # Only write this out if (1) it proved useful OR (2)
                # we've made modifications. Otherwise, we're writing a consolidated
                # file for no good reason.
                # TODO: Consider the correctness here, now that we have a
                # more accurate cache. Should that maybe be AND?
                return self.polling_state.save(self, save_args)
            logger.debug("Cannot justify writing cache file, no hits or misses")

    def restore(self):
        # We must only restore into an empty cache.
        state = self.polling_state
        assert not len(self.local_client) # pylint:disable=len-as-condition
        state.restore(self.adapter, self.local_client)

    def _reset(self, message=None):
        """
        Reset the transaction state of only this instance.

        If this is being done in a transactional way, it must be followed
        by raising an exception. If the *message* parameter is provided,
        then a ``CacheConsistencyError`` will be raised when this
        method returns.
        """
        # As if we've never polled
        self.polling_state.reset_viewer(self)
        self.polling_state.flush_all()
        if message:
            raise CacheConsistencyError(message)

    def clear(self, load_persistent=True):
        """
        Remove all data from the cache, both locally (and shared among
        other instances), and globally.

        Called by speed tests.

        Starting from the introduction of persistent cache files, this
        also results in the local client being repopulated with the
        current set of persistent data. The *load_persistent* keyword
        can be used to control this.

        .. versionchanged:: 2.0b6 Added the ``load_persistent``
           keyword. This argument is provisional.
        """
        self._reset()
        self.polling_state.flush_all()
        self.cache.flush_all()

        if load_persistent:
            self.restore()

    def zap_all(self):
        """
        Remove all data from the cache, both locally (and shared among
        other instances, and globally); in addition, remove any
        persistent cache files on disk.
        """
        self.clear(load_persistent=False)
        self.local_client.zap_all()

    def _check_tid_after_load(self, oid_int, actual_tid_int,
                              expect_tid_int=None):
        """Verify the tid of an object loaded from the database is sane."""
        if actual_tid_int > self.highest_visible_tid:
            # Strangely, the database just gave us data from a future
            # transaction. We can't give the data to ZODB because that
            # would be a consistency violation. However, the cause is
            # hard to track down, so issue a ReadConflictError and
            # hope that the application retries successfully.
            msg = ("Got data for OID 0x%(oid_int)x from "
                   "future transaction %(actual_tid_int)d (%(got_ts)s).  "
                   "Current transaction is %(hvt)s (%(current_ts)s)."
                   % {
                       'oid_int': oid_int,
                       'actual_tid_int': actual_tid_int,
                       'hvt': self.highest_visible_tid,
                       'got_ts': str(TimeStamp(p64(actual_tid_int))),
                       'current_ts': str(TimeStamp(p64(self.highest_visible_tid))),
                   })
            raise ReadConflictError(msg)

        if expect_tid_int is not None and actual_tid_int != expect_tid_int:
            # Uh-oh, the cache is inconsistent with the database.
            # We didn't get a TID from the future, but it's not what we
            # had in our delta_after0 map, which means...we missed a change
            # somewhere.
            #
            # Possible causes:
            #
            # - The database MUST provide a snapshot view for each
            #   session; this error can occur if that requirement is
            #   violated. For example, MySQL's MyISAM engine is not
            #   sufficient for the object_state table because MyISAM
            #   can not provide a snapshot view. (InnoDB is
            #   sufficient.)
            #
            # - (Similar to the last one.) Using too low of a
            #   isolation level for the database connection and
            #   viewing unrelated data.
            #
            # - Something could be writing to the database out
            #   of order, such as a version of RelStorage that
            #   acquires a different commit lock.
            #
            # - A software bug. In the past, there was a subtle bug
            #   in after_poll() that caused it to ignore the
            #   transaction order, leading it to sometimes put the
            #   wrong tid in delta_after*.
            #
            # - Restarting a load connection at a future point we hadn't
            #   actually polled to, such that our current_tid is out of sync
            #   with the connection's *actual* viewable tid?
            msg = ("Detected an inconsistency "
                   "between the RelStorage cache and the database "
                   "while loading an object using the delta_after0 dict.  "
                   "Please verify the database is configured for "
                   "ACID compliance and that all clients are using "
                   "the same commit lock.  "
                   "(oid_int=%(oid_int)r, expect_tid_int=%(expect_tid_int)r, "
                   "actual_tid_int=%(actual_tid_int)r, "
                   "current_tid=%(current_tid)r, "
                   "pid=%(pid)r, thread_ident=%(thread_ident)r)"
                   % {
                       'oid_int': oid_int,
                       'expect_tid_int': expect_tid_int,
                       'actual_tid_int': actual_tid_int,
                       'current_tid': self.highest_visible_tid,
                       'pid': os.getpid(),
                       'thread_ident': threading.current_thread(),
                   })
            # We reset ourself as if we hadn't polled, and hope the transient
            # error gets retried in a working, consistent view.
            self._reset(msg)

    def loadSerial(self, oid_int, tid_int):
        """
        Return the locally cached state for the object *oid_int* as-of
        exactly *tid_int*.

        If that state is not available in the local cache, return
        nothing.

        This is independent of the current transaction and polling state, and
        may return data from the future.

        If the storage hasn't polled invalidations, or if there are other viewers
        open at transactions in the past, it may also return data from the past
        that has been overwritten (in history-free storages).
        """
        # We use only the local client because, for history-free storages,
        # it's the only one we can be reasonably sure has been
        # invalidated by a local pack. Also, our point here is to avoid
        # network traffic, so it's no good going to memcache for what may be
        # a stale answer.

        cache = self.local_client
        # Don't take this as an MRU hit; if we succeed, we'll
        # put new cached data in for this OID and do that anyway.
        cache_data = cache.get((oid_int, tid_int), False)
        if cache_data and cache_data[1] == tid_int:
            return cache_data[0]

    def load(self, cursor, oid_int):
        """
        Load the given object from cache if possible.

        Fall back to loading from the database.

        Returns (state_bytes, tid_int).
        """
        # pylint:disable=too-many-statements,too-many-branches,too-many-locals
        if not self.object_index:
            # No poll has occurred yet. For safety, don't use the cache.
            # Note that without going through the cache, we can't
            # go through tracing either.
            return self.adapter.mover.load_current(cursor, oid_int)

        # Get the object from the transaction specified
        # by the following values, in order:
        #
        #   1. self.object_index[oid_int]
        #
        # An entry in object_index means we've polled for and know the exact
        # TID for this object, either because we polled, or because someone
        # loaded it and put it in the index. If we know a TID, we must *never*
        # use the wildcard frozen value (it's possible to have an older frozen tid that's
        # valid for older transactions, but out of date for this one.) That's handled
        # internally in the clients.


        cache = self.cache
        index = self.object_index
        indexed_tid_int = index[oid_int] # Could be None

        key = (oid_int, indexed_tid_int)
        cache_data = cache[key]
        if cache_data and indexed_tid_int is None and cache_data[1] > self.highest_visible_tid:
            # Cache hit on a wildcard, but we need to verify the wildcard
            # and it didn't pass. This situation should be impossible.
            cache_data = None

        if cache_data:
            # Cache hit, non-wildcard or wildcard matched.
            return cache_data

        # Cache miss.
        state, actual_tid_int = self.adapter.mover.load_current(
            cursor, oid_int)
        if actual_tid_int:
            # If either is None, the object was deleted.
            self._check_tid_after_load(oid_int, actual_tid_int, indexed_tid_int)

            # We may or may not have had an index entry, but make sure we do now.
            # Eventually this will age to be frozen again if needed.
            index[oid_int] = actual_tid_int
            cache[(oid_int, actual_tid_int)] = (state, actual_tid_int)
            return state, actual_tid_int

        # This is in the bytecode as a LOAD_CONST
        return None, None

    def prefetch(self, cursor, oid_ints):
        # Just like load(), but we only fetch the OIDs
        # we can't find in the cache.
        if not self.object_index:
            # No point even trying, we would just throw the results away
            return

        to_fetch = OIDSet()
        cache = self.cache
        index = self.object_index
        for oid_int in oid_ints:
            tid_int = index[oid_int]
            key = (oid_int, tid_int)
            # We don't actually need the cache data, so avoid asking
            # for it. That would trigger stats updates (hits/misses)
            # and move it to the front of the LRU list. But this is just
            # in advance, we don't know if it will actually be used.
            # `in` has a race condition (it could be evicted soon), but
            # if it is, there was probably something else more important
            # going on.
            if key not in cache:
                # That was our one place, so we must fetch
                to_fetch.add(oid_int)

        if not to_fetch:
            return

        for oid, state, tid_int in self.adapter.mover.load_currents(cursor, to_fetch):
            key = (oid, tid_int)
            self._check_tid_after_load(oid, tid_int)
            cache[key] = (state, tid_int)
            index[oid] = tid_int

    def remove_cached_data(self, oid_int, tid_int):
        """
        See notes in `invalidate_all`.
        """
        del self.cache[(oid_int, tid_int)]

    def remove_all_cached_data_for_oids(self, oids):
        """
        Invalidate all cached data for the given OIDs.

        This isn't transactional or locked so it may still result in
        this or others seeing invalid (ha!) states.

        This is a specialized API. It allows violation of our internal
        consistency constraints. It should only be used when the
        database is being manipulated at a low level, such as during
        pack or undo.
        """
        # Erase our knowledge of where to look
        # self._invalidate_all(oids)
        # Remove the data too.
        self.cache.invalidate_all(oids)

    def tpc_begin(self):
        """Prepare temp space for objects to cache."""
        q = self.temp_objects = _TemporaryStorage()
        self.store_temp = q.store_temp
        self.read_temp = q.read_temp

    def after_tpc_finish(self, tid):
        """
        Flush queued changes.

        This is called after the database commit lock is released.

        Now that this tid is known, send all queued objects to the
        cache. The cache will have ``(oid, tid)`` entry for each object
        we have been holding on to (well, in a big transaction, some of them
        might actually not get stored in the cache. But we try!)
        """
        try:
            tid_int = bytes8_to_int64(tid)
            # Let the coordinator know ASAP that we're between transactions,
            # and give it the opportunity to update the object index and cache
            # if possible. Note that *we* cannot update our index: this transaction isn't
            # visible to us until we poll.
            self.polling_state.after_tpc_finish(self, tid_int, self.temp_objects)
        finally:
            self.clear_temp()

    def tpc_abort(self):
        self.clear_temp()
        self.polling_state.tpc_abort(self)

    def afterCompletion(self, load_connection):
        load_connection.rollback_quietly()
        self.polling_state.afterCompletion(self)

    def clear_temp(self):
        """Discard all transaction-specific temporary data.

        Called after transaction finish or abort.
        """
        if self.temp_objects is not None:
            self.store_temp = None
            self.read_temp = None
            self.temp_objects.close()
            self.temp_objects = None

    def poll(self, conn, cursor, ignore_tid):
        try:
            changes = self.polling_state.poll(self, conn, cursor)
        except self.MVCCInternalConsistencyError: # pragma: no cover
            logger.critical(
                "Internal consistency violation in the MVCC coordinator. "
                "Please report a bug to the RelStorage maintainers. "
                "Flushing caches for safety. ",
                exc_info=True
            )
            self._reset("Unknown internal violation")

        if changes is not None:
            return OIDSet(oid for oid, tid in changes if tid != ignore_tid)


class _BeforeStorageCache(StorageCache):

    __slots__ = ()

    def poll(self, conn, cursor, _):
        # Grab whatever index we have the first time we poll,
        # and then immediately drop away from the polling state.
        # We don't want our increasingly-stale view of the database
        # (which isn't even really quite accurate in terms of what we
        # need: TODO: Should we do that?) to hold up vacuums (even
        # though it will hold up vacuum in the real RDBMS).
        self.polling_state.poll(self, conn, cursor)
        # We'll never try to poll again, so this is ok.
        self.polling_state.unregister(self)
        return ()

class _TemporaryStorage(object):
    def __init__(self):
        # start with a fresh in-memory buffer instead of reusing one that might
        # already be spooled to disk.
        # TODO: An alternate idea would be a temporary sqlite database.
        self._queue = AutoTemporaryFile()
        # {oid: (startpos, endpos, prev_tid_int)}
        self._queue_contents = OID_OBJECT_MAP_TYPE()

    def reset(self):
        self._queue_contents.clear()
        self._queue.seek(0)

    def store_temp(self, oid_int, state, prev_tid_int=0):
        """
        Queue an object for caching.

        Typically, we can't actually cache the object yet, because its
        transaction ID is not yet chosen.
        """
        assert isinstance(state, bytes)
        queue = self._queue
        queue.seek(0, 2)  # seek to end
        startpos = queue.tell()
        queue.write(state)
        endpos = queue.tell()
        self._queue_contents[oid_int] = (startpos, endpos, prev_tid_int)

    def __len__(self):
        # How many distinct OIDs have been stored?
        return len(self._queue_contents)

    def __bool__(self):
        return True

    __nonzero__ = __bool__

    @property
    def stored_oids(self):
        return self._queue_contents

    def _read_temp_state(self, startpos, endpos):
        self._queue.seek(startpos)
        length = endpos - startpos
        state = self._queue.read(length)
        if len(state) != length:
            raise AssertionError("Queued cache data is truncated")
        return state

    def read_temp(self, oid_int):
        """
        Return the bytes for a previously stored temporary item.
        """
        startpos, endpos, _ = self._queue_contents[oid_int]
        return self._read_temp_state(startpos, endpos)

    def __iter__(self):
        return self.iter_for_oids(None)

    def iter_for_oids(self, oids):
        read_temp_state = self._read_temp_state
        for startpos, endpos, oid_int, prev_tid_int in self.items(oids):
            state = read_temp_state(startpos, endpos)
            yield state, oid_int, prev_tid_int

    def items(self, oids=None):
        # Order the queue by file position, which should help
        # if the file is large and needs to be read
        # sequentially from disk.
        items = [
            (startpos, endpos, oid_int, prev_tid_int)
            for (oid_int, (startpos, endpos, prev_tid_int)) in iteroiditems(self._queue_contents)
            if oids is None or oid_int in oids
        ]
        items.sort()
        return items

    def close(self):
        self._queue.close()
        self._queue = None
        self._queue_contents = None
