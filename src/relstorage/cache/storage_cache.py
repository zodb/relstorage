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
from ZODB.utils import u64
from zope import interface


from relstorage.autotemp import AutoTemporaryFile
from relstorage._compat import OID_OBJECT_MAP_TYPE
from relstorage._compat import OID_SET_TYPE as OIDSet
from relstorage._compat import iteroiditems


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

def _debug(*_args):
    "Does nothing"
if 0: # pylint:disable=using-constant-test
    debug = print
else:
    debug = _debug


@interface.implementer(IStorageCache, IPersistentCache)
class StorageCache(object):
    """RelStorage integration with memcached or similar.

    Holds a list of memcache clients in order from most local to
    most global.  The first is a LocalClient, which stores the cache
    in the Python process, but shares the cache between threads.
    """
    # pylint:disable=too-many-instance-attributes,too-many-public-methods

    # queue is a _TemporaryStorage used during commit
    temp_objects = None
    # store_temp and read_temp are methods copied from the queue while
    # we are committing.
    store_temp = None
    read_temp = None

    def __init__(self, adapter, options, prefix, _parent=None):
        self.adapter = adapter
        self.options = options
        self.keep_history = options.keep_history
        self.prefix = prefix or ''

        if _parent is None:
            # I must be the master!

            # This is shared between all instances of a cache in a tree,
            # including the master, so that they can share information about
            # polling.
            self.polling_state = MVCCDatabaseCoordinator(self.options)
            self.polling_state.register(self)
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
            self.polling_state = _parent.polling_state
            self.polling_state.register(self)
            self.local_client = _parent.local_client.new_instance()
            self.cache = _parent.cache.new_instance()

        # Once we have registered with the MVCCDatabaseCoordinator,
        # we cannot make any changes to our own mvcc state without
        # letting it know about them. In particular, that means we must
        # not just assign to this object (except under careful circumstances
        # where we're sure to be single threaded.)
        self.object_index = None

        if _parent is None:
            self.restore()

    @property
    def highest_visible_tid(self):
        index = self.object_index
        return index.highest_visible_tid if index else None

    current_tid = highest_visible_tid

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

        If *before* is given, XXX: Precisely what? Freezing and such won't
        work well.
        """
        cache = type(self)(self.adapter, self.options, self.prefix,
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
        # We can't be used to make instances any more.
        self.new_instance = _UsedAfterRelease
        self.object_index = None

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
                #
                # This is the authoritative location. We don't try as hard to
                # store into the caches anymore.
                # TODO: Work on the coupling here.
                return self.polling_state.save(self.local_client, save_args)
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
        if actual_tid_int > self.object_index.highest_visible_tid:
            # Strangely, the database just gave us data from a future
            # transaction. We can't give the data to ZODB because that
            # would be a consistency violation. However, the cause is
            # hard to track down, so issue a ReadConflictError and
            # hope that the application retries successfully.
            msg = ("Got data for OID 0x%(oid_int)x from "
                   "future transaction %(actual_tid_int)d (%(got_ts)s).  "
                   "Current transaction is %(current_tid)d (%(current_ts)s)."
                   % {
                       'oid_int': oid_int,
                       'actual_tid_int': actual_tid_int,
                       'current_tid': self.current_tid,
                       'got_ts': str(TimeStamp(p64(actual_tid_int))),
                       'current_ts': str(TimeStamp(p64(self.current_tid))),
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
                       'current_tid': self.current_tid,
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

        If we're history free, and the tid_int doesn't match our
        knowledge of what the latest tid for the object should be,
        return nothing.
        """
        # We use only the local client because, for history-free storages,
        # it's the only one we can be reasonably sure has been
        # invalidated by a local pack. Also, our point here is to avoid
        # network traffic, so it's no good going to memcache for what may be
        # a stale answer.

        # As for load(), if we haven't polled, we can't trust our cache.
        # If we've polled, but we're being asked for data from the future,
        # we can't answer.
        if not self.highest_visible_tid or tid_int > self.highest_visible_tid:
            #debug("Not loadSerial; my hvt:", self.highest_visible_tid,
            #      "Requested tid", tid_int)
            return None

        if not self.options.keep_history:
            # For history-free, we can only have one state. If we
            # think we know what it is, but they ask for something different,
            # then there's no way it can be found.
            known_tid_int = self.object_index[oid_int] # Yay, it should be visible to me
            if known_tid_int is None or known_tid_int != tid_int:
                # No good. Ok, well, this is for conflict resolution, so if the
                # state was updated by someone else in this same process,
                # and we can find it in our shared polling state we got lucky.
                known_tid_int = self.polling_state.object_index[oid_int]
            if known_tid_int is not None and known_tid_int != tid_int:
                return None
            #debug("Known tid for oid", oid_int, "is", known_tid_int, "need", tid_int)

        # If we've seen this object, it could be in a few places:
        # (oid, tid) (if it was ever in a delta), or (oid, cp0)
        # if it has fallen behind. Regardless, we can only use it if
        # the tids match.
        #
        # We have a multi-query method, but we don't use it because we
        # don't want to move keys around.
        cache = self.local_client
        for tid in (tid_int, -1):
            if not tid:
                break
            cache_data = cache[(oid_int, tid)]
            if cache_data and cache_data[1] == tid_int:
                return cache_data[0]

    def load(self, cursor, oid_int):
        """
        Load the given object from cache if possible.

        Fall back to loading from the database.

        Returns (state_bytes, tid_int).
        """
        # pylint:disable=too-many-statements,too-many-branches,too-many-locals
        if not self.highest_visible_tid:
            # No poll has occurred yet. For safety, don't use the cache.
            # Note that without going through the cache, we can't
            # go through tracing either.
            #debug("No index, db for", oid_int)
            return self.adapter.mover.load_current(cursor, oid_int)

        # Get the object from the transaction specified
        # by the following values, in order:
        #
        #   1. delta_after0[oid_int]
        #   2. checkpoints[0]
        #   3. delta_after1[oid_int]
        #   4. checkpoints[1]
        #   5. The database.
        #
        # checkpoints[0] is the preferred location.
        #
        # If delta_after0 contains oid_int, we should not look at any
        # other cache keys, since the tid_int specified in
        # delta_after0 replaces all older transaction IDs. We *know*
        # that oid_int should be at (exactly) tid_int because we
        # either made that change ourself (after_tpc_finish) *or* we
        # have polled within our current database transaction (or a
        # previous one) and been told that the oid changed in tid.
        #
        # Similarly, if delta_after1 contains oid_int, we should not
        # look at checkpoints[1]. Also, when both checkpoints are set
        # to the same transaction ID, we don't need to ask for the
        # same key twice.
        cache = self.cache
        tid_int = self.object_index[oid_int]
        #debug("Index for", oid_int, "is", tid_int)
        if tid_int:
            # This object changed after checkpoint0, so
            # there is only one place to look for its state: the exact key.
            key = (oid_int, tid_int)
            cache_data = cache[key]
            if cache_data:
                # Cache hit.
                #debug("Cache hit for", oid_int, "at", tid_int, self.object_index)
                assert cache_data[1] == tid_int, (cache_data[1], key)
                return cache_data

            # Cache miss.
            #debug("Cache miss for", oid_int, "at", tid_int)
            state, actual_tid_int = self.adapter.mover.load_current(
                cursor, oid_int)
            if state and actual_tid_int:
                # If either is None, the object was deleted.
                self._check_tid_after_load(oid_int, actual_tid_int, tid_int)

                # At this point we know that tid_int == actual_tid_int
                # XXX: Previously, we did not trace this as a store into the cache.
                # Why?
                cache[key] = (state, actual_tid_int)
            return state, tid_int

        # It could be a frozen object, so try there,
        # but only locally.
        key = (oid_int, -1)
        cache_data = self.local_client[key]
        if cache_data:
            #debug("Found frozen", oid_int)
            assert cache_data[1] <= self.highest_visible_tid, (cache_data[1], key)
            return cache_data

        # Cache miss.
        state, tid_int = self.adapter.mover.load_current(cursor, oid_int)
        if tid_int:
            self._check_tid_after_load(oid_int, tid_int)
            complete_since = self.polling_state.complete_since_tid
            if complete_since and tid_int < complete_since:
                key = (oid_int, -1)
            else:
                key = (oid_int, tid_int)
                self.object_index[oid_int] = tid_int
            #debug("Storing after misses", key)
            cache[key] = (state, tid_int)

        return state, tid_int

    def prefetch(self, cursor, oid_ints):
        # Just like load(), but we only fetch the OIDs
        # we can't find in the cache.
        if not self.object_index:
            # No point even trying, we would just throw the results away
            return

        to_fetch = OIDSet()
        cache = self.cache
        complete_since_tid = self.polling_state.complete_since_tid
        index = self.object_index
        for oid_int in oid_ints:
            tid_int = index[oid_int]
            if tid_int:
                key = (oid_int, tid_int)
            else:
                key = (oid_int, -1)
            cache_data = cache[key]
            if not cache_data:
                # That was our one place, so we must fetch
                to_fetch.add(oid_int)

        if not to_fetch:
            return

        for oid, state, tid_int in self.adapter.mover.load_currents(cursor, to_fetch):
            key = (oid, tid_int) if tid_int > complete_since_tid else (oid, -1)
            # Note that we're losing the knowledge of whether the TID
            # in the key came from delta_after0 or not, so we're not
            # validating that part.
            self._check_tid_after_load(oid, tid_int)
            cache[key] = (state, tid_int)
            index[oid] = tid_int

    def invalidate(self, oid_int, tid_int):
        """
        See notes in `invalidate_all`.
        """
        del self.cache[(oid_int, tid_int)]
        self.polling_state.invalidate(oid_int, tid_int)

    def invalidate_all(self, oids):
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
        self.polling_state.invalidate_all(oids)

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
        tid_int = u64(tid)

        # We DO NOT store anything into our object_index: That's only
        # updated on a poll (if we tried, it would get thrown away as being
        # too new anyway). So how do we get cache hits on these again in the future?
        # Simple, next time we poll, we include polling for this
        # transaction; that way *everyone* can benefit.
        self.cache.set_all_for_tid(tid_int, self.temp_objects)

        self.clear_temp()

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
        changes = self.polling_state.poll(self, conn, cursor)
        if changes is not None:
            return OIDSet(oid for oid, tid in changes if tid != ignore_tid)


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
