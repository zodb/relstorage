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
from relstorage._compat import OID_TID_MAP_TYPE
from relstorage._compat import OID_OBJECT_MAP_TYPE
from relstorage._compat import OID_SET_TYPE
from relstorage._compat import iteroiditems
from relstorage._util import log_timed

from relstorage.cache import persistence
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import CacheConsistencyError
from relstorage.cache.local_client import LocalClient
from relstorage.cache.memcache_client import MemcacheStateCache
from relstorage.cache.trace import ZEOTracer
from relstorage.cache._statecache_wrappers import MultiStateCache
from relstorage.cache._statecache_wrappers import TracingStateCache

logger = log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    size = limit = 0
    def __len__(self):
        return 0
    close = reset_stats = lambda s: None
    stats = lambda s: {}
_UsedAfterRelease = _UsedAfterRelease()



@interface.implementer(IPersistentCache)
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

    # checkpoints, when set, is a tuple containing the integer
    # transaction ID of the two current checkpoints. checkpoint0 is
    # greater than or equal to checkpoint1.
    checkpoints = None

    # current_tid contains the last polled transaction ID. Invariant:
    # when self.checkpoints is not None, self.delta_after0 has info
    # from *all* transactions in the range:
    #
    #   (self.checkpoints[0], self.current_tid]
    #
    # (That is, `tid > self.checkpoints[0] and tid <= self.current_tid`)
    #
    # We assign to this *only* after executing a poll, or
    # when reading data from the persistent cache (which happens at
    # startup, and usually also when someone calls clear())
    #
    # Start with None so we can distinguish the case of never polled/
    # no tid in persistent cache from a TID of 0, which can happen in
    # tests.
    current_tid = None

    _tracer = None

    _delta_map_type = OID_TID_MAP_TYPE

    def __init__(self, adapter, options, prefix, local_client=None,
                 _tracer=None):
        self.adapter = adapter
        self.options = options
        self.prefix = prefix or ''

        # delta_after0 contains {oid: tid} *after* checkpoint 0
        # and before or at self.current_tid.
        self.delta_after0 = self._delta_map_type()

        # delta_after1 contains {oid: tid} *after* checkpoint 1 and
        # *before* or at checkpoint 0. The content of delta_after1 only
        # changes when checkpoints shift and we rebuild it.
        self.delta_after1 = self._delta_map_type()

        # delta_size_limit places an approximate limit on the number of
        # entries in the delta_after maps.
        self.delta_size_limit = options.cache_delta_size_limit

        if local_client is None:
            self.local_client = LocalClient(options, self.prefix)
        else:
            self.local_client = local_client

        shared_cache = MemcacheStateCache.from_options(options, self.prefix)
        if shared_cache is not None:
            self.cache = MultiStateCache(self.local_client, shared_cache)
        else:
            self.cache = self.local_client

        if local_client is None:
            self.restore()

        if _tracer is None:
            tracefile = persistence.trace_file(options, self.prefix)
            if tracefile:
                _tracer = ZEOTracer(tracefile)
                _tracer.trace(0x00)

        self._tracer = _tracer
        if hasattr(self._tracer, 'trace_store_current'):
            self.cache = TracingStateCache(self.cache, _tracer)

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
        return self.local_client.stats()

    def __repr__(self):
        return "<%s at %x size=%d len=%d>" % (
            self.__class__.__name__,
            id(self),
            self.size,
            len(self)
        )

    def reset_stats(self):
        self.local_client.reset_stats()

    def new_instance(self):
        """
        Return a copy of this instance sharing the same local client.
        """
        local_client = self.local_client if self.options.share_local_cache else None

        cache = type(self)(self.adapter, self.options, self.prefix,
                           local_client,
                           _tracer=self._tracer or False)

        # The delta maps get more and more stale the longer time goes on.
        # Maybe we want to try to re-create them based on the local max tids?
        # Also, if there have been enough changes that someone has shifted the
        # checkpoints, cache.checkpoints won't match the global checkpoints
        # and they will wind up discarding the delta maps on the first poll.
        #
        # Alternately, we could watch our children created here, and see
        # which one is still alive and has the highest `current_tid` indicating the
        # most recent poll, and copy that information.
        cache.checkpoints = self.checkpoints
        cache.delta_after0 = self._delta_map_type(self.delta_after0)
        cache.delta_after1 = self._delta_map_type(self.delta_after1)
        cache.current_tid = self.current_tid
        return cache

    def release(self):
        """
        Release resources held by this instance.

        This is usually memcache connections if they're in use.
        """
        self.cache.close()
        # Release our clients. If we had a non-shared local cache,
        # this will also allow it to release any memory it's holding.
        self.local_client = self.cache = _UsedAfterRelease

    def save(self, **save_args):
        """
        Store any persistent client data.
        """
        if self.options.cache_local_dir and len(self): # pylint:disable=len-as-condition
            # (our __bool__ is not consistent with our len)
            stats = self.local_client.stats()
            if stats['hits'] or stats['sets']:
                # Only write this out if (1) it proved useful OR (2)
                # we've made modifications. Otherwise, we're writing a consolidated
                # file for no good reason.
                # TODO: Consider the correctness here, now that we have a
                # more accurate cache. Should that maybe be AND?
                return self.local_client.save(**save_args)
            logger.debug("Cannot justify writing cache file, no hits or misses")

    def restore(self):
        # We must only restore into an empty cache.
        assert not len(self.local_client) # pylint:disable=len-as-condition
        assert not self.checkpoints

        # Note that there may have been a tiny amount of data in the
        # file that we didn't get to actually store but that still
        # comes back in the delta_map; that's ok.
        row_filter = _PersistentRowFilter(self.adapter, self._delta_map_type)
        self.local_client.restore(row_filter)
        self.local_client.remove_invalid_persistent_oids(row_filter.polled_invalid_oids)

        self.checkpoints = self.local_client.get_checkpoints()
        if self.checkpoints:
            # No point keeping the delta maps otherwise,
            # we have to poll. If there were no checkpoints, it means
            # we saved without having ever completed a poll.
            #
            # We choose the cp0 as our beginning TID at which to
            # resume polling. We have information on cached data as it
            # relates to those checkpoints. (TODO: Are we sure that
            # the delta maps we've just built are actually accurate
            # as-of this particular TID we're choosing to poll from?)
            #
            self.current_tid = self.checkpoints[0]
            self.delta_after0 = row_filter.delta_after0
            self.delta_after1 = row_filter.delta_after1

        logger.debug(
            "Restored with current_tid %s and checkpoints %s and deltas %s %s",
            self.current_tid, self.checkpoints,
            len(self.delta_after0), len(self.delta_after1)
        )

    def close(self, **save_args):
        """
        Release resources held by this instance, and
        save any persistent data necessary.
        """
        self.save(**save_args)
        self.release()

        if self._tracer:
            # Note we can't do this in release(). Release is called on
            # all instances, while close() is only called on the main one.
            self._tracer.close()
            del self._tracer

    def _reset(self, message=None):
        """
        Reset the transaction state of only this instance.

        If this is being done in a transactional way, it must be followed
        by raising an exception. If the *message* parameter is provided,
        then a ``CacheConsistencyError`` will be raised when this
        method returns.
        """
        # As if we've never polled
        for name in ('checkpoints', 'current_tid'):
            try:
                delattr(self, name)
            except AttributeError:
                pass
        self.delta_after0 = self._delta_map_type()
        self.delta_after1 = self._delta_map_type()
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
        # After this our current_tid is probably out of sync with the
        # storage's current_tid. Whether or not we load data from
        # persistent caches, it's probably in the past of what the
        # storage thinks.
        # XXX: Ideally, we should be able to populate that information
        # back up so that we get the right polls.

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
        if actual_tid_int > self.current_tid:
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
            cp0, cp1 = self.checkpoints

            msg = ("Detected an inconsistency "
                   "between the RelStorage cache and the database "
                   "while loading an object using the delta_after0 dict.  "
                   "Please verify the database is configured for "
                   "ACID compliance and that all clients are using "
                   "the same commit lock.  "
                   "(oid_int=%(oid_int)r, expect_tid_int=%(expect_tid_int)r, "
                   "actual_tid_int=%(actual_tid_int)r, "
                   "current_tid=%(current_tid)r, cp0=%(cp0)r, cp1=%(cp1)r, "
                   "len(delta_after0)=%(lda0)r, len(delta_after1)=%(lda1)r, "
                   "pid=%(pid)r, thread_ident=%(thread_ident)r)"
                   % {
                       'oid_int': oid_int,
                       'expect_tid_int': expect_tid_int,
                       'actual_tid_int': actual_tid_int,
                       'current_tid': self.current_tid,
                       'cp0': cp0,
                       'cp1': cp1,
                       'lda0': len(self.delta_after0),
                       'lda1': len(self.delta_after1),
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

        if not self.options.keep_history:
            # For history-free, we can only have one state. If we
            # think we know what it is, but they ask for something different,
            # then there's no way it can be found.
            known_tid_int = self.delta_after0.get(oid_int)
            if known_tid_int is not None and known_tid_int != tid_int:
                return None

        # If we've seen this object, it could be in a few places:
        # (oid, tid) (if it was ever in a delta), or (oid, cp0)
        # if it has fallen behind. Regardless, we can only use it if
        # the tids match.
        #
        # We have a multi-query method, but we don't use it because we
        # don't want to move keys around.
        cache = self.local_client
        for tid in (tid_int, self.checkpoints[0] if self.checkpoints else None):
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
        if not self.checkpoints:
            # No poll has occurred yet. For safety, don't use the cache.
            # Note that without going through the cache, we can't
            # go through tracing either.
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
        tid_int = self.delta_after0.get(oid_int)
        if tid_int:
            # This object changed after checkpoint0, so
            # there is only one place to look for its state: the exact key.
            key = (oid_int, tid_int)
            cache_data = cache[key]
            if cache_data:
                # Cache hit.
                assert cache_data[1] == tid_int, (cache_data[1], key)
                return cache_data

            # Cache miss.
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

        # Make a list of cache keys to query. The list will have either
        # 1 or 2 keys.
        cp0, cp1 = self.checkpoints
        tid2 = None
        tid_int = self.delta_after1.get(oid_int)
        if tid_int:
            tid2 = tid_int
        elif cp1 != cp0:
            tid2 = cp1

        preferred_key = (oid_int, cp0)

        # Query the cache. Query multiple keys simultaneously to
        # minimize latency. The client is responsible for moving
        # the data to the preferred key if it wasn't found there.
        response = cache(oid_int, cp0, tid2)
        if response: # We have a hit!
            state, actual_tid = response
            return state, actual_tid

        # Cache miss.
        state, tid_int = self.adapter.mover.load_current(cursor, oid_int)
        if tid_int:
            self._check_tid_after_load(oid_int, tid_int)
            cache[preferred_key] = (state, tid_int)
        return state, tid_int

    def prefetch(self, cursor, oid_ints):
        # Just like load(), but we only fetch the OIDs
        # we can't find in the cache.
        if not self.checkpoints:
            # No point even trying, we would just throw the results away
            return

        to_fetch = OID_OBJECT_MAP_TYPE() # {oid: cache key}
        cache = self.cache
        cp0, cp1 = self.checkpoints
        delta_after0 = self.delta_after0.get
        delta_after1 = self.delta_after1.get
        for oid_int in oid_ints:
            tid_int = delta_after0(oid_int)
            if tid_int:
                key = (oid_int, tid_int)
                cache_data = cache[key]
                if not cache_data:
                    # That was our one place, so we must fetch
                    to_fetch[oid_int] = key
                continue

            tid2 = None
            tid_int = delta_after1(oid_int)
            if tid_int:
                tid2 = tid_int
            elif cp1 != cp0:
                tid2 = cp1

            cache_data = cache(oid_int, cp0, tid2)
            if not cache_data:
                preferred_key = (oid_int, cp0)
                to_fetch[oid_int] = preferred_key

        if not to_fetch:
            return

        for oid, state, tid_int in self.adapter.mover.load_currents(cursor, to_fetch):
            key = to_fetch[oid]
            # Note that we're losing the knowledge of whether the TID
            # in the key came from delta_after0 or not, so we're not
            # validating that part.
            self._check_tid_after_load(oid, tid_int)
            cache[key] = (state, tid_int)

    def invalidate(self, oid_int, tid_int):
        del self.cache[(oid_int, tid_int)]
        if self.delta_after0.get(oid_int) == tid_int:
            del self.delta_after0[oid_int]

    def invalidate_all(self, oids):
        """
        In the local cache only, invalidate all cached data for the
        given OIDs.
        """
        self.local_client.invalidate_all(oids)
        deltas = self.delta_after0, self.delta_after1
        for oid in oids:
            for delta in deltas:
                try:
                    del delta[oid]
                except KeyError:
                    pass

    def tpc_begin(self):
        """Prepare temp space for objects to cache."""
        q = self.temp_objects = _TemporaryStorage()
        self.store_temp = q.store_temp
        self.read_temp = q.read_temp

    def _send_queue(self, tid):
        """
        Now that this tid is known, send all queued objects to the
        cache. The cache will have ``(oid, tid)`` entry for each object
        we have been holding on to (well, in a big transaction, some of them
        might actually not get stored in the cache. But we try!)
        """
        tid_int = u64(tid)

        self.cache.set_all_for_tid(tid_int, self.temp_objects)
        # We only do this because cache_trace_analysis uses us
        # in ways that aren't quite accurate. We'd prefer to call clear_temp()
        # at this point.
        self.temp_objects.reset()

    def after_tpc_finish(self, tid):
        """
        Flush queued changes.

        This is called after the database commit lock is released,
        but before releasing the storage lock that will allow other
        threads to use this instance.
        """
        tid_int = u64(tid)

        if self.checkpoints:
            for oid_int in self.temp_objects.stored_oids:
                # Future cache lookups for oid_int should now use
                # the tid just committed. We're about to flush that
                # data to the cache.
                self.delta_after0[oid_int] = tid_int
        # Under what circumstances would we get here (after commiting
        # a transaction) without ever having polled to establish
        # checkpoints? Turns out that database-level APIs like
        # db.undo() use new storage instances in an unusual way, and
        # will not necessarily have polled by the time they commit.
        #
        # Of course, if we restored from persistent cache files the master
        # could have checkpoints we copied down.
        #
        # TODO: Create a special subclass for MVCC instances and separate
        # the state handling.

        self._send_queue(tid)

    def clear_temp(self):
        """Discard all transaction-specific temporary data.

        Called after transaction finish or abort.
        """
        if self.temp_objects is not None:
            self.store_temp = None
            self.read_temp = None
            self.temp_objects.close()
            self.temp_objects = None

    def after_poll(self, cursor, prev_tid_int, new_tid_int, changes):
        """
        Update checkpoint data after a database poll.

        *cursor* is connected to a load connection.

        *prev_tid_int* is the tid that was last polled (that is, it
        was the *new_tid_int* the last time this was called).

        *changes* lists all [(oid_int, tid_int)] changed after
        *prev_tid_int*, up to and including *new_tid_int*, excluding
        the changes last committed by the associated storage instance.

        *changes* can be None to indicate that the cache is definitely
        in an inconsistent state: too much changed to be specific,
        there is no data at all (in which case *new_tid_int* should be
        0), or the database connection is stale.

        *prev_tid_int* can be None, in which case the changes
        parameter will be ignored. new_tid_int can not be None.
        """
        my_prev_tid_int = self.current_tid or 0
        self.current_tid = new_tid_int

        global_checkpoints = self.cache.get_checkpoints()

        if not global_checkpoints:
            # No other instance has established an opinion yet,
            # so I get to.
            self.__poll_establish_global_checkpoints(new_tid_int)
            return

        global_checkpoints_in_future = global_checkpoints[0] > new_tid_int
        if global_checkpoints_in_future:
            # checkpoint0 is in a future that this instance can't yet
            # see. Ignore the checkpoint change for now, continue
            # with our own.
            global_checkpoints = self.checkpoints
            if not self.checkpoints:
                # How nice, this was our first poll, but
                # yet somehow we are still behind the global
                # checkpoints. The global checkpoints are probably
                # wrong (maybe there's a replica involved and the global
                # cache is now stale).
                global_checkpoints = (new_tid_int, new_tid_int)

        # We want to keep the current checkpoints for speed, but we
        # have to replace them (to avoid consistency violations)
        # if certain conditions happen (like emptying the ZODB Connection cache
        # which happens when `changes` is None).
        if (global_checkpoints == self.checkpoints # In sync with the world
                # Poller didn't give up, and there was data in the database
                and changes is not None
                # The storage had polled before and gotten a response
                # other than 0, meaning no data in the database.
                and prev_tid_int
                # And what we think is the last time we polled
                # is at *least* as new as the last time the storage
                # thinks it polled.
                # Since we only assign to current_tid here (and when we read
                # persistent cache data, which also ultimately came from here)
                # it's not clear how we could get ahead.
                and my_prev_tid_int >= prev_tid_int
                # And the transaction that was just polled is
                # current or in the future. If we went backwards,
                # it's because the underlying data went backwards
                # (possibly we switched to a replica that's out of date)
                # and the user configured `revert-when-stale` to be on.
                # In that case, `changes` should also be None and we really shouldn't
                # get here.
                and new_tid_int >= my_prev_tid_int):
            if changes:
                # All the conditions for keeping the checkpoints were met,
                # so just update self.delta_after0 and self.current_tid.
                self.__poll_update_delta0_from_changes(changes)
        else:
            log.debug(
                "Using new checkpoints: %s. Current cp: %s. "
                "Too many changes? %s. prev_tid_int: %s. my_prev_tid_int: %s. "
                "new_tid_int: %s",
                global_checkpoints, self.checkpoints,
                changes is None, prev_tid_int, my_prev_tid_int,
                new_tid_int
            )
            self.__poll_replace_checkpoints(cursor, global_checkpoints, new_tid_int)

        if not global_checkpoints_in_future and len(self.delta_after0) >= self.delta_size_limit:
            return self._suggest_shifted_checkpoints()

    def __poll_establish_global_checkpoints(self, new_tid_int):
        # Because we *always* have checkpoints in our local_client,
        # once we've set them, not being able to find them there also
        # means that it was our first poll, and so we shouldn't have
        # checkpoints ourself. Of course, with multi-threaded race
        # conditions, that might not actually be the case.

        # assert not self.checkpoints

        # Initialize the checkpoints; we've never polled before.
        log.debug("Initializing checkpoints: %s", new_tid_int)

        self.checkpoints = self.cache.store_checkpoints(new_tid_int, new_tid_int)

    def __poll_update_delta0_from_changes(self, changes):
        m = self.cache.updating_delta_map(self.delta_after0)
        m_get = m.get
        for oid_int, tid_int in changes:
            my_tid_int = m_get(oid_int, -1)
            if tid_int > my_tid_int:
                m[oid_int] = tid_int

    def __poll_replace_checkpoints(self, cursor, new_checkpoints, new_tid_int):
        # We have to replace the checkpoints.
        cp0, cp1 = new_checkpoints

        # Use the checkpoints specified by the cache (or equal to new_tid_int,
        # if the cache was in the future.)

        # Rebuild delta_after0 and delta_after1, if we can.
        # If we can't, because we don't actually have a range, do nothing.
        # If the case that the checkpoints are (new_tid, new_tid),
        # we'll do nothing and have no delta maps. This is because, hopefully,
        # next time we poll we'll be able to use the global checkpoints and
        # catch up then.
        new_delta_after0 = self._delta_map_type()
        new_delta_after1 = self._delta_map_type()
        if cp1 < new_tid_int:
            # poller.list_changes(cp1, new_tid_int) provides an iterator of
            # (oid, tid) where tid > cp1 and tid <= new_tid_int. It is guaranteed
            # that each oid shows up only once.
            change_list = self.adapter.poller.list_changes(
                cursor, cp1, new_tid_int)

            # Put the changes in new_delta_after*.
            # Let the backing cache know about this (this is only done
            # for tracing).
            updating_0 = self.cache.updating_delta_map(new_delta_after0)
            updating_1 = self.cache.updating_delta_map(new_delta_after1)
            for oid_int, tid_int in change_list:
                if tid_int <= cp1 or tid_int > new_tid_int:
                    self._reset(
                        "Requested changes %d < tid <= %d "
                        "but change %d for OID %d out of range." % (
                            cp1, new_tid_int,
                            tid_int, oid_int
                        )
                    )

                if tid_int > cp0:
                    updating_0[oid_int] = tid_int
                else:
                    # This must be > cp1
                    updating_1[oid_int] = tid_int

            # Everybody has a home (we didn't get duplicate entries
            # or multiple entries for the same OID with different TID)
            l0 = len(new_delta_after0)
            l1 = len(new_delta_after1)
            if l0 + l1 != len(change_list):
                self._reset(
                    "Expected delta_after0 (%d) and delta_after1 (%d) "
                    "to have total len %d, not %d" % (
                        l0, l1,
                        len(change_list), l0 + l1
                    )
                )

        self.checkpoints = new_checkpoints
        self.delta_after0 = new_delta_after0
        self.delta_after1 = new_delta_after1

    def _suggest_shifted_checkpoints(self):
        """Suggest that future polls use a new pair of checkpoints.

        This does nothing if another instance has already shifted
        the checkpoints.

        checkpoint0 shifts to checkpoint1 and the tid just committed
        becomes checkpoint0.
        """
        cp0, _cp1 = self.checkpoints
        tid_int = self.current_tid # transaction we just committed.
        assert tid_int >= cp0

        # delta_after0 has reached its limit. The way to shrink it
        # is to shift the checkpoints. Suggest shifted checkpoints
        # for future polls. If delta_after0 is far over the limit
        # (caused by a large transaction), suggest starting new
        # checkpoints instead of shifting.
        delta_size = len(self.delta_after0)
        huge = (delta_size >= self.delta_size_limit * 2)

        if huge:
            # start new checkpoints
            change_to = (tid_int, tid_int)
        else:
            # shift the existing checkpoints
            change_to = (tid_int, cp0)
        expect = self.checkpoints

        logger.debug(
            "Broadcasting shift of checkpoints to %s."
            "len(delta_after0) == %d.",
            change_to,
            delta_size
        )
        old_value = self.cache.get_checkpoints()
        if old_value and old_value != expect:
            log.debug(
                "Checkpoints already shifted to %s, not broadcasting.",
                old_value)
            return None

        # Shift the checkpoints.
        # Although this is a race with other instances, the race
        # should not matter.
        self.cache.store_checkpoints(*change_to)
        # The poll code will later see the new checkpoints
        # and update self.checkpoints and self.delta_after(0|1).

        return change_to

class _PersistentRowFilter(object):

    def __init__(self, adapter, delta_type):
        self.adapter = adapter
        self.delta_after0 = delta_type()
        self.delta_after1 = delta_type()
        self.polled_invalid_oids = OID_SET_TYPE()

    def __call__(self, checkpoints, row_iter):
        if not checkpoints:
            # Nothing to do except put in correct format, no transforms are possible.
            # XXX: Is there really even any reason to return these? We'll probably
            # never generate keys that match them.
            for row in row_iter:
                yield row[:2], row[2:]
        else:
            delta_after0 = self.delta_after0
            delta_after1 = self.delta_after1
            cp0, cp1 = checkpoints

            # {oid: (state, actual_tid)}
            # This holds things that we're not sure about; we hold onto them
            # and run a big query at the end to determine whether they're still valid or
            # not.
            needs_checked = OID_OBJECT_MAP_TYPE()

            for row in row_iter:
                # Rows are (oid, tid, state, tid), where the two tids
                # are always equal.
                key = row[:2]
                value = row[2:]
                oid = key[0]
                actual_tid = value[1]
                # See __poll_replace_checkpoints() to see how we build
                # the delta maps.
                #
                # We'll poll for changes *after* cp0
                # (because we set that as our current_tid/the
                # storage's prev_polled_tid) and update
                # self._delta_after0, but we won't poll for changes
                # *after* cp1. self._delta_after1 is only ever
                # populated when we shift checkpoints; we assume any
                # changes that happen after that point we catch in an
                # updated self._delta_after0.
                #
                # Also, because we're combining data in the local
                # database from multiple sources, it's *possible* that
                # some old cache had checkpoints that are behind what
                # we're working with now. So we can't actually trust
                # anything that we would put in delta_after1 without
                # validating them. We still return it, but we may take
                # it out of delta_after0 if it turns out to be
                # invalid.

                if actual_tid > cp0:
                    delta_after0[oid] = actual_tid
                elif actual_tid > cp1:
                    delta_after1[oid] = actual_tid
                else:
                    # This is too old and outside our checkpoints for
                    # when something changed. It could be good to have it,
                    # it might be something that doesn't change much.
                    # Unfortunately, we can't just stick it in our fallback
                    # keys (oid, cp0) or (oid, cp1), because it might not be current,
                    # and the storage won't poll this far back.
                    #
                    # The solution is to hold onto it and run a manual poll ourself;
                    # if it's still valid, good. If not, someone should
                    # remove it from the database so we don't keep checking.
                    # We also should only do this poll if we have room in our cache
                    # still (that should rarely be an issue; our db write size
                    # matches our in-memory size except for the first startup after
                    # a reduction in in-memory size.)
                    needs_checked[oid] = value
                    continue
                yield key, value

            # Now validate things that need validated.

            # TODO: Should this be a configurable option, like ZEO's
            # 'drop-rather-invalidate'? So far I haven't seen signs that
            # this will be particularly slow or burdensome.
            self._poll_delta_after1()

            if needs_checked:
                self._poll_old_oids_and_remove(needs_checked)
                for oid, value in iteroiditems(needs_checked):
                    # Anything left is guaranteed to still be at the tid we recorded
                    # for it (except in the event of a concurrent transaction that
                    # changed that object; that should be rare.) So these can go in
                    # our fallback keys.
                    yield (oid, cp0), value

    @log_timed
    def _poll_old_oids_and_remove(self, to_check):
        oids = list(to_check)
        # In local tests, this function executes against PostgreSQL 11 in .78s
        # for 133,002 older OIDs; or, .35s for 57,002 OIDs against MySQL 5.7.
        logger.debug("Polling %d older oids stored in cache", len(oids))
        def poll_old_oids_remove(_conn, cursor):
            return self.adapter.mover.current_object_tids(cursor, oids)
        current_tids_for_oids = self.adapter.connmanager.open_and_call(poll_old_oids_remove)

        for oid in oids:
            if (oid not in current_tids_for_oids
                    or to_check[oid][1] != current_tids_for_oids[oid]):
                del to_check[oid]
                self.polled_invalid_oids.add(oid)

        logger.debug("Polled %d older oids stored in cache; %d survived",
                     len(oids), len(to_check))

    @log_timed
    def _poll_delta_after1(self):
        orig_delta_after1 = self.delta_after1
        oids = list(self.delta_after1)
        # TODO: We have a defined transaction range here that we're concerned
        # about. We might be better off using poller.list_changes(), just like
        # __poll_replace_checkpoints() does.
        logger.debug("Polling %d oids in delta_after1", len(oids))
        def poll_oids_delta1(_conn, cursor):
            return self.adapter.mover.current_object_tids(cursor, oids)
        current_tids_for_oids = self.adapter.connmanager.open_and_call(poll_oids_delta1)
        self.delta_after1 = type(self.delta_after1)(current_tids_for_oids)
        invalid_oids = {
            oid
            for oid, tid in iteroiditems(orig_delta_after1)
            if oid not in self.delta_after1 or self.delta_after1[oid] != tid
        }
        self.polled_invalid_oids.update(invalid_oids)
        logger.debug("Polled %d oids in delta_after1; %d survived",
                     len(oids), len(oids) - len(invalid_oids))

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
