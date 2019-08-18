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
import random
import threading

from persistent.timestamp import TimeStamp
from ZODB.POSException import ReadConflictError
from ZODB.utils import p64
from ZODB.utils import u64
from zope import interface


from relstorage.autotemp import AutoTemporaryFile
from relstorage._compat import OID_OBJECT_MAP_TYPE
from relstorage._compat import OID_SET_TYPE
from relstorage._compat import iteroiditems
from relstorage._compat import metricmethod
from relstorage._compat import metricmethod_sampled
from relstorage._util import consume

from relstorage.cache import persistence
from relstorage.cache.interfaces import IStorageCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import CacheConsistencyError
from relstorage.cache.local_client import LocalClient
from relstorage.cache.memcache_client import MemcacheStateCache
from relstorage.cache.trace import ZEOTracer
from relstorage.cache._statecache_wrappers import MultiStateCache
from relstorage.cache._statecache_wrappers import TracingStateCache
from relstorage.cache._util import InvalidationMixin
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
class StorageCache(InvalidationMixin):
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
        # delta_size_limit places an approximate limit on the number of
        # entries in the delta_after maps.
        self.delta_size_limit = options.cache_delta_size_limit

        if _parent is None:
            # I must be the master!

            # This is shared between all instances of a cache in a tree,
            # including the master, so that they can share information about
            # polling.
            self.polling_state = MVCCDatabaseCoordinator()
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

        # These have the same definitions as for _PollingState;
        # whereas polling_state is shared, these are our local values.
        cp, tid, da0, da1 = self.polling_state.snapshot()
        self.checkpoints = cp
        self.current_tid = tid
        self.delta_after0 = da0
        self.delta_after1 = da1

        if _parent is None:
            self.restore()

    @property
    def highest_visible_tid(self):
        return self.current_tid

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
        return "<%s at 0x%x bytes=%d len=%d>" % (
            self.__class__.__name__,
            id(self),
            self.size,
            len(self)
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
        self.delta_after0 = self.delta_after1 = None
        self.checkpoints = None

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
                poll_cp = self.polling_state.checkpoints
                if poll_cp:
                    self.local_client.store_checkpoints(*poll_cp)
                return self.local_client.save(**save_args)
            logger.debug("Cannot justify writing cache file, no hits or misses")

    def restore(self):
        # We must only restore into an empty cache.
        state = self.polling_state
        assert not len(self.local_client) # pylint:disable=len-as-condition
        assert not self.checkpoints
        state.restore(self.adapter, self.local_client)

        cp, tid, da0, da1 = self.polling_state.snapshot()
        self.checkpoints = cp
        self.current_tid = tid
        self.delta_after0 = da0
        self.delta_after1 = da1

    def _reset(self, message=None):
        """
        Reset the transaction state of only this instance.

        If this is being done in a transactional way, it must be followed
        by raising an exception. If the *message* parameter is provided,
        then a ``CacheConsistencyError`` will be raised when this
        method returns.
        """
        # As if we've never polled
        self.checkpoints = None
        self.current_tid = None
        self.delta_after0 = self.polling_state.delta_map_type()
        self.delta_after1 = self.polling_state.delta_map_type()
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

        # As for load(), if we haven't polled, we can't trust our cache.
        if not self.checkpoints:
            return None

        if not self.options.keep_history:
            # For history-free, we can only have one state. If we
            # think we know what it is, but they ask for something different,
            # then there's no way it can be found.
            known_tid_int = self.delta_after0.get(oid_int)
            if known_tid_int is None or known_tid_int != tid_int:
                # No good. Ok, well, this is for conflict resolution, so if the
                # state was updated by someone else in this same process,
                # and we can find it in our shared polling state we got lucky.
                known_tid_int = self.polling_state.delta_after0.get(oid_int)
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
        # This object will get cached under this key. Until the checkpoints change,
        # we'll be able to find it. After checkpoints have shifted forward
        # once (cp1 becomes cp0), if we request it again we'll find it under the secondary
        # key and the cache will automatically move it forward to this key, where, when
        # the checkpoints change once again, we'll still be able to find it. If we don't
        # request it after that first checkpoint change, though, then on the second one
        # we won't be able to find it, even if it's still in memory and we'll go through
        # this all again.
        #
        # TODO: Why don't we just use a specific distinguished TID
        # such as 0 for older objects so we don't have to go through
        # this?

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
        """
        See notes in `invalidate_all`.
        """
        del self.cache[(oid_int, tid_int)]
        self._invalidate(oid_int, tid_int)
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
        self._invalidate_all(oids)
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

        # In particular, it stores the data in delta_after0 so that
        # future cache lookups for oid_int should now use the tid just
        # committed. We're about to flush that data to the cache.

        # We can get here without having ever polled, so no
        # checkpoints and no current_tid. So long as we're careful,
        # that's not a problem. Being careful means being sure to
        # keep the state of the previous polled TID matching, and avoiding
        # unnecessary polls

        # Under what circumstances would we get here (after committing
        # a transaction) without ever having polled to establish
        # checkpoints? Mostly this happens in test cases that directly
        # use storage APIs, but it also turns out that database-level
        # APIs like db.undo() use new storage instances in an unusual
        # way, and will not necessarily have polled by the time they
        # commit.
        #
        # Of course, if we restored from persistent cache files the master
        # could have checkpoints we copied down.
        #
        # TODO: Create a special subclass for MVCC instances and separate
        # the state handling.
        self.cache.set_all_for_tid(tid_int, self.temp_objects)

        # If we aren't keeping history, then a previous revision of
        # the object we happened to know about is now gone. We can
        # pre-emptively throw that away to try to save cache room. Of
        # course, if some in-progress transaction that's behind us
        # (still has an older view of the database) happens to want to
        # load that object, it'll go back in the cache. c'est la vie.
        # (XXX: Why not do this for history preserving too? It seems
        # like historical connections are probably rarely used? since
        # we have a special adapter for them we could do custom logic
        # if any of those are around.)
        #
        # This might seem to result in a violation of our internal
        # constraints: even though we haven't done a poll yet, we've
        # modified our knowledge of canonical information in
        # delta_after0. But we've always done that, and that's because
        # polling specifically excludes the transaction that we just
        # committed.
        #
        # This has the unfortunate side-effect of causing our
        # optimized loadSerial() method to become useless for conflict
        # resolution, however (unless the conflict happens in the same
        # process). See ``checkResolveConflictBetweenConnections()``
        #
        # TODO: Continue enhancing the smarts for this. Maybe move to
        # __poll_update_delta0_from_changes? Maybe make the polling
        # state responsible? It could track invalidations and current
        # tids across all connections and only throw away data when
        # everyone has moved on thus solving the side-effect for
        # loadSerial --- of course, that makes having an accurately
        # sized connection pool and/or timeout important, or at least
        # knowing when a connection is sitting idle and not being
        # used. ``after_tpc_finish`` isn't enough for that, we need a
        # hook from the storage.
        store = self.delta_after0.__setitem__
        if not self.keep_history:
            pop0 = self.delta_after0.pop
            pop1 = self.delta_after1.pop
            # Note that we don't call self.invalidate(). We don't need
            # most of its services; many are redundant with what we're about
            # to do.
            invalidate = self.cache.__delitem__
            for oid_int in self.temp_objects.stored_oids:
                old_tid = pop0(oid_int, None)
                if old_tid:
                    invalidate((oid_int, old_tid))
                old_tid = pop1(oid_int, None)
                if old_tid:
                    invalidate((oid_int, old_tid))

                store(oid_int, tid_int)
        else:
            for oid_int in self.temp_objects.stored_oids:
                # Future cache lookups for oid_int should now use
                # the tid just committed. We're about to flush that
                # data to the cache.
                store(oid_int, tid_int)

        self.polling_state.after_tpc_finish(tid_int, self.temp_objects.stored_oids)

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

        *prev_tid_int* can be ``None``, in which case the *changes*
        parameter will be ignored. *new_tid_int* can not be ``None``.

        If *changes* was not ``None``, this method returns a collection of
        OID integers from it. (Because *changes* is only required to be
        an iterable, you may not be able to iterate it again.)
        """
        logger.info(
            "After poll with my checkpoits %s current_tid %s incoming tid %s",
            self.checkpoints, self.current_tid, new_tid_int
        )
        my_prev_tid_int = self.current_tid or 0
        self.current_tid = new_tid_int


        # Grab without a lock. Ok if slightly stale.
        global_checkpoints = self.polling_state.checkpoints

        if not global_checkpoints:
            # No other instance has established an opinion yet,
            # so I get to. Note that this return of None will
            # drop anything already cached
            self.__poll_establish_global_checkpoints(new_tid_int)
            if changes is not None:
                consume(changes)

            return

        global_checkpoints_in_future = global_checkpoints[0] > new_tid_int
        if global_checkpoints_in_future:
            # checkpoint0 is in a future that this instance can't yet
            # see. Ignore the checkpoint change for now, continue
            # with our own.
            logger.debug(
                "Global checkpoints (%s) are ahead of polled tid %s",
                global_checkpoints, new_tid_int
            )
            global_checkpoints = self.checkpoints
            if not self.checkpoints:
                # How nice, this was our first poll, but
                # yet somehow we are still behind the global
                # checkpoints. The global checkpoints are probably
                # wrong (maybe there's a replica involved and the global
                # cache is now stale).
                global_checkpoints = (new_tid_int, new_tid_int)

        # We want to keep the current delta maps for speed, but we
        # have to replace them (to avoid consistency violations) if
        # certain conditions happen. These conditions are all
        # external, in the database. If `changes` is None, and hence
        # we return None, the entire ZODB Connection cache will be
        # dropped.

        if (
                # We are in sync with the world. If we don't use the
                # same checkpoints as everyone else, then when `load`
                # has a cache miss and needs to store an older object
                # that hasn't changed recently enough to be in the
                # delta maps, it uses OID:CP0 as the key. So if we
                # don't update our checkpoints when everyone else
                # does, we'll be writing two separate sets of keys for
                # older objects that haven't changed, bloating the
                # cache. This shouldn't actually be a consistency
                # issue, just a bloat issue. (Further, it is also this
                # changing of checkpoints that signals us to drop our
                # delta maps, which may or may not actually be full.)
                #
                # TODO: That's probably not a good use of cache. We
                # should use a distinguished key such as -1 for these
                # "frozen" objects.
                #
                # This also happens when the global checkpoints are in
                # the future. We'll throw away our delta maps *now*,
                # making it unlikely we'll get any cache hits. Next time we poll,
                # hopefully we'll be caught up enough to use the global maps
                # and will rebuild our maps then.
                #
                # TODO: Do we really need to throw them away now? Why can't we
                # keep them for the transaction we're about to have?
                global_checkpoints == self.checkpoints
                # Poller didn't give up (which it would if this was
                # our first poll; it won't list the entire database),
                # and there was data in the database (if there's no
                # data, new_tid_int will be 0).
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
                # get here. Note that it could be the same if there were no
                # changes.
                and new_tid_int >= my_prev_tid_int
        ):
            logger.debug("All conditions met to save checkpoitns")
            # All the conditions for keeping the checkpoints were met,
            # so just update self.delta_after0 and self.current_tid.
            changes = self.__poll_update_delta0_from_changes(changes)
            self.polling_state.after_normal_poll(self)
        else:
            log.debug(
                "Replacing checkpoint deltas. {new_cp=%s, current_cp=%s, "
                "prev_tid_int=%s, my_prev_tid_int=%s, new_tid_int=%s} "
                "(Cause: Suggested checkpoint change? %s. Too many changes? %s. "
                "First poll? %s. Polls mismatch? %s. "
                "Transaction went back? %s)",
                global_checkpoints, self.checkpoints,
                prev_tid_int, my_prev_tid_int, new_tid_int,
                global_checkpoints != self.checkpoints, changes is None,
                not bool(prev_tid_int), not ((my_prev_tid_int or -2) < (prev_tid_int or -1)),
                new_tid_int < my_prev_tid_int
            )
            # If the TID went backwards, and we didn't have `revert-when-stale` on,
            # we *must* get a ``changes`` of None in order to signal dropping
            # the connection cache. (A backwards TID causes the poller to raise an
            # exception without that setting.)
            assert new_tid_int >= my_prev_tid_int or changes is None
            if changes is not None:
                changes = OID_SET_TYPE([oid for oid, _tid in changes])

            self.__poll_replace_checkpoints(cursor, global_checkpoints, new_tid_int)

        if not global_checkpoints_in_future and self._should_suggest_shifted_checkpoints():
            # Obviously we can't do this if we're currently behind what the checkpoints
            # are already set to.
            self._suggest_shifted_checkpoints(cursor)

        return changes

    #: By default, a 70% chance when we're full.
    CP_REPLACEMENT_CHANCE_WHEN_FULL = float(
        os.environ.get(
            'RELSTORAGE_CP_REPLACEMENT_CHANCE_WHEN_FULL',
            "0.7"
        )
    )

    #: If we're just close, a 20% chance.
    CP_REPLACEMENT_CHANCE_WHEN_CLOSE = float(
        os.environ.get(
            'RELSTORAGE_CP_REPLACEMENT_CHANCE_WHEN_CLOSE',
            "0.2"
        )
    )

    #: Start considering that we're close when we're 80% full.
    CP_REPLACEMENT_BEGIN_CONSIDERING_PERCENT = float(
        os.environ.get(
            'RELSTORAGE_CP_REPLACEMENT_BEGIN_CONSIDERING_PERCENT',
            "0.8"
        )
    )

    def _should_suggest_shifted_checkpoints(self, _random=random.random):
        """
        Take the size of the checkpoints and our thresholds into account
        and determine whether we should try to replace them.
        """

        # Use the global state-sharing default random generator by
        # default (allow replacement for testing). This ensures our
        # uniform odds are truly uniform (unless someone reseeds the
        # generator) across all instances. (Interestingly, Python 3.7
        # automatically reseeds the generator on fork.) A single shared instance
        # of SystemRandom would get all workers on a single machine sharing the same
        # sequence, but it costs a system call.

        delta_size = len(self.delta_after0)
        limit = self.delta_size_limit

        if delta_size < (limit * self.CP_REPLACEMENT_BEGIN_CONSIDERING_PERCENT):
            return False

        if delta_size >= limit:
            chances = self.CP_REPLACEMENT_CHANCE_WHEN_FULL
            when_dice_not_used = True
        else:
            chances = self.CP_REPLACEMENT_CHANCE_WHEN_CLOSE
            when_dice_not_used = False

        if chances < 1:
            # e.g., for a 90% chance, only 10% of the range of random
            # numbers (uniformly generated in the range [0.0, 1.0))
            # should lead to a false return.
            # 0.0 -- 0.89 < 0.9: True
            # 0.9 -- 0.99 >= 0.9: False
            return _random() < chances

        return when_dice_not_used


    def __poll_establish_global_checkpoints(self, new_tid_int):
        # Because we *always* have checkpoints in our local_client,
        # once we've set them, not being able to find them there also
        # means that it was our first poll, and so we shouldn't have
        # checkpoints ourself. Of course, with multi-threaded race
        # conditions, that might not actually be the case.
        # assert not self.checkpoints
        # XXX: That doesn't sound right, this is a single-threaded object.

        if not new_tid_int:
            # Refuse to set 0 as a first checkpoint. This is before any data
            # in the database.
            logger.debug("Not using %s as initial checkpoint", new_tid_int)
            return

        # Initialize the checkpoints; we've never polled before.
        logger.debug("Initializing checkpoints: %s", new_tid_int)

        # Storing to the cache is here just for test BWC
        self.cache.store_checkpoints(new_tid_int, new_tid_int)
        self.checkpoints = (new_tid_int, new_tid_int)
        self.polling_state.after_established_checkpoints(self)

    @metricmethod_sampled
    def __poll_update_delta0_from_changes(self, changes):
        m = self.cache.updating_delta_map(self.delta_after0)
        m_get = m.get
        changed_oids = OID_SET_TYPE()
        for oid_int, tid_int in changes:
            changed_oids.add(oid_int)
            my_tid_int = m_get(oid_int, -1)
            if tid_int > my_tid_int:
                # XXX: When would it be lower?
                m[oid_int] = tid_int

        return changed_oids

    @metricmethod
    def __poll_replace_checkpoints(self, cursor, new_checkpoints, new_tid_int):
        # We were asked to replace our checkpoints, with the global
        # checkpoints specified by the cache. To keep access to all
        # our cache data, we rebuild our delta maps. The new
        # checkpoints give us our rebuild boundaries.

        # Note that those will both be equal to ``new_tid_int``, if
        # the cache had checkpoints in the future. In that case, we
        # just empty our delta maps and do nothing else (there's
        # nothing to poll for that we could make use of). Hopefully,
        # next time we poll we'll be able to use the global
        # checkpoints; we'll notice that ours don't match and call this again
        # to rebuild.
        assert new_checkpoints is not None
        cp, da0, da1 = self.polling_state.after_poll_with_changed_checkpoints(
            self,
            cursor,
            new_checkpoints,
            new_tid_int
        )
        assert cp is not None
        self.checkpoints = cp
        self.delta_after0 = da0
        self.delta_after1 = da1

    def _suggest_shifted_checkpoints(self, cursor):
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
            "Broadcasting shift of checkpoints to %s. "
            "len(delta_after0) == %d.",
            change_to,
            delta_size
        )

        # In the past, after setting this on the cache, the poll code
        # will later see the new checkpoints and update
        # self.checkpoints and self.delta_after(0|1).
        self.cache.replace_checkpoints(expect, change_to)
        # However, we no longer read from the cache (the set is only there
        # for test compatibility). The polling state handles replacing things
        # for us if necessary (no need to waste a trip around with bad checkpoints)
        return self.polling_state.replace_checkpoints(self, cursor, expect, change_to, tid_int)



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
        try:
            startpos, endpos, _ = self._queue_contents[oid_int]
        except KeyError:
            # XXX: Seeing this on appveyor, only in a few tests,
            # only on MySQL. Not sure why.
            raise KeyError("No oid %d stored in %s" % (
                oid_int,
                list(self._queue_contents)
            ))
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
