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
import os
import threading

from persistent.timestamp import TimeStamp
from ZODB.POSException import ReadConflictError
from ZODB.utils import p64
from ZODB.utils import u64
from zope import interface

from relstorage._compat import iteritems

from relstorage.autotemp import AutoTemporaryFile
from relstorage.cache import persistence
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import OID_TID_MAP_TYPE
from relstorage.cache.local_client import LocalClient
from relstorage.cache.memcache_client import MemcacheStateCache
from relstorage.cache.trace import ZEOTracer

logger = log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    size = limit = 0
    def __len__(self):
        return 0
    close = lambda s: None
    stats = lambda s: {}
_UsedAfterRelease = _UsedAfterRelease()

@interface.implementer(IStateCache)
class _MultiStateCache(object):
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

    def __call__(self, oid, tid1, tid2):
        result = self.l(oid, tid1, tid2)
        if not result:
            result = self.g(oid, tid1, tid2)
            if result:
                self.l[(oid, tid1)] = result
        return result

    def set_multi(self, data):
        self.l.set_multi(data)
        self.g.set_multi(data)

    # Unlike everything else, checkpoints are on the global
    # client first and then the local one.

    def get_checkpoints(self):
        return self.g.get_checkpoints() or self.l.get_checkpoints()

    def store_checkpoints(self, c0, c1):
        # TODO: Is there really much value in storing the checkpoints
        # globally (as opposed to just on a single process)?
        self.g.store_checkpoints(c0, c1)
        return self.l.store_checkpoints(c0, c1)


@interface.implementer(IPersistentCache)
class StorageCache(object):
    """RelStorage integration with memcached or similar.

    Holds a list of memcache clients in order from most local to
    most global.  The first is a LocalClient, which stores the cache
    in the Python process, but shares the cache between threads.
    """
    # pylint:disable=too-many-instance-attributes,too-many-public-methods

    # send_limit: approximate limit on the bytes to buffer before
    # sending to the cache.
    send_limit = 1024 * 1024

    # queue is an AutoTemporaryFile during transaction commit.
    queue = None

    # queue_contents is a map of {oid_int: (startpos, endpos)}
    # during transaction commit.
    queue_contents = None

    # checkpoints, when set, is a tuple containing the integer
    # transaction ID of the two current checkpoints. checkpoint0 is
    # greater than or equal to checkpoint1.
    checkpoints = None

    # current_tid contains the last polled transaction ID. Invariant:
    # when self.checkpoints is not None, self.delta_after0 has info
    # from all transactions in the range:
    #
    #   self.checkpoints[0] < tid <= self.current_tid
    #
    # We assign to this *only* after executing a poll, or
    # when reading data from the persistent cache (which happens at
    # startup, and usually also when someone calls clear())
    current_tid = 0

    _tracer = None

    _delta_map_type = OID_TID_MAP_TYPE

    def __init__(self, adapter, options, prefix, local_client=None,
                 _tracer=None):
        self.adapter = adapter
        self.options = options
        self.prefix = prefix or ''

        # delta_after0 contains {oid: tid} after checkpoint 0
        # and before or at self.current_tid.
        self.delta_after0 = self._delta_map_type()

        # delta_after1 contains {oid: tid} after checkpoint 1 and
        # before or at checkpoint 0. The content of delta_after1 only
        # changes when checkpoints move.
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
            self.cache = _MultiStateCache(self.local_client, shared_cache)
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
            self._trace = self._tracer.trace
            self._trace_store_current = self._tracer.trace_store_current

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
                # Only write this out if (1) it proved useful ON (2)
                # we've made modifications. Otherwise, we're writing a consolidated
                # file for no good reason.
                # TODO: Consider the correctness here, now that we have a
                # more accurate cache.
                return self.local_client.save(**save_args)
            logger.debug("Cannot justify writing cache file, no hits or misses")

    def restore(self):
        # We must only restore into an empty cache.
        assert not len(self.local_client) # pylint:disable=len-as-condition
        assert not self.checkpoints

        # Note that there may have been a tiny amount of data in the
        # file that we didn't get to actually store but that still
        # comes back in the delta_map; that's ok.
        row_filter = _PersistentRowFilter(self._delta_map_type)
        self.local_client.restore(row_filter)

        self.checkpoints = self.local_client.get_checkpoints()
        if self.checkpoints:
            # No point keeping the delta maps otherwise,
            # we have to poll. If there were no checkpoints, it means
            # we saved without having ever completed a poll.
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
            del self._trace
            del self._trace_store_current
            del self._tracer

    def clear(self, load_persistent=True):
        """
        Remove all data from the cache.  Called by speed tests.

        Starting from the introduction of persistent cache files,
        this also results in the local client being repopulated with
        the current set of persistent data. The *load_persistent* keyword can
        be used to control this.

        .. versionchanged:: 2.0b6
           Added the ``load_persistent`` keyword. This argument is provisional.
        """
        self.cache.flush_all()

        self.checkpoints = None
        # After this our current_tid is probably out of sync with the
        # storage's current_tid. Whether or not we load data from
        # persistent caches, it's probably in the past of what the
        # storage thinks.
        # XXX: Ideally, we should be able to populate that information
        # back up so that we get the right polls.
        self.current_tid = 0
        self.delta_after0 = self._delta_map_type()
        self.delta_after1 = self._delta_map_type()

        if load_persistent:
            self.restore()

    @staticmethod
    def _trace(*_args, **_kwargs): # pylint:disable=method-hidden
        # Dummy method for when we don't do tracing
        return

    @staticmethod
    def _trace_store_current(_tid_int, _items): # pylint:disable=method-hidden
        # Dummy method for when we don't do tracing
        return

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
            # Possible causes:
            #
            # - The database MUST provide a snapshot view for each
            #   session; this error can occur if that requirement is
            #   violated. For example, MySQL's MyISAM engine is not
            #   sufficient for the object_state table because MyISAM
            #   can not provide a snapshot view. (InnoDB is
            #   sufficient.)
            #
            # - Something could be writing to the database out
            #   of order, such as a version of RelStorage that
            #   acquires a different commit lock.
            #
            # - A software bug. In the past, there was a subtle bug
            #   in after_poll() that caused it to ignore the
            #   transaction order, leading it to sometimes put the
            #   wrong tid in delta_after*.
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
            raise AssertionError(msg)

    def load(self, cursor, oid_int):
        """
        Load the given object from cache if possible.

        Fall back to loading from the database.

        Returns (state_bytes, tid_int).
        """
        # pylint:disable=too-many-statements,too-many-branches,too-many-locals
        if not self.checkpoints:
            # No poll has occurred yet. For safety, don't use the cache.
            self._trace(0x20, oid_int)
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
        # other cache keys, since the tid_int specified in delta_after0
        # replaces all older transaction IDs. Similarly, if
        # delta_after1 contains oid_int, we should not look at
        # checkpoints[1]. Also, when both checkpoints are set to the
        # same transaction ID, we don't need to ask for the same key
        # twice.
        cache = self.cache
        tid_int = self.delta_after0.get(oid_int)
        if tid_int:
            # This object changed after checkpoint0, so
            # there is only one place to look for its state: the exact key.
            key = (oid_int, tid_int)
            cache_data = cache[key]
            if cache_data:
                # Cache hit.
                assert cache_data[1] == tid_int
                # Note that we trace all cache hits, not just the local cache hit.
                # This makes the simulation less useful, but the stats might still have
                # value to people trying different tuning options manually.
                self._trace(0x22, oid_int, tid_int, dlen=len(cache_data[0]))
                return cache_data

            # Cache miss.
            self._trace(0x20, oid_int)
            state, actual_tid_int = self.adapter.mover.load_current(
                cursor, oid_int)
            self._check_tid_after_load(oid_int, actual_tid_int, tid_int)

            # At this point we know that tid_int == actual_tid_int
            cache[key] = (state, actual_tid_int)
            return state, tid_int

        # Make a list of cache keys to query. The list will have either
        # 1 or 2 keys.
        cp0, cp1 = self.checkpoints
        tid1 = cp0
        tid2 = None
        tid_int = self.delta_after1.get(oid_int)
        if tid_int:
            tid2 = tid_int
        elif cp1 != cp0:
            tid2 = cp1

        preferred_key = (oid_int, tid1)

        # Query the cache. Query multiple keys simultaneously to
        # minimize latency. The client is responsible for moving
        # the data to the preferred key if it wasn't found there.
        response = cache(oid_int, tid1, tid2)
        if response: # We have a hit!
            state, actual_tid = response
            # Cache hit
            self._trace(0x22, oid_int, actual_tid, dlen=len(state))
            return state, actual_tid

        # Cache miss.
        self._trace(0x20, oid_int)
        state, tid_int = self.adapter.mover.load_current(cursor, oid_int)
        if tid_int:
            self._check_tid_after_load(oid_int, tid_int)
            # Record this as a store into the cache, ZEO does.
            self._trace(0x52, oid_int, tid_int, dlen=len(state) if state else 0)
            cache[preferred_key] = (state, tid_int)
        return state, tid_int

    def tpc_begin(self):
        """Prepare temp space for objects to cache."""
        # start with a fresh in-memory buffer instead of reusing one that might
        # already be spooled to disk.
        self.queue = AutoTemporaryFile()
        self.queue_contents = {}

    def store_temp(self, oid_int, state):
        """Queue an object for caching.

        Typically, we can't actually cache the object yet, because its
        transaction ID is not yet chosen.
        """
        assert isinstance(state, bytes)
        queue = self.queue
        queue.seek(0, 2)  # seek to end
        startpos = queue.tell()
        queue.write(state)
        endpos = queue.tell()
        self.queue_contents[oid_int] = (startpos, endpos)


    def _read_temp_state(self, startpos, endpos):
        self.queue.seek(startpos)
        length = endpos - startpos
        state = self.queue.read(length)
        if len(state) != length:
            raise AssertionError("Queued cache data is truncated")
        return state, length

    def read_temp(self, oid_int):
        """
        Return the bytes for a previously stored temporary item.
        """
        startpos, endpos = self.queue_contents[oid_int]
        return self._read_temp_state(startpos, endpos)[0]

    def send_queue(self, tid):
        """Now that this tid is known, send all queued objects to the cache"""
        tid_int = u64(tid)
        send_size = 0
        to_send = {}

        # Order the queue by file position, which should help if the
        # file is large and needs to be read sequentially from disk.
        items = [
            (startpos, endpos, oid_int)
            for (oid_int, (startpos, endpos)) in iteritems(self.queue_contents)
        ]
        items.sort()
        # Trace these. This is the equivalent of ZEOs
        # ClientStorage._update_cache.
        self._trace_store_current(tid_int, items)
        cache = self.cache
        for startpos, endpos, oid_int in items:
            state, length = self._read_temp_state(startpos, endpos)
            cachekey = (oid_int, tid_int)
            item_size = length + len(cachekey)
            if send_size and send_size + item_size >= self.send_limit:
                cache.set_multi(to_send)
                to_send.clear()
                send_size = 0
            to_send[cachekey] = (state, tid_int)
            send_size += item_size

        if to_send:
            cache.set_multi(to_send)

        self.queue_contents.clear()
        self.queue.seek(0)

    def after_tpc_finish(self, tid):
        """
        Flush queued changes.

        This is called after the database commit lock is released,
        but before releasing the storage lock that will allow other
        threads to use this instance.
        """
        tid_int = u64(tid)

        if self.checkpoints:
            for oid_int in self.queue_contents:
                # Future cache lookups for oid_int should now use
                # the tid just committed.
                self.delta_after0[oid_int] = tid_int

        self.send_queue(tid)

    def clear_temp(self):
        """Discard all transaction-specific temporary data.

        Called after transaction finish or abort.
        """
        self.queue_contents = None
        if self.queue is not None:
            self.queue.close()
            self.queue = None

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
        my_prev_tid_int = self.current_tid
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
        # Because we *always* have checkpoints in our
        # local_client, once we've set them, this also means that
        # it was our first poll.
        assert not self.checkpoints

        # Initialize the checkpoints; we've never polled before.
        log.debug("Initializing checkpoints: %s", new_tid_int)

        self.checkpoints = self.cache.store_checkpoints(new_tid_int, new_tid_int)

    def __poll_update_delta0_from_changes(self, changes):
        m = self.delta_after0
        m_get = m.get
        trace = self._trace
        for oid_int, tid_int in changes:
            my_tid_int = m_get(oid_int)
            if my_tid_int is None or tid_int > my_tid_int:
                m[oid_int] = tid_int
                # 0x1C = invalidate (hit, saving non-current)
                trace(0x1C, oid_int, tid_int)

    def __poll_replace_checkpoints(self, cursor, new_checkpoints, new_tid_int):
        # We have to replace the checkpoints.
        cp0, cp1 = new_checkpoints

        # Use the checkpoints specified by the cache.
        # Rebuild delta_after0 and delta_after1.
        new_delta_after0 = self._delta_map_type()
        new_delta_after1 = self._delta_map_type()
        if cp1 < new_tid_int:
            # poller.list_changes(after_tid, last_tid) provides an iterator of
            # (oid, tid) where tid > after_tid and tid <= last_tid.
            change_list = self.adapter.poller.list_changes(
                cursor, cp1, new_tid_int)

            # Make a dictionary that contains, for each oid, the most
            # recent tid listed in changes. This works because sorting the
            # (oid, tid) pairs puts the newest tid at the back, and constructing
            # the dictionary from that sorted list preserves order, keeping the
            # last key that it saw.
            change_list = sorted(change_list) # Note this materializes the iterator
            try:
                change_dict = self._delta_map_type(change_list)
            except TypeError:
                # pg8000 returns a list of lists, not a list of tuples. The
                # BTree constructor is very particular about that. Normally one
                # would use pg8000 on PyPy, where we don't use BTrees, so this shouldn't
                # actually come up in practice.
                change_dict = self._delta_map_type()
                for oid_int, tid_int in change_list:
                    change_dict[oid_int] = tid_int
            del change_list

            # Put the changes in new_delta_after*.
            for oid_int, tid_int in change_dict.items():
                # 0x1C = invalidate (hit, saving non-current)
                self._trace(0x1C, oid_int, tid_int)
                if tid_int > cp0:
                    new_delta_after0[oid_int] = tid_int
                elif tid_int > cp1:
                    new_delta_after1[oid_int] = tid_int

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

    def __init__(self, delta_type):
        self.delta_type = delta_type
        self.delta_after0 = self.delta_type()
        self.delta_after1 = self.delta_type()

    def __call__(self, checkpoints, row_iter):
        if not checkpoints:
            # Nothing to do, no transforms are possible.
            # But we do need to expand it to the expected form
            for row in row_iter:
                oid, actual_tid, state = row
                yield oid, actual_tid, state, actual_tid
        else:
            delta_after0 = self.delta_after0
            delta_after1 = self.delta_after1
            cp0, cp1 = checkpoints
            for row in row_iter:
                oid, actual_tid, state = row

                if actual_tid >= cp0:
                    key_tid = actual_tid
                    delta_after0[oid] = actual_tid
                elif actual_tid >= cp1:
                    key_tid = actual_tid
                    delta_after1[oid] = actual_tid
                else:
                    # Old generation, no delta.
                    # Even though this is old, it could be good to have it,
                    # it might be something that doesn't change much.
                    key_tid = cp0
                yield oid, key_tid, state, actual_tid
