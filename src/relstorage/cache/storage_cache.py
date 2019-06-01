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

import importlib
import logging
import os
import threading
import time

import BTrees
from persistent.timestamp import TimeStamp
from ZODB.POSException import ReadConflictError
from ZODB.utils import p64
from ZODB.utils import u64
from zope import interface

from relstorage._compat import PYPY
from relstorage._compat import iteritems
from relstorage._compat import string_types
from relstorage.autotemp import AutoTemporaryFile
from relstorage.cache import persistence
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.local_client import LocalClient
from relstorage.cache.trace import ZEOTracer

logger = log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    pass
_UsedAfterRelease = _UsedAfterRelease()

@interface.implementer(IPersistentCache)
class _MemcacheStateCache(object):

    def __init__(self, client, prefix):
        self.prefix = prefix
        self.client = client
        # checkpoints_key holds the current checkpoints.
        self.checkpoints_key = ck = '%s:checkpoints' % self.prefix
        # no unicode on Py2
        assert isinstance(ck, str), (ck, type(ck))

    def __getitem__(self, oid_tid):
        return self(*oid_tid)

    def __call__(self, oid, tid1, tid2=None):
        cachekeys = []
        cachekeys.append('%s:state:%d:%d' % (self.prefix, tid1, oid))
        if tid2 is not None:
            cachekeys.append('%s:state:%d:%d' % (self.prefix, tid2, oid))
        response = self.client.get_multi(cachekeys)
        preferred_data = response.get(cachekeys[0])
        if preferred_data and len(preferred_data) >= 8:
            actual_tid_int = u64(preferred_data[:8])
            return preferred_data[8:], actual_tid_int

        backup_data = response.get(cachekeys[1]) if tid2 is not None else None
        if backup_data and len(backup_data) >= 8:
            # Hooray, at least it was there. Go ahead and move it to
            # the preferred position
            actual_tid_int = u64(backup_data[:8])
            state_bytes = backup_data[8:]
            self[(oid, tid1)] = (state_bytes, actual_tid_int)

            return state_bytes, actual_tid_int

    def __setitem__(self, oid_tid, state_bytes_tid):
        oid, tid = oid_tid
        key = '%s:state:%d:%d' % (self.prefix, tid, oid)
        state_bytes, actual_tid = state_bytes_tid
        cache_data = p64(actual_tid) + (state_bytes or b'')
        self.client.set(key, cache_data)

    def set_multi(self, keys_and_values):
        formatted = {
            '%s:state:%d:%d' % (self.prefix, tid, oid): (p64(actual_tid) + (state or b''))
            for (tid, oid), (state, actual_tid) in iteritems(keys_and_values)
        }
        self.client.set_multi(formatted)

    def store_checkpoints(self, cp0_tid, cp1_tid):
        checkpoint_data = '%d %d' % (cp0_tid, cp1_tid)
        checkpoint_data = checkpoint_data.encode('ascii')
        self.client.set(self.checkpoints_key, checkpoint_data)

    def get_checkpoints(self):
        s = self.client.get(self.checkpoints_key)
        if s:
            try:
                c0, c1 = s.split()
                c0 = int(c0)
                c1 = int(c1)
            except ValueError:
                # Invalid checkpoint cache value; ignore it.
                pass
            else:
                return c0, c1

    def close(self):
        self.client.disconnect_all()
        self.client = None

    def flush_all(self):
        self.client.flush_all()


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

    # current_tid contains the last polled transaction ID.  Invariant:
    # when self.checkpoints is not None, self.delta_after0 has info
    # from all transactions in the range:
    #   self.checkpoints[0] < tid <= self.current_tid
    current_tid = 0

    _tracer = None

    # An LLBTree uses much less memory than a dict, and is still plenty fast on CPython;
    # it's just as big and slower on PyPy, though.
    _delta_map_type = BTrees.family64.II.BTree if not PYPY else dict


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

        self.clients_local_first = []
        if local_client is None:
            self.clients_local_first.append(LocalClient(options, self.prefix))
        else:
            self.clients_local_first.append(local_client)

        if options.cache_servers:
            module_name = options.cache_module_name
            module = importlib.import_module(module_name)
            servers = options.cache_servers
            if isinstance(servers, string_types):
                servers = servers.split()
            self.clients_local_first.append(
                _MemcacheStateCache(
                    module.Client(servers),
                    prefix
                )
            )

        # self.clients_local_first is in order from local to global caches,
        # while self.clients_global_first is in order from global to local.
        self.clients_global_first = list(reversed(self.clients_local_first))

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
        if self.clients_local_first is _UsedAfterRelease:
            return 0
        return len(self.local_client)

    @property
    def size(self):
        if self.clients_local_first is _UsedAfterRelease:
            return 0
        return self.local_client.size

    @property
    def limit(self):
        if self.clients_local_first is _UsedAfterRelease:
            return 0
        return self.local_client.limit

    @property
    def local_client(self):
        """
        The (shared) local in-memory cache client.
        """
        return self.clients_local_first[0]

    def stats(self):
        """
        Return stats. This is a debugging aid only. The format is undefined and intended
        for human inspection only.
        """
        try:
            local_client = self.local_client
        except TypeError:
            return {'closed': True}
        else:
            return local_client.stats()

    def new_instance(self):
        """
        Return a copy of this instance sharing the same local client.

        *set_checkpoints* is for testing; usually we do want to set
        the checkpoints to the current values we can find from
        our local cache data.
        """
        local_client = self.local_client if self.options.share_local_cache else None

        cache = type(self)(self.adapter, self.options, self.prefix,
                           local_client,
                           _tracer=self._tracer or False)

        # The delta maps get more and more stale the longer time goes on.
        # Maybe we want to try to re-create them based on the local max tids?
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
        clients = ()
        if self.clients_local_first is not _UsedAfterRelease:
            clients = self.clients_local_first

        for client in clients:
            client.close()

        # Release our clients. If we had a non-shared local cache,
        # this will also allow it to release any memory its holding.
        # Set them to non-iterables to make it obvious if we are used
        # after release.
        self.clients_local_first = _UsedAfterRelease
        self.clients_global_first = _UsedAfterRelease

    def save(self):
        """
        Store any persistent client data.
        """
        if self.options.cache_local_dir and len(self): # pylint:disable=len-as-condition
            # (our __bool__ is not consistent with our len)
            stats = self.local_client.stats()
            if stats['hits'] or stats['sets']:
                # Only write this out if (1) it proved useful or (2)
                # we've made modifications. Otherwise, we're writing a consolidated
                # file for no good reason.
                return persistence.save_local_cache(self.options, self.prefix, self)
            logger.debug("Cannot justify writing cache file: %s", stats)

    def write_to_stream(self, stream):
        # XXX: This is now wrong. Update the comments about how this works.

        # We currently don't write anything to the stream, delegating instead
        # just to the local client.

        # We experimented with trying to save and load chcekpoints and
        # the delta maps, but this turned out to be complex (because
        # the `new_instance`s that have the actual data are released
        # before we are, so their data gets lost, and we have to
        # implement a parent/child relationship to fix that) and no
        # more effective than relying on the default checkpoints we
        # get from polling, if there have been no changes---at least
        # in the case of zodbshootout benchmark (in fact, it was
        # somewhat *slower*, for reasons that aren't fully clear).

        # Note that if we did want to dump the delta maps, we would
        # need to either wrap them in a dict or dump them pairwise; We
        # can't dump a BTree larger than about 25000 without getting
        # into recursion problems.

        # oid -> tid
        newest_tids = self._find_newest_tids_for_oid()
        def key_transform(key, value):
            # Reject anything that's not the most current key we have
            oid, _ = key
            _, actual_tid = value
            if newest_tids[oid] != actual_tid:
                return
            # Transform other keys to their exact match.
            # When we read in, we'll put these into the 'delta_after1' map.
            return (oid, actual_tid)

        return self.local_client.write_to_stream(stream, key_transform)

    def _find_max_tids(self):
        tids = {v[1] for v in self.local_client.values()}
        return sorted(tids, reverse=True)[:3]

    def _find_newest_tids_for_oid(self):
        # All of these, by definition, will be less than or equal to, the
        # value of the max_tid.
        oid_to_tid = self._delta_map_type()
        for k, v in self.local_client.items():
            oid, _ = k
            _, tid = v
            # 0 is a valid value for both; 0 is the root oid,
            # which likely rarely changes, so its helpful to have
            # it in the cache.
            if tid is not None and oid is not None:
                oid_to_tid[oid] = max(tid, 0)
        return oid_to_tid

    def get_cache_modification_time_for_stream(self):
        max_tids = self._find_max_tids()
        if max_tids:
            tid_str = p64(max_tids[0])
            ts = TimeStamp(tid_str)
            return ts.timeTime()

    def restore(self):
        options = self.options
        if options.cache_local_dir:
            begin = time.time()
            loaded_file_count = persistence.load_local_cache(options, self.prefix, self)
            end = time.time()
            logger.debug("Loaded %s cache files in %s", loaded_file_count, end - begin)

    def read_from_stream(self, stream):
        # As we try to store entries, parse tids out of the keys;
        # only store the newest entry for each item.

        # Storing them in delta_after0 gets us better hit ratios, and
        # we don't have to move things to our preferred (cp0) key as
        # often, but seems to lead to conflicts? TODO: Test this
        # rigorously.

        observed_tids = set()
        def key_transform(key, deltas=self.delta_after0):
            # We have a key that exactly describes the state data.
            # If we don't already have something newer, take it
            oid_int, tid_int = key
            observed_tids.add(tid_int)
            if deltas.get(oid_int, 0) < tid_int:
                deltas[oid_int] = tid_int
                return key
            # Otherwise, just drop it.

        examined, stored = self.local_client.read_from_stream(stream, key_transform)
        max_tid = max(observed_tids)

        if not self.checkpoints:
            big_tids = sorted(observed_tids, reverse=True)[:3]
            if len(big_tids) > 1:
                # Use the second biggest tid as our current tid,
                # because we just put everything in delta after 0.
                # This keeps everything nice and consistent.
                # Excellent.
                tid = big_tids[1]
                self.checkpoints = (tid, tid)
                self.current_tid = max_tid
            else:
                # A small cache file with only one transaction. Hmm.
                # Put things into delta_after1, since we don't have a good tid in the past.
                tid = big_tids[0]
                self.checkpoints = (tid, tid)
                self.current_tid = tid
                self.delta_after1 = self.delta_after0
                self.delta_after0 = self._delta_map_type()

        # If we aren't sure to store this, it's probably going to get
        # overwritten on the first poll.
        self.local_client.store_checkpoints(*self.checkpoints)

        return examined, stored

    def close(self):
        """
        Release resources held by this instance, and
        save any persistent data necessary.
        """
        self.save()
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
        for client in self.clients_local_first:
            client.flush_all()

        self.checkpoints = None
        self.delta_after0 = self._delta_map_type()
        self.delta_after1 = self._delta_map_type()
        self.current_tid = 0

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

        tid_int = self.delta_after0.get(oid_int)
        if tid_int:
            # This object changed after checkpoint0, so
            # there is only one place to look for its state: the exact key.
            for client in self.clients_local_first:
                cache_data = client[(oid_int, tid_int)]
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
            for client in self.clients_local_first:
                client[(oid_int, tid_int)] = (state, actual_tid_int)
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

        for client in self.clients_local_first:
            # Query the cache. Query multiple keys simultaneously to
            # minimize latency.
            response = client(oid_int, tid1, tid2)
            if response: # We have a hit!
                state, actual_tid = response
                local_client = self.local_client
                if client is not local_client:
                    # Copy to the local client
                    # at the preferred key
                    local_client[(oid_int, tid1)] = response
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
            for client in self.clients_local_first:
                client[(oid_int, tid1)] = (state, tid_int)
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
        for startpos, endpos, oid_int in items:
            state, length = self._read_temp_state(startpos, endpos)
            cachekey = (tid_int, oid_int)
            item_size = length + len(cachekey)
            if send_size and send_size + item_size >= self.send_limit:
                for client in self.clients_local_first:
                    client.set_multi(to_send)
                to_send.clear()
                send_size = 0
            to_send[cachekey] = (state, tid_int)
            send_size += item_size

        if to_send:
            for client in self.clients_local_first:
                client.set_multi(to_send)

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
        """Update checkpoint data after a database poll.

        cursor is connected to a load connection.

        changes lists all [(oid_int, tid_int)] changed after
        prev_tid_int, up to and including new_tid_int, excluding the
        changes last committed by the associated storage instance.
        changes can be None to indicate too many objects changed
        to list them all.

        prev_tid_int can be None, in which case the changes
        parameter will be ignored.  new_tid_int can not be None.
        """
        # pylint:disable=too-many-statements,too-many-branches,too-many-locals
        #print("After poll. Previous:", prev_tid_int, "New", new_tid_int,
        #      "My current:", self.current_tid, "CPs", self.checkpoints)
        new_checkpoints = None
        for client in self.clients_global_first:
            s = client.get_checkpoints()
            if s:
                c0, c1 = s
                if c0 >= c1:
                    new_checkpoints = (c0, c1)
                    break

        if not new_checkpoints:
            new_checkpoints = (new_tid_int, new_tid_int)
            if not self.checkpoints:
                # Initialize the checkpoints.
                cache_checkpoints = new_checkpoints
                log.debug("Initializing checkpoints: %s", cache_checkpoints)
            else:
                # Suggest reinstatement of the former checkpoints, but
                # use new checkpoints for this instance. Using new
                # checkpoints ensures that we don't build up
                # self.delta_after0 in case the cache is offline.
                cache_checkpoints = self.checkpoints
                log.debug("Reinstating checkpoints: %s", cache_checkpoints)

            for client in self.clients_global_first:
                client.store_checkpoints(*cache_checkpoints)

            self.checkpoints = new_checkpoints
            self.delta_after0 = self._delta_map_type()
            self.delta_after1 = self._delta_map_type()
            self.current_tid = new_tid_int
            return

        allow_shift = True
        if new_checkpoints[0] > new_tid_int:
            # checkpoint0 is in a future that this instance can't
            # yet see.  Ignore the checkpoint change for now.
            new_checkpoints = self.checkpoints
            if not new_checkpoints:
                new_checkpoints = (new_tid_int, new_tid_int)
            allow_shift = False

        # We want to keep the current checkpoints for speed, but we
        # have to replace them (to avoid consistency violations)
        # if certain conditions happen (like emptying the ZODB cache).
        if (new_checkpoints == self.checkpoints
                and changes is not None
                and prev_tid_int
                and prev_tid_int <= self.current_tid
                and new_tid_int >= self.current_tid):
            # All the conditions for keeping the checkpoints were met,
            # so just update self.delta_after0 and self.current_tid.
            m = self.delta_after0
            m_get = m.get
            for oid_int, tid_int in changes:
                my_tid_int = m_get(oid_int)
                if my_tid_int is None or tid_int > my_tid_int:
                    m[oid_int] = tid_int
                    # 0x1E = invalidate (hit, saving non-current)
                    self._trace(0x1C, oid_int, tid_int)
            self.current_tid = new_tid_int
        else:
            # We have to replace the checkpoints.
            cp0, cp1 = new_checkpoints
            log.info(
                "Using new checkpoints: %d %d. Current cp: %s. "
                "Too many changes? %s. prev_tid_int: %s. current_tid: %s. "
                "new_tid_int: %s",
                cp0, cp1, self.checkpoints,
                changes is None, prev_tid_int, self.current_tid,
                new_tid_int
            )
            # Use the checkpoints specified by the cache.
            # Rebuild delta_after0 and delta_after1.
            new_delta_after0 = self._delta_map_type()
            new_delta_after1 = self._delta_map_type()
            if cp1 < new_tid_int:
                # poller.list_changes provides an iterator of
                # (oid, tid) where tid > after_tid and tid <= last_tid.
                change_list = self.adapter.poller.list_changes(
                    cursor, cp1, new_tid_int)

                # Make a dictionary that contains, for each oid, the most
                # recent tid listed in changes. This works because sorting the
                # (oid, tid) pairs puts the newest tid at the back, and constructing
                # the dictionary from that sorted list preserves order, keeping the
                # last key that it saw.
                try:
                    change_dict = self._delta_map_type(sorted(change_list))
                except TypeError:
                    # pg8000 returns a list of lists, not a list of tuples. The
                    # BTree constructor is very particular about that. Normally one
                    # would use pg8000 on PyPy, where we don't use BTrees, so this shouldn't
                    # actually come up in practice.
                    change_dict = self._delta_map_type()
                    for oid_int, tid_int in sorted(change_list):
                        change_dict[oid_int] = tid_int


                # Put the changes in new_delta_after*.
                for oid_int, tid_int in change_dict.items():
                    # 0x1E = invalidate (hit, saving non-current)
                    self._trace(0x1C, oid_int, tid_int)
                    if tid_int > cp0:
                        new_delta_after0[oid_int] = tid_int
                    elif tid_int > cp1:
                        new_delta_after1[oid_int] = tid_int

            self.checkpoints = new_checkpoints
            self.delta_after0 = new_delta_after0
            self.delta_after1 = new_delta_after1
            self.current_tid = new_tid_int

        if allow_shift and len(self.delta_after0) >= self.delta_size_limit:
            # delta_after0 has reached its limit.  The way to
            # shrink it is to shift the checkpoints.  Suggest
            # shifted checkpoints for future polls.
            # If delta_after0 is far over the limit (caused by a large
            # transaction), suggest starting new checkpoints instead of
            # shifting.
            oversize = (len(self.delta_after0) >= self.delta_size_limit * 2)
            self._suggest_shifted_checkpoints(new_tid_int, oversize)


    def _suggest_shifted_checkpoints(self, tid_int, oversize):
        """Suggest that future polls use a new pair of checkpoints.

        This does nothing if another instance has already shifted
        the checkpoints.

        checkpoint0 shifts to checkpoint1 and the tid just committed
        becomes checkpoint0.
        """
        cp0, _cp1 = self.checkpoints
        if tid_int <= cp0:
            # This used to assert this couldn't happen, but with better caching,
            # it sometimes can.
            logger.debug(
                "Not shifting checkpoints; tid_int (%s) = cp0 (%s)",
                tid_int, cp0
            )
            return

        if oversize:
            # start new checkpoints
            change_to = (tid_int, tid_int)
        else:
            # shift the existing checkpoints
            change_to = (tid_int, cp0)
        expect = self.checkpoints

        for client in self.clients_global_first:
            old_value = client.get_checkpoints()
            if old_value:
                break
        if not old_value or old_value == expect:
            # Shift the checkpoints.
            # Although this is a race with other instances, the race
            # should not matter.
            log.debug("Shifting checkpoints to: %s. len(delta_after0) == %d.",
                      change_to, len(self.delta_after0))
            for client in self.clients_global_first:
                client.store_checkpoints(*change_to)
            # The poll code will later see the new checkpoints
            # and update self.checkpoints and self.delta_after(0|1).
        else:
            log.debug("Checkpoints already shifted to %s. "
                      "len(delta_after0) == %d.", old_value, len(self.delta_after0))
