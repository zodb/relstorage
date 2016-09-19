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
from __future__ import absolute_import, print_function, division

from relstorage.autotemp import AutoTemporaryFile
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64
from ZODB.POSException import ReadConflictError
from persistent.timestamp import TimeStamp

from .ring import Ring
if Ring.__name__ == '_DequeRing': # pragma: no cover
    import warnings
    warnings.warn("Install CFFI for best cache performance")

import importlib
import logging
import threading
import time
import zlib
import bz2
import struct

from relstorage._compat import string_types
from relstorage._compat import iteritems
from relstorage._compat import itervalues
from relstorage._compat import PY3
if PY3:
    # On Py3, use the built-in pickle, so that we can get
    # protocol 4 when available. It is *much* faster at writing out
    # individual large objects such as the cache dict (about 3-4x faster)
    from pickle import Unpickler
    from pickle import Pickler
else:
    # On Py2, zodbpickle gives us protocol 3, but we don't
    # use its special binary type
    from relstorage._compat import Unpickler
    from relstorage._compat import Pickler

log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    pass
_UsedAfterRelease = _UsedAfterRelease()

class _ZEOTracer(object):
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

    def trace_store_current(self, tid_int, items):
        # As a locking optimization, we accept this in bulk
        with self._lock:
            now = time.time()
            for startpos, endpos, oid_int in items:
                self._trace(0x52, oid_int, tid_int, dlen=endpos-startpos, now=now)

    def close(self):
        self._trace_file.close()
        del self._trace


class StorageCache(object):
    """RelStorage integration with memcached or similar.

    Holds a list of memcache clients in order from most local to
    most global.  The first is a LocalClient, which stores the cache
    in the Python process, but shares the cache between threads.
    """

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

    def __init__(self, adapter, options, prefix, local_client=None,
                 _tracer=None):
        self.adapter = adapter
        self.options = options
        self.prefix = prefix or ''
        if local_client is None:
            local_client = LocalClient(options, self.prefix)
        self.clients_local_first = [local_client]

        if options.cache_servers:
            module_name = options.cache_module_name
            module = importlib.import_module(module_name)
            servers = options.cache_servers
            if isinstance(servers, string_types):
                servers = servers.split()
            self.clients_local_first.append(module.Client(servers))

        # self.clients_local_first is in order from local to global caches,
        # while self.clients_global_first is in order from global to local.
        self.clients_global_first = list(reversed(self.clients_local_first))

        # checkpoints_key holds the current checkpoints.
        self.checkpoints_key = '%s:checkpoints' % self.prefix
        assert isinstance(self.checkpoints_key, str) # no unicode on Py2

        # delta_after0 contains {oid: tid} after checkpoint 0
        # and before or at self.current_tid.
        self.delta_after0 = {}

        # delta_after1 contains {oid: tid} after checkpoint 1 and
        # before or at checkpoint 0. The content of delta_after1 only
        # changes when checkpoints move.
        self.delta_after1 = {}

        # delta_size_limit places an approximate limit on the number of
        # entries in the delta_after maps.
        self.delta_size_limit = options.cache_delta_size_limit

        if _tracer is None:
            tracefile = _Loader.trace_file(options, self.prefix)
            if tracefile:
                _tracer = _ZEOTracer(tracefile)
                _tracer.trace(0x00)

        self._tracer = _tracer
        if hasattr(self._tracer, 'trace_store_current'):
            self._trace = self._tracer.trace
            self._trace_store_current = self._tracer.trace_store_current


    def new_instance(self):
        """Return a copy of this instance sharing the same local client"""
        local_client = None
        if self.options.share_local_cache:
            local_client = self.clients_local_first[0]

        cache = type(self)(self.adapter, self.options, self.prefix,
                           local_client,
                           _tracer=self._tracer or False)
        return cache

    def release(self):
        """
        Release resources held by this instance.

        This is usually memcache connections if they're in use.
        """
        clients = self.clients_local_first if self.clients_local_first is not _UsedAfterRelease else ()
        for client in clients:
            client.disconnect_all()

        # Release our clients. If we had a non-shared local cache,
        # this will also allow it to release any memory its holding.
        # Set them to non-iterables to make it obvious if we are used
        # after release.
        self.clients_local_first = _UsedAfterRelease
        self.clients_global_first = _UsedAfterRelease

    def close(self):
        """
        Release resources held by this instance, and
        save any persistent data necessary.
        """
        clients = self.clients_local_first if self.clients_local_first is not _UsedAfterRelease else ()
        for client in clients:
            try:
                save = getattr(client, 'save')
            except AttributeError:
                continue
            else:
                save()
        self.release()

        if self._tracer:
            self._tracer.close()
            del self._trace
            del self._trace_store_current
            del self._tracer

    def clear(self):
        """Remove all data from the cache.  Called by speed tests."""
        for client in self.clients_local_first:
            client.flush_all()
        self.checkpoints = None
        self.delta_after0 = {}
        self.delta_after1 = {}
        self.current_tid = 0

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
            # transaction.  We can't give the data to ZODB because that
            # would be a consistency violation.  However, the cause is hard
            # to track down, so issue a ReadConflictError and hope that
            # the application retries successfully.
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
        """Load the given object from cache if possible.

        Fall back to loading from the database.
        """
        if not self.checkpoints:
            # No poll has occurred yet.  For safety, don't use the cache.
            self._trace(0x20, oid_int)
            return self.adapter.mover.load_current(cursor, oid_int)

        prefix = self.prefix

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
            # there is only one place to look for its state.
            cachekey = '%s:state:%d:%d' % (prefix, tid_int, oid_int)
            for client in self.clients_local_first:
                cache_data = client.get(cachekey)
                if cache_data and len(cache_data) >= 8:
                    # Cache hit.
                    # Note that we trace all cache hits, not just the local cache hit.
                    # This makes the simulation less useful, but the stats might still have
                    # value to people trying different tuning options manually.
                    self._trace(0x22, oid_int, tid_int, dlen=len(cache_data) - 8)
                    assert cache_data[:8] == p64(tid_int)
                    return cache_data[8:], tid_int
            # Cache miss.
            self._trace(0x20, oid_int)
            state, actual_tid_int = self.adapter.mover.load_current(
                cursor, oid_int)
            self._check_tid_after_load(oid_int, actual_tid_int, tid_int)

            cache_data = p64(tid_int) + (state or b'')
            for client in self.clients_local_first:
                client.set(cachekey, cache_data)
            return state, tid_int

        # Make a list of cache keys to query. The list will have either
        # 1 or 2 keys.
        cp0, cp1 = self.checkpoints
        cachekeys = []
        cp0_key = '%s:state:%d:%d' % (prefix, cp0, oid_int)
        cachekeys.append(cp0_key)
        da1_key = None
        cp1_key = None
        tid_int = self.delta_after1.get(oid_int)
        if tid_int:
            da1_key = '%s:state:%d:%d' % (prefix, tid_int, oid_int)
            cachekeys.append(da1_key)
        elif cp1 != cp0:
            cp1_key = '%s:state:%d:%d' % (prefix, cp1, oid_int)
            cachekeys.append(cp1_key)

        for client in self.clients_local_first:
            # Query the cache. Query multiple keys simultaneously to
            # minimize latency.
            response = client.get_multi(cachekeys)
            if response:
                cache_data = response.get(cp0_key)
                if cache_data and len(cache_data) >= 8:
                    # Cache hit on the preferred cache key.
                    local_client = self.clients_local_first[0]
                    if client is not local_client:
                        # Copy to the local client.
                        local_client.set(cp0_key, cache_data)
                    self._trace(0x22, oid_int, u64(cache_data[:8]), dlen=len(cache_data) - 8)
                    return cache_data[8:], u64(cache_data[:8])

                if da1_key:
                    cache_data = response.get(da1_key)
                elif cp1_key:
                    cache_data = response.get(cp1_key)
                if cache_data and len(cache_data) >= 8:
                    # Cache hit, but copy the state to
                    # the currently preferred key.
                    self._trace(0x22, oid_int, u64(cache_data[:8]), dlen=len(cache_data) - 8)
                    for client_to_set in self.clients_local_first:
                        client_to_set.set(cp0_key, cache_data)
                    return cache_data[8:], u64(cache_data[:8])

        # Cache miss.
        self._trace(0x20, oid_int)
        state, tid_int = self.adapter.mover.load_current(cursor, oid_int)
        if tid_int:
            self._check_tid_after_load(oid_int, tid_int)
            cache_data = p64(tid_int) + (state or b'')
            # Record this as a store into the cache, ZEO does.
            self._trace(0x52, oid_int, tid_int, dlen=len(state) if state else 0)
            for client in self.clients_local_first:
                client.set(cp0_key, cache_data)
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
        prefix = self.prefix

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
            cachekey = '%s:state:%d:%d' % (prefix, tid_int, oid_int)
            item_size = length + len(cachekey)
            if send_size and send_size + item_size >= self.send_limit:
                for client in self.clients_local_first:
                    client.set_multi(to_send)
                to_send.clear()
                send_size = 0
            to_send[cachekey] = tid + state
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
        new_checkpoints = None
        for client in self.clients_global_first:
            s = client.get(self.checkpoints_key)
            if s:
                try:
                    c0, c1 = s.split()
                    c0 = int(c0)
                    c1 = int(c1)
                except ValueError:
                    # Invalid checkpoint cache value; ignore it.
                    pass
                else:
                    if c0 >= c1:
                        new_checkpoints = (c0, c1)
                        break

        if not new_checkpoints:
            new_checkpoints = (new_tid_int, new_tid_int)

            if not self.checkpoints:
                # Initialize the checkpoints.
                cache_data = '%d %d' % new_checkpoints
                log.debug("Initializing checkpoints: %s", cache_data)
            else:
                # Suggest reinstatement of the former checkpoints, but
                # use new checkpoints for this instance. Using new
                # checkpoints ensures that we don't build up
                # self.delta_after0 in case the cache is offline.
                cache_data = '%d %d' % self.checkpoints
                log.debug("Reinstating checkpoints: %s", cache_data)

            cache_data = cache_data.encode("ascii")
            for client in self.clients_global_first:
                client.set(self.checkpoints_key, cache_data)

            self.checkpoints = new_checkpoints
            self.delta_after0 = {}
            self.delta_after1 = {}
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
                and new_tid_int >= self.current_tid
        ):
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
            log.debug("Using new checkpoints: %d %d", cp0, cp1)
            # Use the checkpoints specified by the cache.
            # Rebuild delta_after0 and delta_after1.
            new_delta_after0 = {}
            new_delta_after1 = {}
            if cp1 < new_tid_int:
                # poller.list_changes provides an iterator of
                # (oid, tid) where tid > after_tid and tid <= last_tid.
                change_list = self.adapter.poller.list_changes(
                    cursor, cp1, new_tid_int)

                # Make a dictionary that contains, for each oid, the most
                # recent tid listed in changes.
                change_dict = {}
                if not isinstance(change_list, list):
                    change_list = list(change_list)
                change_list.sort()
                for oid_int, tid_int in change_list:
                    change_dict[oid_int] = tid_int

                # Put the changes in new_delta_after*.
                for oid_int, tid_int in iteritems(change_dict):
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
        assert tid_int > cp0
        expect = '%d %d' % self.checkpoints
        if oversize:
            # start new checkpoints
            change_to = '%d %d' % (tid_int, tid_int)
        else:
            # shift the existing checkpoints
            change_to = '%d %d' % (tid_int, cp0)
        expect = expect.encode('ascii')
        change_to = change_to.encode('ascii')

        for client in self.clients_global_first:
            old_value = client.get(self.checkpoints_key)
            if old_value:
                break
        if not old_value or old_value == expect:
            # Shift the checkpoints.
            # Although this is a race with other instances, the race
            # should not matter.
            log.debug("Shifting checkpoints to: %s. len(delta_after0) == %d.",
                      change_to, len(self.delta_after0))
            for client in self.clients_global_first:
                client.set(self.checkpoints_key, change_to)
            # The poll code will later see the new checkpoints
            # and update self.checkpoints and self.delta_after(0|1).
        else:
            log.debug("Checkpoints already shifted to %s. "
                      "len(delta_after0) == %d.", old_value, len(self.delta_after0))

_OSA = object.__setattr__

from .lru import SizedLRU as _SizedLRU

class _EdenLRU(_SizedLRU):

    def __init__(self, limit, probation_lru, protected_lru, entry_dict):
        _SizedLRU.__init__(self, limit)
        self.probation_lru = probation_lru
        self.protected_lru = protected_lru
        self.entry_dict = entry_dict

    def add_MRU(self, key, value):
        new_entry = _SizedLRU.add_MRU(self, key, value)

        if not self.over_size:
            return new_entry

        dct = self.entry_dict
        probation_lru = self.probation_lru
        protected_lru = self.protected_lru

        if not probation_lru and not protected_lru.over_size:
            # This is a modification of the algorithm. When we start out
            # go ahead and populate the protected_lru directly
            # from eden; only when its full do we start doing the probationary
            # dance. This helps mitigate any issues with choosing segment sizes;
            # we're going to occupy all the memory anyway, why not, it's reserved for us,
            # so go ahead and fill it.

            while self.over_size:
                eden_oldest = self.get_LRU()
                if eden_oldest.key is key:
                    break

                if eden_oldest.len + protected_lru.size > protected_lru.limit:
                    # This would oversize protected. Move it to probation instead,
                    # which is currently empty, so there's no need to choose a victim.
                    # This may temporarily oversize us.
                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    break
                else:
                    protected_lru.take_ownership_of_entry_MRU(eden_oldest)

            return new_entry

        while self.over_size:
            eden_oldest = self.get_LRU()
            if eden_oldest.key is key:
                break

            #assert eden_oldest.__parent__ is None
            #assert eden_oldest._Persistent__ring is None

            if probation_lru.size + eden_oldest.len < probation_lru.limit:
                # Cool, we can keep it.
                probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                #assert eden_oldest.__parent__ is probation_lru
            else:
                # Snap, somebody has to go.
                try:
                    oldest_main_ring = probation_lru.get_LRU()
                except (StopIteration, KeyError):
                    # probation ring is empty, nothing to eject. This must be a large
                    # item. Well, just accept it then to match what we used to do.
                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    continue

                if oldest_main_ring.frequency > eden_oldest.frequency:
                    # Discard this entry, it loses
                    # print("Completely evicting item", oldest.key,
                    #       "because main ring item", oldest_main_ring.key,
                    #       "has better frequency", oldest_main_ring.frequency, oldest.frequency)
                    self._ring.delete(eden_oldest)
                    del dct[eden_oldest.key]
                else:
                    # eden item is more popular, keep it
                    probation_lru.remove(oldest_main_ring)
                    del dct[oldest_main_ring.key]

                    probation_lru.take_ownership_of_entry_MRU(eden_oldest)
                    #assert eden_oldest.__parent__ is probation_lru
            self.over_size = self.size > self.limit
        return new_entry

class _ProtectedLRU(_SizedLRU):
    pass
    #def add_MRU(self, key, value):
    #    raise NotImplementedError("We only accept ownership")

from .ring import _FFI_RING as ffi

_lru_probation_on_hit = ffi.lru_probation_on_hit

class _ProbationLRU(_SizedLRU):

    promote_count = 0
    demote_count = 0
    remove_count = 0

    def __init__(self, limit, protected_lru, entry_dict):
        _SizedLRU.__init__(self, limit)
        self.protected_lru = protected_lru
        self.entry_dict = entry_dict

    def on_hit(self, entry):
        # Move the entry to the Protected LRU on its very first hit;
        # It will become the MRU there.
        protected_lru = self.protected_lru
        protected_lru.over_size = _lru_probation_on_hit(self._ring.ring_home,
                                                        protected_lru._ring.ring_home,
                                                        entry.cffi_ring_node)


class LocalClientBucket(object):
    """
    A map that keeps a record of its approx. size.

    keys must be `str`` and values must be byte strings.

    This class is not threadsafe, accesses to __setitem__ and get_and_bubble_all
    must be protected by a lock.
    """

    # What multiplier of the number of items in the cache do we apply
    # to determine when to age the frequencies?
    _age_factor = 10

    # When did we last age?
    _aged_at = 0

    # Percentage of our byte limit that should be dedicated
    # to the main "protected" generation
    _gen_protected_pct = 0.8
    # Percentage of our byte limit that should be dedicated
    # to the initial "eden" generation
    _gen_eden_pct = 0.1
    # Percentage of our byte limit that should be dedicated
    # to the "probationary"generation
    _gen_probation_pct = 0.1
    # By default these numbers add up to 1.0, but it would be possible to
    # overcommit by making them sum to more than 1.0. (For very small
    # limits, the rounding will also make them overcommit).

    def __init__(self, limit):
        # We experimented with using OOBTree and LOBTree
        # for the type of self._dict. The OOBTree has a similar
        # but slightly slower performance profile (as would be expected
        # given the big-O complexity) as a dict, but very large ones can't
        # be pickled in a single shot! The LOBTree works faster and uses less
        # memory than the OOBTree or the dict *if* all the keys are integers;
        # which they currently are not. Plus the LOBTrees are slower on PyPy than its
        # own dict specializations. We were hoping to be able to write faster pickles with
        # large BTrees, but since that's not the case, we abandoned the idea.

        # This holds all the ring entries, no matter which ring they are in.
        self._dict = {}


        self._protected = _ProtectedLRU(int(limit * self._gen_protected_pct))
        self._probation = _ProbationLRU(int(limit * self._gen_probation_pct),
                                        self._protected,
                                        self._dict)
        self._eden = _EdenLRU(int(limit * self._gen_eden_pct),
                              self._probation,
                              self._protected,
                              self._dict)
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self.limit = limit

    @property
    def size(self):
        return self._eden.size + self._protected.size + self._probation.size

    def reset_stats(self):
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self._aged_at = 0

    def stats(self):
        total = self._hits + self._misses
        return {
            'hits': self._hits,
            'misses': self._misses,
            'sets': self._sets,
            'ratio': self._hits/total if total else 0,
            'size': len(self._dict),
            'bytes': self.size,
            'eden_stats': self._eden.stats(),
            'prot_stats': self._protected.stats(),
            'prob_stats': self._probation.stats(),
        }

    def __len__(self):
        return len(self._dict)

    def _age(self):
        # Age only when we're full and would thus need to evict; this
        # makes initial population faster.
        if self.size < self.limit:
            return

        # Age the whole thing periodically based on the number of
        # operations we've done that would have altered popularity.
        # Dynamically calculate how often we need to age. By default, this is
        # based on what Caffeine's PerfectFrequency does: 10 * max
        # cache entries
        dct = self._dict
        age_period = self._age_factor * len(dct)
        operations = self._hits + self._sets
        if operations - self._aged_at > age_period:
            now = time.time()
            log.info("Beginning frequency aging for %d cache entries",
                     len(dct))
            self._aged_at = operations
            for entry in itervalues(dct):
                entry.frequency //= 2
            done = time.time()
            log.info("Aged %d cache entries in %s", done - now)
        return self._aged_at

    def __setitem__(self, key, value):
        """
        Set an item.

        If the memory limit would be exceeded, remove old items until
        that is no longer the case.

        If we need to age popularity counts, do so.
        """
        # These types are gated by LocalClient, we don't need to double
        # check.
        #assert isinstance(key, str)
        #assert isinstance(value, bytes)

        dct = self._dict

        if key in dct:
            entry = dct[key]
            entry.__parent__.update_MRU(entry, value)
        else:
            lru = self._eden
            entry = lru.add_MRU(key, value)
            #assert entry.__parent__ is self._eden
            dct[key] = entry

        self._sets += 1

        # Do we need to move this up above the eviction choices?
        self._age()

        return True

    def __contains__(self, key):
        return key in self._dict

    def __delitem__(self, key):
        entry = self._dict[key]
        del self._dict[key]
        entry.__parent__.remove(entry)

    def get_and_bubble_all(self, keys):
        dct = self._dict
        res = {}
        for key in keys:
            entry = dct.get(key)
            if entry is not None:
                self._hits += 1
                entry.__parent__.on_hit(entry)
                res[key] = entry.value
            else:
                self._misses += 1
        return res

    def get(self, key):
        # Testing only. Does not bubble or increment.
        entry = self._dict.get(key)
        if entry is not None:
            return entry.value

    def __getitem__(self, key):
        # Testing only. Doesn't bubble.
        entry = self._dict[key]
        entry.frequency += 1
        return entry.value

    # Benchmark for the general approach:

    # Pickle is about 3x faster than marshal if we write single large
    # objects, surprisingly. If we stick to writing smaller objects, the
    # difference narrows to almost negligible.

    # Writing 525MB of data, 655K keys (no compression):
    # - code as-of commit e58126a (the previous major optimizations for version 1 format)
    #    version 1 format, solid dict under 3.4: write: 3.8s/read 7.09s
    #    2.68s to update ring, 2.6s to read pickle
    #
    # -in a btree under 3.4: write: 4.8s/read 8.2s
    #    written as single list of the items
    #    3.1s to load the pickle, 2.6s to update the ring
    #
    # -in a dict under 3.4: write: 3.7s/read 7.6s
    #    written as the dict and updated into the dict
    #    2.7s loading the pickle, 2.9s to update the dict
    # - in a dict under 3.4: write: 3.0s/read 12.8s
    #    written by iterating the ring and writing one key/value pair
    #     at a time, so this is the only solution that
    #     automatically preserves the LRU property (and would be amenable to
    #     capping read based on time, and written file size); this format also lets us avoid the
    #     full write buffer for HIGHEST_PROTOCOL < 4
    #    2.5s spent in pickle.load, 8.9s spent in __setitem__,5.7s in ring.add
    # - in a dict: write 3.2/read 9.1s
    #    same as above, but custom code to set the items
    #   1.9s in pickle.load, 4.3s in ring.add
    # - same as above, but in a btree: write 2.76s/read 10.6
    #    1.8s in pickle.load, 3.8s in ring.add,
    #
    # For the final version with optimizations, the write time is 2.3s/read is 6.4s

    _FILE_VERSION = 4

    def load_from_file(self, cache_file):
        now = time.time()
        # Unlike write_to_file, using the raw stream
        # is fine for both Py 2 and 3.
        unpick = Unpickler(cache_file)

        # Local optimizations
        load = unpick.load

        version = load()
        if version != self._FILE_VERSION: # pragma: no cover
            raise ValueError("Incorrect version of cache_file")

        entries_oldest_first = list()
        entries_oldest_first_append = entries_oldest_first.append
        try:
            while 1:
                entries_oldest_first_append(load())
        except EOFError:
            pass
        count = len(entries_oldest_first)

        def _insert_entries(entries):
            stored = 0

            # local optimizations
            data = self._dict
            main = self._protected
            ring_add = main.add_MRU
            limit = main.limit

            # Need to reoptimize this.
#            size = self.size # update locally, copy back at end

            for k, v in entries:
                if k in data:
                    continue

                if main.size >= limit:
                    break

                data[k] = ring_add(k, v)

                stored += 1

            return stored

        stored = 0
        if not self._dict:
            # Empty, so quickly take everything they give us,
            # oldest first so that the result is actually LRU
            stored = _insert_entries(entries_oldest_first)
        else:
            # Loading more data into an existing bucket.
            # Load only the *new* keys, trying to get the newest ones
            # because LRU is going to get messed up anyway.

            entries_newest_first = reversed(entries_oldest_first)
            stored = _insert_entries(entries_newest_first)

        then = time.time()
        log.info("Examined %d and stored %d items from %s in %s",
                 count, stored, cache_file, then - now)
        return count, stored

    def write_to_file(self, cache_file):
        now = time.time()
        # pickling the items is about 3x faster than marshal


        # Under Python 2, (or generally, under any pickle protocol
        # less than 4, when framing was introduced) whether we are
        # writing to an io.BufferedWriter, a <file> opened by name or
        # fd, with default buffer or a large (16K) buffer, putting the
        # Pickler directly on top of that stream is SLOW for large
        # singe objects. Writing a 512MB dict takes ~40-50seconds. If
        # instead we use a BytesIO to buffer in memory, that time goes
        # down to about 7s. However, since we switched to writing many
        # smaller objects, that need goes away.

        pickler = Pickler(cache_file, -1) # Highest protocol
        dump = pickler.dump

        dump(self._FILE_VERSION) # Version marker

        # Dump all the entries in increasing order of popularity (
        # so that when we read them back in the least popular items end up LRU).
        # Anything with a popularity of 0 probably hasn't been accessed in a long
        # time, so don't dump it.

        # Age them now, writing only the most popular. (But don't age in place just
        # in case we're still being used.)

        entries = list(sorted((e for e in itervalues(self._dict) if e.frequency // 2),
                              key=lambda e: e.frequency))

        if len(entries) < len(self._dict):
            log.info("Ignoring %d items for writing due to inactivity",
                     len(self._dict) - len(entries))

        # Don't bother writing more than we'll be able to store.
        count_written = 0
        bytes_written = 0
        byte_limit = self._protected.limit
        for entry in entries:
            bytes_written += entry.len
            count_written += 1
            if bytes_written > byte_limit:
                break

            dump((entry.key, entry.value))

        then = time.time()
        stats = self.stats()
        log.info("Wrote %d items to %s in %s. Total hits %s; misses %s; ratio %s",
                 count_written, cache_file, then - now,
                 stats['hits'], stats['misses'], stats['ratio'])


class LocalClient(object):
    """A memcache-like object that stores in Python dictionaries."""

    # Use the same markers as zc.zlibstorage (well, one marker)
    # to automatically avoid double-compression
    _compression_markers = {
        'zlib': (b'.z', zlib.compress),
        'bz2': (b'.b', bz2.compress),
        'none': (None, None)
    }
    _decompression_functions = {
        b'.z': zlib.decompress,
        b'.b': bz2.decompress
    }

    def __init__(self, options, prefix=None):
        self._lock = threading.Lock()
        self.options = options
        self.prefix = prefix or ''
        self._bucket_limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max
        self.__bucket = None
        self.flush_all()

        compression_module = options.cache_local_compression
        try:
            compression_markers = self._compression_markers[compression_module]
        except KeyError:
            raise ValueError("Unknown compression module")
        else:
            self.__compression_marker = compression_markers[0]
            self.__compress = compression_markers[1]
            if self.__compress is None:
                self._compress = None

    def _decompress(self, data):
        pfx = data[:2]
        if pfx not in self._decompression_functions:
            return data
        return self._decompression_functions[pfx](data[2:])

    def _compress(self, data): # pylint:disable=method-hidden
        # We override this if we're disabling compression
        # altogether.
        # Use the same basic rule as zc.zlibstorage, but bump the object size up from 20;
        # many smaller object (under 100 bytes) like you get with small btrees,
        # tend not to compress well, so don't bother.
        if data and (len(data) > 100) and data[:2] not in self._decompression_functions:
            compressed = self.__compression_marker + self.__compress(data)
            if len(compressed) < len(data):
                return compressed
        return data

    def save(self):
        options = self.options
        if options.cache_local_dir and self._bucket0.size:
            _Loader.save_local_cache(options, self.prefix, self._bucket0)

    @property
    def _bucket0(self):
        # For testing only.
        return self.__bucket

    def flush_all(self):
        with self._lock:
            self.__bucket = LocalClientBucket(self._bucket_limit)
            options = self.options
            if options.cache_local_dir:
                _Loader.load_local_cache(options, self.prefix, self._bucket0)

    def reset_stats(self):
        self.__bucket.reset_stats()

    def stats(self):
        return self.__bucket.stats()

    def get(self, key):
        return self.get_multi([key]).get(key)

    def get_multi(self, keys):
        res = {}
        decompress = self._decompress
        get = self.__bucket.get_and_bubble_all

        with self._lock:
            res = get(keys)

        # Finally, while not holding the lock, decompress if needed
        res = {k: decompress(v)
               for k, v in iteritems(res)}

        return res

    def set(self, key, value):
        self.set_multi({key: value})

    def set_multi(self, d, allow_replace=True):
        if not self._bucket_limit:
            # don't bother
            return

        compress = self._compress
        items = [] # [(key, value)]
        for key, value in iteritems(d):
            # This used to allow non-byte values, but that's confusing
            # on Py3 and wasn't used outside of tests, so we enforce it.
            assert isinstance(key, str), (type(key), key)
            assert isinstance(value, bytes)

            cvalue = compress(value) if compress else value

            if len(cvalue) >= self._value_limit:
                # This value is too big, so don't cache it.
                continue
            items.append((key, cvalue))

        bucket0 = self.__bucket
        has_key = bucket0.__contains__
        set_key = bucket0.__setitem__

        with self._lock:
            for key, cvalue in items:
                if not allow_replace and has_key(key):
                    continue
                    # Bucket0 could be shifted out at any
                    # point during this operation, and that's ok
                    # because it would mean the key still goes away
                set_key(key, cvalue)

    def add(self, key, value):
        self.set_multi({key: value}, allow_replace=False)

    def disconnect_all(self):
        # Compatibility with memcache.
        pass


import glob
import gzip
import io
import os
import os.path
import tempfile

class _Loader(object):

    @classmethod
    def _normalize_path(cls, options):
        path = os.path.expanduser(os.path.expandvars(options.cache_local_dir))
        path = os.path.abspath(path)
        return path

    @classmethod
    def _open(cls, _options, filename, *args):
        return io.open(filename, *args, buffering=16384)

    @classmethod
    def _gzip_ext(cls, options):
        if options.cache_local_dir_compress:
            return ".rscache.gz"
        return ".rscache"

    @classmethod
    def _gzip_file(cls, options, filename, fileobj, **kwargs):
        if not options.cache_local_dir_compress:
            return fileobj
        # These files would *appear* to be extremely compressable. One
        # zodbshootout example with random data compressed a 3.4MB
        # file to 393K and a 19M file went to 3M.

        # As far as speed goes: for writing a 512MB file containing
        # 650,987 values with only 1950 distinct values (so
        # potentially highly compressible, although none of the
        # identical items were next to each other in the dict on
        # purpose) of random data under Python 2.7:

        # no GzipFile is                   8s
        # GzipFile with compresslevel=0 is 11s
        # GzipFile with compresslevel=5 is 28s (NOTE: Time was the same for Python 3)

        # But the on disk size at compresslevel=5 was 526,510,662
        # compared to the in-memory size of 524,287,388 (remembering
        # there is more overhead on disk). So its hardly worth it.

        # Under Python 2.7, buffering is *critical* for performance.
        # Python 3 doesn't have this problem as much for reads, but it's nice to still do.

        # For writing, the fileobj itself must be buffered; this is
        # taken care of by passing objects obtained from io.open; without
        # that low-level BufferdWriter, what is 10s to write 512MB in 600K objects
        # becomes 40s.

        gz_cache_file = gzip.GzipFile(filename, fileobj=fileobj, **kwargs)
        if kwargs.get('mode') == 'rb':
            # For reading, 2.7 without buffering 100,000 objects from a
            # 2MB file takes 4 seconds; with it, it takes around 1.3.
            return io.BufferedReader(gz_cache_file)

        return gz_cache_file

    @classmethod
    def _list_cache_files(cls, options, prefix):
        path = cls._normalize_path(options)
        possible_caches = glob.glob(os.path.join(path, 'relstorage-cache-'
                                                 + prefix
                                                 + '.*'
                                                 + cls._gzip_ext(options)))
        return possible_caches

    @classmethod
    def trace_file(cls, options, prefix):
        # Return an open file for tracing to, if that is set up.
        # Otherwise, return nothing.

        # We choose a trace file based on ZEO_CACHE_TRACE. If it is
        # set to 'single', then we use a single file (not suitable for multiple
        # process, but records client opens/closes). If it is set to any other value,
        # we include a pid. If it is not set, we do nothing.
        trace = os.environ.get("ZEO_CACHE_TRACE")
        if not trace or not options.cache_local_dir:
            return None

        if trace == 'single':
            pid = 0
        else: # pragma: no cover
            pid = os.getpid()

        name = 'relstorage-trace-' + prefix + '.' + str(pid) + '.trace'

        parent_dir = cls._normalize_path(options)
        try:
            os.makedirs(parent_dir)
        except os.error:
            pass
        fname = os.path.join(parent_dir, name)
        try:
            tf = open(fname, 'ab')
        except IOError as e: # pragma: no cover
            log.warning("Cannot write tracefile %r (%s)", fname, e)
            tf = None
        else:
            log.info("opened tracefile %r", fname)
        return tf

    @classmethod
    def _stat_cache_files(cls, options, prefix):
        fds = []
        stats = []
        try:
            for possible_cache_path in cls._list_cache_files(options, prefix):
                cache_file = cls._open(options, possible_cache_path, 'rb')
                fds.append(cache_file)
                buf_cache_file = cls._gzip_file(options, possible_cache_path, fileobj=cache_file, mode='rb')
                stats.append((os.fstat(cache_file.fileno()), buf_cache_file, possible_cache_path, cache_file))
        except: # pragma: no cover
            for _f in fds:
                _f.close()
            raise

        # Newest and biggest first
        stats.sort(key=lambda s: (s[0].st_mtime, s[0].st_size), reverse=True)

        return stats

    @classmethod
    def count_cache_files(cls, options, prefix):
        return len(cls._list_cache_files(options, prefix))

    @classmethod
    def load_local_cache(cls, options, prefix, local_client_bucket):
        # Given an options that points to a local cache dir,
        # choose a file from that directory and load it.
        stats = cls._stat_cache_files(options, prefix)
        if not stats:
            log.debug("No cache files found")
        max_load = options.cache_local_dir_read_count or len(stats)
        loaded_count = 0
        try:
            for _, fd, cache_path, _ in stats:
                if loaded_count >= max_load:
                    break

                try:
                    _, stored = local_client_bucket.load_from_file(fd)
                    loaded_count += 1
                    if not stored or local_client_bucket.size >= local_client_bucket.limit:
                        break # pragma: no cover
                except: # pylint:disable=bare-except
                    log.exception("Invalid cache file %s", cache_path)
                    fd.close()
                    os.remove(cache_path)
        finally:
            for e in stats:
                e[1].close()
                e[3].close()
        return loaded_count

    @classmethod
    def save_local_cache(cls, options, prefix, local_client_bucket, _pid=None):
        # Dump the file.
        tempdir = cls._normalize_path(options)
        try:
            # make it if needed. try to avoid a time-of-use/check
            # race (not that it matters here)
            os.makedirs(tempdir)
        except os.error:
            pass

        fd, path = tempfile.mkstemp('._rscache_', dir=tempdir)
        with cls._open(options, fd, 'wb') as f:
            with cls._gzip_file(options, filename=path, fileobj=f, mode='wb', compresslevel=5) as fz:
                try:
                    local_client_bucket.write_to_file(fz)
                except:
                    log.exception("Failed to save cache file %s", path)
                    fz.close()
                    f.close()
                    os.remove(path)
                    return

        # Ok, now pick a place to put it, dropping the oldest file,
        # if necessary.

        files = cls._list_cache_files(options, prefix)
        if len(files) < options.cache_local_dir_count:
            pid = _pid or os.getpid() # allowing passing for testing
            # Odds of same pid existing already are too low to worry about
            new_name = 'relstorage-cache-' + prefix + '.' + str(pid) + cls._gzip_ext(options)
            new_path = os.path.join(tempdir, new_name)
            os.rename(path, new_path)
        else:
            stats = cls._stat_cache_files(options, prefix)
            # oldest and smallest first
            stats.reverse()
            try:
                stats[0][1].close()
                new_path = stats[0][2]
                os.rename(path, new_path)
            finally:
                for e in stats:
                    e[1].close()
                    e[3].close()
        return new_path
