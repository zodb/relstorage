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
from __future__ import absolute_import, print_function

from relstorage.autotemp import AutoTemporaryFile
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.POSException import ReadConflictError
from persistent.TimeStamp import TimeStamp
from persistent.ring import Ring

import importlib
import logging
import threading

from relstorage._compat import string_types
from relstorage._compat import iteritems

log = logging.getLogger(__name__)

class _UsedAfterRelease(object):
    pass
_UsedAfterRelease = _UsedAfterRelease()

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

    def __init__(self, adapter, options, prefix, local_client=None):
        self.adapter = adapter
        self.options = options
        self.prefix = prefix or ''
        if local_client is None:
            local_client = LocalClient(options)
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

    def new_instance(self):
        """Return a copy of this instance sharing the same local client"""
        local_client = None
        if self.options.share_local_cache:
            local_client = self.clients_local_first[0]

        return type(self)(self.adapter, self.options, self.prefix,
                          local_client)

    def release(self):
        """
        Release resources held by this instance.

        This is usually memcache connections if they're in use.
        """
        for client in self.clients_local_first:
            client.disconnect_all()

        # Release our clients. If we had a non-shared local cache,
        # this will also allow it to release any memory its holding.
        # Set them to non-iterables to make it obvious if we are used
        # after release.
        self.clients_local_first = _UsedAfterRelease
        self.clients_global_first = _UsedAfterRelease

    def clear(self):
        """Remove all data from the cache.  Called by speed tests."""
        for client in self.clients_local_first:
            client.flush_all()
        self.checkpoints = None
        self.delta_after0 = {}
        self.delta_after1 = {}
        self.current_tid = 0

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
            import os

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
                    assert cache_data[:8] == p64(tid_int)
                    return cache_data[8:], tid_int
            # Cache miss.
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
                    return cache_data[8:], u64(cache_data[:8])

                if da1_key:
                    cache_data = response.get(da1_key)
                elif cp1_key:
                    cache_data = response.get(cp1_key)
                if cache_data and len(cache_data) >= 8:
                    # Cache hit, but copy the state to
                    # the currently preferred key.
                    for client_to_set in self.clients_local_first:
                        client_to_set.set(cp0_key, cache_data)
                    return cache_data[8:], u64(cache_data[:8])

        # Cache miss.
        state, tid_int = self.adapter.mover.load_current(cursor, oid_int)
        if tid_int:
            self._check_tid_after_load(oid_int, tid_int)
            cache_data = p64(tid_int) + (state or b'')
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



class _RingEntry(object):
    __slots__ = ('_p_oid', '_Persistent__ring', 'value')

    def __init__(self, key, value):
        self._p_oid = key
        self.value = value


class LocalClientBucket(object):
    """
    A map that keeps a record of its approx. size.

    keys must be `str`` and values must be byte strings.

    This class is not threadsafe, accesses to __setitem__ and get_and_bubble_all
    must be protected by a lock.
    """
    __slots__ = ('size', 'limit', '_dict', '_ring')

    def __init__(self, limit):
        self._dict = {}
        self._ring = Ring()
        self.size = 0
        self.limit = limit

    def __setitem__(self, key, value):
        """
        Set an item.

        If the memory limit would be exceeded, remove old items until
        that is no longer the case.
        """
        # These types are gated by LocalClient, we don't need to double
        # check.
        #assert isinstance(key, str)
        #assert isinstance(value, bytes)

        sizedelta = len(value)

        if key in self._dict:
            entry = self._dict[key]
            oldvalue = entry.value
            sizedelta -= len(oldvalue)
            entry.value = value
            self._ring.move_to_head(entry)
        else:
            sizedelta += len(key)
            entry = _RingEntry(key, value)
            self._ring.add(entry)
            self._dict[key] = entry

        while self._dict and self.size + sizedelta > self.limit:
            oldest = next(iter(self._ring))
            if oldest._p_oid is key:
                break
            self.__delitem__(oldest._p_oid)

        self.size += sizedelta
        return True

    def __contains__(self, key):
        return key in self._dict

    def __delitem__(self, key):
        entry = self._dict[key]
        oldvalue = entry.value
        del self._dict[key]
        self._ring.delete(entry)
        sizedelta = len(key)
        sizedelta += len(oldvalue)
        self.size -= sizedelta

    def get_and_bubble_all(self, keys):
        dct = self._dict
        rng = self._ring
        res = {}
        for key in keys:
            entry = dct.get(key)
            if entry is not None:
                rng.move_to_head(entry)
                res[key] = entry.value
        return res

    def get(self, key):
        # Testing only. Does not bubble.
        entry = self._dict.get(key)
        if entry is not None:
            return entry.value


class LocalClient(object):
    """A memcache-like object that stores in Python dictionaries."""

    _compress = None
    _decompress = None

    def __init__(self, options):
        self._lock = threading.Lock()
        self._bucket_limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max
        self.__bucket = None
        self.flush_all()

        compression_module = options.cache_local_compression
        if compression_module and compression_module != 'none':
            module = importlib.import_module(compression_module)
            self._compress = module.compress
            self._decompress = module.decompress

    @property
    def _bucket0(self):
        # For testing only.
        return self.__bucket

    def flush_all(self):
        with self._lock:
            self.__bucket = LocalClientBucket(self._bucket_limit)

    def get(self, key):
        return self.get_multi([key]).get(key)

    def get_multi(self, keys):
        res = {}
        decompress = self._decompress
        get = self.__bucket.get_and_bubble_all

        with self._lock:
            res = get(keys)

        # Finally, while not holding the lock, decompress if needed
        if decompress:
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
