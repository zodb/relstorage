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

from relstorage.autotemp import AutoTemporaryFile
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.POSException import ReadConflictError
from ZODB.TimeStamp import TimeStamp
import logging
import random
import threading

log = logging.getLogger(__name__)


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

    # commit_count contains the last polled value of the
    # :commits cache key.  The most global client currently
    # responding stores the value.
    commit_count = object()

    def __init__(self, adapter, options, prefix, local_client=None):
        self.adapter = adapter
        self.options = options
        self.prefix = prefix or ''
        if local_client is None:
            local_client = LocalClient(options)
        self.clients_local_first = [local_client]

        if options.cache_servers:
            module_name = options.cache_module_name
            module = __import__(module_name, {}, {}, ['Client'])
            servers = options.cache_servers
            if isinstance(servers, basestring):
                servers = servers.split()
            self.clients_local_first.append(module.Client(servers))

        # self.clients_local_first is in order from local to global caches,
        # while self.clients_global_first is in order from global to local.
        self.clients_global_first = list(self.clients_local_first)
        self.clients_global_first.reverse()

        # commit_count_key contains a number that is incremented
        # for every commit.  See tpc_finish().
        self.commit_count_key = '%s:commits' % self.prefix

        # checkpoints_key holds the current checkpoints.
        self.checkpoints_key = '%s:checkpoints' % self.prefix

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
        if self.options.share_local_cache:
            local_client = self.clients_local_first[0]
            return StorageCache(self.adapter, self.options, self.prefix,
                local_client)
        else:
            return StorageCache(self.adapter, self.options, self.prefix)

    def clear(self):
        """Remove all data from the cache.  Called by speed tests."""
        for client in self.clients_local_first:
            client.flush_all()
        self.checkpoints = None
        self.delta_after0 = {}
        self.delta_after1 = {}
        self.current_tid = 0
        self.commit_count = object()

    def _check_tid_after_load(self, oid_int, actual_tid_int,
            expect_tid_int=None):
        """Verify the tid of an object loaded from the database is sane."""

        if actual_tid_int > self.current_tid:
            # Strangely, the database just gave us data from a future
            # transaction.  We can't give the data to ZODB because that
            # would be a consistency violation.  However, the cause is hard
            # to track down, so issue a ReadConflictError and hope that
            # the application retries successfully.
            raise ReadConflictError("Got data for OID 0x%(oid_int)x from "
                "future transaction %(actual_tid_int)d (%(got_ts)s).  "
                "Current transaction is %(current_tid)d (%(current_ts)s)."
                % {
                    'oid_int': oid_int,
                    'actual_tid_int': actual_tid_int,
                    'current_tid': self.current_tid,
                    'got_ts': str(TimeStamp(p64(actual_tid_int))),
                    'current_ts': str(TimeStamp(p64(self.current_tid))),
                })

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
            import thread
            raise AssertionError("Detected an inconsistency "
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
                    'thread_ident': thread.get_ident(),
                })

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

            cache_data = '%s%s' % (p64(tid_int), state or '')
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
            cache_data = '%s%s' % (p64(tid_int), state or '')
            for client in self.clients_local_first:
                client.set(cp0_key, cache_data)
        return state, tid_int


    def tpc_begin(self):
        """Prepare temp space for objects to cache."""
        self.queue = AutoTemporaryFile()
        self.queue_contents = {}

    def store_temp(self, oid_int, state):
        """Queue an object for caching.

        Typically, we can't actually cache the object yet, because its
        transaction ID is not yet chosen.
        """
        assert isinstance(state, str)
        queue = self.queue
        queue.seek(0, 2)  # seek to end
        startpos = queue.tell()
        queue.write(state)
        endpos = queue.tell()
        self.queue_contents[oid_int] = (startpos, endpos)

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
            for (oid_int, (startpos, endpos)) in self.queue_contents.items()
            ]
        items.sort()

        for startpos, endpos, oid_int in items:
            self.queue.seek(startpos)
            length = endpos - startpos
            state = self.queue.read(length)
            if len(state) != length:
                raise AssertionError("Queued cache data is truncated")
            cachekey = '%s:state:%d:%d' % (prefix, tid_int, oid_int)
            item_size = length + len(cachekey)
            if send_size and send_size + item_size >= self.send_limit:
                for client in self.clients_local_first:
                    client.set_multi(to_send)
                to_send.clear()
                send_size = 0
            to_send[cachekey] = '%s%s' % (tid, state)
            send_size += item_size

        if to_send:
            for client in self.clients_local_first:
                client.set_multi(to_send)

        self.queue_contents.clear()
        self.queue.seek(0)

    def after_tpc_finish(self, tid):
        """Update the commit count in the cache.

        This is called after the database commit lock is released,
        but before releasing the storage lock that will allow other
        threads to use this instance.
        """
        tid_int = u64(tid)

        # Why do we cache a commit count instead of the transaction ID?
        # Here's why. This method gets called after the commit lock is
        # released; other threads or processes could have committed
        # more transactions in the time that has passed since releasing
        # the lock, so a cached transaction ID would cause a race. It
        # also wouldn't work to cache the transaction ID before
        # releasing the commit lock, since that could cause some
        # threads or processes watching the cache for changes to think
        # they are up to date when they are not. The commit count
        # solves these problems by ensuring that every commit is
        # followed by a change to the cache that does not conflict with
        # concurrent committers.
        cachekey = self.commit_count_key
        for client in self.clients_global_first:
            if client.incr(cachekey) is None:
                # Initialize commit_count.
                # Use a random number for the base.
                client.add(cachekey, random.randint(1, 1<<31))
                # A concurrent committer could have won the race to set the
                # initial commit_count.  Increment commit_count so that it
                # doesn't matter who won.
                if client.incr(cachekey) is not None:
                    break
                # else the client is dead.  Fall back to the next client.

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

    def need_poll(self):
        """Return True if the commit count has changed"""
        for client in self.clients_global_first:
            new_commit_count = client.get(self.commit_count_key)
            if new_commit_count is not None:
                break
        if new_commit_count != self.commit_count:
            self.commit_count = new_commit_count
            return True
        return False

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
                for oid_int, tid_int in change_dict.iteritems():
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
        cp0, cp1 = self.checkpoints
        assert tid_int > cp0
        expect = '%d %d' % self.checkpoints
        if oversize:
            # start new checkpoints
            change_to = '%d %d' % (tid_int, tid_int)
        else:
            # shift the existing checkpoints
            change_to = '%d %d' % (tid_int, cp0)
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


class SizeOverflow(Exception):
    """Too much memory would be consumed by a new key"""

class LocalClientBucket(dict):
    """A map that keeps a record of its approx. size.

    keys must be strings and most values are strings.
    """

    def __init__(self, limit):
        self.size = 0
        self.limit = limit
        self._super = super(LocalClientBucket, self)

    def __setitem__(self, key, value):
        """Set an item.

        Throws SizeOverflow if the new item would cause this map to
        surpass its memory limit.
        """
        if isinstance(value, basestring):
            sizedelta = len(value)
        else:
            sizedelta = 0
        if key in self:
            oldvalue = self[key]
            if isinstance(oldvalue, basestring):
                sizedelta -= len(oldvalue)
        else:
            sizedelta += len(key)
        if self.size + sizedelta > self.limit:
            raise SizeOverflow()
        self._super.__setitem__(key, value)
        self.size += sizedelta
        return True

    def __delitem__(self, key):
        oldvalue = self[key]
        self._super.__delitem__(key)
        sizedelta = len(key)
        if isinstance(oldvalue, basestring):
            sizedelta += len(oldvalue)
        self.size -= sizedelta


class LocalClient(object):
    """A memcache-like object that stores in Python dictionaries."""

    def __init__(self, options):
        self._lock = threading.Lock()
        self._lock_acquire = self._lock.acquire
        self._lock_release = self._lock.release
        self._bucket_limit = int(1000000 * options.cache_local_mb / 2)
        self._value_limit = options.cache_local_object_max
        self._bucket0 = LocalClientBucket(self._bucket_limit)
        self._bucket1 = LocalClientBucket(self._bucket_limit)

        compression_module = options.cache_local_compression
        if compression_module in ('', None, 'none'):
            self._compress = None
            self._decompress = None
        else:
            module = __import__(
                compression_module, {}, {}, ['compress', 'decompress'])
            self._compress = module.compress
            self._decompress = module.decompress

    def flush_all(self):
        self._lock_acquire()
        try:
            self._bucket0 = LocalClientBucket(self._bucket_limit)
            self._bucket1 = LocalClientBucket(self._bucket_limit)
        finally:
            self._lock_release()

    def get(self, key):
        return self.get_multi([key]).get(key)

    def get_multi(self, keys):
        res = {}
        decompress = self._decompress
        self._lock_acquire()
        try:
            for key in keys:
                cvalue = self._bucket0.get(key)
                if cvalue is None:
                    cvalue = self._bucket1.get(key)
                    if cvalue is None:
                        continue
                    # This key is active, so move it to bucket0.
                    del self._bucket1[key]
                    self._set_one(key, cvalue)

                if decompress is not None:
                    if isinstance(cvalue, basestring):
                        value = decompress(cvalue)
                    else:
                        value = cvalue
                else:
                    value = cvalue

                res[key] = value
        finally:
            self._lock_release()
        return res

    def _set_one(self, key, cvalue):
        try:
            self._bucket0[key] = cvalue
        except SizeOverflow:
            # Shift bucket0 to bucket1.
            self._bucket1 = self._bucket0
            self._bucket0 = LocalClientBucket(self._bucket_limit)
            # Watch for the log message below to decide whether the
            # cache_local_mb parameter is set to a reasonable value.
            # The log message indicates that old cache data has
            # been garbage collected.
            log.debug("LocalClient buckets shifted")

            try:
                self._bucket0[key] = cvalue
            except SizeOverflow:
                # The value doesn't fit in the cache at all, apparently.
                pass

    def set(self, key, value):
        self.set_multi({key: value})

    def set_multi(self, d, allow_replace=True):
        if not self._bucket_limit:
            # don't bother
            return
        compress = self._compress
        self._lock_acquire()
        try:
            for key, value in d.iteritems():
                if isinstance(value, basestring):
                    if len(value) >= self._value_limit:
                        # This value is too big, so don't cache it.
                        continue
                    if compress is not None:
                        cvalue = compress(value)
                    else:
                        cvalue = value

                else:
                    cvalue = value

                if key in self._bucket0:
                    if not allow_replace:
                        continue
                    del self._bucket0[key]

                if key in self._bucket1:
                    if not allow_replace:
                        continue
                    del self._bucket1[key]

                self._set_one(key, cvalue)
        finally:
            self._lock_release()

    def add(self, key, value):
        self.set_multi({key: value}, allow_replace=False)

    def incr(self, key):
        if not self._bucket_limit:
            # don't bother
            return None
        decompress = self._decompress
        self._lock_acquire()
        try:
            cvalue = self._bucket0.get(key)
            if cvalue is None:
                cvalue = self._bucket1.get(key)
                if cvalue is None:
                    return None
                # this key is active, so move it to bucket0
                del self._bucket1[key]

            if decompress is not None:
                if isinstance(cvalue, basestring):
                    value = decompress(cvalue)
                else:
                    value = cvalue
            else:
                value = cvalue

            res = int(value) + 1
            self._set_one(key, res)
            return res
        finally:
            self._lock_release()
