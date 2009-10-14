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
import time


class StorageCache(object):
    """RelStorage integration with memcached or similar.
    """

    # send_limit: max approx. bytes to buffer before sending to the cache
    send_limit = 1024 * 1024

    def __init__(self, options):
        module_name = options.cache_module_name
        module = __import__(module_name, {}, {}, ['Client'])
        servers = options.cache_servers
        if isinstance(servers, basestring):
            servers = servers.split()
        self.client = module.Client(servers)
        self.prefix = options.cache_prefix or ''

        # queue is an AutoTemporaryFile during txn commit.
        self.queue = None

        # queue_contents is a map of {oid: (startpos, endpos)}
        # during txn commit.
        self.queue_contents = None

        # commit_count_key is the cache key to poll for changes
        self.commit_count_key = '%s:commit_count' % self.prefix

        # polled_commit_count contains the last polled value of the
        # 'commit_count' cache key
        self.polled_commit_count = 0

    def flush_all(self):
        """Remove all data from the cache.  Called by RelStorage.zap_all()"""
        self.client.flush_all()

    def load(self, cursor, oid_int, prev_polled_tid, adapter):
        """Load the given object from cache if possible.

        Fall back to loading from the database.
        """
        client = self.client
        state_key = '%s:state:%d' % (self.prefix, oid_int)
        if prev_polled_tid:
            backptr_key = '%s:back:%d:%d' % (
                self.prefix, prev_polled_tid, oid_int)
            v = client.get_multi([state_key, backptr_key])
            if v is not None:
                cache_data = v.get(state_key)
                backptr = v.get(backptr_key)
            else:
                cache_data = None
                backptr = None
        else:
            cache_data = client.get(state_key)
            backptr = None

        state = None
        if cache_data and len(cache_data) >= 8:
            # validate the cache result
            tid = cache_data[:8]
            tid_int = u64(tid)
            if tid_int == prev_polled_tid or tid == backptr:
                # the cached data is current.
                state = cache_data[8:]

        if state is None:
            # could not load from cache, so get from the database
            state, tid_int = adapter.mover.load_current(
                cursor, oid_int)
            state = str(state or '')
            if tid_int is not None:
                # cache the result
                to_cache = {}
                tid = p64(tid_int)
                new_cache_data = tid + state
                if new_cache_data != cache_data:
                    to_cache[state_key] = new_cache_data
                if prev_polled_tid and prev_polled_tid != tid_int:
                    to_cache[backptr_key] = tid
                if to_cache:
                    client.set_multi(to_cache)

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

    def tpc_vote(self, tid):
        """Now that the tid is chosen, send queued objects to the cache.
        """
        client = self.client
        assert len(tid) == 8
        send_size = 0
        to_send = {}

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
            cachekey = '%s:state:%d' % (self.prefix, oid_int)
            to_send[cachekey] = '%s%s' % (tid, state)
            send_size += length + len(cachekey)
            if send_size >= self.send_limit:
                client.set_multi(to_send)
                to_send.clear()
                send_size = 0

        if to_send:
            client.set_multi(to_send)

        self.queue_contents.clear()
        self.queue.seek(0)


    def tpc_finish(self):
        """Update the commit count in the cache."""
        client = self.client
        cachekey = self.commit_count_key
        if client.incr(cachekey) is None:
            # Use the current time as an initial commit_count value.
            client.add(cachekey, int(time.time()))
            # A concurrent committer could have won the race to set the
            # initial commit_count.  Increment commit_count so that it
            # doesn't matter who won.
            client.incr(cachekey)

    def clear_temp(self):
        """Clear any transactional data.  Called after txn finish or abort."""
        self.queue_contents = None
        if self.queue is not None:
            self.queue.close()
            self.queue = None

    def need_poll(self):
        """Return True if the commit count has changed"""
        new_commit_count = self.client.get(self.commit_count_key)
        if new_commit_count != self.polled_commit_count:
            self.polled_commit_count = new_commit_count
            return True
        return False
