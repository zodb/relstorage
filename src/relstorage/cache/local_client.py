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

import bz2
import threading
import time
import zlib

from contextlib import closing

from zope import interface

from relstorage._util import get_memory_usage
from relstorage._util import byte_display
from relstorage._util import timer as _timer
from relstorage._util import log_timed as _log_timed
from relstorage._compat import OID_OBJECT_MAP_TYPE
from relstorage._compat import OID_TID_MAP_TYPE

from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import MAX_TID
from relstorage.cache.interfaces import CacheCorruptedError

from relstorage.cache.mapping import SizedLRUMapping

from relstorage.cache.persistence import sqlite_connect
from relstorage.cache.persistence import sqlite_files
from relstorage.cache.persistence import FAILURE_TO_OPEN_DB_EXCEPTIONS
from relstorage.cache.local_database import Database

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IStateCache,
                       IPersistentCache)
class LocalClient(object):

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

    _bucket_type = SizedLRUMapping

    def __init__(self, options,
                 prefix=None):
        self._lock = threading.Lock()
        self.options = options
        self.checkpoints = None
        self.prefix = prefix or ''
        # XXX: The calc for limit is substantially smaller
        # The real MB value is 1024 * 1024 = 1048576
        self.limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max
        if options.cache_local_storage:
            self._bucket_type = options.cache_local_storage
        self.__bucket = None

        # The {oid: tid} that we read from the cache.
        # These are entries that we know are there, and if we see them
        # change, we need to be sure to update that in the database,
        # *even if they are evicted* and we would otherwise lose
        # knowledge of them before we save. We do this by watching incoming
        # TIDs; only if they were already in here do we continue to keep track.
        # At write time, if we can't meet the requirement ourself, we at least
        # make sure there are no stale entries in the cache database.
        self._min_allowed_writeback = OID_TID_MAP_TYPE()

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

    @property
    def size(self):
        return self.__bucket.size

    def __len__(self):
        return len(self.__bucket)

    def __iter__(self):
        return iter(self.__bucket)

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

    @_log_timed
    def save(self, **sqlite_args):
        options = self.options
        if options.cache_local_dir and self.__bucket.size:
            try:
                conn = sqlite_connect(options, self.prefix,
                                      **sqlite_args)
            except FAILURE_TO_OPEN_DB_EXCEPTIONS:
                logger.exception("Failed to open sqlite to write")
                return 0

            with closing(conn):
                try:
                    self.write_to_sqlite(conn)
                except CacheCorruptedError:
                    # The cache_trace_analysis.rst test fills
                    # us with junk data and triggers this.
                    logger.exception("Failed to save cache")
                    self.flush_all()
                    return 0
            # Testing: Return a signal when we tried to write
            # something.
            return 1

    def restore(self, row_filter=None):
        """
        Load the data from the persistent database.

        If *row_filter* is given, it is a ``callable(checkpoints, row_iter)``
        that should return an iterator of four-tuples: ``(oid, key_tid, state, state_tid)``
        from the input rows ``(oid, state_tid, actual_tid)``. It is guaranteed
        that you won't see the same oid more than once.
        """
        options = self.options
        if options.cache_local_dir:
            try:
                conn = sqlite_connect(options, self.prefix, close_async=False)
            except FAILURE_TO_OPEN_DB_EXCEPTIONS:
                logger.exception("Failed to read data from sqlite")
                return
            with closing(conn):
                self.read_from_sqlite(conn, row_filter)

    @_log_timed
    def remove_invalid_persistent_oids(self, bad_oids):
        """
        Remove data from the persistent cache for the given oids.
        """
        options = self.options
        if not options.cache_local_dir:
            return

        count_removed = 0
        conn = '(no oids to remove)'
        if bad_oids:
            conn = sqlite_connect(options, self.prefix, close_async=False)
            with closing(conn):
                db = Database.from_connection(conn)
                count_removed = db.remove_invalid_persistent_oids(bad_oids)
        logger.debug("Removed %d invalid OIDs from %s", count_removed, conn)

    def zap_all(self):
        _, destroy = sqlite_files(self.options, self.prefix)
        destroy()

    @property
    def _bucket0(self):
        # For testing only.
        return self.__bucket

    @staticmethod
    def key_weight(_):
        # All keys are equally weighted, and we don't count them.
        return 0

    @staticmethod
    def value_weight(value):
        # Values are the (state, actual_tid) pair, and their
        # weight is the size of the state
        return len(value[0] if value[0] else b'')

    def flush_all(self):
        with self._lock:
            self.__bucket = self._bucket_type(
                self.limit,
                key_weight=self.key_weight,
                value_weight=self.value_weight,
            )
            self.checkpoints = None
            self._min_allowed_writeback = OID_TID_MAP_TYPE()

    def reset_stats(self):
        self.__bucket.reset_stats()

    def stats(self):
        return self.__bucket.stats()

    def __getitem__(self, oid_tid):
        return self(*oid_tid)

    def __call__(self, oid, tid1, tid2=None, _keys=None):
        # _keys is a hook for testing.
        decompress = self._decompress
        get = self.__bucket.get_from_key_or_backup_key

        with self._lock:
            if _keys is not None:
                res = get(*_keys)
            else:
                key1 = (oid, tid1)
                backup_key = (oid, tid2) if tid2 is not None else None
                res = get(key1, backup_key)

        # Finally, while not holding the lock, decompress if needed.
        # Recall that for deleted objects, `state` can be None.
        if res is not None:
            state, tid = res
            return decompress(state) if state else state, tid

    def __setitem__(self, oid_tid, state_bytes_tid):
        if not self.limit:
            # don't bother
            return

        # This used to allow non-byte values, but that's confusing
        # on Py3 and wasn't used outside of tests, so we enforce it.
        # A state of 'None' happens for undone transactions.
        state_bytes, tid = state_bytes_tid
        assert isinstance(state_bytes, bytes) or state_bytes is None, type(state_bytes)
        compress = self._compress
        cvalue = compress(state_bytes) if compress else state_bytes # pylint:disable=not-callable

        if cvalue and len(cvalue) >= self._value_limit:
            # This value is too big, so don't cache it.
            return

        with self._lock:
            self.__bucket[oid_tid] = (cvalue, tid) # possibly evicts
            if tid > self._min_allowed_writeback.get(oid_tid[0], MAX_TID):
                self._min_allowed_writeback[oid_tid[0]] = tid

    def set_all_for_tid(self, tid_int, state_oid_iter):
        for state, oid_int, _ in state_oid_iter:
            self[(oid_int, tid_int)] = (state, tid_int)

    def __delitem__(self, oid_tid):
        with self._lock:
            del self.__bucket[oid_tid]
            if oid_tid[1] > self._min_allowed_writeback.get(oid_tid[0], MAX_TID):
                self._min_allowed_writeback[oid_tid[0]] = oid_tid[1]

    def invalidate_all(self, oids):
        with self._lock:
            min_allowed = self._min_allowed_writeback
            for oid, tid in self.__bucket.invalidate_all(oids):
                if tid > min_allowed.get(oid, MAX_TID):
                    min_allowed[oid] = tid

    def store_checkpoints(self, cp0, cp1):
        # No lock, the assignment should be atomic
        # Both checkpoints should be None, or the same integer,
        # or cp0 should be ahead of cp1. Anything else is a bug.
        assert cp0 == cp1 or cp0 > cp1
        cp = self.checkpoints = cp0, cp1
        return cp

    def get_checkpoints(self):
        return self.checkpoints

    def close(self):
        pass

    def items(self):
        return self.__bucket.items()

    def values(self):
        return self.__bucket.values()

    def updating_delta_map(self, deltas):
        return deltas

    @_log_timed
    def read_from_sqlite(self, connection, row_filter=None):
        import gc
        gc.collect() # Free memory, we're about to make big allocations.
        mem_before = get_memory_usage()

        db = Database.from_connection(connection)
        checkpoints = db.checkpoints
        if checkpoints:
            self.store_checkpoints(*checkpoints)

        self._min_allowed_writeback = db.oid_to_tid

        # XXX: The cache_ring is going to consume all the items we
        # give it, even if it doesn't actually add them to the cache.
        # It also creates a very large preallocated array for all to
        # hold the `total_count` of items. We don't want to read more
        # rows than necessary, to keep the delta maps small and to
        # avoid the overhead; we could pass the COUNT(zoid) to
        # `bulk_update`, and have our row iterable be a generator that
        # reads a row and breaks when it reaches the limit, but then
        # we have that whole preallocated array hanging around, which
        # is probably much bigger than we need. So we need to give an
        # accurate count, and the easiest way to do that is to materialize
        # the list of rows.
        #
        # In practice, we can assume that the limit hasn't changed between this
        # run and the last run, so the database will already have been trimmed
        # to fit the desired size, so essentially all the rows in the database
        # should go in our cache.
        @_log_timed
        def data():
            # Do this here so that we don't have the result
            # in a local variable and it can be collected
            # before we measure the memory delta.
            #
            # In large benchmarks, this function accounts for 57%
            # of the total time to load data. 26% of the total is
            # fetching rows from sqlite, and 18% of the total is allocating
            # storage for the blob state.
            #
            # We make one call into sqlite and let it handle the iterating.
            # Items are (oid, key_tid, state, actual_tid). Currently,
            # key_tid == actual_tid
            cur = db.fetch_rows_by_priority()
            items = cur.fetchall()
            cur.close()
            # row_filter produces the sequence ((oid, key_tid) (state, actual_tid))
            if row_filter is not None:
                row_iter = row_filter(checkpoints, items)
                items = list(row_iter)
            else:
                items = [(r[:2], r[2:]) for r in items]

            # Look at them from most to least recent,
            # but insert them the other way.
            items.reverse()
            return items

        # In the large benchmark, this is 25% of the total time.
        # 18% of the total time is preallocating the entry nodes.
        self.__bucket.bulk_update(data(),
                                  source=connection,
                                  log_count=len(self._min_allowed_writeback),
                                  mem_usage_before=mem_before)

    @_log_timed
    def _newest_items(self):
        """
        Return a dict {oid: entry} for just the newest entries.
        """
        # In a very large cache, with absolutely no duplicates,
        # this accounts for 2.5% of the time taken to save.
        newest_entries = OID_OBJECT_MAP_TYPE()
        for entry in self.__bucket.entries():
            oid, _ = entry.key
            stored_entry = newest_entries.get(oid)
            if stored_entry is None:
                newest_entries[oid] = entry
            elif stored_entry.value[1] < entry.value[1]:
                newest_entries[oid] = entry
            elif stored_entry.value[1] == entry.value[1]:
                if stored_entry.value[0] != entry.value[0]:
                    raise CacheCorruptedError(
                        "The object %x has two different states for transaction %x" % (
                            oid, entry.value[1]
                        )
                    )
        return newest_entries

    def _items_to_write(self, stored_oid_tid):
        # pylint:disable=too-many-locals
        all_entries_len = len(self.__bucket)

        # Tune quickly to the newest entries, materializing the list
        # TODO: Should we consolidate frequency information for the OID?
        # That can be expensive in the CFFI ring.
        newest_entries = self._newest_items()

        # Only write the newest entry for each OID.


        # Newly added items have a frequency of 1. They *may* be
        # useless stuff we picked up from an old cache file and
        # haven't touched again, or they may be something that
        # really is new and we have no track history for (that
        # would be in eden generation).
        #
        # Ideally we want to do something here that's similar to what
        # the SegmentedLRU does: if we would reject an item from eden,
        # then only keep it if it's better than the least popular
        # thing in probation.
        #
        # It's not clear that we have a good algorithm for this, and
        # what we tried takes a lot of time, so we don't bother.
        # TODO: Finish tuning these.

        # But we have a problem: If there's an object in the existing
        # cache that we have updated but now exclude, we would fail to
        # write its new data. So we can't exclude anything that's
        # changed without also nuking it from the DB.

        # Also, if we had a value loaded from the DB, and then we
        # modified it, but later ejected all entries for that object
        # from the cache, we wouldn't see it here, and we could be
        # left with stale data in the database. We must track the
        # (oid, tid) we load from the database and record that we
        # should DELETE those rows if that happens.
        # This is done with the __min_allowed_writeback dictionary,
        # which we mutate here: When we find a row we can write that meets
        # the requirement, we take it out of the dictionary.
        # Later, if there's anything left in the dictionary, we *know* there
        # may be stale entries in the cache, and we remove them.

        pop_min_required_tid = self._min_allowed_writeback.pop
        get_min_required_tid = self._min_allowed_writeback.get # pylint:disable=no-member
        get_current_oid_tid = stored_oid_tid.get
        min_allowed_count = 0
        matching_tid_count = 0
        # When we accumulate all the rows here before returning them,
        # this function shows as about 3% of the total time to save
        # in a very large database.
        with _timer() as t:
            for oid, entry in newest_entries.items():
                # We must have something at least this fresh
                # to consider writing it out
                actual_tid = entry.value[1]
                min_allowed = get_min_required_tid(oid, -1)
                if min_allowed > actual_tid:
                    min_allowed_count += 1
                    continue

                # If we have something >= min_allowed, but == this,
                # it's not worth writing to the database (states should be identical).
                current_tid = get_current_oid_tid(oid, -1)
                if current_tid >= actual_tid:
                    matching_tid_count += 1
                    continue

                yield (oid, actual_tid, entry.value[0], entry.frequency)

                # We're able to satisfy this, so we don't need to consider
                # it in our min_allowed set anymore.
                # XXX: This isn't really the right place to do this.
                # Move this and write a test.
                pop_min_required_tid(oid, None)

        removed_entry_count = matching_tid_count + min_allowed_count
        logger.info(
            "Consolidated from %d entries to %d entries "
            "(rejected stale: %d; already in db: %d) in %s",
            all_entries_len, len(newest_entries) - removed_entry_count,
            min_allowed_count, matching_tid_count,
            t.duration)

    @_log_timed
    def write_to_sqlite(self, connection):
        # pylint:disable=too-many-locals
        mem_before = get_memory_usage()

        cur = connection.cursor()

        db = Database.from_connection(connection)
        begin = time.time()

        # In a large benchmark, store_temp() accounts for 32%
        # of the total time, while move_from_temp accounts for 49%.

        # The batch size depends on how many params a stored proc can
        # have; if we go too big we get OperationalError: too many SQL
        # variables. The default is 999.
        # Note that the multiple-value syntax was added in
        # 3.7.11, 2012-03-20.
        with _timer() as batch_timer:
            cur.execute('BEGIN')
            stored_oid_tid = db.oid_to_tid
            fetch_current = time.time()
            count_written, _ = db.store_temp(self._items_to_write(stored_oid_tid))
            cur.execute("COMMIT")


        # Take out (exclusive) locks now; if we don't, we can get
        # 'OperationalError: database is locked' But beginning
        # immediate lets us stand in line.
        with _timer() as exclusive_timer:
            cur.execute('BEGIN IMMEDIATE')
            # During the time it took for us to commit, and the time that we
            # got the lock, it's possible that someone else committed and
            # changed the data in object_state
            # Things might have changed in the database since our
            # snapshot where we put temp objects in, so we can't rely on
            # just assuming that's the truth anymore.
            db.move_from_temp()
            if self.checkpoints:
                db.update_checkpoints(*self.checkpoints)

            cur.execute('COMMIT')
        # TODO: Maybe use BTrees.family.intersection to get the common keys?
        # Or maybe use a SQLite Python function?
        # 'DELETE from object_state where is_stale(zoid, tid)'
        # What's fastest? The custom function version would require scanning the entire
        # table; unfortunately this sqlite interface doesn't allow defining virtual tables.
        with _timer() as trim_timer:
            # Delete anything we still know is stale, because we
            # saw a new TID for it, but we had nothing to replace it with.
            min_allowed_writeback = OID_TID_MAP_TYPE()
            for k, v in self._min_allowed_writeback.items():
                if stored_oid_tid.get(k, MAX_TID) < v:
                    min_allowed_writeback[k] = v
            db.trim_to_size(self.limit, min_allowed_writeback)
            del min_allowed_writeback
        # Typically we write when we're closing and don't expect to do
        # anything else, so there's no need to keep tracking this info.
        self._min_allowed_writeback = OID_TID_MAP_TYPE()

        # Cleanups
        cur.close()

        del db
        del stored_oid_tid

        with _timer() as gc_timer:
            import gc
            gc.collect()

        end = time.time()
        stats = self.stats()
        mem_after = get_memory_usage()
        logger.info(
            "Wrote %d items to %s in %s "
            "(%s to fetch current, %s to insert batch, %s to write, %s to trim, %s to gc). "
            "Used %s memory. (Storage: %s) "
            "Total hits %s; misses %s; ratio %s (stores: %s)",
            count_written, connection, end - begin,
            fetch_current - begin, batch_timer.duration, exclusive_timer.duration,
            trim_timer.duration, gc_timer.duration,
            byte_display(mem_after - mem_before), self._bucket0,
            stats['hits'], stats['misses'], stats['ratio'], stats['sets'])

        return count_written
