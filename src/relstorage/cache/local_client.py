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
import time
import zlib


from contextlib import closing

from zope import interface

from relstorage._util import get_memory_usage
from relstorage._util import byte_display
from relstorage._util import timer as _timer
from relstorage._util import log_timed as _log_timed
from relstorage._util import consume
from relstorage._compat import OID_TID_MAP_TYPE as OidTMap
from relstorage.interfaces import Int

from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import IGenerationalLRUCache
from relstorage.cache.interfaces import IGeneration
from relstorage.cache.interfaces import ILRUEntry


from relstorage.cache.persistence import sqlite_connect
from relstorage.cache.persistence import sqlite_files
from relstorage.cache.persistence import FAILURE_TO_OPEN_DB_EXCEPTIONS
from relstorage.cache.local_database import Database

from relstorage.cache import cache

logger = __import__('logging').getLogger(__name__)

# pylint:disable=too-many-lines

class ICachedValue(ILRUEntry):
    """
    Data stored in the cache for a single OID.

    This may be a single ``(state, tid)`` pair, or it may be multiple
    such pairs, representing evolution of the object.

    Memory and time efficiency both matter. These objects do not know
    their own OID, just the state and tid.

    .. rubric:: Freezing objects

    For any given OID, one TID may be frozen. It may then be looked up
    without knowing its actual TID (using a TID of ``None``).
    This is useful for objects that do
    not change. Invalidations of frozen states happen automatically
    during the MVCC vacuuming process:

        - Freezing happens after a poll, when we have determined that
          an object has not changed within the range of transactions
          visible to all database viewers. The index entry is removed,
          and we begin looking for it at None.

          At this time, any older states cached for the object are removed.
          By definition, there can be no newer states, so only one state is
          accessible. (Of course, if we've just completed a transaction and
          not yet polled, then that's not strictly true; there could be
          cached data from the future not yet visible to any viewers.)

        - If an object previously frozen is changed, we see that in
          our index and won't ask for frozen states anymore.

          If we then load the new state from the DB, we cache it, leaving it
          with two cached states. Older viewers unaware of the change and accessing the database
          prior to it, can still use the frozen
          revision.

          Eventually that index entry reaches the end of its lifetime. If
          the object has not changed again, we will freeze it. This will
          discard the older frozen value and replace it with a new one. If
          it has changed again, we will invalidate the cache for anything
          older than that TID, which includes the first frozen state.

    The implementation and usage of frozen objects is contained entirely within
    this module. Clients are then responsible for making sure that the
    returned tid, if any, is within their viewing range.
    """

    # pylint:disable=no-self-argument,unexpected-special-method-signature,inherit-non-class
    # pylint:disable=arguments-differ

    weight = Int(
        description=u"""The cost (size) of this cached value.""")

    max_tid = Int(
        description=u"""The newest TID cached for the object."""
    )

    newest_value = interface.Attribute(u"The ``(state, tid)`` pair that is the newest.")

    def get_if_tid_matches(tid):
        """
        Return the ``(state, tid)`` for the given TID.

        A special value of None matches any previously frozen TID.

        If no TID matches, returns None.

        """

    def freeze_to_tid(tid):
        """
        Mark the given TID, if it exists, as frozen.

        Returns a new value to store in the cache. If it returns None,
        this entry is removed from the cache (because the TID didn't match,
        and must have been older.)
        """

    def with_later(value):
        """
        Add the ``(state, tid)`` (another ICachedValue) to the list of
        cached entries. Return the new value to place in the cache.
        """

    def discarding_tids_before(tid):
        """
        Remove the tid, and anything older, from the list of cached entries.

        Return the new value to place in the cache. Return None if all values
        were removed and the cached entry should be removed.
        """

interface.classImplements(cache.CachedValue, ICachedValue)
interface.classImplements(cache.PyCache, IGenerationalLRUCache)
interface.classImplements(cache.PyGeneration, IGeneration)


@interface.implementer(IStateCache,
                       IPersistentCache)
class LocalClient(object):
    # pylint:disable=too-many-public-methods,too-many-instance-attributes

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

    # What multiplier of the number of items in the cache do we apply
    # to determine when to age the frequencies?
    _age_factor = 10

    # When did we last age?
    _aged_at = 0
    _next_age_at = 1000

    _cache = None

    # Things copied from self._cache
    _peek = None

    def __init__(self, options,
                 prefix=None):
        self.options = options
        self.prefix = prefix or ''
        # XXX: The calc for limit is substantially smaller
        # The real MB value is 1024 * 1024 = 1048576
        self.limit = int(1000000 * options.cache_local_mb)
        self._value_limit = options.cache_local_object_max

        # The underlying data storage. It maps ``{oid: value}``,
        # where ``value`` is an :class:`ICachedValue`.
        #
        # Keying off of OID directly lets the LRU be based on the
        # popularity of an object, not its particular transaction. It also lets
        # us use a LLBTree to store the data, which can be more memory efficient.
        self._cache = None

        self.flush_all()
        self.__initial_weight = self._cache.weight

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
        return self._cache.weight

    def __len__(self):
        """
        How many distinct OIDs are stored.
        """
        return len(self._cache)

    def __iter__(self):
        return iter(self._cache)

    def keys(self):
        return self._cache.keys()

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
    def save(self, object_index=None, checkpoints=None, **sqlite_args):
        options = self.options
        if options.cache_local_dir and self.size > self.__initial_weight:
            try:
                conn = sqlite_connect(options, self.prefix,
                                      **sqlite_args)
            except FAILURE_TO_OPEN_DB_EXCEPTIONS:
                logger.exception("Failed to open sqlite to write")
                return 0

            with closing(conn):
                self.write_to_sqlite(conn, checkpoints, object_index)
            # Testing: Return a signal when we tried to write
            # something.
            return 1

    def restore(self):
        """
        Load the data from the persistent database.

        Returns the checkpoint data last saved, which may be None if
        there was no data.
        """
        options = self.options
        if options.cache_local_dir:
            try:
                conn = sqlite_connect(options, self.prefix)
            except FAILURE_TO_OPEN_DB_EXCEPTIONS:
                logger.exception("Failed to read data from sqlite")
                return
            with closing(conn):
                return self.read_from_sqlite(conn)

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
            self.invalidate_all(bad_oids)
            conn = sqlite_connect(options, self.prefix)
            with closing(conn):
                db = Database.from_connection(conn)
                count_removed = db.remove_invalid_persistent_oids(bad_oids)

        logger.debug("Removed %d invalid OIDs from %s", count_removed, conn)

    def zap_all(self):
        _, destroy = sqlite_files(self.options, self.prefix)
        destroy()
        # zapping happens frequently during test runs,
        # and during zodbconvert when the process will exist
        # only for a short time.
        self.flush_all()

    def flush_all(self):
        if self._cache or self._cache is None:
            # Only actually abandon the cache object
            # if it has data. Otherwise let it keep the
            # preallocated CFFI objects it may have
            # (those are expensive to create and tests call
            # this a LOT)
            byte_limit = self.limit
            self._cache = cache.PyCache(
                byte_limit * self._gen_eden_pct,
                byte_limit * self._gen_protected_pct,
                byte_limit * self._gen_probation_pct
            )
        self._peek = self._cache.peek
        self.reset_stats()

    def reset_stats(self):
        self._cache.reset_stats()
        self._aged_at = 0
        self._next_age_at = 1000

    def stats(self):
        total = self._cache.hits + self._cache.misses
        return {
            'hits': self._cache.hits,
            'misses': self._cache.misses,
            'sets': self._cache.sets,
            'ratio': self._cache.hits / total if total else 0,
            'len': len(self),
            'bytes': self.size,
        }

    def __contains__(self, oid_tid):
        oid, tid = oid_tid
        return self._cache.get_item_with_tid(oid, tid) is not None

    def get(self, oid_tid, peek=False):
        oid, tid = oid_tid
        assert tid is None or tid >= 0
        decompress = self._decompress
        value = None

        if peek:
            value = self._cache.peek_item_with_tid(oid, tid)
        else:
            value = self._cache.get_item_with_tid(oid, tid)

        # Finally, decompress if needed.
        # Recall that for deleted objects, `state` can be None.
        # TODO: This is making a copy of the string from C++ even if we
        # don't need to decompress.
        if value is not None:
            state, tid = value
            return ((decompress(state) if state else state), tid)

    __getitem__ = get

    def _age(self):
        # Age only when we're full and would thus need to evict; this
        # makes initial population faster. It's cheaper to calculate this
        # AFTER the operations, though, because we read it from C.
        #if self.size < self.limit:
        #    return

        # Age the whole thing periodically based on the number of
        # operations we've done that would have altered popularity.
        # Dynamically calculate how often we need to age. By default, this is
        # based on what Caffeine's PerfectFrequency does: 10 * max
        # cache entries.
        #
        # We don't take a lock to do this; it's fine if two threads
        # attempt it at the same time.
        age_period = self._age_factor * len(self._cache)
        operations = self._cache.hits + self._cache.sets
        if operations - self._aged_at < age_period:
            self._next_age_at = age_period
            return
        if self.size < self.limit:
            return

        self._aged_at = operations
        now = time.time()
        logger.debug("Beginning frequency aging for %d cache entries",
                     len(self._cache))
        self._cache.age_frequencies()
        done = time.time()
        logger.debug("Aged %d cache entries in %s", len(self._cache), done - now)

        self._next_age_at = int(self._aged_at * 1.5) # in case the dict shrinks

        return self._aged_at

    def __setitem__(self, oid_tid, state_bytes_tid):
        if not self.limit:
            # don't bother
            return

        # A state of 'None' happens for undone transactions.
        oid, key_tid = oid_tid
        state_bytes, actual_tid = state_bytes_tid

        # Really key_tid should be > 0; we allow >= for tests.
        assert key_tid == actual_tid and key_tid >= 0
        self.set_all_for_tid(key_tid, [(state_bytes, oid, None)])

    def set_all_for_tid(self, tid_int, state_oid_iter):
        if self.limit:
            self._cache.set_all_for_tid(tid_int, state_oid_iter, self._compress, self._value_limit)
            # Inline some of the logic about whether to age or not; avoiding the
            # call helps speed
            if self._cache.hits + self._cache.sets > self._next_age_at:
                self._age()

    def __delitem__(self, oid_tid):
        self.delitems({oid_tid[0]: oid_tid[1]})

    def delitems(self, oids_tids):
        """
        For each OID/TID pair in the items, remove all cached values
        for OID that are older than TID.
        """
        self._cache.delitems(oids_tids)

    def invalidate_all(self, oids):
        self._cache.del_oids(oids)

    def freeze(self, oids_tids):
        self._cache.freeze(oids_tids)

    def close(self):
        pass

    release = close

    def new_instance(self):
        return self

    def updating_delta_map(self, deltas):
        return deltas

    def _bulk_update(self, keys_and_values,
                     source='<unknown>',
                     log_count=None,
                     mem_usage_before=None):
        """
        Insert all the
        ``(oid, (state, actual_tid, frozen, frequency)))`` pairs
        found in *keys_and_values*, rejecting entries once we get too full.

        *keys_and_values* should be ordered from least to most recently used.

        This can only be done in an empty cache.
        """
        now = time.time()
        mem_usage_before = mem_usage_before if mem_usage_before is not None else get_memory_usage()
        mem_usage_before_this = get_memory_usage()
        log_count = log_count or len(keys_and_values)

        data = self._cache
        if data:
            raise ValueError("Only bulk load into an empty cache.")

        stored = data.add_MRUs(
            keys_and_values,
            return_count_only=True)

        then = time.time()
        del keys_and_values # For memory reporting.
        mem_usage_after = get_memory_usage()
        logger.info(
            "Bulk update of empty cache: Examined %d and stored %d items from %s in %s using %s. "
            "(%s local) (%s)",
            log_count, stored, getattr(source, 'name', source),
            then - now,
            byte_display(mem_usage_after - mem_usage_before),
            byte_display(mem_usage_after - mem_usage_before_this),
            self)
        return log_count, stored


    @_log_timed
    def read_from_sqlite(self, connection):
        import gc
        gc.collect() # Free memory, we're about to make big allocations.
        mem_before = get_memory_usage()

        db = Database.from_connection(connection)
        checkpoints = db.checkpoints

        @_log_timed
        def fetch_and_filter_rows():
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
            # Items are (oid, key_tid, state, actual_tid).
            # key_tid may equal the actual tid, or be -1 when the row was previously
            # frozen;
            # That doesn't matter to us, we always freeze all rows.
            size = 0
            limit = self.limit
            items = []
            rows = db.fetch_rows_by_priority()
            for oid, frozen, state, actual_tid, frequency in rows:
                size += len(state)
                if size > limit:
                    break
                items.append((oid, (state, actual_tid, frozen, frequency)))
            consume(rows)
            # Rows came to us MRU to LRU, but we need to feed them the other way.
            items.reverse()
            return items

        # In the large benchmark, this is 25% of the total time.
        # 18% of the total time is preallocating the entry nodes.
        self._bulk_update(fetch_and_filter_rows(),
                          source=connection,
                          mem_usage_before=mem_before)
        return checkpoints

    def _items_to_write(self, stored_oid_tid):
        # pylint:disable=too-many-locals
        all_entries_len = len(self._cache)

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

        # The *object_index* is our best polling data; anything it has it gospel,
        # so if it has an entry for an object, it superceeds our own.
        get_current_oid_tid = stored_oid_tid.get
        matching_tid_count = 0
        # When we accumulate all the rows here before returning them,
        # this function shows as about 3% of the total time to save
        # in a very large database.
        with _timer() as t:
            for oid, lru_entry in self._cache.iteritems():
                newest_value = lru_entry.newest_value
                # We must have something at least this fresh
                # to consider writing it out
                if newest_value is None:
                    raise AssertionError("Value should not be none", oid, lru_entry)
                actual_tid = newest_value.tid

                # If we have something >= min_allowed, matching
                # what's in the database, or even older (somehow),
                # it's not worth writing to the database (states should be identical).
                current_tid = get_current_oid_tid(oid, -1)
                if current_tid >= actual_tid:
                    matching_tid_count += 1
                    continue

                yield (oid, actual_tid, newest_value.frozen,
                       bytes(newest_value.state),
                       lru_entry.frequency)

                # We're able to satisfy this, so we don't need to consider
                # it in our min_allowed set anymore.
                # XXX: This isn't really the right place to do this.
                # Move this and write a test.


        removed_entry_count = matching_tid_count
        logger.info(
            "Storing persistent cache: Examined %d entries and rejected %d "
            "(already in db: %d) in %s",
            all_entries_len, removed_entry_count,
            matching_tid_count,
            t.duration)

    @_log_timed
    def write_to_sqlite(self, connection, checkpoints, object_index=None):
        # pylint:disable=too-many-locals
        mem_before = get_memory_usage()
        object_index = object_index or OidTMap()

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


        # Take out (reserved write) locks now; if we don't, we can get
        # 'OperationalError: database is locked' (the SQLITE_BUSY
        # error code; the SQLITE_LOCKED error code is about table
        # locks and has the string 'database table is locked') But
        # beginning immediate lets us stand in line.
        #
        # Note that the SQLITE_BUSY error on COMMIT happens "if an
        # another thread or process has a shared lock on the database
        # that prevented the database from being updated.", But "When
        # COMMIT fails in this way, the transaction remains active and
        # the COMMIT can be retried later after the reader has had a
        # chance to clear." So we could retry the commit a couple
        # times and give up if can't get there. It looks like one
        # production database takes about 2.5s to execute this entire function;
        # it seems like most instances that are shutting down get to this place
        # at roughly the same time and stack up waiting:
        #
        # Instance | Save Time | write_to_sqlite time
        #     1    |   2.36s   |   2.35s
        #     2    |   5.31s   |   5.30s
        #     3    |   6.51s   |   6.30s
        #     4    |   7.91s   |   7.90s
        #
        # That either suggests that all waiting happens just to acquire this lock and
        # commit this transaction (and that subsequent operations are inconsequential
        # in terms of time) OR that the exclusive lock isn't truly getting dropped,
        # OR that some function in subsequent processes is taking longer and longer.
        # And indeed, our logging showed that a gc.collect() at the end of this function was
        # taking more and more time:
        #
        # Instance | GC Time  | Reported memory usage after GC
        #    1     |   2.00s  |     34.4MB
        #    2     |   3.50s  |     83.2MB
        #    3     |   4.94s  |    125.9MB
        #    4     |   6.44   |    202.1MB
        #
        # The exclusive lock time did vary, but not as much; trim time
        # didn't really vary:
        #
        # Instance |  Exclusive Write Time | Batch Insert Time | Fetch Current
        #    1     |   0.09s               |   0.12s           |   0.008s
        #    2     |   1.19s               |   0.44s           |   0.008s
        #    3     |   0.91s               |   0.43s           |   0.026s
        #    4     |   0.92s               |   0.34s           |   0.026s
        with _timer() as exclusive_timer:
            cur.execute('BEGIN IMMEDIATE')
            # During the time it took for us to commit, and the time that we
            # got the lock, it's possible that someone else committed and
            # changed the data in object_state
            # Things might have changed in the database since our
            # snapshot where we put temp objects in, so we can't rely on
            # just assuming that's the truth anymore.
            rows_inserted = db.move_from_temp()
            if checkpoints:
                db.update_checkpoints(*checkpoints)

            cur.execute('COMMIT')
        # TODO: Maybe use BTrees.family.intersection to get the common keys?
        # Or maybe use a SQLite Python function?
        # 'DELETE from object_state where is_stale(zoid, tid)'
        # What's fastest? The custom function version would require scanning the entire
        # table; unfortunately this sqlite interface doesn't allow defining virtual tables.
        with _timer() as trim_timer:
            # Delete anything we still know is stale, because we
            # saw a new TID for it, but we had nothing to replace it with.
            min_allowed_writeback = OidTMap()
            db.trim_to_size(self.limit, min_allowed_writeback)
            del min_allowed_writeback

        # Cleanups
        cur.close()
        del cur
        db.close() # closes the connection.
        del db
        del stored_oid_tid

        # We're probably shutting down, don't perform a GC; we see
        # that can get quite lengthy.

        end = time.time()
        stats = self.stats()
        mem_after = get_memory_usage()
        logger.info(
            "Wrote %d items to %s in %s "
            "(%s to fetch current, %s to insert batch (%d rows), %s to write, %s to trim). "
            "Used %s memory. (Storage: %s) "
            "Total hits %s; misses %s; ratio %s (stores: %s)",
            count_written, connection, end - begin,
            fetch_current - begin, batch_timer.duration, rows_inserted, exclusive_timer.duration,
            trim_timer.duration,
            byte_display(mem_after - mem_before), self._cache,
            stats['hits'], stats['misses'], stats['ratio'], stats['sets'])

        return count_written
