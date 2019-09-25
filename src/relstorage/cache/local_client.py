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
import collections
import threading
import time
import zlib

from contextlib import closing

from zope import interface

from relstorage._util import get_memory_usage
from relstorage._util import byte_display
from relstorage._util import timer as _timer
from relstorage._util import log_timed as _log_timed
from relstorage._compat import OidTMap_intersection
from relstorage._compat import OID_TID_MAP_TYPE as OidTMap
from relstorage._compat import iteroiditems
from relstorage.interfaces import Int

from relstorage.cache.interfaces import IStateCache
from relstorage.cache.interfaces import IPersistentCache
from relstorage.cache.interfaces import MAX_TID
from relstorage.cache.interfaces import CacheConsistencyError

from relstorage.cache.lru_cffiring import CFFICache

from relstorage.cache.persistence import sqlite_connect
from relstorage.cache.persistence import sqlite_files
from relstorage.cache.persistence import FAILURE_TO_OPEN_DB_EXCEPTIONS
from relstorage.cache.local_database import Database

logger = __import__('logging').getLogger(__name__)

# pylint:disable=too-many-lines

class ICachedValue(interface.Interface):
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

    def __mod__(tid):
        """
        Return the ``(state, tid)`` for the given TID.

        A special value of None matches any previously frozen TID.

        If no TID matches, returns None.

        We use the % operator because ``__getitem__`` was taken.
        """

    def __ilshift__(tid):
        """
        Mark the given TID, if it exists, as frozen.

        Returns a new value to store in the cache. If it returns None,
        this entry is removed from the cache (because the TID didn't match,
        and must have been older.)
        """

    def __iadd__(value):
        """
        Add the ``(state, tid)`` (another ICachedValue) to the list of
        cached entries. Return the new value to place in the cache.
        """

    def __isub__(tid):
        """
        Remove the tid, and anything older, from the list of cached entries.

        Return the new value to place in the cache. Return None if all values
        were removed and the cached entry should be removed.
        """

@interface.implementer(ICachedValue)
class _MultipleValues(list):
    __slots__ = ()

    frozen = False

    def __init__(self, *values):
        list.__init__(self, values)

    @property
    def weight(self):
        return sum(
            len(value[0])
            for value in self
            if value[0]
        )

    @property
    def max_tid(self):
        return max(x[1] for x in self)

    @property
    def newest_value(self):
        value = (None, -1)
        for entry in self:
            if entry[1] > value[1]:
                value = entry
        return value

    def __mod__(self, tid):
        for entry in self:
            entry = entry % tid
            if entry is not None:
                return entry

    def __ilshift__(self, tid):
        # If we have the TID, everything else should be older,
        # unless we just overwrote and haven't made the transaction visible yet.
        # By (almost) definition, nothing newer, but if there is, we shouldn't
        # drop it.
        # So this works like invalidation: drop everything older than the
        # tid; if we still have anything left, find and freeze the tid;
        # if that's the *only* thing left, return that, otherwise return ourself.
        to_save = [v for v in self if v[1] >= tid]
        if not to_save:
            return None

        if len(to_save) == 1:
            # One item, either it or not
            result = to_save[0]
            result <<= tid
            return result

        # Multiple items, possibly in the future.
        self[:] = to_save
        for i, entry in enumerate(self):
            if entry[1] == tid:
                self[i] <<= tid
                break
        return self

    def __iadd__(self, value):
        self.append(value)
        return self

    def __isub__(self, tid):
        to_save = [v for v in self if v[1] > tid]
        if not to_save:
            del self[:]
            return None

        if len(to_save) == 1:
            return to_save[0]
        self[:] = to_save
        return self


@interface.implementer(ICachedValue)
class _SingleValue(collections.namedtuple('_ValueBase', ('state_pickle', 'tid'))):
    __slots__ = ()
    # TODO: Maybe we should represent simple values as just byte strings;
    # the key will match the TID in that case.
    frozen = False

    @property
    def max_tid(self):
        return self[1]

    @property
    def newest_value(self):
        return self

    @property
    def weight(self):
        return len(self[0]) if self[0] else 0

    def __mod__(self, tid):
        if tid == self[1]:
            return self

    def __ilshift__(self, tid):
        # We could be newer
        if self[1] > tid:
            return self
        if tid == self[1]:
            return _FrozenValue(*self)
        # if we're older, fall off the end and discard.

    def __iadd__(self, value):
        if value == self:
            return value # Let us become frozen if desired.
        if value[1] == self[1] and value[0] != self[0]:
            raise CacheConsistencyError(
                "Detected two different values for same TID",
                self,
                value
            )
        return _MultipleValues(self, value)

    def __isub__(self, tid):
        if tid <= self[1]:
            return None
        return self

class _FrozenValue(_SingleValue):
    __slots__ = ()
    frozen = True

    def __mod__(self, tid):
        if tid in (None, self[1]):
            return self

    def __ilshift__(self, tid):
        # This method can get called if two different transaction views
        # tried to load an object at the same time and store it in the cache.
        if tid == self[1]:
            return self


@interface.implementer(IStateCache,
                       IPersistentCache)
class LocalClient(object):
    # pylint:disable=too-many-public-methods,too-many-instance-attributes

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

    _hits = 0
    _misses = 0
    _sets = 0
    _cache = None
    _cache_type = CFFICache

    # Things copied from self._cache
    _peek = None
    _cache_mru = None

    def __init__(self, options,
                 prefix=None):
        self._lock = threading.Lock()
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

        # The {oid: tid} that we read from the cache.
        # These are entries that we know are there, and if we see them
        # change, we need to be sure to update that in the database,
        # *even if they are evicted* and we would otherwise lose
        # knowledge of them before we save. We do this by watching incoming
        # TIDs; only if they were already in here do we continue to keep track.
        # At write time, if we can't meet the requirement ourself, we at least
        # make sure there are no stale entries in the cache database.
        self._min_allowed_writeback = OidTMap()

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
        return self._cache.size

    def __len__(self):
        """
        How many distinct OIDs are stored.
        """
        return len(self._cache)

    def __iter__(self):
        for oid, lru_entry in iteroiditems(self._cache.data):
            value = lru_entry.value
            if isinstance(value, _MultipleValues):
                for entry in value:
                    yield (oid, entry[1])
            else:
                yield (oid, value[1])

    def keys(self):
        return self._cache.data.keys()

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
        if options.cache_local_dir and self.size:
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
                conn = sqlite_connect(options, self.prefix, close_async=False)
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
            conn = sqlite_connect(options, self.prefix, close_async=False)
            with closing(conn):
                db = Database.from_connection(conn)
                count_removed = db.remove_invalid_persistent_oids(bad_oids)

        logger.debug("Removed %d invalid OIDs from %s", count_removed, conn)

    def zap_all(self):
        _, destroy = sqlite_files(self.options, self.prefix)
        destroy()

    @staticmethod
    def key_weight(_):
        # All keys are equally weighted, and we don't count them.
        return 0

    @staticmethod
    def value_weight(value):
        # Values are the (state, actual_tid) pairs, or lists of the same, and their
        # weight is the size of the state
        return value.weight

    def flush_all(self):
        with self._lock:
            self._cache = self._cache_type(
                self.limit,
                key_weight=self.key_weight,
                value_weight=self.value_weight,
                empty_value=_SingleValue(b'', 0)
            )
            self._peek = self._cache.peek
            self._cache_mru = self._cache.__getitem__
            self._min_allowed_writeback = OidTMap()
            self.reset_stats()

    def reset_stats(self):
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self._aged_at = 0
        self._next_age_at = 1000

    def stats(self):
        total = self._hits + self._misses
        return {
            'hits': self._hits,
            'misses': self._misses,
            'sets': self._sets,
            'ratio': self._hits / total if total else 0,
            'len': len(self),
            'bytes': self.size,
            'lru_stats': self._cache.stats(),
        }

    def __contains__(self, oid_tid):
        oid, tid = oid_tid
        entry = self._peek(oid)
        return entry is not None and entry % tid is not None

    def get(self, oid_tid, update_mru=True):
        oid, tid = oid_tid
        assert tid is None or tid >= 0
        decompress = self._decompress
        value = None

        with self._lock:
            entry = self._peek(oid)
            if entry is not None:
                value = entry % tid
            if value is not None:
                self._hits += 1
                if update_mru:
                    self._cache_mru(oid) # Make an actual hit.
            else:
                self._misses += 1

        # Finally, while not holding the lock, decompress if needed.
        # Recall that for deleted objects, `state` can be None.
        if value is not None:
            state, tid = value
            return decompress(state) if state else state, tid

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
        # cache entries
        age_period = self._age_factor * len(self._cache)
        operations = self._hits + self._sets
        if operations - self._aged_at < age_period:
            self._next_age_at = age_period
            return
        if self.size < self.limit:
            return

        self._aged_at = operations
        now = time.time()
        logger.debug("Beginning frequency aging for %d cache entries",
                     len(self._cache))
        self._cache.age_lists()
        done = time.time()
        logger.debug("Aged %d cache entries in %s", len(self._cache), done - now)

        self._next_age_at = int(self._aged_at * 1.5) # in case the dict shrinks

        return self._aged_at

    def __setitem__(self, oid_tid, state_bytes_tid):
        if not self.limit:
            # don't bother
            return

        # This used to allow non-byte values, but that's confusing
        # on Py3 and wasn't used outside of tests, so we enforce it.
        # A state of 'None' happens for undone transactions.
        oid, key_tid = oid_tid
        state_bytes, actual_tid = state_bytes_tid

        assert isinstance(state_bytes, bytes) or state_bytes is None, type(state_bytes)
        # Really key_tid should be > 0; we allow >= for tests.
        assert key_tid == actual_tid and key_tid >= 0
        self.__set_many([
            (oid, key_tid, state_bytes)
        ])

    def __set_many(self, oid_tid_state_iter):
        if not self.limit:
            # don't bother
            return

        compress = self._compress
        peek = self._peek
        value_limit = self._value_limit
        min_allowed = self._min_allowed_writeback
        lock = self._lock
        store = self._cache.__setitem__
        sets = 0

        for oid, tid, state_bytes in oid_tid_state_iter:
            # A state of 'None' happens for undone transactions.
            state_bytes = compress(state_bytes) if compress else state_bytes # pylint:disable=not-callable

            if state_bytes and len(state_bytes) >= value_limit:
                # This value is too big, so don't cache it.
                continue

            value = _SingleValue(state_bytes, tid)

            with lock:
                existing = peek(oid)
                if existing:
                    existing += value
                    value = existing

                store(oid, value) # possibly evicts

                if tid > min_allowed.get(oid, MAX_TID):
                    min_allowed[oid] = tid

                sets += 1

        with lock:
            self._sets += sets
            # Do we need to move this up above the eviction choices?
            # Inline some of the logic about whether to age or not; avoiding the
            # call helps speed
            if self._hits + self._sets > self._next_age_at:
                self._age()

    def set_all_for_tid(self, tid_int, state_oid_iter):
        self.__set_many((
            (oid_int, tid_int, state)
            for (state, oid_int, _)
            in state_oid_iter
        ))

    def __delitem__(self, oid_tid):
        self.delitems({oid_tid[0]: oid_tid[1]})

    def delitems(self, oids_tids):
        """
        For each OID/TID pair in the items, remove all cached values
        for OID that are older than TID.
        """
        peek = self._peek
        replace = self._cache.replace_or_remove_smaller_value
        min_allowed = self._min_allowed_writeback
        with self._lock:
            for oid, expected_tid in iteroiditems(oids_tids):
                entry = peek(oid)
                if entry is not None:
                    entry -= expected_tid
                    replace(oid, entry)
                if expected_tid > min_allowed.get(oid, MAX_TID):
                    min_allowed[oid] = expected_tid

    def invalidate_all(self, oids):
        min_allowed = self._min_allowed_writeback
        peek = self._peek
        delitem = self._cache.__delitem__
        with self._lock:
            for oid in oids:
                entry = peek(oid)
                if entry is not None:
                    delitem(oid)
                    tid = entry.max_tid
                    if tid > min_allowed.get(oid, MAX_TID):
                        min_allowed[oid] = tid

    def freeze(self, oids_tids):
        # The idea is to *move* the data, or make it available,
        # *without* copying it.
        replace = self._cache.replace_or_remove_smaller_value
        peek = self._peek
        with self._lock:
            # This shuffles them around the LRU order. We probably don't actually
            # want to do that.
            for oid, tid in iteroiditems(oids_tids):
                entry = peek(oid)
                if entry is not None:
                    entry <<= tid
                    replace(oid, entry)

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
        Insert all the ``(key, value)`` pairs found in *keys_and_values*.

        This will permute the most-recently-used status of any existing entries.
        Entries in the *keys_and_values* iterable should be returned from
        least recent to most recent, as the items at the end will be considered to be
        the most recent. (Alternately, you can think of them as needing to be in order
        from lowest priority to highest priority.)

        This will never evict existing entries from the cache.
        """
        now = time.time()
        mem_usage_before = mem_usage_before if mem_usage_before is not None else get_memory_usage()
        mem_usage_before_this = get_memory_usage()
        log_count = log_count or len(keys_and_values)

        data = self._cache

        if data:
            # Loading more data into an existing bucket.
            # Load only the *new* keys, trying to get the newest ones
            # because LRU is going to get messed up anyway.
            #
            # If we were empty, then take what they give us, LRU
            # first, so that as we iterate the last item in the list
            # becomes the MRU item.
            new_entries_newest_first = [t for t in keys_and_values
                                        if t[0] not in data]
            new_entries_newest_first.reverse()
            keys_and_values = new_entries_newest_first
            del new_entries_newest_first

        stored = data.add_MRUs(
            keys_and_values,
            return_count_only=True)

        then = time.time()
        del keys_and_values # For memory reporting.
        mem_usage_after = get_memory_usage()
        logger.info(
            "Examined %d and stored %d items from %s in %s using %s. "
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
        # should go in our cache. However, the order matters: The DB gives them to us in
        # priority order, and those are the ones we want to add to the memory cache.
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
            for oid, _, state, actual_tid in db.list_rows_by_priority():
                size += len(state)
                if size > limit:
                    break
                items.append((oid, _FrozenValue(state, actual_tid)))

            items.reverse()
            return items

        # In the large benchmark, this is 25% of the total time.
        # 18% of the total time is preallocating the entry nodes.
        self._bulk_update(fetch_and_filter_rows(),
                          source=connection,
                          log_count=len(self._min_allowed_writeback),
                          mem_usage_before=mem_before)
        return checkpoints

    def _items_to_write(self, object_index, stored_oid_tid):
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
        self._min_allowed_writeback.update(object_index)
        pop_min_required_tid = self._min_allowed_writeback.pop
        get_min_required_tid = self._min_allowed_writeback.get # pylint:disable=no-member
        get_current_oid_tid = stored_oid_tid.get
        min_allowed_count = 0
        matching_tid_count = 0
        # When we accumulate all the rows here before returning them,
        # this function shows as about 3% of the total time to save
        # in a very large database.
        with _timer() as t:
            for oid, lru_entry in iteroiditems(self._cache.data):
                newest_value = lru_entry.value.newest_value
                # We must have something at least this fresh
                # to consider writing it out
                actual_tid = newest_value[1]
                min_allowed = get_min_required_tid(oid, -1)
                if min_allowed > actual_tid:
                    min_allowed_count += 1
                    continue

                # If we have something >= min_allowed, matching
                # what's in the database, or even older (somehow),
                # it's not worth writing to the database (states should be identical).
                current_tid = get_current_oid_tid(oid, -1)
                if current_tid >= actual_tid:
                    matching_tid_count += 1
                    continue

                yield (oid, actual_tid, newest_value.frozen, newest_value[0], lru_entry.frequency)

                # We're able to satisfy this, so we don't need to consider
                # it in our min_allowed set anymore.
                # XXX: This isn't really the right place to do this.
                # Move this and write a test.
                pop_min_required_tid(oid, None)

        removed_entry_count = matching_tid_count + min_allowed_count
        logger.info(
            "Examined %d entries and rejected %d "
            "(rejected stale: %d; already in db: %d; might prune: %d) in %s",
            all_entries_len, removed_entry_count,
            min_allowed_count, matching_tid_count,
            len(self._min_allowed_writeback),
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
            count_written, _ = db.store_temp(self._items_to_write(object_index, stored_oid_tid))
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
            oids_in_both = OidTMap_intersection(self._min_allowed_writeback, stored_oid_tid)
            get_min_required = self._min_allowed_writeback.__getitem__
            get_stored = stored_oid_tid.__getitem__
            for oid in oids_in_both:
                min_required_tid = get_min_required(oid)
                stored_tid = get_stored(oid)
                if stored_tid < min_required_tid:
                    min_allowed_writeback[oid] = min_required_tid
            logger.debug(
                "Comparing %d DB entries against %d known minimums; "
                "(overlap: %d) "
                "found %d to remove",
                len(stored_oid_tid), len(self._min_allowed_writeback),
                len(oids_in_both),
                len(min_allowed_writeback)
            )
            db.trim_to_size(self.limit, min_allowed_writeback)
            del min_allowed_writeback
        # Typically we write when we're closing and don't expect to do
        # anything else, so there's no need to keep tracking this info.
        self._min_allowed_writeback = OidTMap()

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
