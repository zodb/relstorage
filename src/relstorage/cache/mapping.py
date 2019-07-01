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
import operator
import time

from relstorage._util import get_memory_usage
from relstorage._util import byte_display
from relstorage.cache.lru_cffiring import CFFICache
from relstorage.cache.persistence import Pickler
from relstorage.cache.persistence import Unpickler

log = logging.getLogger(__name__)


class SizedLRUMapping(object):
    """
    A map that keeps a record of its approx. weight (size), evicting
    the least useful items when that size is exceeded.

    Keys and values can be arbitrary, but should be of homogeneous
    types that cannot be self-referential; immutable objects like
    tuples, ints, and strings are best.

    In order for this class to properly handle evicting values when it
    gets too big, it must be able to determine the size of the keys
    and values. If :func:`len` is not appropriate for this, supply
    your own *key_weight* and *value_weight* functions.

    This class is not threadsafe, accesses to :meth:`__setitem__` and
    :meth:`get_and_bubble_all` must be protected by a lock.
    """

    # What multiplier of the number of items in the cache do we apply
    # to determine when to age the frequencies?
    _age_factor = 10

    # When did we last age?
    _aged_at = 0

    _cache_type = CFFICache

    def __init__(self, weight_limit,
                 key_weight=len, value_weight=len):
        self._cache = self._cache_type(weight_limit, key_weight, value_weight)
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self.limit = weight_limit
        self._next_age_at = 1000

        # Copy some methods for speed
        self.entries = self._cache.entries
        self.peek = self._cache.peek

    def keys(self):
        return iter(self._cache)

    # Backwards compatibility for testing
    @property
    def _eden(self):
        return self._cache.eden

    @property
    def _protected(self):
        return self._cache.protected

    @property
    def _probation(self):
        return self._cache.probation
    # End BWC

    @property
    def size(self):
        return self._cache.size

    def reset_stats(self):
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self._aged_at = 0
        self._next_age_at = 0

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

    def __len__(self):
        return len(self._cache)

    def __iter__(self):
        return iter(self._cache)

    def __repr__(self):
        return "<%s at %x size=%d limit=%d len=%d hit_ratio=%d cache=%r>" % (
            self.__class__.__name__, id(self),
            self.size, self.limit, len(self), self.stats()['hits'],
            self._cache
        )

    def values(self):
        for entry in self.entries():
            yield entry.value

    def items(self):
        for entry in self.entries():
            yield (entry.key, entry.value)

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
        log.debug("Beginning frequency aging for %d cache entries",
                  len(self._cache))
        self._cache.age_lists()
        done = time.time()
        log.debug("Aged %d cache entries in %s", len(self._cache), done - now)

        self._next_age_at = int(self._aged_at * 1.5) # in case the dict shrinks

        return self._aged_at

    def __setitem__(self, key, value):
        """
        Set an item.

        If the memory limit would be exceeded, remove old items until
        that is no longer the case.

        If we need to age popularity counts, do so.

        The item is considered to be the most-recently-used item
        (because this is called in the event of a cache miss, when
        we needed the item).

        """
        self._cache[key] = value
        self._sets += 1

        # Do we need to move this up above the eviction choices?
        # Inline some of the logic about whether to age or not; avoiding the
        # call helps speed
        if self._hits + self._sets > self._next_age_at:
            self._age()

        return True

    def __contains__(self, key):
        return key in self._cache

    def __delitem__(self, key):
        # The underlying bucket may or may not throw a key error
        try:
            del self._cache[key]
        except KeyError:
            pass

    def invalidate_all(self, oids):
        # The SQL mapping could do this much more efficiently
        to_remove = [k for k in self.keys() if k[0] in oids]
        for k in to_remove:
            try:
                del self._cache[k]
            except KeyError:
                pass
        return to_remove

    def get_and_bubble_all(self, keys):
        # This is only used in testing now, the higher levels
        # use `get_from_key_or_backup_key`
        cache = self._cache
        res = {}
        for key in keys:
            value = cache[key]
            if value is not None:
                res[key] = value

        self._hits += len(res)
        self._misses += len(keys) - len(res)
        return res

    def get_from_key_or_backup_key(self, pref_key, backup_key):
        value = self._cache.get_from_key_or_backup_key(pref_key, backup_key)

        if value is None:
            self._misses += 1
        else:
            self._hits += 1
        return value

    def __getitem__(self, key):
        value = self._cache[key]
        if value is None:
            raise KeyError(key)
        return value


    # See micro_benchmark_results.rst for a discussion about the approach.

    _FILE_VERSION = 5

    def read_from_stream(self, cache_file):
        # Unlike write_to_stream, using the raw stream
        # is fine for both Py 2 and 3.
        mem_usage_before = get_memory_usage()
        unpick = Unpickler(cache_file)

        # Local optimizations
        load = unpick.load

        version = load()
        if version != self._FILE_VERSION: # pragma: no cover
            raise ValueError("Incorrect version of cache_file")

        keys_and_values = []
        try:
            while 1:
                k_v = load()
                keys_and_values.append(k_v)
        except EOFError:
            pass

        # Reclaim memory
        del load
        del unpick

        return self.bulk_update(keys_and_values, cache_file, mem_usage_before=mem_usage_before)

    def bulk_update(self, keys_and_values,
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

        stored = data.add_MRUs(keys_and_values, return_count_only=True)

        then = time.time()
        del keys_and_values # For memory reporting.
        mem_usage_after = get_memory_usage()
        log.info(
            "Examined %d and stored %d items from %s in %s using %s. "
            "(%s local) (%s)",
            log_count, stored, getattr(source, 'name', source),
            then - now,
            byte_display(mem_usage_after - mem_usage_before),
            byte_display(mem_usage_after - mem_usage_before_this),
            self)
        return log_count, stored

    def items_to_write(self,
                       byte_limit=None):
        """
        Return an iterator of entry objects.

        The items are returned in **reverse** frequency order, the
        ones with the highest frequency (most used) being last in the
        list. (Unless you specify *sort* to be false, in which case
        the order is not specified.)
        """
        entries = self.entries()

        # Adding key as a tie-breaker makes no sense, and is slow.
        # We use an attrgetter directly on the node for speed
        frequency_getter = operator.attrgetter('cffi_entry.frequency')
        entries = sorted(entries, key=frequency_getter)

        # Write up to the byte limit
        if byte_limit:
            # They provided us a byte limit. Our normal approach of
            # writing LRU won't work, because we'd wind up chopping off
            # the most frequent items! So first we begin by taking out
            # everything until we fit.
            bytes_written = 0
            entries_to_write = []
            for entry in reversed(entries):
                bytes_written += entry.weight
                if bytes_written > byte_limit:
                    bytes_written -= entry.weight
                    break
                entries_to_write.append(entry)
            # Now we can write in reverse popularity order
            entries_to_write.reverse()
            entries = entries_to_write
            bytes_written = 0
            del entries_to_write

        return entries

    def write_to_stream(self, cache_file, byte_limit=None, pickle_fast=False):
        # give *pickle_fast* as True if you know you don't need the pickle memo.
        now = time.time()
        # pickling the items is about 3x faster than marshal


        # Under Python 2, (or generally, under any pickle protocol
        # less than 4, when framing was introduced) whether we are
        # writing to an io.BufferedWriter, a <file> opened by name or
        # fd, with default buffer or a large (16K) buffer, putting the
        # Pickler directly on top of that stream is SLOW for large
        # single objects. Writing a 512MB dict takes ~40-50seconds. If
        # instead we use a BytesIO to buffer in memory, that time goes
        # down to about 7s. However, since we switched to writing many
        # smaller objects, that need goes away.
        pickler = Pickler(cache_file, -1) # Highest protocol
        if pickle_fast:
            pickler.fast = True
        dump = pickler.dump

        dump(self._FILE_VERSION) # Version marker

        # Dump all the entries in increasing order of popularity (so
        # that when we read them back in the least popular items end
        # up LRU).

        # Note that we write the objects, regardless of frequency. We
        # don't age them here, either. This is one of the goals of the
        # cache is to speed up startup, which (during initialization)
        # may access objects that are never or rarely used again.
        # They'll tend to wind up in the probation space over time, or
        # at least have a very low frequency. But if they're still here,
        # go ahead and write them.

        # Also note that we *do not* try to preserve the frequency in the cache file.
        # If we did, that would penalize new entries that the new process creates. It's
        # workload may be very different than the one that wrote this cache file. Allow
        # the new process to build up its own frequencies.

        # Also note that entries with the same frequency are stored in the order of iteration.
        # Sorting is guaranteed to be stable, so this means that MRU of the same frequency comes
        # before less recently used.

        # We get the entries from our MRU lists (in careful order) rather than from the dict
        # so that we have stable iteration order regardless of PYTHONHASHSEED or insertion order.
        entries = self.items_to_write(byte_limit)
        bytes_written = 0
        count_written = 0
        for entry in entries:
            k = entry.key
            v = entry.value
            weight = entry.weight
            bytes_written += weight
            count_written += 1
            dump((k, v))

        then = time.time()
        stats = self.stats()
        log.debug("Wrote %d items (%d bytes) to %s in %s. Total hits %s; misses %s; ratio %s",
                  count_written, bytes_written, getattr(cache_file, 'name', cache_file),
                  then - now,
                  stats['hits'], stats['misses'], stats['ratio'])

        return count_written
