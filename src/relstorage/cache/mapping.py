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

from relstorage._compat import iteritems
from relstorage._compat import itervalues
from relstorage._compat import iterkeys
from relstorage._compat import get_memory_usage
from relstorage.cache.cache_ring import Cache
from relstorage.cache.persistence import Pickler
from relstorage.cache.persistence import Unpickler
from relstorage.cache.interfaces import CacheCorruptedError

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

    When items are evicted as a result of exceeding this object's
    configured *weight_limit*, the method :meth:`handle_evicted_items`
    method is called. This method can be replaced on an instance with
    an attribute to handle the evicted items; by default we do
    nothing. Methods that can evict objects are documented as such.

    This class is not threadsafe, accesses to :meth:`__setitem__` and
    :meth:`get_and_bubble_all` must be protected by a lock.
    """

    # What multiplier of the number of items in the cache do we apply
    # to determine when to age the frequencies?
    _age_factor = 10

    # When did we last age?
    _aged_at = 0

    _cache_type = Cache

    def __init__(self, weight_limit, key_weight=len, value_weight=len):
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
        cache = self._cache = self._cache_type(weight_limit, key_weight, value_weight)
        self._dict = cache.data

        self._protected = cache.protected
        self._probation = cache.probation
        self._eden = cache.eden
        self._gens = cache.generations

        self._hits = 0
        self._misses = 0
        self._sets = 0
        self.limit = weight_limit
        self._next_age_at = 1000

    @property
    def size(self):
        return self._eden.size + self._protected.size + self._probation.size

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
            'size': len(self._dict),
            'bytes': self.size,
            'eden_stats': self._eden.stats(),
            'prot_stats': self._protected.stats(),
            'prob_stats': self._probation.stats(),
        }

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __repr__(self):
        return "<%s at %x size=%d limit=%d len=%d hit_ratio=%d>" % (
            self.__class__.__name__, id(self),
            self.size, self.limit, len(self), self.stats()['hits']
        )

    def values(self):
        for entry in itervalues(self._dict):
            yield entry.value

    def items(self):
        for k, entry in iteritems(self._dict):
            yield k, entry.value

    def keys(self):
        return iterkeys(self._dict)

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
        dct = self._dict
        age_period = self._age_factor * len(dct)
        operations = self._hits + self._sets
        if operations - self._aged_at < age_period:
            self._next_age_at = age_period
            return
        if self.size < self.limit:
            return

        self._aged_at = operations
        now = time.time()
        log.debug("Beginning frequency aging for %d cache entries",
                  len(dct))
        self._cache.age_lists()
        done = time.time()
        log.debug("Aged %d cache entries in %s", len(dct), done - now)

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

        This operation may evict existing items. If it does, they are
        passed to the :meth:`handle_evicted_items` method.
        """
        dct = self._dict

        if key in dct:
            entry = dct[key]
            # This bumps its frequency, and potentially ejects other items.
            evicted_items = self._gens[entry.cffi_entry.r_parent].update_MRU(entry, value)
        else:
            # New values have a frequency of 1 and might evict other
            # items.
            lru = self._eden
            entry, evicted_items = lru.add_MRU(key, value)
            dct[key] = entry

        if evicted_items:
            self.handle_evicted_items(evicted_items)
        self._sets += 1

        # TODO: Notifications about evicted keys

        # Do we need to move this up above the eviction choices?
        # Inline some of the logic about whether to age or not; avoiding the
        # call helps speed
        if self._hits + self._sets > self._next_age_at:
            self._age()

        return True

    def handle_evicted_items(self, items):
        """Does nothing."""

    def __contains__(self, key):
        return key in self._dict

    def __delitem__(self, key):
        entry = self._dict[key]
        del self._dict[key]
        self._gens[entry.cffi_entry.r_parent].remove(entry)

    def get_and_bubble_all(self, keys):
        # This is only used in testing now, the higher levels
        # use `get_from_key_or_backup_key`
        dct = self._dict
        gens = self._gens
        res = {}
        for key in keys:
            entry = dct.get(key)
            if entry is not None:
                gens[entry.cffi_entry.r_parent].on_hit(entry)
                res[key] = entry.value

        self._hits += len(res)
        self._misses += len(keys) - len(res)
        return res

    def get_from_key_or_backup_key(self, pref_key, backup_key):
        dct = self._dict

        entry = dct.get(pref_key)
        at_backup = False
        if entry is None and backup_key is not None:
            at_backup = True
            entry = dct.get(backup_key)

        if entry is None:
            self._misses += 1
            return

        # TODO: Tests specifically for this.
        self._hits += 1
        result = entry.value
        if not at_backup:
            self._gens[entry.cffi_entry.r_parent].on_hit(entry)
        else:
            # Move the backup key to the current key.
            # Compensate for what we're about to do,
            # we don't want this to show up as a set in our
            # stats, and it shouldn't cause anything to be evicted
            self._sets -= 1
            # TODO: We could probably more directly use this entry
            # object. As it is, it goes back on the freelist
            del entry
            del self[backup_key]
            self[pref_key] = result
        return result


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

        log_count = log_count or len(keys_and_values)

        data = self._dict

        if data:
            # Loading more data into an existing bucket.
            # Load only the *new* keys, trying to get the newest ones
            # because LRU is going to get messed up anyway.
            #
            # If we were empty, then take what they give us, LRU
            # first, so that as we iterate the last item in the list
            # becomes the MRU item.
            new_entries_newest_first = [t for t in keys_and_values
                                        if t[0] not in self._dict]
            new_entries_newest_first.reverse()
            keys_and_values = new_entries_newest_first

        added_entries = self._eden.add_MRUs(keys_and_values)
        stored = len(added_entries)
        for e in added_entries:
            # XXX: Why doesn't eden.add_MRUs do this? It has access
            # to the data dictionary.
            assert e.key not in data, (e.key, e)
            assert e.cffi_entry.r_parent, e.key
            data[e.key] = e

        then = time.time()
        del keys_and_values # For memory reporting.
        del added_entries
        mem_usage_after = get_memory_usage()
        log.debug(
            "Examined %d and stored %d items from %s in %s using %s bytes.",
            log_count, stored, getattr(source, 'name', source),
            then - now, mem_usage_after - mem_usage_before)
        return log_count, stored

    def items_to_write(self, byte_limit=None, sort=True):
        """
        Return an sequence of ``(key, value, total_weight, frequency, generation)`` pairs.

        The items are returned in **reverse** frequency order, the ones
        with the highest frequency (most used) being last in the list. (Unless you specify *sort*
        to be false, in which case the order is not specified.)
        """
        entries = list(self._probation)
        entries.extend(self._protected)
        entries.extend(self._eden)

        if len(entries) != len(self._dict): # pragma: no cover
            raise CacheCorruptedError(
                "Cache consistency problem. There are %d ring entries and %d dict entries. "
                "Refusing to write." % (
                    len(entries), len(self._dict)))

        # Adding key as a tie-breaker makes no sense, and is slow.
        # We use an attrgetter directly on the node for speed
        if sort:
            frequency_getter = operator.attrgetter('cffi_entry.frequency')
            entries.sort(key=frequency_getter)

        # Write up to the byte limit
        if byte_limit:
            # They provided us a byte limit. Our normal approach of
            # writing LRU won't work, because we'd wind up chopping off
            # the most frequent items! So first we begin by taking out
            # everything until we fit.
            bytes_written = 0
            entries_to_write = []
            for entry in reversed(entries):
                bytes_written += entry.len
                if bytes_written > byte_limit:
                    bytes_written -= entry.len
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
            weight = entry.len
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
