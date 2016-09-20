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

import logging

import time

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


from .lru import SizedLRU
from .lru import ProtectedLRU
from .lru import ProbationLRU
from .lru import EdenLRU

log = logging.getLogger(__name__)


class SizedLRUMapping(object):
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


        self._protected = ProtectedLRU(int(limit * self._gen_protected_pct))
        self._probation = ProbationLRU(int(limit * self._gen_probation_pct),
                                        self._protected,
                                        self._dict)
        self._eden = EdenLRU(int(limit * self._gen_eden_pct),
                             self._probation,
                             self._protected,
                             self._dict)
        self._gens = [None, None, None, None] # 0 isn't used
        for x in (self._protected, self._probation, self._eden):
            self._gens[x.PARENT_CONST] = x
        self._gens = tuple(self._gens)
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self.limit = limit
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
        SizedLRU.age_lists(self._eden, self._probation, self._protected)
        done = time.time()
        log.debug("Aged %d cache entries in %s", done - now)

        self._next_age_at = int(self._aged_at * 1.5) # in case the dict shrinks

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
            self._gens[entry.cffi_ring_node.r_parent].update_MRU(entry, value)
        else:
            lru = self._eden
            entry = lru.add_MRU(key, value)
            dct[key] = entry

        self._sets += 1

        # Do we need to move this up above the eviction choices?
        # Inline some of the logic about whether to age or not; avoiding the
        # call helps speed
        if self._hits + self._sets > self._next_age_at:
            self._age()

        return True

    def __contains__(self, key):
        return key in self._dict

    def __delitem__(self, key):
        entry = self._dict[key]
        del self._dict[key]
        self._gens[entry.cffi_ring_node.r_parent].remove(entry)

    def get_and_bubble_all(self, keys):
        dct = self._dict
        gens = self._gens
        res = {}
        for key in keys:
            entry = dct.get(key)
            if entry is not None:
                self._hits += 1
                gens[entry.cffi_ring_node.r_parent].on_hit(entry)
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

        # XXX: Together with only writing what will fit in the protected space,
        # is this optimal? One of the goals is to speed up startup, which may access
        # objects that are never or rarely used again. They'll tend to wind up in
        # the probation space over time, or at least have a very low frequency.
        # Maybe we shouldn't prevent writing aged items, and maybe we should fill up
        # probation and eden too. We probably want to allow the user to specify
        # a size limit at this point.

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
