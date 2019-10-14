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


from hamcrest import assert_that
from nti.testing.matchers import validly_provides

# The overhead of cache values, and thus how much fits in a ring or the
# cache, depends on 32 or 64 bit, whether or not we copy
# bytes into native code std::string, and how the compiler lays out
# the objects. We only copy strings on PyPy, but we have little control
# over the object layout, especially with the various MSVC compilers
# we have to deal with. So that explains the tests that have a range of sizes.

from relstorage.tests import TestCase
from relstorage.cache import interfaces
from . import Cache

class GenerationTests(TestCase):

    def _makeCache(self, limit):
        from . import Cache as BaseCache
        return BaseCache(limit)

    def _makeOne(self, limit):
        return self._makeCache(limit).eden

    def test_bool(self):
        cache = self._makeCache(100)
        lru = cache.eden
        self.assertFalse(lru)

        cache[1] = (b'', 0)
        self.assertTrue(lru)
        self.assertEqual(1, len(lru))

        del cache[1]
        self.assertFalse(lru)

class EdenTests(TestCase):

    def _makeOne(self, limit):
        from . import Cache as BaseCache
        return BaseCache(limit)

    def test_add_MRUs_empty(self):
        lru = self._makeOne(100)
        self.assertEqual((), lru.add_MRUs([]))

    def test_add_MRUs_too_many(self):
        lru = self._makeOne(1000)
        too_many = [(i, (b'a' * i, 0, 0, 1)) for i in range(50)]
        # They just exceed the limit
        added = lru.add_MRUs(too_many)
        # Much less got added
        self.assertGreaterEqual(len(added), 7)
        self.assertLessEqual(len(added), 9)

class NoOverheadSizeCache(Cache):
    def __init__(self, byte_limit):
        Cache.__init__(self, byte_limit)
        self.base_size = self.weight
        self[0] = (b'', 0)
        self.entry_size = self[0].weight
        del self[0]

    @property
    def weight(self):
        weight = super(NoOverheadSizeCache, self).size
        weight -= self.base_size
        weight -= self.entry_size * len(self)
        return weight

    @property
    def size(self):
        return self.weight

    def __getitem__(self, oid):
        entry = Cache.__getitem__(self, oid)
        if entry is not None:
            return self.get_item_with_tid(oid, entry.tid)


class GenericLRUCacheTests(TestCase):
    """
    Generic LRU caching tests that can be applied to any
    LRU implementation, using the kind of keys and
    values that we actually use: ``(oid_int, tid_int)`` and
    ``(state_bytes, tid_int)``.
    """

    def _getClass(self):
        return NoOverheadSizeCache

    def _makeOne(self, limit, kind=None):
        kind = kind or self._getClass()
        return kind(limit)

    def _getIface(self):
        return interfaces.ILRUCache

    def test_implements(self):
        cache = self._makeOne(100)
        assert_that(cache,
                    validly_provides(self._getIface()))
        return cache

    def test_eden_implements(self):
        cache = self._makeOne(100)
        assert_that(cache.eden,
                    validly_provides(interfaces.IGeneration))

    def test_item_implements(self):
        cache = self._makeOne(20)
        cache[1] = (b'', 0)
        entrya = cache[1]
        assert_that(entrya, validly_provides(interfaces.ILRUEntry))

    def test_add_too_many(self):
        class _Cache(self._getClass()):
            pass
        cache = _Cache(20 + _Cache(20).base_size + (_Cache(20).entry_size * 2))

        entry_count = 10
        entries = cache.add_MRUs(list(reversed([
            # oid, state, frozen, frequency
            (x, (b'abcde', 0, False, x))
            for x
            in range(1, entry_count)
        ])))

        self.assertEqual(
            [5 + cache.entry_size] * len(entries),
            [e.weight for e in entries])
        self.assertLessEqual(
            cache.weight,
            cache.limit
        )
        self.assertEqual(
            [e.key for e in entries],
            [e.frequency for e in entries])
        self.assertEqual(
            [8, 7, 6, 5],
            [e.key for e in entries]
        )
        self.assertEqual(4, len(cache))
        return cache

    def test_age(self):
        cache = self._makeOne(100)
        base_size = cache.base_size
        entry_size = cache.entry_size
        cache = self._getClass()(100 + base_size + entry_size)


        entries = cache.add_MRUs([
            (1, (b'abcde', 0, False, 1)),
            (2, (b'abcde', 0, False, 1)),
            (3, (b'abcde', 0, False, 1)),
            (0, (b'abcde', 0, False, 1)),
        ])

        self.assertIn(
            [e.key for e in entries],
            ([1, 2, 3, 0], [2, 3, 0])
        )

        for _ in range(4):
            for e in entries:
                _ = cache[e.key]

        freqs = [e.frequency for e in cache.values()]
        self.assertEqual([5] * len(entries), freqs)

        # By half each time
        cache.age_frequencies()
        freqs = [e.frequency for e in cache.values()]
        self.assertEqual([2] * len(entries), freqs)
        return cache


    def test_delete(self):
        cache = self._makeOne(20)
        cache[1] = (b'abc', 0)
        self.assertIn(1, cache)
        self.assertEqual(1, len(cache))
        self.assertEqual(3, cache.size)
        self.assertEqual(cache[1], (b'abc', 0))
        self.assertEqual(list(cache), [(1, 0)])
        del cache[1]
        self.assertNotIn(1, cache)
        self.assertEqual(0, len(cache))
        self.assertEqual(0, cache.size)
        self.assertIsNone(cache[1])
        self.assertEqual(list(cache), [])

    def test_entries(self):
        cache = self._makeOne(20)
        cache[1] = (b'abc', 0)
        entries = list(cache.values())
        self.assertEqual(1, len(entries))
        entry = entries[0]
        assert_that(entry, validly_provides(interfaces.ILRUEntry))
        self.assertEqual(1, entry.key)
        self.assertEqual(b'abc', entry.value)
        self.assertEqual(1, entry.frequency)

        # Getting it again updates its frequency, not
        # necessarily on the same object though.
        self.assertIsNotNone(cache[1])
        entries = list(cache.values())
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual(1, entry.key)
        self.assertEqual(b'abc', entry.value)
        self.assertEqual(2, entry.frequency)

    def test_add_too_many_MRUs_works_aronud_big_entry(self):
        cache = self._getClass()(20)
        base_size = cache.base_size
        entry_size = cache.entry_size
        cache = self._getClass()(40 + base_size + entry_size)

        entries = cache.add_MRUs([
            (1, (b'abc', 0, False, 1)),
            # This entry itself will fit nowhere
            (2, (b'12345678901234567890' * 20, 0, False, 1)),
            (3, (b'bcd', 0, False, 1)),
            (4, (b'cde', 0, False, 1)),
            (5, (b'dehi', 0, False, 1)),
            (6, (b'edghijkl', 0, False, 1)),
        ])
        self.assertGreaterEqual(len(cache), 3)
        self.assertLessEqual(len(cache), 4)

        self.assertIn(
            [e.key for e in entries],
            ([1, 3, 4, 5], [3, 4, 5]))
        return cache


class GenericGenerationalLRUCacheTests(GenericLRUCacheTests):
    """
    Tests for any generational LRU cache.
    """

    def test_implements(self):
        cache = super(GenericGenerationalLRUCacheTests, self).test_implements()
        assert_that(cache.eden,
                    validly_provides(interfaces.IGeneration))
        assert_that(cache.protected,
                    validly_provides(interfaces.IGeneration))
        assert_that(cache.probation,
                    validly_provides(interfaces.IGeneration))


    def test_bad_generation_index_attribute_error(self):
        cache = self._makeOne(20)
        # Check proper init
        getattr(cache.generations[1], 'limit')
        getattr(cache.generations[2], 'limit')
        getattr(cache.generations[3], 'limit')

        # Gen 0 should be missing
        with self.assertRaisesRegex(AttributeError,
                                    "Generation 0 has no attribute 'on_hit'"):
            cache.generations[0].on_hit()

    def test_add_MRUs_reject_sets_sentinel_values(self):
        # When we find an item that completely fills the cache,
        # all the rest of the items are marked as rejected.
        cache = self._getClass()(20)
        base_size = cache.base_size
        entry_size = cache.entry_size
        cache = self._getClass()(20 + base_size + entry_size)

        added_entries = cache.add_MRUs([
            # over fill eden with item of size 15
            (1, (b'012345678901234' * 20, 0, False, 1)),
            # 1 goes to protected, filling it. eden is also over full with 2. probation is empty
            (2, (b'012', 0, False, 1)),
            # 3 fills eden, bumping 2 to probation. But probation is actually overfull now
            # so we'd like to spill something if we could (but we can't.)
            (3, (b'0', 0, False, 1)),
            # 4 should never be added because it won't fit anywhere.
            (4, (b'ee', 0, False, 1)),
        ])

        def keys(x):
            return [e.key for e in x]

        self.assertEqual(keys(cache.protected), [3, 2])
        self.assertEqual(keys(cache.probation), [])

        self.assertEqual(keys(cache.eden), [4])
        self.assertEqual(
            [2, 3, 4],
            [e.key for e in added_entries])

        self.assertEqual(3, len(added_entries))
        self.assertEqual(3, len(cache))
        self.assertEqual(3, len(list(cache)))

class CFFICacheTests(TestCase):
    """
    Tests that are specific to the CFFI implementation
    of the cache.

    These can use arbitrary keys and values.
    """

    def _getClass(self):
        return NoOverheadSizeCache

    def _makeOne(self, limit, kind=None):
        self.skipTest("Weights not supported")
        kind = kind or self._getClass()
        return kind(limit,
                    key_weight=self.key_weight,
                    value_weight=self.value_weight)

    def key_weight(self, k):
        return len(k)

    def value_weight(self, v):
        return len(v)

    def test_free_reuse(self):
        cache = self._makeOne(20)
        lru = cache.protected
        self.assertEqual(lru.limit, 16)
        entrya = lru.add_MRU('a', b'')
        entryb = lru.add_MRU('b', b'')
        entryc = lru.add_MRU('c', b'1')
        entryd = lru.add_MRU('d', b'1')
        evicted = lru.update_MRU(entryb, b'1234567890')
        self.assertEqual(evicted, ())
        # Not changing the size is just a hit, it doesnt't
        # evict anything.
        evicted = lru.update_MRU(entryb, b'1234567890')
        self.assertEqual(evicted, ())
        evicted = lru.update_MRU(entryc, b'1234567890')

        # a and d were evicted and placed on the freelist
        self.assertEqual(entrya.key, None)
        self.assertEqual(entrya.value, None)
        self.assertEqual(entryd.key, None)
        self.assertEqual(entryd.key, None)

        self.assertEqual(evicted,
                         [('a', b''),
                          ('d', b'1')])
        self.assertEqual(2, len(lru.node_free_list))

        lru.add_MRU('c', b'1')
        self.assertEqual(1, len(lru.node_free_list))
