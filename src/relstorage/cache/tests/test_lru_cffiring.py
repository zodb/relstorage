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

from relstorage.tests import TestCase
from relstorage.cache import interfaces

class GenerationTests(TestCase):

    def _makeOne(self, limit):
        from relstorage.cache.lru_cffiring import Generation
        return Generation(limit, None)

    def test_mru_lru_ring(self):
        lru = self._makeOne(100)
        entrya = lru.add_MRU(b'a', b'1')
        self.assertEqual(lru.get_LRU(), entrya)

        entryb = lru.add_MRU(b'b', b'2')
        self.assertEqual(lru.get_LRU(), entrya)

        entryc = lru.add_MRU(b'c', b'3')
        self.assertEqual(lru.get_LRU(), entrya)

        lru.make_MRU(entryb)
        self.assertEqual(lru.get_LRU(), entrya)

        lru.make_MRU(entrya)
        self.assertEqual(lru.get_LRU(), entryc)

        self.assertEqual(len(lru), 3)
        self.assertEqual(len(list(lru)), 3)

    def test_bool(self):
        lru = self._makeOne(100)
        self.assertFalse(lru)
        entrya = lru.add_MRU('a', b'b')
        self.assertTrue(lru)
        lru.remove(entrya)
        self.assertFalse(lru)

class EdenTests(TestCase):

    def setUp(self):
        self._generations = []

    tearDown = setUp

    def _makeOne(self, limit, key_weight=len, value_weight=len):
        from . import Cache
        # Must hold on to *all* of them, or they get deallocated
        # and bad things happen.
        generations = Cache.create_generations(eden_limit=limit,
                                               key_weight=key_weight,
                                               value_weight=value_weight)
        self._generations.append(generations)
        return generations['eden']

    def test_add_MRUs_empty(self):
        lru = self._makeOne(100)
        self.assertEqual((), lru.add_MRUs([]))

    def test_add_MRUs_too_many(self):
        lru = self._makeOne(100)
        too_many = [(str(i), 'a' * i) for i in range(50)]
        # Make sure we have more then enough on the free list.
        lru.init_node_free_list(len(too_many) + 1)
        # They just exceed the limit
        added = lru.add_MRUs(too_many)
        # Much less got added.
        self.assertEqual(len(added), 13)

class GenericLRUCacheTests(TestCase):
    """
    Generic LRU caching tests that can be applied to any
    LRU implementation, using the kind of keys and
    values that we actually use: ``(oid_int, tid_int)`` and
    ``(state_bytes, tid_int)``.
    """

    def _getClass(self):
        from . import Cache
        return Cache

    def _makeOne(self, limit, kind=None):
        kind = kind or self._getClass()
        return kind(limit,
                    key_weight=self.key_weight,
                    value_weight=self.value_weight)

    def _getIface(self):
        return interfaces.ILRUCache

    @staticmethod
    def key_weight(_):
        return 0

    @staticmethod
    def value_weight(v):
        state, _ = v
        return len(state)

    def test_implements(self):
        cache = self._makeOne(100)
        assert_that(cache,
                    validly_provides(self._getIface()))
        self.assertIsInstance(cache.stats(), dict)
        return cache

    def test_item_implements(self):
        cache = self._makeOne(20)
        entrya = cache.add_MRU((1, 0), (b'', 0))
        assert_that(entrya, validly_provides(interfaces.ILRUEntry))

    def test_add_too_many(self):
        class _Cache(self._getClass()):
            _preallocate_entries = False

        cache = _Cache(20,
                       key_weight=GenericLRUCacheTests.key_weight,
                       value_weight=GenericLRUCacheTests.value_weight)

        entries = cache.add_MRUs([
            ((1, 0), (b'abcde', 0)),
            ((2, 0), (b'abcde', 0)),
            ((3, 0), (b'abcde', 0)),
            ((4, 0), (b'abcde', 0)),
            ((5, 0), (b'abcde', 0)),
            ((6, 0), (b'abcde', 0)),
        ])

        self.assertEqual(
            [5] * len(entries),
            [e.weight for e in entries])
        self.assertEqual(
            [(1, 0), (2, 0), (3, 0), (4, 0)],
            [e.key for e in entries]
        )
        self.assertEqual(4, len(cache))
        return cache

    def test_age(self):
        cache = self._makeOne(100)

        entries = cache.add_MRUs([
            ((1, 0), (b'abcde', 0)),
            ((2, 0), (b'abcde', 0)),
            ((3, 0), (b'abcde', 0)),
            ((4, 0), (b'abcde', 0)),
        ])

        self.assertEqual(
            [(1, 0), (2, 0), (3, 0), (4, 0)],
            [e.key for e in entries]
        )

        for _ in range(4):
            for e in entries:
                _ = cache[e.key]

        freqs = [e.frequency for e in cache.entries()]
        self.assertEqual([5] * len(entries), freqs)

        # By half each time
        cache.age_frequencies()
        freqs = [e.frequency for e in cache.entries()]
        self.assertEqual([2] * len(entries), freqs)
        return cache


    def test_delete(self):
        cache = self._makeOne(20)
        cache[(1, 0)] = (b'abc', 0)
        self.assertIn((1, 0), cache)
        self.assertEqual(1, len(cache))
        self.assertEqual(3, cache.size)
        self.assertEqual((b'abc', 0), cache[(1, 0)])
        self.assertEqual(list(cache), [(1, 0)])
        del cache[(1, 0)]
        self.assertNotIn((1, 0), cache)
        self.assertEqual(0, len(cache))
        self.assertEqual(0, cache.size)
        self.assertIsNone(cache[(1, 0)])
        self.assertEqual(list(cache), [])

    def test_get_backup_not_found(self):
        c = self._makeOne(100)
        r = c.get_from_key_or_backup_key((1, 0), None)
        self.assertIsNone(r)

    def test_get_backup_at_pref(self):
        c = self._makeOne(100)
        c[(1, 0)] = (b'1', 0)
        c[(1, 1)] = (b'2', 0)

        result = c.get_from_key_or_backup_key((1, 0), None)
        self.assertEqual(result, (b'1', 0))

    def test_get_backup_at_backup(self):
        c = self._makeOne(100)
        c[(1, 1)] = (b'2', 0)

        result = c.get_from_key_or_backup_key((1, 0), (1, 1))
        self.assertEqual(result, (b'2', 0))
        self.assertEqual(len(c), 1)
        self.assertIn((1, 0), c)
        self.assertNotIn((1, 1), c)

    def test_entries(self):
        cache = self._makeOne(20)
        cache[(1, 0)] = (b'abc', 0)
        entries = list(cache.entries())
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual((1, 0), entry.key)
        self.assertEqual((b'abc', 0), entry.value)
        self.assertEqual(1, entry.frequency)

        # Getting it again updates its frequency, not
        # necessarily on the same object though.
        self.assertIsNotNone(cache[(1, 0)])
        entries = list(cache.entries())
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual((1, 0), entry.key)
        self.assertEqual((b'abc', 0), entry.value)
        self.assertEqual(2, entry.frequency)

    def test_add_too_many_MRUs_works_aronud_big_entry(self):
        cache = self._getClass()(20,
                                 key_weight=GenericLRUCacheTests.key_weight,
                                 value_weight=GenericLRUCacheTests.value_weight)

        entries = cache.add_MRUs([
            ((1, 0), (b'abc', 0)),
            # This entry itself will fit nowhere
            ((2, 0), (b'12345678901234567890', 0)),
            ((3, 0), (b'bcd', 0)),
            ((4, 0), (b'cde', 0)),
            ((5, 0), (b'dehi', 0)),
            ((6, 0), (b'edghijkl', 0)),
        ])

        self.assertEqual(
            [(1, 0), (3, 0), (4, 0), (5, 0)],
            [e.key for e in entries])
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


    def test_add_too_many(self):
        cache = super(GenericGenerationalLRUCacheTests, self).test_add_too_many()
        self.assertEqual(2, len(cache.eden.node_free_list))
        self.assertIsNone(cache.eden.node_free_list[0].key)
        self.assertIsNone(cache.eden.node_free_list[0].value)

    def test_add_too_many_MRUs_works_aronud_big_entry(self):
        s = super(GenericGenerationalLRUCacheTests, self)
        cache = s.test_add_too_many_MRUs_works_aronud_big_entry()

        self.assertEqual(2, len(cache.eden.node_free_list))
        for e in cache.eden.node_free_list:
            self.assertIsNone(e.key)
            self.assertIsNone(e.value)

        entry = cache.eden.node_free_list[-1]
        cache.add_MRU((1, 0), (b'1', 0))
        self.assertEqual(1, len(cache.eden.node_free_list))

        self.assertEqual(cache.eden.PARENT_CONST, entry.cffi_ring_node.u.entry.r_parent)

    def test_add_MRUs_reject_sets_sentinel_values(self):
        # When we find an item that completely fills the cache,
        # all the rest of the items are marked as rejected.
        cache = self._makeOne(20)
        self.assertEqual(2, cache.eden.limit)
        self.assertEqual(2, cache.probation.limit)
        self.assertEqual(16, cache.protected.limit)

        added_entries = cache.add_MRUs([
            # over fill eden with item of size 15
            ((1, 0), (b'012345678901234', 0)),
            # 1 goes to protected, filling it. eden is also over full with 2. probation is empty
            ((2, 0), (b'012', 0)),
            # 3 fills eden, bumping 2 to probation. But probation is actually overfull now
            # so we'd like to spill something if we could (but we can't.)
            ((3, 0), (b'0', 0)),
            # 4 should never be added because it won't fit anywhere.
            ((4, 0), (b'ee', 0)),
        ])

        def keys(x):
            return [e.key for e in x]

        self.assertEqual(keys(cache.protected), [(1, 0)])
        self.assertEqual(keys(cache.probation), [(2, 0)])

        self.assertEqual(keys(cache.eden), [(3, 0)])
        self.assertEqual(
            [(1, 0), (2, 0), (3, 0)],
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
        from . import Cache
        return Cache

    def _makeOne(self, limit, kind=None):
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

    def test_add_MRUs_uses_existing_free_list(self):
        class _Cache(self._getClass()):
            _preallocate_avg_size = 7
            _preallocate_entries = True

        cache = _Cache(20)
        self.assertEqual(2, len(cache.eden.node_free_list))

        begin_nodes = list(cache.eden.node_free_list)

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),
                                       ('5', 'defg'),
                                       ('6', 'defg'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        for i, e in enumerate(begin_nodes):
            self.assertIs(e, entries[i])
        self.assertEqual(2, len(cache.eden.node_free_list))
        last_entry = entries[-1]
        for free in cache.eden.node_free_list:
            self.assertIs(last_entry._cffi_owning_node, free._cffi_owning_node)

        # Now just one that exactly fits.
        cache = _Cache(20)
        self.assertEqual(2, len(cache.eden.node_free_list))

        begin_nodes = list(cache.eden.node_free_list)

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),])
        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        for i, e in enumerate(begin_nodes):
            self.assertIs(e, entries[i])
        self.assertEqual(0, len(cache.eden.node_free_list))

    def test_disable_preallocate(self):
        class _Cache(self._getClass()):
            _preallocate_entries = False

        cache = _Cache(100)
        self.assertEqual(0, len(cache.eden.node_free_list))
