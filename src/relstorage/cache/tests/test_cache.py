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

from relstorage.tests import TestCase

from . import Cache

class CacheRingTests(TestCase):

    def _makeOne(self, limit):
        from relstorage.cache.cache_ring import CacheRing
        return CacheRing(limit)

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

    def test_add_MRUs_empty(self):
        from relstorage.cache.cache_ring import EdenRing
        lru = EdenRing(100, len, len)
        self.assertEqual((), lru.add_MRUs([]))

    def test_bool(self):
        lru = self._makeOne(100)
        self.assertFalse(lru)
        entrya = lru.add_MRU('a', b'b')
        self.assertTrue(lru)
        lru.remove(entrya)
        self.assertFalse(lru)

class CacheTests(TestCase):

    def test_bad_generation_index_attribute_error(self):
        cache = Cache(20)
        # Check proper init
        getattr(cache.generations[1], 'limit')
        getattr(cache.generations[2], 'limit')
        getattr(cache.generations[3], 'limit')

        # Gen 0 should be missing
        with self.assertRaisesRegex(AttributeError,
                                    "Generation 0 has no attribute 'on_hit'"):
            cache.generations[0].on_hit()

    def test_free_reuse(self):
        cache = Cache(20)
        lru = cache.protected
        self.assertEqual(lru.limit, 16)
        entrya = lru.add_MRU('a', b'')
        entryb = lru.add_MRU('b', b'')
        entryc = lru.add_MRU('c', b'1')
        entryd = lru.add_MRU('d', b'1')
        for e in entrya, entryb, entryc, entryd:
            cache.data[e.key] = e
        lru.update_MRU(entryb, b'1234567890')
        lru.update_MRU(entryb, b'1234567890') # coverage
        lru.update_MRU(entryc, b'1234567890')
        self.assertEqual(2, len(lru.node_free_list))

        lru.add_MRU('c', b'1')
        self.assertEqual(1, len(lru.node_free_list))

    def test_add_too_many_MRUs_goes_to_free_list(self):
        class _Cache(Cache):
            _preallocate_entries = False

        cache = _Cache(20)
        self.assertEqual(0, len(cache.eden.node_free_list))

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),
                                       ('5', 'defg'),
                                       ('6', 'defg'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        self.assertEqual(2, len(cache.eden.node_free_list))
        self.assertIsNone(cache.eden.node_free_list[0].key)
        self.assertIsNone(cache.eden.node_free_list[0].value)

    def test_add_too_many_MRUs_works_aronud_big_entry(self):
        cache = Cache(20)

        entries = cache.eden.add_MRUs([('1', 'a'),
                                       # This entry itself will fit nowhere
                                       ('2', '12345678901234567890'),
                                       ('3', 'bc'),
                                       ('4', 'cd'),
                                       ('5', 'deh'),
                                       ('6', 'efghijkl'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '3', '4', '5'], [e.key for e in entries])
        self.assertEqual(2, len(cache.eden.node_free_list))
        for e in cache.eden.node_free_list:
            self.assertIsNone(e.key)
            self.assertIsNone(e.value)

        entry = cache.eden.node_free_list[-1]
        cache.eden.add_MRU('1', b'1')
        self.assertEqual(1, len(cache.eden.node_free_list))

        self.assertEqual(cache.eden.PARENT_CONST, entry.cffi_ring_node.u.entry.r_parent)

    def test_add_MRUs_uses_existing_free_list(self):
        class _Cache(Cache):
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

    def test_add_MRUs_reject_sets_sentinel_values(self):
        # When we find an item that completely fills the cache,
        # all the rest of the items are marked as rejected.
        cache = Cache(20)
        self.assertEqual(2, cache.eden.limit)
        self.assertEqual(2, cache.probation.limit)
        self.assertEqual(16, cache.protected.limit)

        added_entries = cache.eden.add_MRUs([
            # over fill eden
            ('1', b'012345678901234'),
            # 1 goes to protected, filling it. eden is also over full with 2. probation is empty
            ('2', b'012'),
            # 3 fills eden, bumping 2 to probation. But probation is actually overfull now
            # so we'd like to spill something if we could (but we can't.)
            ('3', b'0'),
            # 4 should never be added because it won't fit anywhere.
            ('4', b'e'),
        ])

        def keys(x):
            return [e.key for e in x]

        self.assertEqual(keys(cache.eden), ['3'])
        self.assertEqual(keys(cache.protected), ['1'])
        self.assertEqual(keys(cache.probation), ['2'])
        self.assertEqual('1 2 3'.split(), [e.key for e in added_entries])
        self.assertEqual(3, len(added_entries))
