# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

from relstorage.cache import mvcc


class TestMVCCDatabaseCorrdinator(TestCase):

    def _makeOne(self):
        return mvcc.MVCCDatabaseCoordinator()

    def test_implements(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStorageCacheMVCCDatabaseCoordinator))

    def test_register(self):
        c = self._makeOne()
        c.register(self)
        self.assertTrue(c.is_registered(self))
        c.unregister(self)
        self.assertFalse(c.is_registered(self))


class TestTransactionRangeObjectIndex(TestCase):

    def _makeOne(self,
                 highest_visible_tid,
                 complete_since_tid,
                 data):
        return mvcc._TransactionRangeObjectIndex(
            highest_visible_tid,
            complete_since_tid,
            data)

    def test_bad_tid_in_ctor(self):
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=1, complete_since_tid=2, data=())

    def test_bad_data(self):
        # Too high
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          complete_since_tid=0,
                          data=[(1, 3)])

        # Too low
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          complete_since_tid=0,
                          data=[(1, 0)])

        # Just right
        c = self._makeOne(highest_visible_tid=3,
                          complete_since_tid=1,
                          data=[(1, 2)])

        self.assertEqual(3, c.highest_visible_tid)
        self.assertEqual(1, c.complete_since_tid)

    def test_complete_to(self):
        # This map has no guarantees about completeness and can
        # have values <= 1.
        old_map = self._makeOne(1, complete_since_tid=None, data=())
        old_map[1] = 1

        # This one does guarantee completeness
        new_map = self._makeOne(4, complete_since_tid=1, data=((1, 2),
                                                               (2, 2)))

        old_map.complete_to(new_map)
        # Check constraints
        old_map.verify(initial=False)
        self.assertEqual(old_map.complete_since_tid, new_map.complete_since_tid)
        self.assertEqual(old_map.highest_visible_tid, new_map.highest_visible_tid)
        self.assertEqual(old_map[1], 2)
        self.assertEqual(old_map[2], 2)

    def test_merge_same_tid(self):
        # A complete map
        old_map = self._makeOne(3, complete_since_tid=2, data=((1, 3),))
        old_map[2] = 1

        # Another one, with more data.
        new_map = self._makeOne(3, complete_since_tid=1, data=((1, 3),
                                                               (3, 2)))

        old_map.merge_same_tid(new_map)
        # Check constraints
        old_map.verify(initial=False)
        self.assertEqual(old_map.complete_since_tid, new_map.complete_since_tid)
        self.assertEqual(old_map.highest_visible_tid, new_map.highest_visible_tid)
        self.assertEqual(old_map[1], 3)
        self.assertEqual(old_map[2], 1)
        self.assertEqual(old_map[3], 2)

    def test_can_hold_old_data(self):
        # Complete maps are only complete over their range:
        # ``complete_since_tid < tid <= highest_visible_tid``.
        # They can have partial incomplete data older than that.

        new_map = self._makeOne(4, complete_since_tid=1, data=((1, 2),
                                                               (2, 2)))
        new_map[3] = 1
        new_map.verify(initial=False)

class TestObjectIndex(TestCase):

    def _makeOne(self, highest_visible_tid=1, data=()):
        return mvcc._ObjectIndex(highest_visible_tid, data=data)

    def test_ctor_empty(self):
        ix = self._makeOne()
        self.assertEqual(1, len(ix.maps))
        self.assertIsInstance(ix.maps[0], mvcc._TransactionRangeObjectIndex)
        self.assertEqual(1, ix.highest_visible_tid)
        self.assertEqual(1, ix.maximum_highest_visible_tid)
        self.assertEqual(1, ix.minimum_highest_visible_tid)

    def test__setitem__out_of_range(self):
        ix = self._makeOne(highest_visible_tid=2)
        ix[1] = 1
        self.assertEqual(ix[1], 1)
        ix[2] = 3
        self.assertNotIn(2, ix)

    def test_ctr_data(self):
        # Too high
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          data=[(1, 3)])

        # Too low
        with self.assertRaises(AssertionError):
            self._makeOne(highest_visible_tid=2,
                          data=[(1, -1)])

        # Just right
        c = self._makeOne(highest_visible_tid=2,
                          data=[(1, 1)])

        self.assertEqual(2, c.highest_visible_tid)

    def test_polled_changes_first_poll_go_forward(self, new_polled_tid=2):
        initial_tid = 1
        # Initially incomplete.
        ix = self._makeOne(highest_visible_tid=initial_tid)
        initial_map = ix.maps[0]

        # cache some data.
        oid = 1
        oid2 = 2
        oid3 = 3
        ix[oid] = initial_tid
        ix[oid2] = initial_tid

        # Poll for > 1
        complete_since_tid = 1
        new_polled_tid = 2
        changes = [
            (oid, new_polled_tid),
            (oid3, new_polled_tid)
        ]

        ix2 = ix.with_polled_changes(new_polled_tid, complete_since_tid, changes)
        # Same object back.
        self.assertIs(ix2, ix)
        self.assertIs(ix2.maps[0], initial_map)
        # Verifies.
        ix2.verify()
        self.assertEqual(ix2.maximum_highest_visible_tid, new_polled_tid)
        self.assertEqual(ix2.minimum_highest_visible_tid, new_polled_tid)
        # Change data is visible
        self.assertEqual(ix[oid], new_polled_tid)
        self.assertEqual(ix[oid3], new_polled_tid)

        # Incomplete data is preserved.
        self.assertEqual(ix[oid2], initial_tid)

    def test_polled_changes_first_poll_stay_same(self):
        self.test_polled_changes_first_poll_go_forward(new_polled_tid=1)

    def test_polled_changes_does_not_allow_None_changes(self):
        ix = self._makeOne(highest_visible_tid=1)
        with self.assertRaises(AssertionError):
            ix.with_polled_changes(2, 1, None)

    def test_polled_empty_but_wrong_tid(self):
        # We assert for this impossible case
        initial_tid = 1
        ix = self._makeOne(highest_visible_tid=1)
        first_poll_tid = 1
        ix = ix.with_polled_changes(highest_visible_tid=first_poll_tid,
                                    complete_since_tid=initial_tid, changes=())
        self.assertEqual(ix.maps[-1].complete_since_tid, first_poll_tid)


        with self.assertRaises(AssertionError):
            ix.with_polled_changes(first_poll_tid + 1, first_poll_tid, ())

    def test_polled_same_tid_back_complete(self):
        initial_tid = 2
        ix = self._makeOne(highest_visible_tid=1)
        first_poll_tid = 4

        ix = ix.with_polled_changes(
            highest_visible_tid=first_poll_tid,
            complete_since_tid=initial_tid,
            changes=(
                (1, first_poll_tid),
            ))

        ix2 = ix.with_polled_changes(
            highest_visible_tid=first_poll_tid,
            complete_since_tid=initial_tid - 1,
            changes=(
                (1, first_poll_tid),
                (2, initial_tid)
            )
        )

        self.assertIs(ix2, ix)
        self.assertEqual(ix[1], first_poll_tid)
        self.assertEqual(ix[2], initial_tid)

    def test_polled_newer_tid_after_first(self):
        initial_tid = 2
        ix = self._makeOne(highest_visible_tid=1)
        first_poll_tid = 4

        ix = ix2 = ix.with_polled_changes(
            highest_visible_tid=first_poll_tid,
            complete_since_tid=initial_tid,
            changes=(
                (1, first_poll_tid),
            ))

        self.assertIs(ix, ix2)

        second_poll_tid = 6
        ix2 = ix.with_polled_changes(
            highest_visible_tid=second_poll_tid,
            complete_since_tid=first_poll_tid,
            changes=(
                (2, second_poll_tid - 1),
                (3, second_poll_tid)
            )
        )

        self.assertIsNot(ix, ix2)
        self.assertEqual(ix2[2], second_poll_tid - 1)
        self.assertEqual(ix2[3], second_poll_tid)
        self.assertEqual(ix2[1], first_poll_tid)
