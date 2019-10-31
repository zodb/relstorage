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
from relstorage.tests import MockAdapter
from relstorage.options import Options
from relstorage.cache import interfaces
from relstorage.cache import mvcc

from . import LocalClient

class MockObjectIndex(object):

    maps = ()
    detached = False
    highest_visible_tid = None
    complete_since_tid = None

    def __setitem__(self, k, v):
        raise AttributeError

class MockViewer(object):

    object_index = None # type: MockObjectIndex
    highest_visible_tid = None
    detached = False

    def __init__(self, options=None):
        options = options or Options()
        self.adapter = MockAdapter()
        self.local_client = LocalClient(options)

    def __repr__(self):
        return "<MockViewer at 0x%x index=%r>" % (
            id(self),
            self.object_index,
        )

class TestMVCCDatabaseCorrdinator(TestCase):

    expected_poll_conn = None
    expected_poll_cursor = None
    expected_poll_last_tid = None
    expected_poll_result = None

    def setUp(self):
        self.coord = self._makeOne()
        self.viewer = MockViewer()
        self.coord.register(self.viewer)
        self.polled_tid = 0
        self.polled_changes = None
        self.viewer.adapter.poller.poll_invalidations = self.poll_invalidations
        self.viewer.adapter.poller.get_current_tid = self.get_current_tid

    def get_current_tid(self, _cursor):
        return self.polled_tid

    def poll_invalidations(self, conn, cursor, last_tid):
        self.assertEqual(last_tid, self.expected_poll_last_tid)
        self.assertEqual(conn, self.expected_poll_conn)
        self.assertEqual(cursor, self.expected_poll_cursor)
        return self.polled_changes, self.polled_tid

    def add_viewer(self):
        viewer = MockViewer()
        viewer.adapter = self.viewer.adapter
        self.coord.register(viewer)
        return viewer

    def tearDown(self):
        del self.viewer.adapter.poller.poll_invalidations
        self.coord.unregister(self.viewer)
        self.viewer = None
        self.coord = None

    def _makeOne(self):
        return mvcc.MVCCDatabaseCoordinator()

    def test_implements(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStorageCacheMVCCDatabaseCoordinator))

    def assertNoPollingState(self):
        self.none(self.coord.object_index)
        self.none(self.viewer.object_index)
        self.none(self.viewer.highest_visible_tid)
        self.none(self.coord.maximum_highest_visible_tid)

    def do_poll(self, viewer=None):
        viewer = viewer or self.viewer
        result = self.coord.poll(viewer, None, None)
        if result:
            result = list(result)

        self.assertEqual(result, self.expected_poll_result)
        if self.polled_tid:
            # Only change if we expect a different poll to come in;
            # polling with 0 shouldn't change.
            self.expected_poll_last_tid = self.polled_tid

    def test_poll_no_index_when_0(self):
        # Starting from blank state, we only begin making an index
        # when there's data in the DB.
        cache = self.viewer
        cache.object_index = MockObjectIndex()
        coord = self.coord
        self.assertIsNone(coord.object_index)
        assert (self.polled_tid, self.polled_changes) == (0, None)
        # As long as it keeps returning 0, we don't try to begin caching
        for _ in range(2):
            self.do_poll()
            self.assertIsNone(coord.object_index)

        self.assertNoPollingState()

    def test_poll_no_index_begins(self, polled_tid=1):
        cache = self.viewer
        coord = self.coord

        self.polled_tid = polled_tid
        self.do_poll()
        self.expected_poll_last_tid = self.polled_tid

        self.assertIsNotNone(coord.object_index)
        self.assertIs(cache.object_index, coord.object_index)
        self.assertEqual(polled_tid, cache.object_index.highest_visible_tid)
        self.assertEqual(polled_tid, coord.maximum_highest_visible_tid)
        self.assertEqual(polled_tid, coord.minimum_highest_visible_tid)

    def test_tid_goes_0_after_begin(self):
        self.test_poll_no_index_begins()
        # zap the database
        self.polled_tid = 0

        self.do_poll()

        # We lost all our state
        self.assertNoPollingState()

    def test_tid_goes_back_after_begin(self):
        self.test_poll_no_index_begins(polled_tid=2)
        second_viewer = self.add_viewer()
        second_viewer.object_index = MockObjectIndex()


        # switch to a replica
        self.polled_tid = self.polled_tid - 1
        assert self.polled_tid == 1
        self.do_poll()
        self.assertNoPollingState()

        # Other registered viewers were marked invalid.
        self.assertTrue(second_viewer.detached)

        # Poll the first guy again, begin getting state.
        self.expected_poll_last_tid = None
        self.do_poll()
        self.assertIs(self.viewer.object_index, self.coord.object_index)
        self.assertIsNotNone(self.viewer.object_index)

        # And for the still-invalid viewer, polling continues from
        # where we just polled.
        self.expected_poll_last_tid = 1
        self.polled_changes = []
        self.expected_poll_result = None
        second_viewer.adapter = self.viewer.adapter
        self.do_poll(viewer=second_viewer)
        self.assertIs(second_viewer.object_index, self.viewer.object_index)

    def test_poll_many_times_vacuums_one_viewer(self):
        # Only one viewer, so state management is simple.

        # Start us off.
        self.test_poll_no_index_begins(2)

        # Put something in here that can get frozen.
        # Before we began polling.
        self.assertEqual(self.viewer.object_index.highest_visible_tid, 2)
        self.viewer.local_client[(0, 1)] = (b'cached data', 1)
        self.viewer.object_index[0] = 1

        # Move forward
        for poll_num in range(3, 20):
            __traceback_info__ = poll_num
            prev_poll = self.expected_poll_last_tid
            oid = poll_num
            tid = poll_num
            self.viewer.local_client[(oid, 1)] = (b'cached data', 1)
            self.assertIn((oid, 1), self.viewer.local_client)
            self.viewer.object_index[oid] = 1
            self.polled_tid = poll_num
            self.polled_changes = self.expected_poll_result = [(oid, tid)]
            self.do_poll()
            self.assertLength(self.viewer.object_index.maps, 1)
            self.assertEqual(self.coord.minimum_highest_visible_tid, poll_num)
            self.assertEqual(self.coord.maximum_highest_visible_tid, poll_num)
            # Our completion is our last poll.
            self.assertEqual(self.viewer.object_index.complete_since_tid, prev_poll)

        # All of the OIDs I put in the local client got invalidated
        # as we went, except for the first one, which got frozen
        # while remaining accessible at the old key without actually increasing
        # the number of entries in the cache or its byte size.
        self.assertEqual(len(self.viewer.local_client), 1)
        self.assertIn((0, None), self.viewer.local_client)
        self.assertIn((0, 1), self.viewer.local_client)


    def test_poll_many_times_vacuums_two_viewer(self):
        # A viewer that keeps moving forward, and a viewer that
        # is stuck in the past.
        second_viewer = self.add_viewer()

        self.test_poll_no_index_begins()
        # Second guy will actually ask for changes;
        # give him the same TID.
        self.polled_changes = ()
        self.expected_poll_result = None
        self.do_poll(viewer=second_viewer)

        # They have the same index to start with.
        self.assertIs(self.viewer.object_index, second_viewer.object_index)

        # Now move forward; the old viewer that's not updating his
        # state is keeping us from
        for poll_num in range(2, 20):
            __traceback_info__ = poll_num
            oid = poll_num
            tid = poll_num
            self.polled_tid = poll_num
            self.polled_changes = self.expected_poll_result = [(oid, tid)]
            self.do_poll()
            self.assertLength(self.viewer.object_index.maps, poll_num)
            # The max keeps going up.
            self.assertEqual(self.coord.maximum_highest_visible_tid, poll_num)
            # The min stays pinned
            self.assertEqual(self.coord.minimum_highest_visible_tid,
                             second_viewer.object_index.highest_visible_tid)
            self.assertEqual(self.viewer.object_index.complete_since_tid, 1)

        # At the end, the index is not the same.
        self.assertIsNot(self.viewer.object_index, second_viewer.object_index)
        # But the original map still is.
        self.assertIs(self.viewer.object_index.maps[-1],
                      second_viewer.object_index.maps[0])

    def test_poll_many_times_vacuums_several_viewers(self):
        # A viewer that keeps moving forward, and a viewer that
        # is stuck in the past.
        second_viewer = self.add_viewer()

        self.test_poll_no_index_begins()
        # Second guy will actually ask for changes;
        # give him the same TID. Because its his first time,
        # though, we return no changes to him.
        self.polled_changes = ()
        self.expected_poll_result = None
        self.do_poll(viewer=second_viewer)

        # Third guy exists, hasn't actually
        # viewed anything yet.
        third_viewer = self.add_viewer()

        # They have the same index to start with.
        self.assertIs(self.viewer.object_index, second_viewer.object_index)
        self.none(third_viewer.object_index)

        # Now move forward; the old viewer that's not updating his
        # state is keeping us from
        def poll_for_poll_num(poll_num):
            __traceback_info__ = poll_num
            oid = poll_num
            tid = poll_num
            self.polled_tid = poll_num
            self.polled_changes = self.expected_poll_result = [(oid, tid)]
            self.do_poll()
            self.assertLength(self.viewer.object_index.maps, poll_num)
            # The max keeps going up.
            self.assertEqual(self.coord.maximum_highest_visible_tid, poll_num)
            # The min stays pinned
            self.assertEqual(self.coord.minimum_highest_visible_tid,
                             second_viewer.object_index.highest_visible_tid)
            self.assertEqual(self.viewer.object_index.complete_since_tid, 1)

        for poll_num in range(2, 20):

            poll_for_poll_num(poll_num)

        # Now plop the third viewer down right in the middle of this sequence
        # and then go again.
        self.polled_tid = poll_num
        self.polled_changes = ()
        self.expected_poll_result = None
        self.do_poll(viewer=third_viewer)
        self.assertIs(self.viewer.object_index, third_viewer.object_index)
        for poll_num in range(20, 30):
            poll_for_poll_num(poll_num)

        # All three share the original map
        self.assertIs(self.viewer.object_index.maps[-1],
                      second_viewer.object_index.maps[0])
        self.assertIs(third_viewer.object_index.maps[-1],
                      second_viewer.object_index.maps[0])
        # The second and third share the back 20 maps
        new_maps = self.viewer.object_index.maps[10:]
        back_maps = third_viewer.object_index.maps[:]
        self.assertEqual(new_maps, back_maps)

        # Change settings: limit the depth
        self.coord.max_allowed_index_depth = 2
        # Poll; this throws off the oldest reader.
        poll_num += 1
        self.polled_tid = oid = tid = poll_num
        self.polled_changes = self.expected_poll_result = [(oid, tid)]
        self.do_poll()
        # The oldest reader is now invalid
        self.assertTrue(second_viewer.detached)
        # And the maps have been combined, back to the third_viewer
        # 12 = third_viewer + 10 intermediates + most recent poll
        self.assertLength(self.viewer.object_index.maps, 12)

        # polling again will drop the third viewer and shrink everything up.
        poll_num += 1
        self.polled_tid = oid = tid = poll_num
        self.polled_changes = self.expected_poll_result = [(oid, tid)]
        self.do_poll()

        self.assertTrue(third_viewer.detached)
        self.assertLength(self.viewer.object_index.maps, 1)

    def test_poll_produces_gap_vacuum(self):
        # TIDs do not always strictly go up; if there's a delay polling,
        # one viewer may not poll as far forward as everyone else and so
        # will produce a divergent index. vacuum need sto handle that.

        self.test_poll_no_index_begins()

        second_viewer = self.add_viewer()
        # Second guy will actually ask for changes;
        # give him the same TID. Because its his first time,
        # though, we return no changes to him.
        self.polled_changes = ()
        self.expected_poll_result = None
        self.do_poll(viewer=second_viewer)

        # Both now have the same min
        self.assertEqual(self.viewer.highest_visible_tid, second_viewer.highest_visible_tid)
        self.assertEqual(self.viewer.highest_visible_tid, self.coord.minimum_highest_visible_tid)
        orig_index = self.coord.object_index

        # Pull the first one out ahead
        changes_5 = (1, 5)
        changes_10 = (2, 10)
        self.polled_tid = 10
        self.polled_changes = self.expected_poll_result = [changes_5, changes_10]
        self.do_poll()
        index_10 = self.coord.object_index
        self.assertIsNot(orig_index, index_10)
        self.assertEqual(1, self.coord.minimum_highest_visible_tid)

        # Pull the other one to the middle, gapping the index.
        # In order to do this, we have to pretend that we started with
        # the old index and got interrupted before we finished.
        self.coord.object_index = orig_index
        self.expected_poll_last_tid = orig_index.minimum_highest_visible_tid
        self.polled_tid = 5
        self.polled_changes = self.expected_poll_result = [changes_5]
        self.do_poll(viewer=second_viewer)

        # Now the first one goes ahead and polls farther to the future.
        # Vacuum happens, but doesn't remove the original transaction index
        # we still need.
        self.coord.object_index = index_10
        self.assertEqual(self.coord.minimum_highest_visible_tid, 5)
        self.assertEqual(self.coord.object_index.depth, 2)
        self.expected_poll_last_tid = 10
        self.polled_tid = 15
        self.polled_changes = self.expected_poll_result = [(1, 15)]
        self.do_poll()
        # We vacuumed as far forward as we could.
        self.assertEqual(self.coord.minimum_highest_visible_tid, 5)
        self.assertEqual(self.coord.object_index.depth, 3)


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

    def test_merge_older_not_modify_older(self):
        orig = self._makeOne(2, complete_since_tid=None, data=())
        orig[2] = 1
        orig[1] = 1
        old_map = self._makeOne(3, complete_since_tid=2, data=((1, 3),))

        old_map.merge_older_tid(orig)
        self.assertEqual(dict(orig), {1: 1, 2: 1})
        # The merged map has both
        self.assertEqual(dict(old_map), {1: 3, 2: 1})


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
        # We did not get the same object back, because the tid changed,
        # and we can't do that.
        self.assertIsNot(ix2, ix)
        del ix
        self.assertIsNot(ix2.maps[0], initial_map)
        # Verifies.
        ix2.verify()
        self.assertEqual(ix2.maximum_highest_visible_tid, new_polled_tid)
        # Keeps connection to oldest map.
        self.assertEqual(ix2.minimum_highest_visible_tid, initial_tid)

        # Incomplete data is preserved, and
        # Complete incoming data is visible
        self.assertEqual(dict(ix2), {
            oid: new_polled_tid,
            oid2: initial_tid,
            oid3: new_polled_tid
        })


    def test_polled_changes_first_poll_get_changes_same_tid(self):
        # Some other entity had a TID in the past (since we began) and
        # they polled and got changes that overlapped our
        # initial set.
        self.test_polled_changes_first_poll_go_forward(new_polled_tid=1)

    def test_polled_changes_does_not_allow_None_changes(self):
        ix = self._makeOne(highest_visible_tid=1)
        with self.assertRaises(AssertionError):
            ix.with_polled_changes(2, 1, None)

    def test_polled_empty_not_complete(self):
        # We assert for this impossible case
        initial_tid = 1
        ix = self._makeOne(highest_visible_tid=1)
        first_poll_tid = 1
        ix = ix.with_polled_changes(highest_visible_tid=first_poll_tid,
                                    complete_since_tid=initial_tid, changes=())
        # We didn't actually change the complete_since value, because without
        # changes we can't actually be sure we're complete.
        self.assertEqual(ix.maps[-1].complete_since_tid, None)

    def test_polled_same_tid_back_complete(self):
        initial_tid = 2

        ix = self._makeOne(highest_visible_tid=initial_tid)

        first_poll_tid = initial_tid + 2

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
        ix = self._makeOne(highest_visible_tid=1)
        initial_tid = 1
        first_poll_tid = 4

        # Get changes for 3 and above
        ix2 = ix.with_polled_changes(
            highest_visible_tid=first_poll_tid,
            complete_since_tid=initial_tid,
            changes=(
                (1, first_poll_tid),
            ))

        self.assertIsNot(ix, ix2)

        second_poll_tid = 6
        ix2 = ix2.with_polled_changes(
            highest_visible_tid=second_poll_tid,
            complete_since_tid=first_poll_tid,
            changes=(
                (2, second_poll_tid - 1),
                (3, second_poll_tid)
            )
        )

        self.assertEqual(ix2[2], second_poll_tid - 1)
        self.assertEqual(ix2[3], second_poll_tid)
        self.assertEqual(ix2[1], first_poll_tid)
