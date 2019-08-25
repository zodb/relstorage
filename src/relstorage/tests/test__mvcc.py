# -*- coding: utf-8 -*-
"""
Tests for _mvcc.py

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from .. import interfaces
from .. import _mvcc as mvcc
from . import TestCase

MockViewer = mvcc.DetachableMVCCDatabaseViewer

class TestDetachableMVCCDatabaseCoordinator(TestCase):

    def _makeOne(self):
        return mvcc.DetachableMVCCDatabaseCoordinator()

    def test_ctor(self):
        c = self._makeOne()
        self.assertEqual(c.minimum_highest_visible_tid, None)
        self.assertEqual(c.maximum_highest_visible_tid, None)

    def test_implements(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IMVCCDatabaseCoordinator))

    def test_register_no_hvt(self):
        c = self._makeOne()
        viewer = MockViewer()
        c.register(viewer)
        self.assertTrue(c.is_registered(viewer))
        self.assertEqual(0, c._viewer_count_at_min)
        self.assertEqual(None, c.maximum_highest_visible_tid)
        self.assertEqual(None, c.minimum_highest_visible_tid)

        viewer2 = MockViewer()
        c.register(viewer2)
        self.assertTrue(c.is_registered(viewer2))
        self.assertEqual(0, c._viewer_count_at_min)
        self.assertEqual(None, c.maximum_highest_visible_tid)
        self.assertEqual(None, c.minimum_highest_visible_tid)

        c.unregister(viewer)
        self.assertFalse(c.is_registered(viewer))
        self.assertEqual(0, c._viewer_count_at_min)
        self.assertEqual(None, c.maximum_highest_visible_tid)
        self.assertEqual(None, c.minimum_highest_visible_tid)

    def test_register_min_hvt(self):
        c = self._makeOne()
        viewer = MockViewer()
        viewer.highest_visible_tid = 1
        c.register(viewer)
        self.assertTrue(c.is_registered(viewer))
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # Unregistering someone not registered does nothing.
        viewer2 = MockViewer()
        viewer2.highest_visible_tid = 1
        c.unregister(viewer2)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # dup registration does nothing
        viewer.highest_visible_tid = 2 # cheat
        c.register(viewer)
        self.assertTrue(c.is_registered(viewer))
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        viewer.detached = False
        viewer.highest_visible_tid = 1
        c.unregister(viewer)
        self.assertFalse(c.is_registered(viewer))
        self.assertEqual(0, c._viewer_count_at_min)
        self.assertEqual(None, c.maximum_highest_visible_tid)
        self.assertEqual(None, c.minimum_highest_visible_tid)

    def test_register_multiple(self):
        c = self._makeOne()
        viewer = MockViewer()
        viewer.highest_visible_tid = 1
        c.register(viewer)
        self.assertTrue(c.is_registered(viewer))
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # Without setting a min hvt, does nothing
        viewer2 = MockViewer()
        c.register(viewer2)
        self.assertTrue(c.is_registered(viewer2))
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

    def test_register_multiple_min_hvt(self):
        c = self._makeOne()
        viewer = MockViewer()
        viewer.highest_visible_tid = 1
        c.register(viewer)
        self.assertTrue(c.is_registered(viewer))
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        viewer2 = MockViewer()
        viewer2.highest_visible_tid = 1
        c.register(viewer2)
        self.assertTrue(c.is_registered(viewer2))
        self.assertEqual(2, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        c.unregister(viewer2)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

    def test_detach(self):
        c = self._makeOne()
        viewer = MockViewer()
        viewer.highest_visible_tid = 1
        c.register(viewer)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        viewer1 = MockViewer()
        viewer1.highest_visible_tid = 1
        c.register(viewer1)
        self.assertEqual(2, c._viewer_count_at_min)
        self.assertEqual(1, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        viewer2 = MockViewer()
        viewer2.highest_visible_tid = 2
        c.register(viewer2)
        self.assertTrue(c.is_registered(viewer2))
        self.assertEqual(2, c._viewer_count_at_min)
        self.assertEqual(2, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # Detach one of the guys making our minimum
        c.detach(viewer)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(2, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # And again, no changes
        c.detach(viewer)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(2, c.maximum_highest_visible_tid)
        self.assertEqual(1, c.minimum_highest_visible_tid)

        # detach other minimum
        c.detach(viewer1)
        self.assertEqual(1, c._viewer_count_at_min)
        self.assertEqual(2, c.maximum_highest_visible_tid)
        self.assertEqual(2, c.minimum_highest_visible_tid)

        # detach the max
        c.detach(viewer2)
        self.assertEqual(0, c._viewer_count_at_min)
        self.assertEqual(None, c.maximum_highest_visible_tid)
        self.assertEqual(None, c.minimum_highest_visible_tid)

    def test_viewers_at_or_before(self):
        viewer1 = MockViewer()
        viewer1.highest_visible_tid = 1
        viewer2 = MockViewer()
        viewer2.highest_visible_tid = 2
        viewer3 = MockViewer()
        viewer3.highest_visible_tid = 3
        c = self._makeOne()

        c.register(viewer1)
        c.register(viewer2)
        c.register(viewer3)

        self.assertEqual(c.viewers_at_or_before(4), {viewer1, viewer2, viewer3})
        self.assertEqual(c.viewers_at_or_before(3), {viewer1, viewer2, viewer3})
        self.assertEqual(c.viewers_at_or_before(2), {viewer1, viewer2})
        self.assertEqual(c.viewers_at_or_before(1), {viewer1})
        self.assertEqual(c.viewers_at_minimum(), {viewer1})

        c.detach(viewer2)
        self.assertEqual(c.viewers_at_or_before(3), {viewer1, viewer3})
        self.assertEqual(c.viewers_at_or_before(2), {viewer1})

        c.change(viewer1, None)
        self.assertEqual(c.viewers_at_or_before(3), {viewer3})
        self.assertEqual(c.viewers_at_or_before(2), ())

        c.change(viewer3, 1)
        self.assertEqual(c.viewers_at_or_before(3), {viewer3})
        self.assertEqual(c.viewers_at_or_before(2), {viewer3})
        self.assertEqual(c.viewers_at_or_before(1), {viewer3})

        c.change(viewer1, 4)
        self.assertEqual(c.viewers_at_or_before(4), {viewer3, viewer1})
        self.assertEqual(c.viewers_at_or_before(3), {viewer3})
        self.assertEqual(c.viewers_at_or_before(2), {viewer3})
        self.assertEqual(c.viewers_at_or_before(1), {viewer3})

        c.detach_all()
        self.assertEqual(c.viewers_at_or_before(4), ())

        c.change(viewer1, 4)
        self.assertFalse(viewer1.detached)
        self.assertEqual(c.viewers_at_or_before(4), {viewer1})
        self.assertEqual(c.viewers_at_or_before(3), ())


    def detach_at_min(self):
        viewer1 = MockViewer()
        viewer1.highest_visible_tid = 1
        viewer2 = MockViewer()
        viewer2.highest_visible_tid = 2

        c = self._makeOne()
        c.register(viewer1)
        c.register(viewer2)

        self.assertEqual(c.maximum_highest_visible_tid, 2)
        self.assertEqual(c.minimum_highest_visible_tid, 1)

        c.detach_viewers_at_minimum()
        self.assertTrue(viewer1.detached)
        self.assertEqual(c.maximum_highest_visible_tid, 2)
        self.assertEqual(c.minimum_highest_visible_tid, 2)
