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
from relstorage.cache import _statecache_wrappers as wrappers

class MockCache(object):
    released = False

    def new_instance(self):
        return type(self)()

    def release(self):
        self.released = True

class TestMultiStateCache(TestCase):

    def _makeOne(self, l=None, g=None):
        return wrappers.MultiStateCache(l, g)

    def test_provides(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStateCache))

    def test_delitem_found(self):
        c = self._makeOne({(1, 1): None}, {(1, 1): None})
        del c[(1, 1)]

    def test_new_instance_and_release(self):
        root = self._makeOne(MockCache(), MockCache())
        child = root.new_instance()
        self.assertIsNot(child.l, root.l)
        self.assertIsNot(child.g, root.g)

        child.release()
        self.assertIsNone(child.l)
        self.assertIsNone(child.g)

class MockTracer(object):
    def __init__(self):
        self.trace_events = []

    def trace(self, code, oid_int=0, tid_int=0, end_tid_int=0, dlen=0):
        self.trace_events.append((code, oid_int, tid_int, end_tid_int, dlen))

    def trace_store_current(self, tid_int, state_oid_iter):
        """Does nothing"""

    def close(self):
        self.trace_events = tuple(self.trace_events)

class TestTracingCacheWrapper(TestCase):

    def _makeOne(self, kind=None):
        kind = kind or wrappers.MultiStateCache

        return wrappers.TracingStateCache(
            kind(MockCache(), MockCache()),
            MockTracer())

    def test_provides(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStateCache))
        assert_that(self._makeOne().new_instance(),
                    validly_provides(interfaces.IStateCache))

    def test_new_instance_and_release(self):
        root = self._makeOne()
        child = root.new_instance()
        self.assertIs(root.tracer, child.tracer)
        child_cache = child.cache
        child_cache_l = child.cache.l
        child_cache_g = child.cache.g

        child.release()

        self.assertIsNotNone(child.tracer)
        self.assertIsNone(child.cache)
        self.assertIsNone(child_cache.l)
        self.assertIsNone(child_cache.g)
        self.assertTrue(child_cache_l.released)
        self.assertTrue(child_cache_g.released)
        self.assertFalse(root.cache.l.released)
        self.assertFalse(root.cache.g.released)

        child.close()
        self.assertIsNone(child.tracer)

    def test_get_miss(self):
        from . import LocalClient
        c = self._makeOne(lambda *args: LocalClient(100))

        self.assertIsNone(c[(1, 0)])

        self.assertEqual(
            c.tracer.trace_events,
            [(0x20, 1, 0, 0, 0,)]
        )

    def test_get_hit(self):
        from . import LocalClient
        c = self._makeOne(lambda *args: LocalClient(100))
        c.cache[(1, 1)] = (b'abc', 1)
        self.assertEqual(c[(1, 1)], (b'abc', 1))

        self.assertEqual(
            c.tracer.trace_events,
            [(0x22, 1, 1, 0, 3,)]
        )
