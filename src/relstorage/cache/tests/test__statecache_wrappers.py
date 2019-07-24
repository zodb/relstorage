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

class TestMultiStateCache(TestCase):

    def _makeOne(self, l=None, g=None):
        return wrappers.MultiStateCache(l, g)

    def test_provides(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStateCache))

    def test_delitem_found(self):
        c = self._makeOne({(1, 1): None}, {(1, 1): None})
        del c[(1, 1)]

class MockTracer(object):
    def __init__(self):
        self.trace_events = []

    def trace(self, code, oid_int=0, tid_int=0, end_tid_int=0, dlen=0):
        self.trace_events.append((code, oid_int, tid_int, end_tid_int, dlen))

    def trace_store_current(self, tid_int, state_oid_iter):
        """Does nothing"""

class TestTracingCacheWrapper(TestCase):

    def _makeOne(self, kind=None):
        kind = kind or wrappers.MultiStateCache

        return wrappers.TracingStateCache(
            kind(None, None),
            MockTracer())

    def test_provides(self):
        assert_that(self._makeOne(),
                    validly_provides(interfaces.IStateCache))

    def test_get_miss(self):
        from . import Cache
        c = self._makeOne(lambda *args: Cache(100))

        self.assertIsNone(c[(1, 0)])

        self.assertEqual(
            c.tracer.trace_events,
            [(0x20, 1, 0, 0, 0,)]
        )

    def test_get_hit(self):
        from . import Cache
        c = self._makeOne(lambda *args: Cache(100))
        # Deliberately mismatched tids in key and value;
        # not the concern at this level.
        c.cache[(1, 0)] = (b'abc', 1)
        self.assertEqual(c[(1, 0)], (b'abc', 1))

        self.assertEqual(
            c.tracer.trace_events,
            [(0x22, 1, 0, 0, 3,)]
        )
