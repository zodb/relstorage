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

import unittest

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.cache.interfaces import IStateCache
from relstorage.tests import TestCase

from . import MockOptions
from . import MockOptionsWithFakeMemcache

class AbstractStateCacheTests(TestCase):
    """
    Tests that all implementations of `IStateCache`
    should be able to pass.
    """

    def getClass(self):
        raise unittest.SkipTest("No implementation defined.")

    Options = MockOptions

    def _makeOne(self, **kw):
        options = self.Options.from_args(**kw)
        inst = self.getClass()(options)
        return inst

    def test_provides(self):
        assert_that(self._makeOne(), validly_provides(IStateCache))

    def test_delitem_not_there(self):
        c = self._makeOne()
        del c[(1, 1)]

    def test_set_all_for_tid(self):
        c = self._makeOne()

        c.set_all_for_tid(
            1,
            [(b'abc', 0, -1),
             (b'ghi', 1, -1),])
        c.set_all_for_tid(
            2,
            [(b'def', 0, -1)]
        )
        # Hits on primary key
        self.assertEqual(c[0, 1],
                         (b'abc', 1))
        self.assertEqual(c[1, 1],
                         (b'ghi', 1))

        self.assertEqual(c[0, 2],
                         (b'def', 2))

    def test_updating_delta_map(self):
        self.assertIs(self._makeOne().updating_delta_map(self), self)

class MemcacheClientTests(AbstractStateCacheTests):

    def setUp(self):
        from relstorage.tests.fakecache import data
        data.clear()

    tearDown = setUp

    Options = MockOptionsWithFakeMemcache

    def getClass(self):
        from relstorage.cache.storage_cache import MemcacheStateCache
        return MemcacheStateCache.from_options

    def test_new_instance_is_really_new(self):
        inst = self._makeOne()
        new = inst.new_instance()
        self.assertIsNot(inst, new)
