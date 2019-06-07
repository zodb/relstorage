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
from nti.testing.matchers import verifiably_provides

from relstorage.tests import TestCase
from relstorage.cache import interfaces

class CacheTests(TestCase):

    def _getClass(self):
        from relstorage.cache.lru_sqlite import SQLiteCache
        return SQLiteCache

    def _makeOne(self, limit):
        return self._getClass()(limit)

    def test_implements(self):
        assert_that(self._makeOne(100), verifiably_provides(interfaces.ILRUCache))

    def test_item_implements(self):
        cache = self._makeOne(20)
        entrya = cache.add_MRU((0, 0), (b'', 0))[0]
        assert_that(entrya, verifiably_provides(interfaces.ILRUItem))
