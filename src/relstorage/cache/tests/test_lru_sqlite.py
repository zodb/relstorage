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

from relstorage.cache.tests.test_lru_cffiring import GenericLRUCacheTests

class CacheTests(GenericLRUCacheTests):

    def _getClass(self):
        from relstorage.cache.lru_sqlite import SQLiteCache
        return SQLiteCache

    def test_age(self):
        cache = super(CacheTests, self).test_age()
        # In our case, aging also trims
        self.assertEqual(4, len(cache))
        self.assertEqual(20, cache.size)
        cache.limit = 10
        cache.age_frequencies()
        self.assertEqual(2, len(cache))
        self.assertEqual(10, cache.size)
        freqs = [e.frequency for e in cache.entries()]
        self.assertEqual([1] * len(freqs), freqs)

        # A frequency going to 0 removes it entirely
        cache.age_frequencies()
        self.assertEqual(0, len(cache))
