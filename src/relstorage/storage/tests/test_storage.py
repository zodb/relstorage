# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2008, 2019 Zope Foundation and Contributors.
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

"""
Tests for RelStorage

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from hamcrest import assert_that
from hamcrest import is_not as does_not
from nti.testing.matchers import verifiably_provides

from ZODB.interfaces import IExternalGC

from relstorage.interfaces import IRelStorage

from relstorage.tests import TestCase
from relstorage.tests import MockOptions
from relstorage.tests import MockAdapter

class TestRelStorage(TestCase):

    def makeOne(self, adapter=None, **kw):
        from relstorage.storage import RelStorage
        # Constructed so as to avoid the need to use a database connection.
        return RelStorage(adapter or MockAdapter(),
                          create=False,
                          cache_prefix='Mock',
                          **kw)

    def test_provides(self):
        storage = self.makeOne()
        assert_that(storage, verifiably_provides(IRelStorage))
        assert_that(storage, does_not(verifiably_provides(IExternalGC)))

    def test_provides_external_gc(self):
        adapter = MockAdapter()
        adapter.packundo.deleteObject = True
        storage = self.makeOne(adapter)

        assert_that(storage, verifiably_provides(IRelStorage))
        assert_that(storage, verifiably_provides(IExternalGC))
