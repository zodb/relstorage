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
from relstorage.tests import MockDriver

from ..adapter import OracleAdapter as BaseAdapter

from ... import interfaces


class Adapter(BaseAdapter):
    # We don't usually have cx_oracle installed, so
    # fake it.

    def _select_driver(self, options=None):
        d = MockDriver()
        d.connect = None
        d.NUMBER = None
        d.BLOB = None
        d.BINARY = None
        d.STRING = None
        d.Binary = None
        return d

class TestAdapter(TestCase):

    def _makeOne(self):
        return Adapter()

    def test_implements(self):
        adapter = self._makeOne()
        assert_that(adapter, validly_provides(interfaces.IRelStorageAdapter))
