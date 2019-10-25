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

from unittest import skipUnless

from ..adapter import Sqlite3Adapter as Adapter
from ..drivers import Sqlite3Driver
from ...tests import test_adapter

@skipUnless(Sqlite3Driver.STATIC_AVAILABLE, "Driver not available")
class TestAdapter(test_adapter.AdapterTestBase):

    def _makeOne(self, options):
        return Adapter(":memory:",
                       options=options, pragmas={})
