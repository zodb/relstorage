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

from relstorage.adapters.tests import test_drivers
from relstorage.tests import TestCase

from ..drivers import Sqlite3GeventDriver

class Driver(Sqlite3GeventDriver):
    def get_driver_module(self):
        import sqlite3
        return sqlite3

    def connect(self): # pylint:disable=arguments-differ
        return self._connect(':memory:')

@skipUnless(Driver.STATIC_AVAILABLE, "Driver not available")
class TestSqlite3GeventDriver(test_drivers.IDBDriverSupportsCriticalTestMixin,
                              TestCase):

    def _makeOne(self):
        return Driver()
