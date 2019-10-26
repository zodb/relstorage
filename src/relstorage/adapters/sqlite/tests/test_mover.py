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

from relstorage.tests import TestCase
from relstorage.tests import MockOptions

from ..mover import Sqlite3ObjectMover
from ..drivers import Sqlite3Driver
from ...drivers import DriverNotAvailableError

class TestMoverQueries(TestCase):
    maxDiff = None

    def _makeOne(self, keep_history=True):
        try:
            driver = Sqlite3Driver()
        except DriverNotAvailableError as e:
            self.skipTest(e)
        options = MockOptions()
        options.keep_history = keep_history
        return Sqlite3ObjectMover(driver, options)

    def test_detect_conflict_query_hp(self):
        self.assertEqual(
            "SELECT zoid, tid, prev_tid, current_object_state.state "
            "FROM temp_store CROSS JOIN current_object_state USING (zoid) "
            "WHERE (tid <> prev_tid)",
            str(self._makeOne()._detect_conflict_query))

    def test_detect_conflict_query_hf(self):
        self.assertEqual(
            "SELECT zoid, tid, temp_store.prev_tid, object_state.state "
            "FROM temp_store CROSS JOIN object_state USING (zoid) "
            "WHERE (tid <> prev_tid)",
            str(self._makeOne(False)._detect_conflict_query))
