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

from ..adapter import MySQLAdapter as Adapter
from ...tests import test_adapter

class MockVersionDetector(object):

    def supports_good_stored_procs(self, _cursor):
        return True

class MockConnection(object):

    cursor = None

class MockDriver(object):

    last_callproc_args = None
    last_callproc_kwargs = None
    last_callproc_multi_result = ((),)

    def callproc_multi_result(self, _cur, *args, **kwargs):
        self.last_callproc_args = args
        self.last_callproc_kwargs = kwargs
        return self.last_callproc_multi_result

class TestAdapter(test_adapter.AdapterTestBase):

    def _makeOne(self, options):
        return Adapter(options=options)

    def _test_lock_database_and_move_ends_critical_section_on_commit(self, commit):
        adapter = self._makeOne(None)
        driver = adapter.driver = MockDriver()
        driver.last_callproc_multi_result = (([42],),)
        adapter.version_detector = MockVersionDetector()

        result = adapter.lock_database_and_move(
            MockConnection(),
            None,
            (),
            commit=commit
        )

        self.assertEqual(result, (42, "-"))

        self.assertEqual(
            driver.last_callproc_kwargs,
            {'exit_critical_phase': commit}
        )

    def test_lock_database_and_move_ends_critical_section_on_commit(self):
        self._test_lock_database_and_move_ends_critical_section_on_commit(True)

    def test_lock_database_and_move_keeps_critical_section_wo_commit(self):
        self._test_lock_database_and_move_ends_critical_section_on_commit(False)
