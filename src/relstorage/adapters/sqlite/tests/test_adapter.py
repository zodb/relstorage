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

class MockConnection(object):

    @property
    def cursor(self):
        return self

    @property
    def connection(self):
        return self

    in_transaction = False
    in_critical_phase = True

    def execute(self, query, _args=()):
        if query.startswith('UPDATE'):
            self.in_transaction = True

    def fetchall(self):
        return ()

    def commit(self):
        self.in_transaction = False
        self.in_critical_phase = False

class MockBlobHelper(object):

    txn_has_blobs = False

@skipUnless(Sqlite3Driver.STATIC_AVAILABLE, "Driver not available")
class TestAdapter(test_adapter.AdapterTestBase):

    def _makeOne(self, options):
        return Adapter(":memory:",
                       options=options, pragmas={})

    def _test_lock_database_and_move_ends_critical_section_on_commit(self, commit):
        from relstorage.options import Options
        options = Options()
        options.driver = 'gevent sqlite3'
        adapter = self._makeOne(None)
        conn = MockConnection()
        assert conn.in_critical_phase

        result = adapter.lock_database_and_move(
            conn,
            MockBlobHelper(),
            (b'username', b'desc', b'ext'),
            commit=commit
        )

        self.assertIsNotNone(result)
        self.assertEqual(conn.in_critical_phase, not commit)

    def test_lock_database_and_move_ends_critical_section_on_commit(self):
        self._test_lock_database_and_move_ends_critical_section_on_commit(True)

    def test_lock_database_and_move_keeps_critical_section_wo_commit(self):
        self._test_lock_database_and_move_ends_critical_section_on_commit(False)
