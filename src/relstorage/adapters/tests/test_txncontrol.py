# -*- coding: utf-8 -*-
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

from relstorage.tests import TestCase
from relstorage.tests import MockConnection
from relstorage.tests import MockCursor

class MockConnmanager(object):

    def rollback(self, conn, _cursor):
        conn.rollback()

class TestTransactionControl(TestCase):

    def _getClass(self):
        from ..txncontrol import GenericTransactionControl
        return GenericTransactionControl

    def Binary(self, arg):
        if not isinstance(arg, bytes):
            arg = arg.encode('ascii')
        return arg

    def _makeOne(self, keep_history=True, binary=None):
        return self._getClass()(MockConnmanager(), keep_history, binary or self.Binary)

    def _get_hf_tid_query(self):
        return self._getClass()._get_tid_queries[1]

    def _get_hp_tid_query(self):
        return self._getClass()._get_tid_queries[0]

    def _check_get_tid_query(self, keep_history, expected_query):
        inst = self._makeOne(keep_history)
        cur = MockCursor()
        cur.results = [(1,)]

        inst.get_tid(cur)

        self.assertEqual(cur.executed.pop(),
                         (expected_query, None))

    def test_get_tid_hf(self):
        self._check_get_tid_query(False, self._get_hf_tid_query())

    def test_get_tid_hp(self):
        self._check_get_tid_query(True, self._get_hp_tid_query())

    def test_get_tid_empty_db(self):
        inst = self._makeOne()
        cur = MockCursor()
        cur.results = None

        self.assertEqual(inst.get_tid(cur), 0)

    def test_add_transaction_hp(self):
        inst = self._makeOne()
        cur = MockCursor()

        inst.add_transaction(cur, 1, u'user', u'desc', u'ext')

        self.assertEqual(
            cur.executed.pop(),
            (inst._add_transaction_query,
             (1, False, b'user', b'desc', b'ext'))
        )

        inst.add_transaction(cur, 1, u'user', u'desc', u'ext', packed=True)

        self.assertEqual(
            cur.executed.pop(),
            (inst._add_transaction_query,
             (1, True, b'user', b'desc', b'ext'))
        )

    def test_commit_phase1(self):
        inst = self._makeOne()
        result = inst.commit_phase1(None, None, None)
        self.assertEqual(result, '-')

    def test_commit_phase2(self):
        inst = self._makeOne()
        conn = MockConnection()
        inst.commit_phase2(conn, None, None)
        self.assertTrue(conn.committed)

    def test_abort(self):
        inst = self._makeOne()
        conn = MockConnection()
        inst.abort(conn, None, None)
        self.assertTrue(conn.rolled_back)
