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

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase
from relstorage.tests import MockConnection
from relstorage.tests import MockConnectionManager
from relstorage.tests import MockCursor
from relstorage.tests import MockPoller

from ..interfaces import ITransactionControl

class MockStoreConnection(object):

    def __init__(self, conn):
        self.connection = conn

    def rollback_quietly(self):
        self.connection.rollback()

    def commit(self):
        self.connection.commit()

class TestTransactionControl(TestCase):

    def _getClass(self):
        from ..txncontrol import GenericTransactionControl
        return GenericTransactionControl

    def Binary(self, arg):
        if not isinstance(arg, bytes):
            arg = arg.encode('ascii')
        return arg

    def _makeOne(self, keep_history=True, binary=None):
        return self._getClass()(MockConnectionManager(), MockPoller(),
                                keep_history, binary or self.Binary)

    def test_provides(self):
        assert_that(self._makeOne(), validly_provides(ITransactionControl))

    def test_get_tid_empty_db(self):
        inst = self._makeOne()
        inst.poller.poll_tid = 0
        cur = MockCursor()
        cur.results = None

        self.assertEqual(inst.get_tid(cur), 0)

    def test_add_transaction_hp(self):
        inst = self._makeOne()
        cur = MockCursor(self)
        __traceback_info__ = inst.__dict__
        inst.add_transaction(cur, 1, u'user', u'desc', u'ext')
        self.assertEqual(
            cur.executed.pop(),
            (str(inst._add_transaction_query),
             (1, False, b'user', b'desc', b'ext'))
        )

        inst.add_transaction(cur, 1, u'user', u'desc', u'ext', packed=True)

        self.assertEqual(
            cur.executed.pop(),
            (str(inst._add_transaction_query),
             (1, True, b'user', b'desc', b'ext'))
        )

    def test_commit_phase1(self):
        inst = self._makeOne()
        result = inst.commit_phase1(None, None)
        self.assertEqual(result, '-')

    def test_commit_phase2(self):
        inst = self._makeOne()
        conn = MockConnection()
        inst.commit_phase2(MockStoreConnection(conn), None)
        self.assertTrue(conn.committed)

    def test_abort(self):
        inst = self._makeOne()
        conn = MockConnection()
        inst.abort(MockStoreConnection(conn), None)
        self.assertTrue(conn.rolled_back)
