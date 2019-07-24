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
from relstorage.tests import MockCursor
from relstorage.tests import MockConnection
from relstorage.tests import MockOptions
from relstorage.tests import MockDriver
from relstorage.tests import DisconnectedException
from relstorage.tests import CloseException

from relstorage.adapters.connmanager import AbstractConnectionManager
from relstorage.adapters import interfaces

class TestAbstractConnectionManager(TestCase):

    def _makeOne(self, **options):
        return AbstractConnectionManager(
            MockOptions.from_args(**options),
            MockDriver()
        )

    def test_provides(self):
        cm = self._makeOne()
        assert_that(cm, validly_provides(interfaces.IConnectionManager))

    def test_close_reports_exception(self):
        cm = self._makeOne()

        self.assertTrue(cm.close(MockConnection()))

        class O(object):
            ex = CloseException
            def close(self):
                raise self.ex

        o = O()
        # As a connection
        self.assertFalse(cm.close(o))
        # As a cursor
        self.assertFalse(cm.close(MockConnection(), o))

        # Raising DisconnectedException instead of CloseException
        o.ex = DisconnectedException
        self.assertFalse(cm.close(o))
        self.assertFalse(cm.close(MockConnection(), o))

    def test_rollback_raises_exception(self):
        cm = self._makeOne()

        # With no exceptions, we should get a success report.
        self.assertTrue(cm.rollback(MockConnection(), MockCursor()))

        class O(object):
            closed = False
            fetched = False
            ex = CloseException
            def rollback(self):
                raise self.ex

            def close(self):
                self.closed = True
                raise CloseException

            def fetchall(self):
                self.fetched = True

        o = O()
        # As a cursor, there's only fetchall(), so nothing is reported.
        conn = MockConnection()
        self.assertTrue(cm.rollback(conn, o))
        self.assertTrue(o.fetched)
        self.assertFalse(o.closed)
        self.assertFalse(conn.closed)

        # As a connection the error is reraised and everything is closed.
        o = O()
        cur = MockCursor()
        cur.fetchall = lambda: setattr(cur, 'fetched', True)
        with self.assertRaises(o.ex):
            cm.rollback(o, cur)

        self.assertTrue(o.closed)
        self.assertTrue(cur.fetched) # pylint:disable=no-member
        self.assertTrue(cur.closed)

        # Raising DisconnectedException instead of CloseException
        o = O()
        o.ex = DisconnectedException
        conn = MockConnection()
        self.assertTrue(cm.rollback(conn, o))
        self.assertTrue(o.fetched)
        self.assertFalse(o.closed)
        self.assertFalse(conn.closed)

        o = O()
        o.ex = DisconnectedException
        cur = MockCursor()
        with self.assertRaises(o.ex):
            cm.rollback(o, cur)

        self.assertTrue(o.closed)
        self.assertTrue(cur.closed)

    def test_rollback_and_close_reports_exception(self):
        cm = self._makeOne()

        # With no exceptions, we should get a success report.
        self.assertTrue(cm.rollback_and_close(MockConnection(), MockCursor()))

        class O(object):
            closed = False
            fetched = False
            ex = CloseException
            def rollback(self):
                raise self.ex

            def close(self):
                self.closed = True
                raise CloseException

            def fetchall(self):
                self.fetched = True

        o = O()
        # As a cursor, we fetchall, then we close, so we get a report.
        self.assertFalse(cm.rollback_and_close(MockConnection(), o))
        self.assertTrue(o.fetched)
        self.assertTrue(o.closed)

        o = O()
        # As a connection the error is reported and everything is closed.
        self.assertFalse(cm.rollback_and_close(o, MockCursor()))
        self.assertTrue(o.closed)

        # Raising DisconnectedException instead of CloseException
        o = O()
        o.ex = DisconnectedException
        self.assertFalse(cm.rollback_and_close(MockConnection(), o))
        self.assertTrue(o.fetched)
        self.assertTrue(o.closed)

        o = O()
        o.ex = DisconnectedException
        self.assertFalse(cm.rollback_and_close(o, MockCursor()))
        self.assertTrue(o.closed)

    def test_rollback_quietly_reports_exception(self):
        cm = self._makeOne()
        meth = cm.rollback_quietly
        # With no exceptions, we should get a success report.
        self.assertTrue(meth(MockConnection(), MockCursor()))

        class O(object):
            closed = False
            fetched = False
            ex = CloseException
            def rollback(self):
                raise self.ex

            def close(self):
                self.closed = True
                raise CloseException

            def fetchall(self):
                self.fetched = True

        o = O()
        # As a cursor, we fetchall, but don't close, so we get no report.
        self.assertTrue(meth(MockConnection(), o))
        self.assertTrue(o.fetched)
        self.assertFalse(o.closed)

        o = O()
        # As a connection the error is reported and everything is closed.
        self.assertFalse(meth(o, MockCursor()))
        self.assertTrue(o.closed)

        # Raising DisconnectedException instead of CloseException
        o = O()
        o.ex = DisconnectedException
        self.assertTrue(meth(MockConnection(), o))
        self.assertTrue(o.fetched)
        self.assertFalse(o.closed)

        o = O()
        o.ex = DisconnectedException
        self.assertFalse(meth(o, MockCursor()))
        self.assertTrue(o.closed)

    def test_without_replica_conf(self):
        cm = self._makeOne()

        conn = MockConnection()
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)

        conn = MockConnection()
        cm.restart_store(conn, MockCursor())
        self.assertTrue(conn.rolled_back)

    def test_with_replica_conf(self):
        import os
        import relstorage.tests
        tests_dir = relstorage.tests.__file__
        replica_conf = os.path.join(os.path.dirname(tests_dir), 'replicas.conf')

        cm = self._makeOne(replica_conf=replica_conf)

        conn = MockConnection()
        conn.replica = '127.0.0.1'
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(interfaces.ReplicaClosedException,
                          cm.restart_load, conn, MockCursor())

        conn = MockConnection()
        conn.replica = '127.0.0.1'
        cm.restart_store(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(interfaces.ReplicaClosedException,
                          cm.restart_store, conn, MockCursor())

    def test_with_ro_replica_conf(self):
        import os
        import relstorage.tests
        tests_dir = relstorage.tests.__file__
        replica_conf = os.path.join(os.path.dirname(tests_dir),
                                    'replicas.conf')
        ro_replica_conf = os.path.join(os.path.dirname(tests_dir),
                                       'ro_replicas.conf')

        cm = self._makeOne(replica_conf=replica_conf, ro_replica_conf=ro_replica_conf)

        conn = MockConnection()
        conn.replica = 'readonlyhost'
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(interfaces.ReplicaClosedException,
                          cm.restart_load, conn, MockCursor())
