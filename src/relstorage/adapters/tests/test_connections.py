# -*- coding: utf-8 -*-
"""
Tests for connections.py

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase
from relstorage.tests import MockConnectionManager

from ..interfaces import IManagedStoreConnection
from ..interfaces import IManagedLoadConnection
from ..interfaces import IManagedDBConnection
from ..connections import StoreConnection
from ..connections import LoadConnection
from ..connections import ClosedConnection

class TestConnectionCommon(TestCase):

    klass = StoreConnection
    iface = IManagedStoreConnection

    def _makeArgument(self):
        return MockConnectionManager()

    def _makeOne(self):
        return self.klass(self._makeArgument())

    def test_provides(self):
        assert_that(self._makeOne(), validly_provides(self.iface))

class TestConnection(TestConnectionCommon):

    def test_restart_and_call_does_not_activate(self):
        manager = self._makeOne()
        self.assertFalse(manager.active)
        self.assertFalse(manager)

        def on_first_use(_conn, _cursor):
            self.fail('Should not call')

        cc = []

        def on_opened(conn, cursor):
            self.assertIsNotNone(conn)
            self.assertIsNotNone(cursor)
            cc.append(conn)
            cc.append(cursor)

        manager.on_first_use = on_first_use
        manager.on_opened = on_opened

        manager.restart_and_call(lambda _conn, _cursor: None)

        # If we try to access the cursor, it gets called, though.
        with self.assertRaises(AssertionError):
            getattr(manager, 'cursor')

        del manager.on_opened
        del manager.on_first_use
        self.assertEqual(cc, [manager.connection, manager.cursor])

    def test_restart_and_call_opens(self):
        manager = self._makeOne()
        opened = [0]
        def on_opened(_conn, _cursor):
            opened[0] += 1
        manager.on_opened = on_opened
        manager.restart_and_call(lambda _conn, _cursor: None)

        self.assertEqual(opened, [1])

    def test_restart_and_call_rollback(self):
        manager = self._makeOne()
        opened = [0]
        def on_rolledback(_conn, _cursor):
            opened[0] += 1
        manager.on_rolledback = on_rolledback

        manager.restart_and_call(lambda _conn, _cursor: None)
        # That was just an open, so no rollback
        self.assertEqual(opened, [0])

        # But second time is the charm
        manager.restart_and_call(lambda _conn, _cursor: None)
        self.assertEqual(opened, [1])

    def test_drop_activates(self, meth='drop'):
        count = [0]

        def on_first_use(_conn, _cursor):
            count[0] += 1

        manager = self._makeOne()
        manager.on_first_use = on_first_use

        getattr(manager, 'cursor')
        self.assertEqual(count, [1])

        getattr(manager, meth)()
        self.assertEqual(count, [1])

        getattr(manager, 'cursor')
        self.assertEqual(count, [2])

    def test_rollback_activates(self):
        self.test_drop_activates('rollback_quietly')

    def test_call_reconnect(self):
        manager = self._makeOne()
        manager.open_if_needed()
        called = []
        def f(_conn, _cur, fresh):
            called.append(fresh)
            if not fresh:
                raise manager.connmanager.driver.disconnected_exceptions[0]

        manager.call(f, can_reconnect=True)

        self.assertEqual(called, [False, True])

class TestLoadConnection(TestConnection):
    klass = LoadConnection
    iface = IManagedLoadConnection

class TestClosedConnection(TestConnectionCommon):
    klass = ClosedConnection
    iface = IManagedDBConnection
