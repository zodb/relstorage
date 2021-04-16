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
from ..connections import StoreConnectionPool

class TestConnectionCommon(TestCase):

    klass = StoreConnection
    iface = IManagedStoreConnection

    def _makeArgument(self):
        return MockConnectionManager(clean_rollback=False)

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



class TestStoreConnectionPool(TestCase):

    def test_borrowing_doesnt_leak_if_creating_raises_exception(self):
        connmanager = MockConnectionManager()

        class MyException(Exception):
            pass

        class StoreConnectionNoBegin(StoreConnection):
            dropped = 0

            def drop(self):
                type(self).dropped += 1
                StoreConnection.drop(self)

            def begin(self):
                raise MyException

        def check(factory, ex):
            pool = StoreConnectionPool(connmanager)

            pool._factory = factory

            with self.assertRaises(ex):
                with pool.borrowing():
                    pass
            self.assertLength(pool._connections, 0)

        check(StoreConnectionNoBegin, MyException)
        self.assertEqual(StoreConnectionNoBegin.dropped, 1)

        class MyBeginException(Exception):
            pass

        def factory(_):
            raise MyBeginException

        check(factory, MyBeginException)

    def test_shrinking_removes_at_beginning(self):
        connmanager = MockConnectionManager(clean_rollback=True)
        connmanager.begin = lambda *_args: None

        pool = StoreConnectionPool(connmanager)

        borrowed = [pool.new_instance().borrow() for _ in range(10)]

        for c in borrowed:
            pool.replace(c)

        self.assertEqual(borrowed, pool._connections)

        pool.MAX_STORE_CONNECTIONS_IN_POOL = 5
        remaining = pool._connections[5:]
        pool._shrink()
        self.assertEqual(remaining, pool._connections)

    def test_returning_dropped_does_not_add_to_pool(self):
        connmanager = MockConnectionManager(clean_rollback=True)
        connmanager.begin = lambda *_args: None

        pool = StoreConnectionPool(connmanager)
        conn = pool.borrow()
        assert conn.connection is not None
        conn.drop()
        assert conn.connection is None
        pool.replace(conn)
        self.assertEqual(0, pool.pooled_connection_count)

    def _makeSemiConnManager(self):
        """
        Return a real conn manager, with minimal mocking.
        """
        from ..connmanager import AbstractConnectionManager
        from relstorage.tests import MockOptions
        from relstorage.tests import MockDriver
        from relstorage.tests import MockConnection

        class ConnManager(AbstractConnectionManager):
            def __init__(self, options=None, driver=None):
                super(ConnManager, self).__init__(options or MockOptions(),
                                                  driver or MockDriver())

                self.begin_count = 0
                self.restart_count = 0

            def restart_store(self, *_args, **_kwargs):
                self.restart_count += 1
                super(ConnManager, self).restart_store(*_args, **_kwargs)

            def begin(self, conn, cursor):
                self.begin_count += 1
                super(ConnManager, self).begin(conn, cursor)

            def open(self, *_args, **_kwargs):
                conn = MockConnection()
                return conn, conn.cursor()

            def _do_open_for_load(self, *_args, **_kwargs):
                raise NotImplementedError
        return ConnManager

    def test_borrow_calls_on_opened_restart(self):
        on_opened_count = []
        def do_on_store_opened(_cursor, restart=None):
            on_opened_count.append(restart)

        connmanager = self._makeSemiConnManager()()
        connmanager.add_on_store_opened(do_on_store_opened)

        pool = StoreConnectionPool(connmanager)

        # restart isn't called to borrow the connection
        store_conn = pool.borrow()
        self.assertEqual(1, connmanager.begin_count)
        self.assertEqual(0, connmanager.restart_count)
        # nor to open it,
        store_conn.open_if_needed()
        # nor to get the cursor, making it active.
        getattr(store_conn, 'cursor')
        self.assertTrue(store_conn)
        self.assertTrue(store_conn.active)
        self.assertEqual(1, connmanager.begin_count)
        self.assertEqual(0, connmanager.restart_count)
        self.assertEqual(on_opened_count, [False])

        pool.replace(store_conn)
        self.assertFalse(store_conn)
        self.assertFalse(store_conn.active)

        # Begin is called on borrowing,
        # and so is restart
        store_conn2 = pool.borrow()
        self.assertIs(store_conn, store_conn2)
        self.assertEqual(2, connmanager.begin_count)
        self.assertEqual(1, connmanager.restart_count)
        self.assertEqual(on_opened_count, [False, True])

    def test_restart_handles_replica_closed(self):
        class MockReplicaSelector(object):
            def current(self):
                return 1

        connmanager = self._makeSemiConnManager()()
        assert hasattr(connmanager, 'replica_selector')
        connmanager.replica_selector = MockReplicaSelector()
        pool = StoreConnectionPool(connmanager)

        store_conn = pool.borrow()
        orig_raw_conn = store_conn.connection

        store_conn.open_if_needed()
        getattr(store_conn, 'cursor')
        pool.replace(store_conn)

        store_conn2 = pool.borrow()
        # Same object,
        self.assertIs(store_conn, store_conn2)
        del store_conn
        # But different internals
        self.assertIsNot(store_conn2.connection, orig_raw_conn)
