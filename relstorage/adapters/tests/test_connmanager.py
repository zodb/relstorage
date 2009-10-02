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

import unittest

class AbstractConnectionManagerTests(unittest.TestCase):

    def test_without_replica_conf(self):
        from relstorage.adapters.connmanager import AbstractConnectionManager
        cm = AbstractConnectionManager()

        conn = MockConnection()
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)

        conn = MockConnection()
        cm.restart_store(conn, MockCursor())
        self.assertTrue(conn.rolled_back)

    def test_with_replica_conf(self):
        import tempfile
        f = tempfile.NamedTemporaryFile()
        f.write("example.com:1234\n")
        f.flush()

        from relstorage.adapters.connmanager import AbstractConnectionManager
        from relstorage.adapters.interfaces import ReplicaClosedException
        cm = AbstractConnectionManager(f.name)

        conn = MockConnection()
        conn.replica = 'example.com:1234'
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(ReplicaClosedException,
            cm.restart_load, conn, MockCursor())

        conn = MockConnection()
        conn.replica = 'example.com:1234'
        cm.restart_store(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(ReplicaClosedException,
            cm.restart_store, conn, MockCursor())


class ReplicaSelectorTests(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.f = tempfile.NamedTemporaryFile()
        self.f.write(
            "# Replicas\n\nexample.com:1234\nlocalhost:4321\n"
            "\nlocalhost:9999\n")
        self.f.flush()

    def tearDown(self):
        self.f.close()

    def test__read_config_normal(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        rs = ReplicaSelector(self.f.name)
        self.assertEqual(rs._replicas,
            ['example.com:1234', 'localhost:4321', 'localhost:9999'])

    def test__read_config_empty(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        self.f.seek(0)
        self.f.truncate()
        self.assertRaises(IndexError, ReplicaSelector, self.f.name)

    def test__is_config_modified(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        import time
        rs = ReplicaSelector(self.f.name)
        self.assertEqual(rs._is_config_modified(), False)
        # change the file
        rs._config_modified = 0
        # don't check the file yet
        rs._config_checked = time.time() + 3600
        self.assertEqual(rs._is_config_modified(), False)
        # now check the file
        rs._config_checked = 0
        self.assertEqual(rs._is_config_modified(), True)

    def test__select(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        rs = ReplicaSelector(self.f.name)
        rs._select(0)
        self.assertEqual(rs._current_replica, 'example.com:1234')
        self.assertEqual(rs._current_index, 0)
        self.assertEqual(rs._expiration, None)
        rs._select(1)
        self.assertEqual(rs._current_replica, 'localhost:4321')
        self.assertEqual(rs._current_index, 1)
        self.assertNotEqual(rs._expiration, None)

    def test_current(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        rs = ReplicaSelector(self.f.name)
        self.assertEqual(rs.current(), 'example.com:1234')
        # change the file and get the new current replica
        self.f.seek(0)
        self.f.write('localhost\nalternate\n')
        self.f.flush()
        rs._config_checked = 0
        rs._config_modified = 0
        self.assertEqual(rs.current(), 'localhost')
        # switch to the alternate
        rs._select(1)
        self.assertEqual(rs.current(), 'alternate')
        # expire the alternate
        rs._expiration = 0
        self.assertEqual(rs.current(), 'localhost')

    def test_next_iteration(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        rs = ReplicaSelector(self.f.name)

        # test forward iteration
        self.assertEqual(rs.current(), 'example.com:1234')
        self.assertEqual(rs.next(), 'localhost:4321')
        self.assertEqual(rs.next(), 'localhost:9999')
        self.assertEqual(rs.next(), None)

        # test iteration that skips over the replica that failed
        self.assertEqual(rs.current(), 'example.com:1234')
        self.assertEqual(rs.next(), 'localhost:4321')
        self.assertEqual(rs.current(), 'localhost:4321')
        # next() after current() indicates the last replica failed
        self.assertEqual(rs.next(), 'example.com:1234')
        self.assertEqual(rs.next(), 'localhost:9999')
        self.assertEqual(rs.next(), None)

    def test_next_only_one_server(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        self.f.seek(0)
        self.f.write('localhost\n')
        self.f.flush()
        self.f.truncate()
        rs = ReplicaSelector(self.f.name)
        self.assertEqual(rs.current(), 'localhost')
        self.assertEqual(rs.next(), None)

    def test_next_with_new_conf(self):
        from relstorage.adapters.connmanager import ReplicaSelector
        rs = ReplicaSelector(self.f.name)
        self.assertEqual(rs.current(), 'example.com:1234')
        self.assertEqual(rs.next(), 'localhost:4321')
        # interrupt the iteration by changing the replica conf file
        self.f.seek(0)
        self.f.write('example.com:9999\n')
        self.f.flush()
        self.f.truncate()
        rs._config_checked = 0
        rs._config_modified = 0
        self.assertEqual(rs.next(), 'example.com:9999')
        self.assertEqual(rs.next(), None)


class MockConnection:
    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True

class MockCursor:
    def close(self):
        self.closed = True


def test_suite():
    suite = unittest.TestSuite()
    for klass in [
            AbstractConnectionManagerTests,
            ReplicaSelectorTests,
            ]:
        suite.addTest(unittest.makeSuite(klass))
    return suite
