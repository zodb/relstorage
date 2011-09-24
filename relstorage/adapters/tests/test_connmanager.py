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
        cm = AbstractConnectionManager(MockOptions())

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
        options = MockOptions(replica_conf)

        from relstorage.adapters.connmanager \
            import AbstractConnectionManager
        from relstorage.adapters.interfaces import ReplicaClosedException
        cm = AbstractConnectionManager(options)

        conn = MockConnection()
        conn.replica = 'localhost'
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(ReplicaClosedException,
            cm.restart_load, conn, MockCursor())

        conn = MockConnection()
        conn.replica = 'localhost'
        cm.restart_store(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(ReplicaClosedException,
            cm.restart_store, conn, MockCursor())

    def test_with_ro_replica_conf(self):
        import os
        import relstorage.tests
        tests_dir = relstorage.tests.__file__
        replica_conf = os.path.join(os.path.dirname(tests_dir),
            'replicas.conf')
        ro_replica_conf = os.path.join(os.path.dirname(tests_dir),
            'ro_replicas.conf')
        options = MockOptions(replica_conf, ro_replica_conf)

        from relstorage.adapters.connmanager \
            import AbstractConnectionManager
        from relstorage.adapters.interfaces import ReplicaClosedException
        cm = AbstractConnectionManager(options)

        conn = MockConnection()
        conn.replica = 'readonlyhost'
        cm.restart_load(conn, MockCursor())
        self.assertTrue(conn.rolled_back)
        conn.replica = 'other'
        self.assertRaises(ReplicaClosedException,
            cm.restart_load, conn, MockCursor())


class MockOptions:
    def __init__(self, fn=None, ro_fn=None):
        self.replica_conf = fn
        self.ro_replica_conf = ro_fn
        self.replica_timeout = 600.0

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
    suite.addTest(unittest.makeSuite(AbstractConnectionManagerTests))
    return suite
