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
        options = MockOptions(f.name)

        from relstorage.adapters.connmanager import AbstractConnectionManager
        from relstorage.adapters.interfaces import ReplicaClosedException
        cm = AbstractConnectionManager(options)

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


class MockOptions:
    def __init__(self, fn):
        self.replica_conf = fn
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
