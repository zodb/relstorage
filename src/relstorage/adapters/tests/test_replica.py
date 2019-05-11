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


class ReplicaSelectorTests(unittest.TestCase):

    def setUp(self):
        import os
        import tempfile
        fd, self.fn = tempfile.mkstemp()
        os.write(fd,
                 b"# Replicas\n\nexample.com:1234\nlocalhost:4321\n"
                 b"\nlocalhost:9999\n")
        os.close(fd)

    def tearDown(self):
        import os
        os.remove(self.fn)

    def test__read_config_normal(self):
        from relstorage.adapters.replica import ReplicaSelector
        rs = ReplicaSelector(self.fn, 600.0)
        self.assertEqual(rs._replicas,
                         ['example.com:1234', 'localhost:4321', 'localhost:9999'])

    def test__read_config_empty(self):
        from relstorage.adapters.replica import ReplicaSelector
        open(self.fn, 'w').close()  # truncate the replica list file
        self.assertRaises(IndexError, ReplicaSelector, self.fn, 600.0)

    def test__is_config_modified(self):
        from relstorage.adapters.replica import ReplicaSelector
        import time
        rs = ReplicaSelector(self.fn, 600.0)
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
        from relstorage.adapters.replica import ReplicaSelector
        rs = ReplicaSelector(self.fn, 600.0)
        rs._select(0)
        self.assertEqual(rs._current_replica, 'example.com:1234')
        self.assertEqual(rs._current_index, 0)
        self.assertEqual(rs._expiration, None)
        rs._select(1)
        self.assertEqual(rs._current_replica, 'localhost:4321')
        self.assertEqual(rs._current_index, 1)
        self.assertNotEqual(rs._expiration, None)

    def test_current(self):
        from relstorage.adapters.replica import ReplicaSelector
        rs = ReplicaSelector(self.fn, 600.0)
        self.assertEqual(rs.current(), 'example.com:1234')
        # change the file and get the new current replica
        f = open(self.fn, 'w')
        f.write('localhost\nalternate\n')
        f.close()
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
        from relstorage.adapters.replica import ReplicaSelector
        rs = ReplicaSelector(self.fn, 600.0)

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
        from relstorage.adapters.replica import ReplicaSelector
        f = open(self.fn, 'w')
        f.write('localhost\n')
        f.close()
        rs = ReplicaSelector(self.fn, 600.0)
        self.assertEqual(rs.current(), 'localhost')
        self.assertEqual(rs.next(), None)

    def test_next_with_new_conf(self):
        from relstorage.adapters.replica import ReplicaSelector
        rs = ReplicaSelector(self.fn, 600.0)
        self.assertEqual(rs.current(), 'example.com:1234')
        self.assertEqual(rs.next(), 'localhost:4321')
        # interrupt the iteration by changing the replica conf file
        f = open(self.fn, 'w')
        f.write('example.com:9999\n')
        f.close()
        rs._config_checked = 0
        rs._config_modified = 0
        self.assertEqual(rs.next(), 'example.com:9999')
        self.assertEqual(rs.next(), None)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ReplicaSelectorTests))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
