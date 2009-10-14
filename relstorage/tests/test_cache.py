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

class StorageCacheTests(unittest.TestCase):

    def setUp(self):
        from relstorage.tests.fakecache import data
        data.clear()

    tearDown = setUp

    def getClass(self):
        from relstorage.cache import StorageCache
        return StorageCache

    def test_ctor(self):
        from relstorage.tests.fakecache import Client
        c = self.getClass()(MockOptions())
        self.assert_(isinstance(c.client, Client))
        self.assertEqual(c.client.servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')

    def test_flush_all(self):
        from relstorage.tests.fakecache import data
        data.clear()
        c = self.getClass()(MockOptions())
        data['x'] = '1'
        c.flush_all()
        self.assert_(not data)

    def test_load_from_current_transaction(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        tid_int = 50
        tid = p64(tid_int)
        data['myprefix:state:2'] = tid + 'STATE'
        state, got_tid_int = c.load(None, 2, tid_int, None)
        self.assertEqual(state, 'STATE')
        self.assertEqual(got_tid_int, tid_int)

    def test_load_from_backptr(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        tid_int = 50
        tid = p64(tid_int)
        data['myprefix:state:2'] = tid + 'STATE'
        data['myprefix:back:60:2'] = tid
        state, got_tid_int = c.load(None, 2, 60, None)
        self.assertEqual(state, 'STATE')
        self.assertEqual(got_tid_int, tid_int)

    def test_load_backptr_missing(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        tid_int = 50
        tid = p64(tid_int)
        data['myprefix:state:2'] = tid + 'STATE'
        adapter = MockAdapter()
        adapter.mover.data[2] = ('STATE', 50)
        state, got_tid_int = c.load(None, 2, 60, adapter)
        self.assertEqual(state, 'STATE')
        self.assertEqual(got_tid_int, 50)
        self.assertEqual(data, {
            'myprefix:state:2': tid + 'STATE',
            'myprefix:back:60:2': tid,
            })

    def test_load_state_expired(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        tid_int = 50
        tid = p64(tid_int)
        data['myprefix:state:2'] = tid + 'STATE'
        adapter = MockAdapter()
        adapter.mover.data[2] = ('NEWSTATE', 55)
        state, got_tid_int = c.load(None, 2, 60, adapter)
        self.assertEqual(state, 'NEWSTATE')
        self.assertEqual(got_tid_int, 55)
        self.assertEqual(data, {
            'myprefix:state:2': p64(55) + 'NEWSTATE',
            'myprefix:back:60:2': p64(55),
            })

    def test_load_state_missing(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        tid_int = 50
        tid = p64(tid_int)
        adapter = MockAdapter()
        adapter.mover.data[2] = ('NEWSTATE', 55)
        state, got_tid_int = c.load(None, 2, 60, adapter)
        self.assertEqual(state, 'NEWSTATE')
        self.assertEqual(got_tid_int, 55)
        self.assertEqual(data, {
            'myprefix:state:2': p64(55) + 'NEWSTATE',
            'myprefix:back:60:2': p64(55),
            })

    def test_load_no_object(self):
        c = self.getClass()(MockOptions())
        adapter = MockAdapter()
        state, got_tid_int = c.load(None, 2, 60, adapter)
        self.assertEqual(state, '')
        self.assertEqual(got_tid_int, None)

    def test_store_temp(self):
        c = self.getClass()(MockOptions())
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(1, 'def')
        c.store_temp(2, 'ghi')
        self.assertEqual(c.queue_contents, {1: (3, 6), 2: (6, 9)})
        c.queue.seek(0)
        self.assertEqual(c.queue.read(), 'abcdefghi')

    def test_tpc_vote_small(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(3, 'def')
        tid = p64(55)
        c.tpc_vote(tid)
        self.assertEqual(data, {
            'myprefix:state:2': tid + 'abc',
            'myprefix:state:3': tid + 'def',
            })

    def test_tpc_vote_large(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        c.send_limit = 100
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(3, 'def' * 100)
        tid = p64(55)
        c.tpc_vote(tid)
        self.assertEqual(data, {
            'myprefix:state:2': tid + 'abc',
            'myprefix:state:3': tid + ('def' * 100),
            })

    def test_tpc_vote_none(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self.getClass()(MockOptions())
        c.tpc_begin()
        tid = p64(55)
        c.tpc_vote(tid)
        self.assertEqual(data, {})

    def test_tpc_finish(self):
        from relstorage.tests.fakecache import data
        c = self.getClass()(MockOptions())
        c.tpc_finish()
        count = data['myprefix:commit_count']
        self.assert_(count > 0)
        c.tpc_finish()
        newcount = data['myprefix:commit_count']
        self.assert_(newcount == count + 1)

    def test_clear_temp(self):
        c = self.getClass()(MockOptions())
        c.tpc_begin()
        c.clear_temp()
        self.assertEqual(c.queue_contents, None)
        self.assertEqual(c.queue, None)

    def test_need_poll(self):
        c = self.getClass()(MockOptions())
        self.assertTrue(c.need_poll())
        self.assertFalse(c.need_poll())
        self.assertFalse(c.need_poll())
        c.tpc_finish()
        self.assertTrue(c.need_poll())
        self.assertFalse(c.need_poll())
        self.assertFalse(c.need_poll())


class MockOptions:
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'
    cache_prefix = 'myprefix'

class MockAdapter:
    def __init__(self):
        self.mover = MockObjectMover()

class MockObjectMover:
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}
    def load_current(self, cursor, oid_int):
        return self.data.get(oid_int, (None, None))

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StorageCacheTests))
    return suite
