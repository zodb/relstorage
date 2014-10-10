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

    def _makeOne(self):
        return self.getClass()(MockAdapter(), MockOptionsWithFakeCache(),
            'myprefix')

    def test_ctor(self):
        from relstorage.tests.fakecache import Client
        c = self._makeOne()
        self.assertEqual(len(c.clients_local_first), 2)
        self.assertEqual(len(c.clients_global_first), 2)
        self.assert_(isinstance(c.clients_global_first[0], Client))
        self.assertEqual(c.clients_global_first[0].servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')

    def test_clear(self):
        from relstorage.tests.fakecache import data
        data.clear()
        c = self._makeOne()
        data['x'] = '1'
        c.clear()
        self.assert_(not data)
        self.assertEqual(c.checkpoints, None)
        self.assertEqual(c.delta_after0, {})
        self.assertEqual(c.delta_after1, {})

    def test_load_without_checkpoints(self):
        c = self._makeOne()
        res = c.load(None, 2)
        self.assertEqual(res, (None, None))

    def test_load_using_delta_after0_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        data['myprefix:state:55:2'] = p64(55) + 'abc'
        res = c.load(None, 2)
        self.assertEqual(res, ('abc', 55))

    def test_load_using_delta_after0_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = ('abc', 55)
        res = c.load(None, 2)
        self.assertEqual(res, ('abc', 55))

    def test_load_using_delta_after0_inconsistent(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = ('abc', 56)
        try:
            c.load(None, 2)
        except AssertionError, e:
            self.assertTrue('Detected an inconsistency' in e.args[0])
        else:
            self.fail("Failed to report cache inconsistency")

    def test_load_using_delta_after0_future_error(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 55
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = ('abc', 56)
        from ZODB.POSException import ReadConflictError
        try:
            c.load(None, 2)
        except ReadConflictError, e:
            self.assertTrue('future' in e.message)
        else:
            self.fail("Failed to generate a conflict error")

    def test_load_using_checkpoint0_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        data['myprefix:state:50:2'] = p64(45) + 'xyz'
        res = c.load(None, 2)
        self.assertEqual(res, ('xyz', 45))

    def test_load_using_checkpoint0_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        adapter.mover.data[2] = ('xyz', 45)
        res = c.load(None, 2)
        self.assertEqual(res, ('xyz', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + 'xyz')

    def test_load_using_delta_after1_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after1[2] = 45
        data['myprefix:state:45:2'] = p64(45) + 'abc'
        res = c.load(None, 2)
        self.assertEqual(res, ('abc', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + 'abc')

    def test_load_using_delta_after1_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after1[2] = 45
        adapter.mover.data[2] = ('abc', 45)
        res = c.load(None, 2)
        self.assertEqual(res, ('abc', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + 'abc')

    def test_load_using_checkpoint1_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        data['myprefix:state:40:2'] = p64(35) + '123'
        res = c.load(None, 2)
        self.assertEqual(res, ('123', 35))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(35) + '123')

    def test_load_using_checkpoint1_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        adapter.mover.data[2] = ('123', 35)
        res = c.load(None, 2)
        self.assertEqual(res, ('123', 35))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(35) + '123')

    def test_store_temp(self):
        c = self._makeOne()
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(1, 'def')
        c.store_temp(2, 'ghi')
        self.assertEqual(c.queue_contents, {1: (3, 6), 2: (6, 9)})
        c.queue.seek(0)
        self.assertEqual(c.queue.read(), 'abcdefghi')

    def test_send_queue_small(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(3, 'def')
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + 'abc',
            'myprefix:state:55:3': tid + 'def',
            })

    def test_send_queue_large(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.send_limit = 100
        c.tpc_begin()
        c.store_temp(2, 'abc')
        c.store_temp(3, 'def' * 100)
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + 'abc',
            'myprefix:state:55:3': tid + ('def' * 100),
            })

    def test_send_queue_none(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.tpc_begin()
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {})

    def test_after_tpc_finish(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.tpc_begin()
        c.after_tpc_finish(p64(55))
        count = data['myprefix:commits']
        self.assert_(count > 0)
        c.after_tpc_finish(p64(55))
        newcount = data['myprefix:commits']
        self.assert_(newcount == count + 1)

    def test_clear_temp(self):
        c = self._makeOne()
        c.tpc_begin()
        c.clear_temp()
        self.assertEqual(c.queue_contents, None)
        self.assertEqual(c.queue, None)

    def test_need_poll(self):
        from ZODB.utils import p64
        c = self._makeOne()
        self.assertTrue(c.need_poll())
        self.assertFalse(c.need_poll())
        self.assertFalse(c.need_poll())
        c.tpc_begin()
        c.after_tpc_finish(p64(55))
        self.assertTrue(c.need_poll())
        self.assertFalse(c.need_poll())
        self.assertFalse(c.need_poll())

    def test_after_poll_init_checkpoints(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], '50 50')

    def test_after_poll_ignore_garbage_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = 'baddata'
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], '50 50')

    def test_after_poll_ignore_invalid_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '60 70'  # bad: c0 < c1
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], '50 50')

    def test_after_poll_reinstate_checkpoints(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], '40 30')

    def test_after_poll_future_checkpoints_when_cp_exist(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '90 80'
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.after_poll(None, 40, 50, [(2, 45)])
        # This instance can't yet see txn 90, so it sticks with
        # the existing checkpoints.
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], '90 80')
        self.assertEqual(c.delta_after0, {2: 45})
        self.assertEqual(c.delta_after1, {})

    def test_after_poll_future_checkpoints_when_cp_nonexistent(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '90 80'
        c = self._makeOne()
        c.after_poll(None, 40, 50, [(2, 45)])
        # This instance can't yet see txn 90, and there aren't any
        # existing checkpoints, so fall back to the current tid.
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], '90 80')
        self.assertEqual(c.delta_after0, {})
        self.assertEqual(c.delta_after1, {})

    def test_after_poll_retain_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '40 30'
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.delta_after1 = {1: 35}
        c.after_poll(None, 40, 50, [(2, 45), (2, 41)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], '40 30')
        self.assertEqual(c.delta_after0, {2: 45})
        self.assertEqual(c.delta_after1, {1: 35})

    def test_after_poll_new_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '50 40'
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        # Note that OID 3 changed twice.  list_changes is not required
        # to provide the list of changes in order, so simulate
        # a list of changes that is out of order.
        adapter.poller.changes = [(3, 42), (1, 35), (2, 45), (3, 41)]
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.after_poll(None, 40, 50, [(3, 42), (2, 45), (3, 41)])
        self.assertEqual(c.checkpoints, (50, 40))
        self.assertEqual(data['myprefix:checkpoints'], '50 40')
        self.assertEqual(c.delta_after0, {})
        self.assertEqual(c.delta_after1, {2: 45, 3: 42})

    def test_after_poll_gap(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '40 30'
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        adapter.poller.changes = [(3, 42), (1, 35), (2, 45)]
        c.checkpoints = (40, 30)
        c.current_tid = 40
        # provide a prev_tid_int that shows a gap in the polled
        # transaction list, forcing a rebuild of delta_after(0|1).
        c.after_poll(None, 43, 50, [(2, 45)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], '40 30')
        self.assertEqual(c.delta_after0, {2: 45, 3: 42})
        self.assertEqual(c.delta_after1, {1: 35})

    def test_after_poll_shift_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = '40 30'
        c = self._makeOne()
        c.delta_size_limit = 2
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.after_poll(None, 40, 314, [(1, 45), (2, 46)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], '314 40')
        self.assertEqual(c.delta_after0, {1: 45, 2: 46})
        self.assertEqual(c.delta_after1, {})


class LocalClientBucketTests(unittest.TestCase):

    def getClass(self):
        from relstorage.cache import LocalClientBucket
        return LocalClientBucket

    def test_set_string_value(self):
        b = self.getClass()(100)
        self.assertEqual(b.size, 0)
        b['abc'] = 'defghi'
        self.assertEqual(b.size, 9)
        b['abc'] = '123'
        self.assertEqual(b.size, 6)
        b['abc'] = ''
        self.assertEqual(b.size, 3)
        b['abc'] = 'defghi'
        self.assertEqual(b.size, 9)
        del b['abc']
        self.assertEqual(b.size, 0)

    def test_set_integer_value(self):
        b = self.getClass()(100)
        self.assertEqual(b.size, 0)
        b['abc'] = 5
        self.assertEqual(b.size, 3)
        b['abc'] = -7
        self.assertEqual(b.size, 3)
        b['abc'] = 0
        self.assertEqual(b.size, 3)
        del b['abc']
        self.assertEqual(b.size, 0)

    def test_set_limit(self):
        from relstorage.cache import SizeOverflow
        b = self.getClass()(5)
        self.assertEqual(b.size, 0)
        b['abc'] = 'xy'
        self.assertEqual(b.size, 5)
        b['abc'] = 'z'
        self.assertEqual(b.size, 4)
        self.assertRaises(SizeOverflow, b.__setitem__, 'abc', 'xyz')
        self.assertEqual(b['abc'], 'z')


class LocalClientTests(unittest.TestCase):

    def getClass(self):
        from relstorage.cache import LocalClient
        return LocalClient

    def _makeOne(self, **kw):
        options = MockOptions()
        vars(options).update(kw)
        return self.getClass()(options)

    def test_ctor(self):
        c = self._makeOne()
        self.assertEqual(c._bucket_limit, 500000)
        self.assertEqual(c._value_limit, 16384)

    def test_set_and_get_string_compressed(self):
        c = self._makeOne(cache_local_compression='zlib')
        c.set('abc', 'def')
        self.assertEqual(c.get('abc'), 'def')
        self.assertEqual(c.get('xyz'), None)

    def test_set_and_get_string_uncompressed(self):
        c = self._makeOne(cache_local_compression='none')
        c.set('abc', 'def')
        self.assertEqual(c.get('abc'), 'def')
        self.assertEqual(c.get('xyz'), None)

    def test_set_and_get_tuple_compressed(self):
        c = self._makeOne(cache_local_compression='zlib')
        c.set('abc', ('one', 'two'))
        self.assertEqual(c.get('abc'), ('one', 'two'))

    def test_set_and_get_object_too_large(self):
        c = self._makeOne(cache_local_compression='none')
        c.set('abc', 'abcdefgh' * 10000)
        self.assertEqual(c.get('abc'), None)

    def test_set_with_zero_space(self):
        options = MockOptions()
        options.cache_local_mb = 0
        c = self.getClass()(options)
        self.assertEqual(c._bucket_limit, 0)
        self.assertEqual(c._value_limit, 16384)
        c.set('abc', 1)
        c.set('def', '')
        self.assertEqual(c.get('abc'), None)
        self.assertEqual(c.get('def'), None)

    def test_set_multi_and_get_multi(self):
        c = self._makeOne()
        c.set_multi({'k0': 'abc', 'k1': 'def'})
        self.assertEqual(c.get_multi(['k0', 'k1']), {'k0': 'abc', 'k1': 'def'})
        self.assertEqual(c.get_multi(['k0', 'k2']), {'k0': 'abc'})
        self.assertEqual(c.get_multi(['k2', 'k3']), {})

    def test_bucket_sizes_without_compression(self):
        # LocalClient is a simple LRU cache.  Confirm it keeps the right keys.
        c = self._makeOne(cache_local_compression='none')
        c._bucket_limit = 51
        c.flush_all()
        for i in range(5):
            # add 10 bytes
            c.set('k%d' % i, '01234567')
        self.assertEqual(c._bucket0.size, 50)
        self.assertEqual(c._bucket1.size, 0)
        c.set('k5', '01234567')
        self.assertEqual(c._bucket0.size, 10)
        self.assertEqual(c._bucket1.size, 50)
        v = c.get('k2')
        self.assertEqual(v, '01234567')
        self.assertEqual(c._bucket0.size, 20)
        self.assertEqual(c._bucket1.size, 40)
        for i in range(5):
            # add 10 bytes
            c.set('x%d' % i, '01234567')
        self.assertEqual(c._bucket0.size, 20)
        self.assertEqual(c._bucket1.size, 50)
        self.assertEqual(c.get('x0'), '01234567')
        self.assertEqual(c.get('x1'), '01234567')
        self.assertEqual(c.get('x2'), '01234567')
        self.assertEqual(c.get('x3'), '01234567')
        self.assertEqual(c.get('x4'), '01234567')
        self.assertEqual(c._bucket0.size, 50)
        self.assertEqual(c._bucket1.size, 20)
        self.assertEqual(c.get('k0'), None)
        self.assertEqual(c.get('k1'), None)
        self.assertEqual(c.get('k2'), '01234567')
        self.assertEqual(c.get('k3'), None)
        self.assertEqual(c.get('k4'), None)
        self.assertEqual(c.get('k5'), None)

        self.assertEqual(c._bucket0.size, 10)
        self.assertEqual(c._bucket1.size, 50)

        c.set('z0', '01234567')
        self.assertEqual(c._bucket0.size, 20)
        self.assertEqual(c._bucket1.size, 50)

    def test_bucket_sizes_with_compression(self):
        c = self._makeOne(cache_local_compression='zlib')
        c._bucket_limit = 21 * 2 + 1
        c.flush_all()

        c.set('k0', '01234567' * 10)
        self.assertEqual(c._bucket0.size, 21)
        self.assertEqual(c._bucket1.size, 0)

        c.set('k1', '76543210' * 10)
        self.assertEqual(c._bucket0.size, 21 * 2)
        self.assertEqual(c._bucket1.size, 0)

        c.set('k2', 'abcdefgh' * 10)
        self.assertEqual(c._bucket0.size, 21)
        self.assertEqual(c._bucket1.size, 21 * 2)

        v = c.get('k0')
        self.assertEqual(v, '01234567' * 10)
        self.assertEqual(c._bucket0.size, 21 * 2)
        self.assertEqual(c._bucket1.size, 21)

        v = c.get('k1')
        self.assertEqual(v, '76543210' * 10)
        self.assertEqual(c._bucket0.size, 21)
        self.assertEqual(c._bucket1.size, 21 * 2)

        v = c.get('k2')
        self.assertEqual(v, 'abcdefgh' * 10)
        self.assertEqual(c._bucket0.size, 21 * 2)
        self.assertEqual(c._bucket1.size, 21)

    def test_add(self):
        c = self._makeOne()
        c.set('k0', 'abc')
        c.add('k0', 'def')
        c.add('k1', 'ghi')
        self.assertEqual(c.get_multi(['k0', 'k1']), {'k0': 'abc', 'k1': 'ghi'})

    def test_incr_normal(self):
        c = self._makeOne()
        c.set('k0', 41)
        self.assertEqual(c.incr('k0'), 42)
        self.assertEqual(c.incr('k1'), None)

    def test_incr_string_with_compression(self):
        c = self._makeOne(cache_local_compression='zlib')
        c.set('k0', '41')
        self.assertEqual(c.incr('k0'), 42)
        self.assertEqual(c.incr('k1'), None)

    def test_incr_string_without_compression(self):
        c = self._makeOne(cache_local_compression='none')
        c.set('k0', '41')
        self.assertEqual(c.incr('k0'), 42)
        self.assertEqual(c.incr('k1'), None)

    def test_incr_hit_size_limit(self):
        c = self._makeOne()
        c._bucket_limit = 4
        c.flush_all()
        c.set('k0', 14)
        c.set('key1', 27)
        self.assertEqual(c._bucket0.size, 4)
        self.assertEqual(c._bucket1.size, 2)
        self.assertEqual(c.incr('k0'), 15)  # this moves k0 to bucket0
        self.assertEqual(c._bucket0.size, 2)
        self.assertEqual(c._bucket1.size, 4)

    def test_incr_with_zero_space(self):
        options = MockOptions()
        options.cache_local_mb = 0
        c = self.getClass()(options)
        self.assertEqual(c.incr('abc'), None)


class MockOptions:
    cache_module_name = ''
    cache_servers = ''
    cache_local_mb = 1
    cache_local_object_max = 16384
    cache_local_compression = 'zlib'
    cache_delta_size_limit = 10000

class MockOptionsWithFakeCache:
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'
    cache_local_mb = 1
    cache_local_object_max = 16384
    cache_local_compression = 'zlib'
    cache_delta_size_limit = 10000

class MockAdapter:
    def __init__(self):
        self.mover = MockObjectMover()
        self.poller = MockPoller()

class MockObjectMover:
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}
    def load_current(self, cursor, oid_int):
        return self.data.get(oid_int, (None, None))

class MockPoller:
    def __init__(self):
        self.changes = []  # [(oid, tid)]
    def list_changes(self, cursor, after_tid, last_tid):
        return ((oid, tid) for (oid, tid) in self.changes
                if tid > after_tid and tid <= last_tid)

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StorageCacheTests))
    suite.addTest(unittest.makeSuite(LocalClientBucketTests))
    suite.addTest(unittest.makeSuite(LocalClientTests))
    return suite
