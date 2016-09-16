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
from __future__ import print_function, absolute_import
import unittest

from .util import skipOnCI

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
        self.assertIsInstance(c.clients_global_first[0], Client)
        self.assertEqual(c.clients_global_first[0].servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')

        # can be closed multiple times
        c.close()
        c.close()

    def test_clear(self):
        from relstorage.tests.fakecache import data
        data.clear()
        c = self._makeOne()
        data['x'] = '1'
        c.clear()
        self.assertFalse(data)
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
        data['myprefix:state:55:2'] = p64(55) + b'abc'
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 55))

    def test_load_using_delta_after0_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = (b'abc', 55)
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 55))

    def test_load_using_delta_after0_inconsistent(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = (b'abc', 56)
        try:
            c.load(None, 2)
        except AssertionError as e:
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
        except ReadConflictError as e:
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
        data['myprefix:state:50:2'] = p64(45) + b'xyz'
        res = c.load(None, 2)
        self.assertEqual(res, (b'xyz', 45))

    def test_load_using_checkpoint0_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        adapter.mover.data[2] = (b'xyz', 45)
        res = c.load(None, 2)
        self.assertEqual(res, (b'xyz', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + b'xyz')

    def test_load_using_delta_after1_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after1[2] = 45
        data['myprefix:state:45:2'] = p64(45) + b'abc'
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + b'abc')

    def test_load_using_delta_after1_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after1[2] = 45
        adapter.mover.data[2] = (b'abc', 45)
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 45))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(45) + b'abc')

    def test_load_using_checkpoint1_hit(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        data['myprefix:state:40:2'] = p64(35) + b'123'
        res = c.load(None, 2)
        self.assertEqual(res, (b'123', 35))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(35) + b'123')

    def test_load_using_checkpoint1_miss(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        adapter.mover.data[2] = (b'123', 35)
        res = c.load(None, 2)
        self.assertEqual(res, (b'123', 35))
        self.assertEqual(data.get('myprefix:state:50:2'), p64(35) + b'123')

    def test_store_temp(self):
        c = self._makeOne()
        c.tpc_begin()
        c.store_temp(2, b'abc')
        c.store_temp(1, b'def')
        c.store_temp(2, b'ghi')
        self.assertEqual(c.queue_contents, {1: (3, 6), 2: (6, 9)})
        c.queue.seek(0)
        self.assertEqual(c.queue.read(), b'abcdefghi')

    def test_send_queue_small(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.tpc_begin()
        c.store_temp(2, b'abc')
        c.store_temp(3, b'def')
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + b'abc',
            'myprefix:state:55:3': tid + b'def',
            })

    def test_send_queue_large(self):
        from relstorage.tests.fakecache import data
        from ZODB.utils import p64
        c = self._makeOne()
        c.send_limit = 100
        c.tpc_begin()
        c.store_temp(2, b'abc')
        c.store_temp(3, b'def' * 100)
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + b'abc',
            'myprefix:state:55:3': tid + (b'def' * 100),
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
        c.after_tpc_finish(p64(55))
        # XXX: This test doesn't actually assert anything. It used to check
        # the commit-count key, but we don't use that anymore.

    def test_clear_temp(self):
        c = self._makeOne()
        c.tpc_begin()
        c.clear_temp()
        self.assertEqual(c.queue_contents, None)
        self.assertEqual(c.queue, None)

    def test_after_poll_init_checkpoints(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'50 50')

    def test_after_poll_ignore_garbage_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = 'baddata'
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'50 50')

    def test_after_poll_ignore_invalid_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'60 70'  # bad: c0 < c1
        c = self._makeOne()
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'50 50')

    def test_after_poll_reinstate_checkpoints(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.after_poll(None, 40, 50, [])
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'40 30')

    def test_after_poll_future_checkpoints_when_cp_exist(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'90 80'
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.after_poll(None, 40, 50, [(2, 45)])
        # This instance can't yet see txn 90, so it sticks with
        # the existing checkpoints.
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], b'90 80')
        self.assertEqual(c.delta_after0, {2: 45})
        self.assertEqual(c.delta_after1, {})

    def test_after_poll_future_checkpoints_when_cp_nonexistent(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'90 80'
        c = self._makeOne()
        c.after_poll(None, 40, 50, [(2, 45)])
        # This instance can't yet see txn 90, and there aren't any
        # existing checkpoints, so fall back to the current tid.
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'90 80')
        self.assertEqual(c.delta_after0, {})
        self.assertEqual(c.delta_after1, {})

    def test_after_poll_retain_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'40 30'
        c = self._makeOne()
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.delta_after1 = {1: 35}
        c.after_poll(None, 40, 50, [(2, 45), (2, 41)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], b'40 30')
        self.assertEqual(c.delta_after0, {2: 45})
        self.assertEqual(c.delta_after1, {1: 35})

    def test_after_poll_new_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'50 40'
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
        self.assertEqual(data['myprefix:checkpoints'], b'50 40')
        self.assertEqual(c.delta_after0, {})
        self.assertEqual(c.delta_after1, {2: 45, 3: 42})

    def test_after_poll_gap(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'40 30'
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        adapter.poller.changes = [(3, 42), (1, 35), (2, 45)]
        c.checkpoints = (40, 30)
        c.current_tid = 40
        # provide a prev_tid_int that shows a gap in the polled
        # transaction list, forcing a rebuild of delta_after(0|1).
        c.after_poll(None, 43, 50, [(2, 45)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], b'40 30')
        self.assertEqual(c.delta_after0, {2: 45, 3: 42})
        self.assertEqual(c.delta_after1, {1: 35})

    def test_after_poll_shift_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'40 30'
        c = self._makeOne()
        c.delta_size_limit = 2
        c.checkpoints = (40, 30)
        c.current_tid = 40
        c.after_poll(None, 40, 314, [(1, 45), (2, 46)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(data['myprefix:checkpoints'], b'314 40')
        self.assertEqual(c.delta_after0, {1: 45, 2: 46})
        self.assertEqual(c.delta_after1, {})


class LocalClientBucketTests(unittest.TestCase):

    def getClass(self):
        from relstorage.cache import LocalClientBucket
        return LocalClientBucket

    def test_set_bytes_value(self):
        b = self.getClass()(100)
        self.assertEqual(b.size, 0)
        b['abc'] = b'defghi'
        self.assertEqual(b.size, 9)
        b['abc'] = b'123'
        self.assertEqual(b.size, 6)
        b['abc'] = b''
        self.assertEqual(b.size, 3)
        b['abc'] = b'defghi'
        self.assertEqual(b.size, 9)
        del b['abc']
        self.assertEqual(b.size, 0)

    def test_set_limit(self):
        b = self.getClass()(5)
        self.assertEqual(b.size, 0)
        b['abc'] = b'xy'
        self.assertEqual(b.size, 5)
        b['abc'] = b'z'
        self.assertEqual(b.size, 4)
        b['abcd'] = b'xyz'
        # In the past this was 7 and 'abc' was ejected. But the generational
        # system lets us go a bit over.
        self.assertEqual(b.size, 11)
        self.assertEqual(b.get('abc'), b'z')
        self.assertEqual(b.get("abcd"), b'xyz')

    def _load(self, bio, bucket, options):
        from relstorage.cache import _Loader
        bio.seek(0)
        reader = _Loader._gzip_file(options, None, bio, mode='rb')
        return bucket.load_from_file(reader)

    def _save(self, bio, bucket, options):
        from relstorage.cache import _Loader
        bio.seek(0)
        if options.cache_local_dir_compress:
            self.assertEqual(".rscache.gz", _Loader._gzip_ext(options))
        writer = _Loader._gzip_file(options, None, bio, mode='wb')
        bucket.write_to_file(writer)
        writer.flush()
        if writer is not bio:
            writer.close()
        bio.seek(0)
        return bio

    def test_load_and_store(self, options=None):
        from io import BytesIO
        if options is None:
            options = MockOptions()
        client1 = self.getClass()(100)
        client1['abc'] = b'xyz'

        bio = BytesIO()

        self._save(bio, client1, options)

        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2, options)
        self.assertEqual(count, stored)
        self.assertEqual(count, 1)
        self.assertEqual(client1['abc'], client2['abc'])
        self.assertEqual(1, len(client2))
        self.assertEqual(client1.size, client2.size)

        client1.reset_stats()
        client1['def'] = b'123'
        self.assertEqual(2, len(client1))
        client1_max_size = client1.size
        self._save(bio, client1, options)

        # This time there's too much data, so an arbitrary
        # entry gets dropped
        client2 = self.getClass()(3)
        count, stored = self._load(bio, client2, options)
        self.assertEqual(1, len(client2))
        self.assertEqual(count, 2)
        self.assertEqual(stored, 1)


        # Duplicate keys ignored.
        # Note that we do this in client1, because if we do it in client2,
        # the first key (abc) will push out the existing 'def' and get
        # inserted, and then 'def' will push out 'abc'
        count, stored = self._load(bio, client1, options)
        self.assertEqual(count, 2)
        self.assertEqual(stored, 0)
        self.assertEqual(2, len(client1))

        # Half duplicate keys
        del client1['abc']
        self.assertEqual(1, len(client1))

        count, stored = self._load(bio, client1, options)
        self.assertEqual(client1['def'], b'123')
        self.assertEqual(client1['abc'], b'xyz')
        self.assertEqual(count, 2)
        self.assertEqual(stored, 1)
        self.assertEqual(client1.size, client1_max_size)

    def test_load_and_store_to_gzip(self):
        options = MockOptions()
        options.cache_local_dir_compress = True
        self.test_load_and_store(options)

    @skipOnCI("Sometimes the files_loaded is just 1 on Travis.")
    def test_load_from_multiple_files_hit_limit(self):
        from relstorage.cache import _Loader
        import tempfile
        client = self.getClass()(100)
        options = MockOptions()
        options.cache_local_dir_count = 5
        options.cache_local_dir_read_count = 2
        options.cache_local_dir = tempfile.mkdtemp()

        for i in range(5):
            # They all have to have unique keys so something gets loaded
            # from each one
            if i > 0:
                del client[str(i - 1)]
            client[str(i)] = b'abc'
            _Loader.save_local_cache(options, 'test', client, _pid=i)
            self.assertEqual(_Loader.count_cache_files(options, 'test'),
                             i + 1)

        files_loaded = _Loader.load_local_cache(options, 'test', client)
        # XXX: This sometimes fails on Travis, returning 1 Why?
        self.assertEqual(files_loaded, 2)

        import shutil
        shutil.rmtree(options.cache_local_dir)

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
        self.assertEqual(c._bucket_limit, 1000000)
        self.assertEqual(c._value_limit, 16384)
        # cover
        self.assertIn('hits', c.stats())
        c.reset_stats()
        c.disconnect_all()

        self.assertRaises(ValueError,
                          self._makeOne,
                          cache_local_compression='unsup')

    def test_set_and_get_string_compressed(self):
        c = self._makeOne(cache_local_compression='zlib')
        c.set('abc', b'def')
        self.assertEqual(c.get('abc'), b'def')
        self.assertEqual(c.get('xyz'), None)

    def test_set_and_get_string_uncompressed(self):
        c = self._makeOne(cache_local_compression='none')
        c.set('abc', b'def')
        self.assertEqual(c.get('abc'), b'def')
        self.assertEqual(c.get('xyz'), None)

    def test_set_and_get_object_too_large(self):
        c = self._makeOne(cache_local_compression='none')
        c.set('abc', b'abcdefgh' * 10000)
        self.assertEqual(c.get('abc'), None)

    def test_set_with_zero_space(self):
        options = MockOptions()
        options.cache_local_mb = 0
        c = self.getClass()(options)
        self.assertEqual(c._bucket_limit, 0)
        self.assertEqual(c._value_limit, 16384)
        c.set('abc', 1)
        c.set('def', b'')
        self.assertEqual(c.get('abc'), None)
        self.assertEqual(c.get('def'), None)

    def test_set_multi_and_get_multi(self):
        c = self._makeOne()
        c.set_multi({'k0': b'abc', 'k1': b'def'})
        self.assertEqual(c.get_multi(['k0', 'k1']), {'k0': b'abc', 'k1': b'def'})
        self.assertEqual(c.get_multi(['k0', 'k2']), {'k0': b'abc'})
        self.assertEqual(c.get_multi(['k2', 'k3']), {})

    def test_bucket_sizes_without_compression(self):
        # LocalClient is a simple w-TinyLRU cache.  Confirm it keeps the right keys.
        c = self._makeOne(cache_local_compression='none')
        c._bucket_limit = 51
        c.flush_all()
        for i in range(5):
            # add 10 bytes
            c.set('k%d' % i, b'01234567')
        self.assertEqual(c._bucket0.size, 50)

        c.set('k5', b'01234567')
        self.assertEqual(c._bucket0.size, 50)

        v = c.get('k2')
        self.assertEqual(v, b'01234567')
        self.assertEqual(c._bucket0.size, 50)

        for i in range(4):
            # add 10 bytes (2 for the key, 8 for the value)
            c.set('x%d' % i, b'01234567')
        self.assertEqual(c._bucket0.size, 50)

        # x0 and x1 started in eden and got promoted to the main ring.
        # x2 was pushed out of eden and the main ring was full.
        # k2 was allowed to remain because it'd been accessed
        # more often
        self.assertEqual(c.get('x0'), b'01234567')
        self.assertEqual(c.get('x1'), b'01234567')
        self.assertEqual(c.get('x2'), None)
        self.assertEqual(c.get('x3'), b'01234567')
        self.assertEqual(c.get('k2'), b'01234567')
        self.assertEqual(c._bucket0.size, 50)


        self.assertEqual(c.get('k0'), None)
        self.assertEqual(c.get('k1'), None)
        self.assertEqual(c.get('k2'), b'01234567')
        self.assertEqual(c.get('k3'), None)
        self.assertEqual(c.get('k4'), None)
        self.assertEqual(c.get('k5'), b'01234567')


        self.assertEqual(c._bucket0.size, 50)


        c.set('z0', b'01234567')
        self.assertEqual(c._bucket0.size, 50)


    def test_bucket_sizes_with_compression(self):
        c = self._makeOne(cache_local_compression='zlib')
        c._bucket_limit = 23 * 2 + 1
        c.flush_all()

        c.set('k0', b'01234567' * 15)
        self.assertEqual(c._bucket0.size, 23)

        c.set('k1', b'76543210' * 15)
        self.assertEqual(len(c._bucket0), 2)
        self.assertEqual(c._bucket0.size, 23 * 2)

        c.set('k2', b'abcdefgh' * 15)
        self.assertEqual(c._bucket0.size, 23 * 2)

        v = c.get('k0')
        self.assertEqual(v, None) # This one got evicted :(

        v = c.get('k1')
        self.assertEqual(v, b'76543210' * 15)
        self.assertEqual(c._bucket0.size, 46)

        v = c.get('k2')
        self.assertEqual(v, b'abcdefgh' * 15)
        self.assertEqual(c._bucket0.size, 46)

    def test_add(self):
        c = self._makeOne()
        c.set('k0', b'abc')
        self.assertEqual(c.get('k0'), b'abc')
        c.add('k0', b'def')
        c.add('k1', b'ghi')
        self.assertEqual(c.get_multi(['k0', 'k1']), {'k0': b'abc', 'k1': b'ghi'})

    def test_load_and_save(self, _make_dir=True):
        import tempfile
        import shutil
        import os
        temp_dir = tempfile.mkdtemp(".rstest_cache")
        root_temp_dir = temp_dir
        if not _make_dir:
            temp_dir = os.path.join(temp_dir, 'child1', 'child2')
        try:
            c = self._makeOne(cache_local_dir=temp_dir)
            # No files yet.
            self.assertEqual([], os.listdir(temp_dir) if _make_dir else [])
            self.assertEqual([], os.listdir(root_temp_dir))
            # Saving an empty bucket does nothing
            c.save()
            self.assertEqual([], os.listdir(temp_dir) if _make_dir else [])
            self.assertEqual([], os.listdir(root_temp_dir))

            c.set('k0', b'abc')
            c.save()
            cache_files = os.listdir(temp_dir)
            self.assertEqual(1, len(cache_files))
            self.assertTrue(cache_files[0].startswith('relstorage-cache-'), cache_files)

            # Loading it works
            c2 = self._makeOne(cache_local_dir=temp_dir)
            self.assertEqual(c2.get('k0'), b'abc')

            # Change and save and we overwrite the
            # existing file.
            c2.set('k1', b'def')
            c2.save()
            new_cache_files = os.listdir(temp_dir)
            self.assertEqual(cache_files, new_cache_files)

            c3 = self._makeOne(cache_local_dir=temp_dir)
            self.assertEqual(c3.get('k0'), b'abc')
            self.assertEqual(c3.get('k1'), b'def')

            # If we corrupt the file, it is silently ignored and removed
            with open(os.path.join(temp_dir, new_cache_files[0]), 'wb') as f:
                f.write(b'Nope!')

            c3 = self._makeOne(cache_local_dir=temp_dir)
            self.assertEqual(c3.get('k0'), None)
            cache_files = os.listdir(temp_dir)
            self.assertEqual(0, len(cache_files))

            # Now lets break saving
            def badwrite(*args):
                raise OSError("Nope")
            c2._bucket0.write_to_file = badwrite

            c2.save()
            cache_files = os.listdir(temp_dir)
            self.assertEqual(0, len(cache_files))

        finally:
            shutil.rmtree(root_temp_dir)

    def test_load_and_save_new_dir(self):
         # automatically create directories as needed
         self.test_load_and_save(False)

from relstorage.options import Options

class MockOptions(Options):
    cache_module_name = ''
    cache_servers = ''
    cache_local_mb = 1
    cache_local_object_max = 16384
    cache_local_compression = 'zlib'
    cache_delta_size_limit = 10000
    cache_local_dir = None
    cache_local_dir_compress = False
    cache_local_dir_count = 1

class MockOptionsWithFakeCache(MockOptions):
    cache_module_name = 'relstorage.tests.fakecache'
    cache_servers = 'host:9999'

class MockAdapter(object):
    def __init__(self):
        self.mover = MockObjectMover()
        self.poller = MockPoller()

class MockObjectMover(object):
    def __init__(self):
        self.data = {}  # {oid_int: (state, tid_int)}
    def load_current(self, cursor, oid_int):
        return self.data.get(oid_int, (None, None))

class MockPoller(object):
    def __init__(self):
        self.changes = []  # [(oid, tid)]
    def list_changes(self, cursor, after_tid, last_tid):
        return ((oid, tid) for (oid, tid) in self.changes
                if tid > after_tid and tid <= last_tid)

def local_benchmark():
    from relstorage.cache import LocalClient, LocalClientBucket
    options = MockOptions()
    options.cache_local_mb = 100
    options.cache_local_compression = 'none'

    REPEAT_COUNT = 4

    KEY_GROUP_SIZE = 400
    DATA_SIZE = 1024

    # With 1000 in a key group, and 1024 bytes of data, we produce
    # 909100 keys, and 930918400 = 887MB of data, which will overflow
    # a cache of 500 MB.

    # A group size of 100 produces 9100 keys with 9318400 = 8.8MB of data.
    # Likewise, group of 200 produces 36380 keys with 35.5MB of data.

    # Group size of 400 produces 145480 keys with 142MB of data.

    # Most of our time is spent in compression, it seems.
    # In the 8.8mb case, populating all the data with default compression
    # takes about 2.5-2.8s. Using no compression, it takes 0.38 to 0.42s.
    # Reading is the same at about 0.2s.

    # Before any changes:
    # cache_local_mb = 500, datasize = 142, comp=none
    # epop average 3.304353015982391 stddev 0.1057548559581552
    # mix  average 2.922693134013874 stddev 0.014240008454610707
    # pop  average 2.2137693379966854 stddev 0.09458639191519878
    # read average 1.0852473539998755 stddev 0.023173488388016424

    # cache_local_mb = 100, datasize=142, comp=default
    # epop average 30.283703678986058 stddev 0.349105696895158
    # mix  average 32.43547729967395 stddev 0.6131160273617585
    # pop  average 31.683537834013503 stddev 0.9313916809959417
    # read average 0.7965960823348723 stddev 0.013812922826548332

    # cache_local_mb = 100, datasize=142, comp=none
    # epop average 3.8289742503354014 stddev 0.138905518890246
    # mix  average 6.044395485989905 stddev 0.12402917755863634
    # pop  average 4.849317686690483 stddev 0.3407186386084065
    # read average 0.7788464936699407 stddev 0.011301417502572604

    # Following numbers are all with 100/142/none

    # Tracking popularity, but not aging:
    # epop average 3.8351666433348632 stddev 0.016045702030828404
    # mix  average 6.063804395322222 stddev 0.05007505835225963
    # pop  average 4.915782862672738 stddev 0.20628836098923425
    # read average 0.8606604933350658 stddev 0.01461748647882393

    # Aging periodically, adjusted for size. We aged three times
    # during the 'mixed' workload at about  0.024s each. That should be
    # linear, so the 800MB case would take 0.14s....but it would only be done
    # every 9,091,000 operations.

    # I noticed differences accounted for by hash ranomization between runs.
    # From now on, run with 'PYTHONHASHSEED=0 python -O ...'
    # Still same code
    # epop average 3.896360943307324 stddev 0.05112068256616049
    # mix  average 6.08575853901372 stddev 0.0651629903238879
    # pop  average 4.854507520659051 stddev 0.16709270096300968
    # read average 0.9192146409768611 stddev 0.010830646982195127

    # Eden generation, unoptimized
    # epop average 8.394099957639506 stddev 0.1772435870640342
    # mix  average 5.722020475019235 stddev 0.11354930215079416
    # pop  average 9.779178152016053 stddev 0.2953541870308067
    # read average 0.9772441836539656 stddev 0.010378042791130002

    with open('/dev/urandom', 'rb') as f:
        random_data = f.read(DATA_SIZE)

    key_groups = []
    key_groups.append([str(i) for i in range(KEY_GROUP_SIZE)])
    for i in range(1, KEY_GROUP_SIZE):
        keys = [str(i) + str(j) for j in range(KEY_GROUP_SIZE)]
        assert len(set(keys)) == len(keys)
        key_groups.append(keys)

    ALL_DATA = {}
    for group in key_groups:
        for key in group:
            ALL_DATA[key] = random_data
    print(len(ALL_DATA), sum((len(v) for v in ALL_DATA.values()))/1024/1024)
    assert all(isinstance(k, str) for k in ALL_DATA)

    def do_times(client_type=LocalClient):
        client = client_type(options)
        print("Testing", type(client._bucket0._dict))

        def populate():
            for k, v in ALL_DATA.items():
                client.set(k, v)

        def populate_empty():
            c = LocalClient(options)
            for k, v in ALL_DATA.items():
                c.set(k, v)

        def read():
            for keys in key_groups:
                res = client.get_multi(keys)
                #assert len(res) == len(keys)
                assert res.popitem()[1] == random_data

        def mixed():
            hot_keys = key_groups[0]
            i = 0
            for k, v in ALL_DATA.items():
                i += 1
                client.set(k, v)
                if i == len(hot_keys):
                    client.get_multi(hot_keys)
                    i = 0

        import timeit
        import statistics
        try:
            import cProfile, pstats
            raise ImportError
        except ImportError:
            class cProfile(object):
                class Profile(object):
                    def enable(self): pass
                    def disable(self): pass
            class pstats(object):
                class Stats(object):
                    def __init__(self, *args): pass
                    def sort_stats(self, *args): return self
                    def print_stats(self, *args): pass


        number = REPEAT_COUNT
        def run_func(func):
            print("Timing func", func)
            pop_timer = timeit.Timer(func)
            pr = cProfile.Profile()
            pr.enable()
            pop_times = pop_timer.repeat(number=number)
            pr.disable()
            ps = pstats.Stats(pr).sort_stats('cumulative')
            ps.print_stats(.4)

            return pop_times

        times = {}
        for name, func in (('pop ', populate),
                           ('epop', populate_empty),
                           ('read', read),
                           ('mix ', mixed)):
            times[name] = run_func(func)

        for name, time in sorted(times.items()):

            print(name, "average", statistics.mean(time), "stddev", statistics.stdev(time))


    do_times()

def save_load_benchmark():
    from relstorage.cache import LocalClientBucket, _Loader
    from io import BytesIO
    import os
    import itertools

    import sys
    sys.setrecursionlimit(500000)
    bucket = LocalClientBucket(500*1024*1024)
    print("Testing", type(bucket._dict))


    size_dists = [100] * 800 + [300] * 500 + [1024] * 300 + [2048] * 200 + [4096] * 150

    with open('/dev/urandom', 'rb') as rnd:
        data = [rnd.read(x) for x in size_dists]
    data_iter = itertools.cycle(data)

    for j, datum in enumerate(data_iter):
        if len(datum) > bucket.limit or bucket.size + len(datum) > bucket.limit:
            break
        # To ensure the pickle memo cache doesn't just write out "use object X",
        # but distinct copies of the strings, we need to copy them
        bucket[str(j)] = datum[:-1] + b'x'
        assert bucket[str(j)] is not datum
        #print("Len", len(bucket), "size", bucket.size, "dlen", len(datum))

    print("Len", len(bucket), "size", bucket.size)
    number = 1
    import timeit
    import statistics
    import cProfile
    import pstats

    cache_pfx = "pfx"
    cache_options = MockOptions()
    cache_options.cache_local_dir = '/tmp'
    cache_options.cache_local_dir_compress = False

    def write():
        fname = _Loader.save_local_cache(cache_options, cache_pfx, bucket)
        os.remove(fname)


    def load():
        b2 = LocalClientBucket(bucket.limit)
        _Loader.load_local_cache(cache_options, cache_pfx, b2)

    #write_timer = timeit.Timer(write)
    #write_times = write_timer.repeat(number=number)
    #print("write average", statistics.mean(write_times), "stddev", statistics.stdev(write_times))

    #read_timer = timeit.Timer(load)
    #read_times = read_timer.repeat(number=number)
    #print("read average", statistics.mean(read_times), "stddev", statistics.stdev(read_times))

    #pr = cProfile.Profile()
    #pr.enable()

    fname = _Loader.save_local_cache(cache_options, cache_pfx, bucket)
    print("Saved to", fname)
    #pr.disable()
    #ps = pstats.Stats(pr).sort_stats('cumulative')
    #ps.print_stats()
    #return

    pr = cProfile.Profile()
    pr.enable()
    _Loader.load_local_cache(cache_options, cache_pfx, LocalClientBucket(bucket.limit))
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')
    ps.print_stats(.4)


    #os.remove(fname)

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StorageCacheTests))
    suite.addTest(unittest.makeSuite(LocalClientBucketTests))
    suite.addTest(unittest.makeSuite(LocalClientTests))
    return suite

if __name__ == '__main__':
    import sys
    if '--localbench' in sys.argv:
        local_benchmark()
    elif '--iobench' in sys.argv:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        save_load_benchmark()
    else:
        unittest.main(defaultTest='test_suite')
