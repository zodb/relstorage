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

# pylint:disable=too-many-lines,abstract-method,too-many-public-methods,attribute-defined-outside-init
import unittest
from functools import partial

from ZODB.utils import p64

from relstorage.cache.cache_ring import Cache as _BaseCache
from relstorage.cache.local_client import LocalClient as _BaseLocalClient
from relstorage.cache.mapping import SizedLRUMapping as _BaseSizedLRUMapping
from relstorage.options import Options


class Cache(_BaseCache):
    # Tweak the generation sizes to match what we developed the tests with
    _gen_protected_pct = 0.8
    _gen_eden_pct = 0.1


class SizedLRUMapping(_BaseSizedLRUMapping):
    _cache_type = Cache


class LocalClient(_BaseLocalClient):
    _bucket_type = SizedLRUMapping


class StorageCacheTests(unittest.TestCase):

    def setUp(self):
        from relstorage.tests.fakecache import data
        data.clear()
        self._instances = []

    def tearDown(self):
        from relstorage.tests.fakecache import data
        data.clear()
        for inst in self._instances:
            inst.close()
            assert len(inst) == 0 # pylint:disable=len-as-condition
            assert bool(inst)
            assert inst.size == 0
            assert inst.limit == 0

    def getClass(self):
        from relstorage.cache import StorageCache
        return StorageCache

    def _makeOne(self, **kwargs):
        options = MockOptionsWithFakeCache.from_args(**kwargs)
        inst = self.getClass()(MockAdapter(), options,
                               'myprefix')
        self._instances.append(inst)
        return inst.new_instance() # coverage and sharing testing

    def test_ctor(self):
        from relstorage.tests.fakecache import Client
        from relstorage.cache.storage_cache import _MemcacheStateCache
        c = self._makeOne()
        self.assertEqual(len(c.clients_local_first), 2)
        self.assertEqual(len(c.clients_global_first), 2)
        self.assertIsInstance(c.clients_global_first[0], _MemcacheStateCache)
        self.assertIsInstance(c.clients_global_first[0].client, Client)
        self.assertEqual(c.clients_global_first[0].client.servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')

        # can be closed multiple times
        c.close()
        c.close()

    def test_stats(self):
        inst = self._makeOne()
        self.assertIsInstance(inst.stats(), dict)
        inst.close()
        self.assertIsInstance(inst.stats(), dict)

    def test_save(self):
        import threading
        import tempfile
        import os
        import shutil
        import time

        active_threads = threading.active_count()

        c = self._makeOne()
        c.checkpoints = (0, 0)
        c.tpc_begin()
        c.store_temp(2, b'abc')
        # tid is 2016-09-29 11:35:58,120
        tid = 268595726030645777
        c.after_tpc_finish(p64(268595726030645777))

        key = list(iter(c.local_client))[0]
        self.assertEqual((2, tid), key)

        c.options.cache_local_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, c.options.cache_local_dir, True)

        c.save()
        files = os.listdir(c.options.cache_local_dir)
        __traceback_info__ = files
        # Older versions of sqlite may leave -shm and -wal
        # files around.
        self.assertGreaterEqual(len(files), 1)

        # Creating one in the same place automatically loads it.
        c2 = self._makeOne(cache_local_dir=c.options.cache_local_dir)
        self.assertEqual(1, len(c2))

        # Avoid warnings from testrunner
        while threading.active_count() > active_threads:
            time.sleep(0.01)

    def test_clear(self):
        from relstorage.tests.fakecache import data
        data.clear()
        c = self._makeOne()
        data['x'] = '1'
        c.clear()
        self.assertFalse(data)
        self.assertEqual(c.checkpoints, None)
        self.assertEqual(dict(c.delta_after0), {})
        self.assertEqual(dict(c.delta_after1), {})

    def test_load_without_checkpoints(self):
        c = self._makeOne()
        res = c.load(None, 2)
        self.assertEqual(res, (None, None))

    def test_load_using_delta_after0_hit(self):
        from relstorage.tests.fakecache import data
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        data['myprefix:state:55:2'] = p64(55) + b'abc'
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 55))

    def test_load_using_delta_after0_miss(self):
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = (b'abc', 55)
        res = c.load(None, 2)
        self.assertEqual(res, (b'abc', 55))

    def test_load_using_delta_after0_inconsistent(self):
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
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        data['myprefix:state:50:2'] = p64(45) + b'xyz'
        res = c.load(None, 2)
        self.assertEqual(res, (b'xyz', 45))

    def test_load_using_checkpoint0_miss(self):
        from relstorage.tests.fakecache import data
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
        __traceback_info__ = data
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
        self.assertEqual(b'ghi', c.read_temp(2))
        self.assertEqual(c.queue_contents, {1: (3, 6), 2: (6, 9)})
        c.queue.seek(0)
        self.assertEqual(c.queue.read(), b'abcdefghi')
        c.checkpoints = (1, 0)
        c.after_tpc_finish(p64(3))

        self.assertEqual(dict(c.delta_after0), {2: 3, 1: 3})

    def test_send_queue_small(self):
        from relstorage.tests.fakecache import data
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
        self.assertEqual(len(c), 2)

    def test_send_queue_large(self):
        from relstorage.tests.fakecache import data
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
        c = self._makeOne()
        c.tpc_begin()
        tid = p64(55)
        c.send_queue(tid)
        self.assertEqual(data, {})

    def test_after_tpc_finish(self):
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
        self.assertEqual(data, {})
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
        self.assertEqual(dict(c.delta_after0), {2: 45})
        self.assertEqual(dict(c.delta_after1), {})

    def test_after_poll_future_checkpoints_when_cp_nonexistent(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'90 80'
        c = self._makeOne()
        c.after_poll(None, 40, 50, [(2, 45)])
        # This instance can't yet see txn 90, and there aren't any
        # existing checkpoints, so fall back to the current tid.
        self.assertEqual(c.checkpoints, (50, 50))
        self.assertEqual(data['myprefix:checkpoints'], b'90 80')
        self.assertEqual(dict(c.delta_after0), {})
        self.assertEqual(dict(c.delta_after1), {})

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
        self.assertEqual(dict(c.delta_after0), {2: 45})
        self.assertEqual(dict(c.delta_after1), {1: 35})

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
        self.assertEqual(dict(c.delta_after0), {})
        self.assertEqual(dict(c.delta_after1), {2: 45, 3: 42})

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
        self.assertEqual(dict(c.delta_after0), {2: 45, 3: 42})
        self.assertEqual(dict(c.delta_after1), {1: 35})

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
        self.assertEqual(dict(c.delta_after0), {1: 45, 2: 46})
        self.assertEqual(dict(c.delta_after1), {})

def list_lrukeys_(lru, lru_name):
    # Remember, these lists will be from LRU to MRU
    return [e.key for e in getattr(lru, '_' + lru_name)]


def list_lrufreq_(lru, lru_name):
    return [e.frequency for e in getattr(lru, '_' + lru_name)]


class SizedLRUMappingTests(unittest.TestCase):

    def assertNone(self, o):
        if o is not None:
            raise AssertionError("Expected None, not %r" % (o,))

    def assertNotNone(self, o):
        if o is None:
            raise AssertionError("Expected not None")

    def getClass(self):
        return SizedLRUMapping

    def test_age_empty(self):
        c = self.getClass()(100)
        c._age_factor = 0
        c._age()

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

    def test_increasing_size_in_eden_w_empty_protected_bumps_to_protected(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # Get eden to exactly its size.
        b['a'] = b'x'
        self.assertEqual(b.size, 2)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        b['b'] = b'y'
        self.assertEqual(b.size, 4)
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden, and bumping down to protected.
        b['a'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['a'])
        self.assertEqual(list_lrukeys('protected'), ['b'])

        self.assertEqual(b.size, 6)

    def test_increasing_size_in_eden_w_partial_protected_bumps_to_protected(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # Fill up eden and begin spilling to protected
        b['a'] = b'x'
        self.assertEqual(b.size, 2)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        b['b'] = b'y'
        self.assertEqual(b.size, 4)
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])

        b['c'] = b'z'
        self.assertEqual(b.size, 6)
        self.assertEqual(list_lrukeys('eden'), ['b', 'c'])
        self.assertEqual(list_lrukeys('protected'), ['a'])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden, and bumping down to protected.
        b['b'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a', 'c'])
        self.assertEqual(b.size, 8)

    def test_increasing_size_in_eden_w_full_protected_bumps_to_probation(self):
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        # This actually stays in eden because it's the newest key,
        # even though it's too big
        b['a'] = b'x' * 31
        self.assertEqual(b.size, 32)
        self.assertEqual(list_lrukeys('eden'), ['a'])

        # But this will immediately force a into protected
        b['b'] = b'y'
        self.assertEqual(b.size, 34)
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), [])

        # Ok, now fill up eden with another key
        b['c'] = b'z'
        self.assertEqual(b.size, 36)
        self.assertEqual(list_lrukeys('eden'), ['b', 'c'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), [])

        # Now increase an existing key, thus making it in MRU,
        # and going over size of eden. protected is full, so we go to probation.
        b['b'] = b'xyz'
        self.assertEqual(list_lrukeys('eden'), ['b'])
        self.assertEqual(list_lrukeys('protected'), ['a'])
        self.assertEqual(list_lrukeys('probation'), ['c'])
        self.assertEqual(b.size, 38)

        # Nothing was evicted
        self.assertEqual(b['a'], b'x' * 31)
        self.assertEqual(b['b'], b'xyz')
        self.assertEqual(b['c'], b'z')

    def test_increasing_size_in_full_protected_bumps_to_probation(self):
        # Fill up in the normal way
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        for k in range(10):
            # 10 4 byte entries
            b[str(k)] = 'abc'

        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['0', '1', '2', '3', '4', '5', '6', '7'])
        self.assertEqual(list_lrukeys('probation'), ['8'])
        self.assertEqual(b.size, 40)

        # Now bump protected over size, ejecting to probation.
        # Note that we drop an element to get us in size
        b['3'] = 'abcd'
        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['1', '2', '4', '5', '6', '7', '3'])
        self.assertEqual(list_lrukeys('probation'), ['0'])
        self.assertEqual(b.size, 37)

        # We can access only the ones that remain
        for k in range(8):
            self.assertNotNone(b.get(str(k)))

        self.assertNone(b.get('8'))
        self.assertNotNone(b.get('9'))

    def test_increasing_size_in_full_probation_full_protection_bumps_to_probation(self):
        # Fill up in the normal way
        b = self.getClass()(40)
        list_lrukeys = partial(list_lrukeys_, b)

        self.assertEqual(b._eden.limit, 4)
        self.assertEqual(b._probation.limit, 4)
        self.assertEqual(b._protected.limit, 32)

        for k in range(10):
            # 10 4 byte entries
            b[str(k)] = 'abc'

        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['0', '1', '2', '3', '4', '5', '6', '7'])
        self.assertEqual(list_lrukeys('probation'), ['8'])
        self.assertEqual(b.size, 40)

        # Now increase an entry in probation. This will move it to protected, which
        # will now be oversize.
        # Note that we drop an element to get us within size
        b['8'] = 'abcd'
        self.assertEqual(list_lrukeys('eden'), ['9'])
        self.assertEqual(list_lrukeys('protected'), ['2', '3', '4', '5', '6', '7', '8'])
        self.assertEqual(list_lrukeys('probation'), ['1'])
        self.assertEqual(b.size, 37)

        # We can access only the ones that remain
        for k in range(1, 10):
            self.assertNotNone(b.get(str(k)))

        self.assertNone(b.get('0'))

    def _load(self, bio, bucket):
        bio.seek(0)
        return bucket.read_from_stream(bio)

    def _save(self, bio, bucket, options, byte_limit=None):
        bio.seek(0)
        writer = bio
        count = bucket.write_to_stream(writer, byte_limit or options.cache_local_dir_write_max_size)
        writer.flush()
        bio.seek(0)
        return count

    def test_load_and_store(self, options=None):
        # pylint:disable=too-many-statements
        from io import BytesIO
        if options is None:
            options = MockOptions()
        client1 = self.getClass()(100)
        client1['abc'] = b'xyz'

        bio = BytesIO()

        self._save(bio, client1, options)
        # Regardless of its read frequency, it's still written
        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, stored)
        self.assertEqual(count, 1)

        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, stored)
        self.assertEqual(count, 1)

        self.assertEqual(client1['abc'], client2['abc'])
        self.assertEqual(1, len(client2))
        self.assertEqual(client1.size, client2.size)

        client1.reset_stats()
        client1['def'] = b'123'
        _ = client1['def']
        self.assertEqual(2, len(client1))
        client1_max_size = client1.size
        bio = BytesIO()
        count_written = self._save(bio, client1, options)
        self.assertEqual(2, len(client1))
        self.assertEqual(count_written, len(client1))

        # This time there's too much data, so an arbitrary
        # entry gets dropped
        client2 = self.getClass()(7)
        count, stored = self._load(bio, client2)
        self.assertEqual(0, len(client2))
        self.assertEqual(count, 2)
        self.assertEqual(stored, 0)

        client2 = self.getClass()(8)
        count, stored = self._load(bio, client2)
        self.assertEqual(2, len(client2))
        self.assertEqual(count, 2)
        self.assertEqual(stored, 2)


        # Duplicate keys ignored.
        # Note that we do this in client1, because if we do it in client2,
        # the first key (abc) will push out the existing 'def' and get
        # inserted, and then 'def' will push out 'abc'
        count, stored = self._load(bio, client1)
        self.assertEqual(count, 2)
        self.assertEqual(stored, 0)
        self.assertEqual(2, len(client1))


        # Half duplicate keys
        self.assertEqual(2, len(client1))
        del client1['abc']
        self.assertEqual(1, len(client1))

        count, stored = self._load(bio, client1)
        self.assertEqual(client1['def'], b'123')
        self.assertEqual(client1['abc'], b'xyz')
        self.assertEqual(count, 2)
        self.assertEqual(stored, 1)
        self.assertEqual(client1.size, client1_max_size)

        # Even keys that have been aged down to 0 still get
        # written.
        # Force the conditions for it to actually do something.
        client1.limit = 0
        client1._age_factor = 0
        client1._age()
        client1._age()
        self.assertEqual(len(client1), 2)
        self.assertEqual(client1.size, client1_max_size)

        bio = BytesIO()
        self._save(bio, client1, options, client1_max_size)


        client1 = self.getClass()(100)
        count, stored = self._load(bio, client1)
        self.assertEqual(count, 2)
        self.assertEqual(stored, 2)
        self.assertEqual(client1.size, client1_max_size)

        list_lrukeys = partial(list_lrukeys_, client1)
        self.assertEqual(list_lrukeys('eden'), ['abc'])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), ['def'])

        # Don't write anything if the limit is too small, but
        # we can still read it.
        bio = BytesIO()
        self._save(bio, client1, options, 1)

        client2 = self.getClass()(3)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, 0)
        self.assertEqual(stored, 0)

        # If the limit is smaller than the size, write the most frequently used
        # items
        client1 = self.getClass()(100)
        list_lrukeys = partial(list_lrukeys_, client1)
        client1['a'] = b'1'
        client1['b'] = b'2'
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])
        client1.get_and_bubble_all(('a',))
        client1.get_and_bubble_all(('a',))
        client1.get_and_bubble_all(('a',))
        self.assertEqual(list_lrukeys('eden'), ['b', 'a'])
        client1.get_and_bubble_all(('b',))
        self.assertEqual(list_lrukeys('eden'), ['a', 'b'])
        client1.get_and_bubble_all(('a',))
        self.assertEqual(list_lrukeys('eden'), ['b', 'a'])

        # A is much more popular than b

        bio = BytesIO()
        self._save(bio, client1, options, 2)

        client2 = self.getClass()(100)
        count, stored = self._load(bio, client2)
        self.assertEqual(count, 1)
        self.assertEqual(stored, 1)
        self.assertEqual(list_lrukeys_(client2, 'eden'), ['a'])


    def test_load_and_store_to_gzip(self):
        options = MockOptions()
        options.cache_local_dir_compress = True
        self.test_load_and_store(options)

class LocalClientStrKeysValues(LocalClient):
    # Make the cache accept and return str keys and values,
    # for ease of dealing with size limits (and compatibility with old tests)

    key_weight = len

    def value_weight(self, value):
        return len(value[0] if value[0] else b'')

    def __setitem__(self, key, val):
        super(LocalClientStrKeysValues, self).__setitem__(key, (val, 0))

    def __getitem__(self, key):
        v = self(None, None, None, (key,))
        if v is not None:
            v = v[0]
        return v

class LocalClientStrKeysValuesTests(unittest.TestCase):
    def getClass(self):
        return LocalClientStrKeysValues

    def _makeOne(self, **kw):
        options = MockOptions.from_args(**kw)
        inst = self.getClass()(options, 'pfx')
        inst.restore()
        return inst

    def test_ctor(self):
        c = self._makeOne()
        self.assertEqual(c.limit, 1000000)
        self.assertEqual(c._value_limit, 16384)
        # cover
        self.assertIn('hits', c.stats())
        c.reset_stats()
        c.close()

        self.assertRaises(ValueError,
                          self._makeOne,
                          cache_local_compression='unsup')

    def test_set_and_get_string_compressed(self):
        c = self._makeOne(cache_local_compression='zlib')
        c['abc'] = b'def'
        self.assertEqual(c['abc'], b'def')
        self.assertEqual(c['xyz'], None)

    def test_set_and_get_string_uncompressed(self):
        c = self._makeOne(cache_local_compression='none')
        c['abc'] = b'def'
        self.assertEqual(c['abc'], b'def')
        self.assertEqual(c['xyz'], None)

    def test_set_and_get_object_too_large(self):
        c = self._makeOne(cache_local_compression='none')
        c['abc'] = b'abcdefgh' * 10000
        self.assertEqual(c['abc'], None)

    def test_set_with_zero_space(self):
        options = MockOptions()
        options.cache_local_mb = 0
        c = self.getClass()(options)
        self.assertEqual(c.limit, 0)
        self.assertEqual(c._value_limit, 16384)
        c['abc'] = 1
        c['def'] = b''
        self.assertEqual(c['abc'], None)
        self.assertEqual(c['def'], None)

    def test_bucket_sizes_without_compression(self):
        # pylint:disable=too-many-statements
        # LocalClient is a simple w-TinyLRU cache.  Confirm it keeps the right keys.
        c = self._makeOne(cache_local_compression='none')
        # This limit will result in
        # eden and probation of 5, protected of 40. This means that eden
        # and probation each can hold one item, while protected can hold 4,
        # so our max size will be 60
        c.limit = 51
        c.flush_all()

        list_lrukeys = partial(list_lrukeys_, c._bucket0)
        list_lrufreq = partial(list_lrufreq_, c._bucket0)

        k = None

        for i in range(5):
            # add 10 bytes (2 for the key, 8 for the value)
            k = 'k%d' % i
            # This will go to eden, replacing any value that was there
            # into probation.
            c[k] = b'01234567'


        # While we have the room, we initially put items into the protected
        # space when they graduate from eden.
        self.assertEqual(list_lrukeys('eden'), ['k4'])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), ['k0', 'k1', 'k2', 'k3'])
        self.assertEqual(c._bucket0.size, 50)

        c['k5'] = b'01234567'

        # Right now, we're one entry over size, because we put k5
        # in eden, which dropped k4 to probation; since probation was empty, we
        # allowed it to stay there
        self.assertEqual(list_lrukeys('eden'), ['k5'])
        self.assertEqual(list_lrukeys('probation'), ['k4'])
        self.assertEqual(list_lrukeys('protected'), ['k0', 'k1', 'k2', 'k3'])
        self.assertEqual(c._bucket0.size, 60)

        v = c['k2']
        self.assertEqual(v, b'01234567')
        self.assertEqual(c._bucket0.size, 60)

        c['k1'] = b'b'
        self.assertEqual(list_lrukeys('eden'), ['k5'])
        self.assertEqual(list_lrukeys('probation'), ['k4'])
        self.assertEqual(list_lrukeys('protected'), ['k0', 'k3', 'k2', 'k1'])

        self.assertEqual(c._bucket0.size, 53)

        for i in range(4):
            # add 10 bytes (2 for the key, 8 for the value)
            c['x%d' % i] = b'01234567'
            # Notice that we're not promoting these through the layers. So
            # when we're done, we'll wind up with one key each in
            # eden and probation, and all the K keys in protected (since
            # they have been promoted)


        # x0 and x1 started in eden and got promoted to the probation ring,
        # from whence they were ejected because of never being accessed.
        # k2 was allowed to remain because it'd been accessed
        # more often
        self.assertEqual(list_lrukeys('eden'), ['x3'])
        self.assertEqual(list_lrukeys('probation'), ['x2'])
        self.assertEqual(list_lrukeys('protected'), ['k0', 'k3', 'k2', 'k1'])
        self.assertEqual(c._bucket0.size, 53)

        #pprint.pprint(c._bucket0.stats())
        self.assertEqual(c['x0'], None)
        self.assertEqual(c['x1'], None)
        self.assertEqual(c['x2'], b'01234567')
        self.assertEqual(c['x3'], b'01234567')
        self.assertEqual(c['k2'], b'01234567')
        self.assertEqual(c._bucket0.size, 53)

        # Note that this last set of checks perturbed protected and probation;
        # We lost a key
        #pprint.pprint(c._bucket0.stats())
        self.assertEqual(list_lrukeys('eden'), ['x3'])
        self.assertEqual(list_lrukeys('probation'), ['k0'])
        self.assertEqual(list_lrukeys('protected'), ['k3', 'k1', 'x2', 'k2'])
        self.assertEqual(c._bucket0.size, 53)

        self.assertEqual(c['k0'], b'01234567')
        self.assertEqual(c['k0'], b'01234567') # One more to increase its freq count
        self.assertEqual(c['k1'], b'b')
        self.assertEqual(c['k2'], b'01234567')
        self.assertEqual(c['k3'], b'01234567')
        self.assertEqual(c['k4'], None)
        self.assertEqual(c['k5'], None)

        # Let's promote from probation, causing places to switch.
        # First, verify our current state after those gets.
        self.assertEqual(list_lrukeys('eden'), ['x3'])
        self.assertEqual(list_lrukeys('probation'), ['x2'])
        self.assertEqual(list_lrukeys('protected'), ['k0', 'k1', 'k2', 'k3'])
        # Now get and switch
        c.__getitem__('x2')
        self.assertEqual(list_lrukeys('eden'), ['x3'])
        self.assertEqual(list_lrukeys('probation'), ['k0'])
        self.assertEqual(list_lrukeys('protected'), ['k1', 'k2', 'k3', 'x2'])
        self.assertEqual(c._bucket0.size, 53)

        # Confirm frequency counts
        self.assertEqual(list_lrufreq('eden'), [2])
        self.assertEqual(list_lrufreq('probation'), [3])
        self.assertEqual(list_lrufreq('protected'), [3, 4, 2, 3])
        # A brand new key is in eden, shifting eden to probation

        c['z0'] = b'01234567'

        # Now, because we had accessed k0 (probation) more than we'd
        # accessed the last key from eden (x3), that's the one we keep
        self.assertEqual(list_lrukeys('eden'), ['z0'])
        self.assertEqual(list_lrukeys('probation'), ['k0'])
        self.assertEqual(list_lrukeys('protected'), ['k1', 'k2', 'k3', 'x2'])

        self.assertEqual(list_lrufreq('eden'), [1])
        self.assertEqual(list_lrufreq('probation'), [3])
        self.assertEqual(list_lrufreq('protected'), [3, 4, 2, 3])

        self.assertEqual(c._bucket0.size, 53)

        self.assertEqual(c['x3'], None)
        self.assertEqual(list_lrukeys('probation'), ['k0'])


    def test_bucket_sizes_with_compression(self):
        # pylint:disable=too-many-statements
        c = self._makeOne(cache_local_compression='zlib')
        c.limit = 23 * 2 + 1
        c.flush_all()
        list_lrukeys = partial(list_lrukeys_, c._bucket0)

        k0_data = b'01234567' * 15
        c['k0'] = k0_data
        self.assertEqual(c._bucket0.size, 23) # One entry in eden
        self.assertEqual(list_lrukeys('eden'), ['k0'])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), [])

        k1_data = b'76543210' * 15

        c['k1'] = k1_data
        self.assertEqual(len(c._bucket0), 2)

        self.assertEqual(c._bucket0.size, 23 * 2)
        # Since k0 would fit in protected and we had nothing in
        # probation, that's where it went
        self.assertEqual(list_lrukeys('eden'), ['k1'])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), ['k0'])

        k2_data = b'abcdefgh' * 15
        c['k2'] = k2_data

        # New key is in eden, old eden goes to probation because
        # protected is full. Note we're slightly oversize
        self.assertEqual(list_lrukeys('eden'), ['k2'])
        self.assertEqual(list_lrukeys('probation'), ['k1'])
        self.assertEqual(list_lrukeys('protected'), ['k0'])

        self.assertEqual(c._bucket0.size, 23 * 3)

        v = c['k0']
        self.assertEqual(v, k0_data)
        self.assertEqual(list_lrukeys('eden'), ['k2'])
        self.assertEqual(list_lrukeys('probation'), ['k1'])
        self.assertEqual(list_lrukeys('protected'), ['k0'])


        v = c['k1']
        self.assertEqual(v, k1_data)
        self.assertEqual(c._bucket0.size, 23 * 3)
        self.assertEqual(list_lrukeys('eden'), ['k2'])
        self.assertEqual(list_lrukeys('probation'), ['k0'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])


        v = c['k2']
        self.assertEqual(v, k2_data)
        self.assertEqual(c._bucket0.size, 23 * 3)
        self.assertEqual(list_lrukeys('eden'), ['k2'])
        self.assertEqual(list_lrukeys('probation'), ['k0'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

        c['k3'] = b'1'
        self.assertEqual(list_lrukeys('eden'), ['k3'])
        self.assertEqual(list_lrukeys('probation'), ['k2'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

        c['k4'] = b'1'
        self.assertEqual(list_lrukeys('eden'), ['k4'])
        self.assertEqual(list_lrukeys('probation'), ['k2'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

        c['k5'] = b''
        self.assertEqual(list_lrukeys('eden'), ['k5'])
        self.assertEqual(list_lrukeys('probation'), ['k2'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

        c['k6'] = b''
        self.assertEqual(list_lrukeys('eden'), ['k5', 'k6'])
        self.assertEqual(list_lrukeys('probation'), ['k2'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])


        c.__getitem__('k6')
        c.__getitem__('k6')
        c.__getitem__('k6')
        c['k7'] = b''
        self.assertEqual(list_lrukeys('eden'), ['k6', 'k7'])
        self.assertEqual(list_lrukeys('probation'), ['k2'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

        c['k8'] = b''
        self.assertEqual(list_lrukeys('eden'), ['k7', 'k8'])
        self.assertEqual(list_lrukeys('probation'), ['k6'])
        self.assertEqual(list_lrukeys('protected'), ['k1'])

    def test_load_and_save(self):
        # pylint:disable=too-many-statements,too-many-locals
        import tempfile
        import shutil
        import os

        root_temp_dir = tempfile.mkdtemp(".rstest_cache")
        self.addCleanup(shutil.rmtree, root_temp_dir, True)
        # Intermediate directories will be auto-created
        temp_dir = os.path.join(root_temp_dir, 'child1', 'child2')

        c = self._makeOne(cache_local_dir=temp_dir)
        # Doing the restore created the database.
        files = os.listdir(temp_dir)
        __traceback_info__ = files
        # There may be up to 3 files here, including the -wal and -shm
        # files, depending on how the database is closed and how the database
        # was configured. It also depends os when we look: some of the wal cleanup
        # we push to a background thread.
        def get_cache_files():
            return [x for x in os.listdir(temp_dir) if x.endswith('sqlite3')]
        cache_files = get_cache_files()
        len_initial_cache_files = len(cache_files)
        self.assertEqual(len_initial_cache_files, 1)
        # Saving an empty bucket does nothing
        self.assertFalse(c.save(close_async=False))

        # Watch the tids here: The LocalClientStrKeysValues layer
        # puts a tid of 0 in the value portion, and then later
        # we normalize all those on read.
        key = (0, 0)
        val = b'abc'
        c[key] = val
        c.__getitem__(key) # Increment the count so it gets saved
        self.assertTrue(c.save(close_async=False))
        cache_files = get_cache_files()
        self.assertEqual(len(cache_files), len_initial_cache_files)
        self.assertTrue(cache_files[0].startswith('relstorage-cache-'), cache_files)

        # Loading it works
        c2 = self._makeOne(cache_local_dir=temp_dir)
        self.assertEqual(c2[key], val)
        cache_files = get_cache_files()
        self.assertEqual(len_initial_cache_files, len(cache_files))

        # Add a new key and saving updates the existing file.
        key2 = (1, 1)
        val2 = b'def'
        c2[key2] = val2
        c2.__getitem__(key2) # increment

        c2.save(close_async=False)
        new_cache_files = get_cache_files()
        # Same file still
        self.assertEqual(cache_files, new_cache_files)

        # And again
        cache_files = new_cache_files
        c2.save(close_async=False)
        new_cache_files = get_cache_files()
        self.assertEqual(cache_files, new_cache_files)

        # Notice, though, that we normalized the tid value
        # on reading.
        c3 = self._makeOne(cache_local_dir=temp_dir)
        self.assertEqual(c3[key], val)
        self.assertIsNone(c3[key2])
        self.assertEqual(c3[(1, 0)], val2)

        # If we corrupt the file, it is silently ignored and removed
        for f in new_cache_files:
            with open(os.path.join(temp_dir, f), 'wb') as f:
                f.write(b'Nope!')

        c3 = self._makeOne(cache_local_dir=temp_dir)
        self.assertEqual(c3[key], None)
        cache_files = get_cache_files()
        self.assertEqual(len_initial_cache_files, len(cache_files))


class CacheRingTests(unittest.TestCase):

    def _makeOne(self, limit):
        from relstorage.cache.cache_ring import CacheRing
        return CacheRing(limit)

    def test_mru_lru_ring(self):
        lru = self._makeOne(100)
        entrya = lru.add_MRU(b'a', b'1')
        self.assertEqual(lru.get_LRU(), entrya)

        entryb = lru.add_MRU(b'b', b'2')
        self.assertEqual(lru.get_LRU(), entrya)

        entryc = lru.add_MRU(b'c', b'3')
        self.assertEqual(lru.get_LRU(), entrya)

        lru.make_MRU(entryb)
        self.assertEqual(lru.get_LRU(), entrya)

        lru.make_MRU(entrya)
        self.assertEqual(lru.get_LRU(), entryc)

        self.assertEqual(len(lru), 3)

    def test_add_MRUs_empty(self):
        from relstorage.cache.cache_ring import EdenRing
        lru = EdenRing(100, len, len)
        self.assertEqual((), lru.add_MRUs([]))

    def test_bool(self):
        lru = self._makeOne(100)
        self.assertFalse(lru)
        entrya = lru.add_MRU('a', b'b')
        self.assertTrue(lru)
        lru.remove(entrya)
        self.assertFalse(lru)

class LocalClientOIDTests(unittest.TestCase):
    # Uses true oid/int keys and state/tid values.

    def getClass(self):
        return LocalClient

    def _makeOne(self, **kw):
        options = MockOptions.from_args(**kw)
        inst = self.getClass()(options)
        return inst

    def test_set_multi_and_get_multi(self):
        c = self._makeOne()

        c.set_multi({(0, 0): (b'abc', 0),
                     (0, 1): (b'def', 1),
                     (1, 0): (b'ghi', 0)})
        # Hits on primary key
        self.assertEqual(c(0, 0),
                         (b'abc', 0))
        self.assertEqual(c(1, 0),
                         (b'ghi', 0))
        # Hits on secondary key
        self.assertEqual(c(0, -1, 1),
                         (b'def', 1))
        self.assertEqual(c(1, -1, 0),
                         (b'ghi', 0))

        # And those actually copied the data to the primary key, which
        # is now a hit.
        self.assertEqual(c(0, -1),
                         (b'def', 1))
        self.assertEqual(c(1, -1),
                         (b'ghi', 0))

class MemcacheClientOIDTests(LocalClientOIDTests):

    def setUp(self):
        from relstorage.tests.fakecache import data
        data.clear()

    tearDown = setUp

    def getClass(self):
        from relstorage.cache.storage_cache import _MemcacheStateCache
        class _StateCache(_MemcacheStateCache):
            def __init__(self, _):
                from relstorage.tests.fakecache import Client
                super(_StateCache, self).__init__(Client(''), 'pfx')
        return _StateCache

class CacheTests(unittest.TestCase):

    def test_bad_generation_index_attribute_error(self):
        cache = Cache(20)
        # Check proper init
        getattr(cache.generations[1], 'limit')
        getattr(cache.generations[2], 'limit')
        getattr(cache.generations[3], 'limit')

        # Gen 0 should be missing
        with self.assertRaises(AttributeError) as ex:
            cache.generations[0].on_hit()

        msg = "Generation 0 has no attribute 'on_hit'"
        self.assertEqual(ex.exception.args[0], msg)

    def test_free_reuse(self):
        cache = Cache(20)
        lru = cache.protected
        self.assertEqual(lru.limit, 16)
        entrya = lru.add_MRU('a', b'')
        entryb = lru.add_MRU('b', b'')
        entryc = lru.add_MRU('c', b'1')
        entryd = lru.add_MRU('d', b'1')
        for e in entrya, entryb, entryc, entryd:
            cache.data[e.key] = e
        lru.update_MRU(entryb, b'1234567890')
        lru.update_MRU(entryb, b'1234567890') # coverage
        lru.update_MRU(entryc, b'1234567890')
        self.assertEqual(2, len(lru.node_free_list))

        lru.add_MRU('c', b'1')
        self.assertEqual(1, len(lru.node_free_list))

    def test_add_too_many_MRUs_goes_to_free_list(self):
        class _Cache(Cache):
            _preallocate_entries = False

        cache = _Cache(20)
        self.assertEqual(0, len(cache.eden.node_free_list))

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),
                                       ('5', 'defg'),
                                       ('6', 'defg'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        self.assertEqual(2, len(cache.eden.node_free_list))
        self.assertIsNone(cache.eden.node_free_list[0].key)
        self.assertIsNone(cache.eden.node_free_list[0].value)

    def test_add_too_many_MRUs_works_aronud_big_entry(self):
        cache = Cache(20)

        entries = cache.eden.add_MRUs([('1', 'a'),
                                       # This entry itself will fit nowhere
                                       ('2', '12345678901234567890'),
                                       ('3', 'bc'),
                                       ('4', 'cd'),
                                       ('5', 'deh'),
                                       ('6', 'efghijkl'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '3', '4', '5'], [e.key for e in entries])
        self.assertEqual(2, len(cache.eden.node_free_list))
        for e in cache.eden.node_free_list:
            self.assertIsNone(e.key)
            self.assertIsNone(e.value)

        entry = cache.eden.node_free_list[-1]
        cache.eden.add_MRU('1', b'1')
        self.assertEqual(1, len(cache.eden.node_free_list))

        self.assertEqual(cache.eden.PARENT_CONST, entry.cffi_ring_node.u.entry.r_parent)

    def test_add_MRUs_uses_existing_free_list(self):
        class _Cache(Cache):
            _preallocate_avg_size = 7
            _preallocate_entries = True

        cache = _Cache(20)
        self.assertEqual(2, len(cache.eden.node_free_list))

        begin_nodes = list(cache.eden.node_free_list)

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),
                                       ('5', 'defg'),
                                       ('6', 'defg'),])

        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        for i, e in enumerate(begin_nodes):
            self.assertIs(e, entries[i])
        self.assertEqual(2, len(cache.eden.node_free_list))
        last_entry = entries[-1]
        for free in cache.eden.node_free_list:
            self.assertIs(last_entry._cffi_owning_node, free._cffi_owning_node)

        # Now just one that exactly fits.
        cache = _Cache(20)
        self.assertEqual(2, len(cache.eden.node_free_list))

        begin_nodes = list(cache.eden.node_free_list)

        entries = cache.eden.add_MRUs([('1', 'abcd'),
                                       ('2', 'defg'),
                                       ('3', 'defg'),
                                       ('4', 'defg'),])
        self.assertEqual(4, len(entries))
        self.assertEqual(['1', '2', '3', '4'], [e.key for e in entries])
        for i, e in enumerate(begin_nodes):
            self.assertIs(e, entries[i])
        self.assertEqual(0, len(cache.eden.node_free_list))

    def test_add_MRUs_reject_sets_sentinel_values(self):
        # When we find an item that completely fills the cache,
        # all the rest of the items are marked as rejected.
        cache = Cache(20)
        self.assertEqual(2, cache.eden.limit)
        self.assertEqual(2, cache.probation.limit)
        self.assertEqual(16, cache.protected.limit)

        added_entries = cache.eden.add_MRUs([
            # over fill eden
            ('1', b'012345678901234'),
            # 1 goes to protected, filling it. eden is also over full with 2. probation is empty
            ('2', b'012'),
            # 3 fills eden, bumping 2 to probation. But probation is actually overfull now
            # so we'd like to spill something if we could (but we can't.)
            ('3', b'0'),
            # 4 should never be added because it won't fit anywhere.
            ('4', b'e'),
        ])

        def keys(x):
            return [e.key for e in x]

        self.assertEqual(keys(cache.eden), ['3'])
        self.assertEqual(keys(cache.protected), ['1'])
        self.assertEqual(keys(cache.probation), ['2'])
        self.assertEqual('1 2 3'.split(), [e.key for e in added_entries])
        self.assertEqual(3, len(added_entries))


class MockOptions(Options):
    cache_module_name = '' # disable
    cache_servers = ''
    cache_local_mb = 1
    cache_local_dir_count = 1 # shrink

    @classmethod
    def from_args(cls, **kwargs):
        inst = cls()
        for k, v in kwargs.items():
            setattr(inst, k, v)
        return inst

    def __setattr__(self, name, value):
        if name not in Options.valid_option_names():
            raise AttributeError("Invalid option", name)
        object.__setattr__(self, name, value)

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
    def load_current(self, _cursor, oid_int):
        return self.data.get(oid_int, (None, None))

class MockPoller(object):
    def __init__(self):
        self.changes = []  # [(oid, tid)]
    def list_changes(self, _cursor, after_tid, last_tid):
        return ((oid, tid) for (oid, tid) in self.changes
                if tid > after_tid and tid <= last_tid)

def test_suite():
    return unittest.defaultTestLoader.loadTestsFromName(__name__)

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
