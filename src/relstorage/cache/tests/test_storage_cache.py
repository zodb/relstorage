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


import threading

from ZODB.utils import p64

from relstorage.tests import TestCase

from . import MockOptionsWithFakeMemcache as MockOptionsWithFakeCache
from . import MockAdapter

class StorageCacheTests(TestCase):
    # pylint:disable=too-many-public-methods

    def setUp(self):
        from relstorage.tests.fakecache import data
        data.clear()
        self._instances = []

    def tearDown(self):
        from relstorage.tests.fakecache import data
        data.clear()
        for inst in self._instances:
            inst.options.cache_local_dir = None
            inst.close(close_async=False)

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
        from relstorage.cache.memcache_client import MemcacheStateCache
        c = self._makeOne()
        cache = c.cache
        self.assertIsInstance(cache.g, MemcacheStateCache)
        self.assertIsInstance(cache.g.client, Client)
        self.assertEqual(cache.g.client.servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')
        self.assertEqual(c.size, 0)
        self.assertEqual(c.limit, MockOptionsWithFakeCache.cache_local_mb * 1000000)

        # can be closed multiple times
        c.close()
        c.close()
        self.test_closed_state(c)

    def test_closed_state(self, c=None):
        if c is None:
            c = self._makeOne()
        c.close(close_async=False)

        self.assertEqual(len(c), 0)
        self.assertTrue(c)
        self.assertEqual(c.size, 0)
        self.assertEqual(c.limit, 0)
        # At no point did we spawn threads.
        self.assertEqual(threading.active_count(), 1)

    def test_stats(self):
        inst = self._makeOne()
        self.assertIsInstance(inst.stats(), dict)
        inst.close()
        self.assertIsInstance(inst.stats(), dict)

    def _setup_for_save(self):
        import tempfile
        import shutil

        c = self._makeOne()
        c.checkpoints = (0, 0)
        c.tpc_begin()
        # tid is 2016-09-29 11:35:58,120
        # (That used to matter when we stored that information as a
        # filesystem modification time.)
        tid = 268595726030645777
        oid = 2
        c.store_temp(oid, b'abc')
        c.after_tpc_finish(p64(tid))

        key = list(iter(c.local_client))[0]
        self.assertEqual((2, tid), key)

        c.options.cache_local_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, c.options.cache_local_dir, True)

        return c, oid, tid

    def assertNoPersistentCache(self, cache):
        import os
        from relstorage.cache.persistence import sqlite_files
        fname, _ = sqlite_files(cache.options, cache.prefix)
        if fname:
            self.assertFalse(os.path.exists(fname), fname)

    def assertPersistentCache(self, cache):
        import os
        from relstorage.cache.persistence import sqlite_files
        fname, _ = sqlite_files(cache.options, cache.prefix)
        if fname:
            self.assertTrue(os.path.exists(fname), fname)

    def test_save(self):
        c, oid, tid = self._setup_for_save()
        self.assertNoPersistentCache(c)
        c.save(overwrite=True, close_async=False)
        self.assertPersistentCache(c)

        # Creating one in the same place automatically loads it.
        c2 = self._makeOne(cache_local_dir=c.options.cache_local_dir)
        self.assertEqual(1, len(c2))

        # The data is there, but there were no checkpoints stored in the
        # local client --- that happens from polling --- so there's
        # no delta maps or checkpoints here
        self.assertIsNone(c2.checkpoints)
        self.assertIsEmpty(c2.delta_after0)
        self.assertIsEmpty(c2.delta_after1)

        # This time, write the checkpoints.
        c.local_client.store_checkpoints(0, 0)
        c.save(close_async=False)

        c2 = self._makeOne(cache_local_dir=c.options.cache_local_dir)
        self.assertEqual(1, len(c2))
        self.assertEqual(c2.checkpoints, (0, 0))
        self.assertEqual(dict(c2.delta_after0), {oid: tid})
        self.assertIsEmpty(c2.delta_after1)

        c.options.cache_local_dir = None
        c2.options.cache_local_dir = None

        self.test_closed_state(c2)
        self.test_closed_state(c)

    def test_save_no_hits_no_sets(self):
        c, _, _ = self._setup_for_save()
        c.local_client.reset_stats()
        c.save(close_async=False)
        self.assertNoPersistentCache(c)

    def test_zap_all(self):
        c, _, _ = self._setup_for_save()
        self.assertNoPersistentCache(c)

        c.save(overwrite=True, close_async=False)
        self.assertPersistentCache(c)

        c.zap_all()
        self.assertEmpty(c)
        self.assertNoPersistentCache(c)

        # We can do it again and again
        c.zap_all()
        self.assertEmpty(c)
        self.assertNoPersistentCache(c)

    def test_zap_all_no_local_dir(self):
        c, _, _ = self._setup_for_save()
        self.assertNoPersistentCache(c)
        c.options.cache_local_dir = None

        c.save(overwrite=True, close_async=False)
        self.assertNoPersistentCache(c)

        c.zap_all()
        self.assertEmpty(c)
        self.assertNoPersistentCache(c)

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
        # And it got copied to the local cache
        self.assertEqual(c.local_client[(2, 55)], (b'abc', 55))

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
        from relstorage.cache.interfaces import CacheConsistencyError
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 60
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = (b'abc', 56)
        with self.assertRaisesRegex(CacheConsistencyError, "Detected an inconsistency"):
            c.load(None, 2)
        self.assertIsNone(c.checkpoints)

    def test_load_using_delta_after0_future_error(self):
        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.current_tid = 55
        c.checkpoints = (50, 40)
        c.delta_after0[2] = 55
        adapter.mover.data[2] = ('abc', 56)
        from ZODB.POSException import ReadConflictError
        with self.assertRaisesRegex(ReadConflictError, "future"):
            c.load(None, 2)

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
        self.assertEqual(dict(c.temp_objects.stored_oids),
                         {1: (3, 6, 0), 2: (6, 9, 0)})
        f = c.temp_objects._queue
        f.seek(0)
        self.assertEqual(f.read(), b'abcdefghi')
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
        c._send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + b'abc',
            'myprefix:state:55:3': tid + b'def',
            })
        self.assertEqual(len(c), 2)

    def test_send_queue_large(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        self.assertEqual(c.cache.g.send_limit, 1024 * 1024)
        c.cache.g.send_limit = 100
        c.tpc_begin()
        c.store_temp(2, b'abc')
        c.store_temp(3, b'def' * 100)
        tid = p64(55)
        c._send_queue(tid)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + b'abc',
            'myprefix:state:55:3': tid + (b'def' * 100),
            })

    def test_send_queue_none(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        c.tpc_begin()
        tid = p64(55)
        c._send_queue(tid)
        self.assertEqual(data, {})

    def test_after_tpc_finish(self):
        c = self._makeOne()
        c.tpc_begin()
        c.after_tpc_finish(p64(55))
        # XXX: This test doesn't actually assert anything. It used to check
        # the commit-count key, but we don't use that anymore.

    def test_clear_temp(self):
        c = self._makeOne()
        c.tpc_begin()
        c.clear_temp()
        self.assertIsNone(c.temp_objects)

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

    def test_after_poll_new_checkpoints_bad_changes_duplicates(self):
        from relstorage.tests.fakecache import data
        from relstorage.cache.interfaces import CacheConsistencyError
        data['myprefix:checkpoints'] = b'50 40'

        # Note that OID 3 changed twice. `list_changes` is required
        # to only produce the most recent change for a given
        # OID, so this data is in violation of the contract. Make sure
        # that we catch that.
        changes = [(3, 42), (1, 35), (2, 45), (3, 41)]

        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        adapter.poller.changes = changes
        __traceback_info__ = adapter.poller.changes
        c.checkpoints = (40, 30)
        c.current_tid = 40

        with self.assertRaisesRegex(CacheConsistencyError, "to have total len"):
            c.after_poll(None, 40, 50, None)
        self.assertIsNone(c.checkpoints)
        self.assertIsNone(c.current_tid)

    def test_after_poll_new_checkpoints_bad_changes_out_of_order(self):
        from relstorage.tests.fakecache import data
        from relstorage.cache.interfaces import CacheConsistencyError
        data['myprefix:checkpoints'] = b'50 40'

        adapter = MockAdapter()
        c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
        c.checkpoints = (40, 30)
        c.current_tid = 40

        # Too high
        adapter.poller.list_changes = lambda *args: [(3, 51)]
        with self.assertRaisesRegex(CacheConsistencyError, "out of range"):
            c.after_poll(None, 40, 50, None)
        self.assertIsNone(c.checkpoints)
        self.assertIsNone(c.current_tid)

        # Too low
        c.checkpoints = (40, 30)
        c.current_tid = 40
        adapter.poller.list_changes = lambda *args: [(3, 40)]
        with self.assertRaisesRegex(CacheConsistencyError, "out of range"):
            c.after_poll(None, 40, 50, None)
        self.assertIsNone(c.checkpoints)
        self.assertIsNone(c.current_tid)

    def test_after_poll_new_checkpoints(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'50 40'

        # list_changes isn't required to provide changes in any particular
        # order.
        # list_changes isn't not required to provide a list of tuples;
        # it could provide a list of lists. That turns out to matter
        # to the BTree constructor.
        changes = [(3, 42), (1, 35), (2, 45)]

        for f in (tuple, list):
            adapter = MockAdapter()
            c = self.getClass()(adapter, MockOptionsWithFakeCache(), 'myprefix')
            adapter.poller.changes = [f(t) for t in changes]
            __traceback_info__ = adapter.poller.changes
            c.checkpoints = (40, 30)
            c.current_tid = 40

            shifted_checkpoints = c.after_poll(None, 40, 50, None)

            self.assertEqual(c.checkpoints, (50, 40))
            self.assertIsNone(shifted_checkpoints)
            self.assertEqual(data['myprefix:checkpoints'], b'50 40')
            self.assertEqual(dict(c.delta_after0), {})
            self.assertEqual(dict(c.delta_after1), {2: 45, 3: 42})
            self.assertEqual(c.local_client.get_checkpoints(), shifted_checkpoints)

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
        shifted_checkpoints = c.after_poll(None, 40, 314, [(1, 45), (2, 46)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(shifted_checkpoints, (314, 40))
        self.assertEqual(c.local_client.get_checkpoints(), shifted_checkpoints)
        self.assertEqual(dict(c.delta_after0), {1: 45, 2: 46})
        self.assertEqual(dict(c.delta_after1), {})

    def test_after_poll_shift_checkpoints_already_changed(self):
        # We can arrange for the view to be inconsistent by
        # interjecting some code to change things.
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'40 30'
        c = self._makeOne()
        c.delta_size_limit = 2
        c.checkpoints = (40, 30)
        c.current_tid = 40
        old_suggest = c._suggest_shifted_checkpoints
        def suggest():
            data['myprefix:checkpoints'] = b'1 1'
            return old_suggest()
        c._suggest_shifted_checkpoints = suggest

        shifted_checkpoints = c.after_poll(None, 40, 314, [(1, 45), (2, 46)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertIsNone(shifted_checkpoints)
        self.assertEqual(c.local_client.get_checkpoints(), shifted_checkpoints)
        self.assertEqual(dict(c.delta_after0), {1: 45, 2: 46})
        self.assertEqual(dict(c.delta_after1), {})

    def test_after_poll_shift_checkpoints_huge(self):
        from relstorage.tests.fakecache import data
        data['myprefix:checkpoints'] = b'40 30'
        c = self._makeOne()
        c.delta_size_limit = 0
        c.checkpoints = (40, 30)
        c.current_tid = 40
        shifted_checkpoints = c.after_poll(None, 40, 314, [(1, 45), (2, 46)])
        self.assertEqual(c.checkpoints, (40, 30))
        self.assertEqual(shifted_checkpoints, (314, 314))
        self.assertEqual(c.local_client.get_checkpoints(), shifted_checkpoints)
        self.assertEqual(dict(c.delta_after0), {1: 45, 2: 46})
        self.assertEqual(dict(c.delta_after1), {})


class PersistentRowFilterTests(TestCase):

    def _makeOne(self):
        from relstorage.cache.storage_cache import _PersistentRowFilter
        adapter = MockAdapter()
        return _PersistentRowFilter(adapter, dict)

    def test_no_checkpoints(self):
        f = self._makeOne()

        rows = [(1, 2, 3, 2)]
        results = list(f(None, rows))
        self.assertEqual(results, [((1, 2), (3, 2))])
        self.assertEmpty(f.delta_after0)
        self.assertEmpty(f.delta_after1)

    def test_deltas(self):
        f = self._makeOne()

        cp0 = 5000
        cp1 = 4000

        tid_after0 = 5001
        tid_after1 = 4001
        # The old_tid, outside the checkpoint range,
        # will get completely dropped.
        old_tid = 3999

        rows = [
            (0, tid_after0, b'0', tid_after0),
            (1, cp0, b'1', cp0),
            (2, tid_after1, b'2', tid_after1),
            (3, cp1, b'3', cp1),
            (4, old_tid, b'4', old_tid)
        ]

        results = list(f((cp0, cp1), rows))

        self.assertEqual(results, [
            (rows[0][:2], rows[0][2:]),
            (rows[1][:2], rows[1][2:]),
            (rows[2][:2], rows[2][2:]),
        ])

        self.assertEqual(dict(f.delta_after0), {0: 5001})
        # We attempted validation on this, and we found nothing,
        # so we can't claim knowledge.
        self.assertEqual(dict(f.delta_after1), {})
        # 1 and 2 were polled because they would go in delta_after_1,
        # 3 and 4 were polled because they fall outside the checkpoint ranges
        self.assertEqual(set(f.polled_invalid_oids), {1, 2, 3, 4})

        # Let's verify we can find things we poll for.
        f = self._makeOne()
        f.adapter.mover.data[2] = (b'', tid_after1)
        f.adapter.mover.data[4] = (b'', old_tid)
        results = list(f((cp0, cp1), rows))

        self.assertEqual(results, [
            (rows[0][:2], rows[0][2:]),
            (rows[1][:2], rows[1][2:]),
            (rows[2][:2], rows[2][2:]),
            ((4, 5000), rows[4][2:]),
        ])

        self.assertEqual(dict(f.delta_after0), {0: tid_after0})
        self.assertEqual(dict(f.delta_after1), {2: tid_after1})
        self.assertEqual(set(f.polled_invalid_oids), {1, 3})

        # Test when the tid doesn't match
        f = self._makeOne()
        f.adapter.mover.data[2] = (b'', tid_after1 + 2)
        f.adapter.mover.data[4] = (b'', old_tid + 1)
        results = list(f((cp0, cp1), rows))

        self.assertEqual(results, [
            (rows[0][:2], rows[0][2:]),
            (rows[1][:2], rows[1][2:]),
            (rows[2][:2], rows[2][2:]),
        ])

        self.assertEqual(dict(f.delta_after0), {0: tid_after0})
        self.assertEqual(dict(f.delta_after1), {2: tid_after1 + 2})
        self.assertEqual(set(f.polled_invalid_oids), {1, 2, 3, 4})
