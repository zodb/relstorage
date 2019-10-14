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

from hamcrest import assert_that
from nti.testing.matchers import validly_provides

from relstorage.tests import TestCase
from relstorage.storage.tpc.temporary_storage import TemporaryStorage

from ..interfaces import IStorageCache
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
            inst.close()

    def getClass(self):
        from relstorage.cache.storage_cache import StorageCache
        return StorageCache

    def _makeOne(self, current_oids=None, **kwargs):
        options = MockOptionsWithFakeCache.from_args(**kwargs)
        adapter = MockAdapter()
        if current_oids:
            adapter.mover.data.update({oid: (b'', tid) for oid, tid in current_oids.items()})
        inst = self.getClass()(adapter, options,
                               'myprefix')
        self._instances.append(inst)
        inst = inst.new_instance() # coverage and sharing testing

        return inst

    def test_ctor(self):
        from relstorage.tests.fakecache import Client
        from relstorage.cache.memcache_client import MemcacheStateCache
        c = self._makeOne()
        assert_that(c, validly_provides(IStorageCache))

        cache = c.cache
        self.assertIsInstance(cache.g, MemcacheStateCache)
        self.assertIsInstance(cache.g.client, Client)
        self.assertEqual(cache.g.client.servers, ['host:9999'])
        self.assertEqual(c.prefix, 'myprefix')
        self.assertEqual(len(c), 0) # size may be greater than 0
        self.assertEqual(c.limit, MockOptionsWithFakeCache.cache_local_mb * 1000000)

        # can be closed multiple times
        c.close()
        c.close()
        self.test_closed_state(c)

    def test_closed_state(self, c=None):
        if c is None:
            c = self._makeOne()
        c.close()

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
        temp_storage = TemporaryStorage()
        # tid is 2016-09-29 11:35:58,120
        # (That used to matter when we stored that information as a
        # filesystem modification time.)
        tid = 268595726030645777
        oid = 2
        temp_storage.store_temp(oid, b'abc')
        # Flush to the cache.
        c.after_tpc_finish(p64(tid), temp_storage)

        key = list(iter(c.local_client))[0]
        self.assertEqual((2, tid), key)

        c.options.cache_local_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, c.options.cache_local_dir, True)

        # Make like we polled.
        from relstorage.cache import mvcc
        ix = mvcc._ObjectIndex(tid - 1)
        ix = ix.with_polled_changes(tid, tid - 1, [(oid, tid)])
        c.polling_state.object_index = ix
        c.object_index = ix

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

    def test_save_and_clear(self):
        c, oid, tid = self._setup_for_save()
        self.assertNoPersistentCache(c)
        c.save(overwrite=True)
        self.assertPersistentCache(c)

        # Creating one in the same place automatically loads it.
        c2 = self._makeOne(current_oids={oid: tid},
                           cache_local_dir=c.options.cache_local_dir)
        self.assertEqual(1, len(c2))

        c.save()

        # Creating a new one loads the stored data.
        c2 = self._makeOne(current_oids={oid: tid},
                           cache_local_dir=c.options.cache_local_dir)
        self.assertEqual(1, len(c2))
        # Invalidating one oid invalidates the polling_state too
        c2.remove_cached_data(oid, tid)
        self.assertEqual(0, len(c2))
        # We can do it again.
        c2.remove_cached_data(oid, tid)
        self.assertEqual(0, len(c2))

        # Resetting also loads the stored data by default.
        c2.clear()
        self.assertEqual(1, len(c2))
        # Invalidating a group of oids invalidates the polling_state too
        c2.remove_all_cached_data_for_oids((oid,))
        self.assertEqual(0, len(c2))
        # We can do it again.
        c2.remove_all_cached_data_for_oids((oid,))
        self.assertEqual(0, len(c2))

        # But can be told to ignore it
        c2.clear(load_persistent=False)
        self.assertEqual(0, len(c2))

        c.options.cache_local_dir = None
        c2.options.cache_local_dir = None

        self.test_closed_state(c2)
        self.test_closed_state(c)

    def test_save_no_hits_no_sets(self):
        c, _, _ = self._setup_for_save()
        c.local_client.reset_stats()
        c.save()
        self.assertNoPersistentCache(c)

    def test_zap_all(self):
        c, _, _ = self._setup_for_save()
        self.assertNoPersistentCache(c)

        c.save(overwrite=True)
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

        c.save(overwrite=True)
        self.assertNoPersistentCache(c)

        c.zap_all()
        self.assertEmpty(c)
        self.assertNoPersistentCache(c)

    def test_clear_no_persistent_data(self):
        from relstorage.tests.fakecache import data
        data.clear()
        c = self._makeOne()
        data['x'] = '1'
        c.clear()
        self.assertFalse(data)
        # self.assertEqual(c.checkpoints, None)
        # self.assertEqual(dict(c.delta_after0), {})
        # self.assertEqual(dict(c.delta_after1), {})

    def test_load_without_checkpoints(self):
        c = self._makeOne()
        res = c.load(None, 2)
        self.assertEqual(res, (None, None))

    def test_store_temp(self):
        c = self._makeOne()
        temp_storage = TemporaryStorage()
        temp_storage.store_temp(2, b'abc')
        temp_storage.store_temp(1, b'def')
        temp_storage.store_temp(2, b'ghi')
        self.assertEqual(b'ghi', temp_storage.read_temp(2))
        self.assertEqual(dict(temp_storage.stored_oids),
                         {1: (3, 6, 0), 2: (6, 9, 0)})
        self.assertEqual(temp_storage.max_stored_oid, 2)
        f = temp_storage._queue
        f.seek(0)
        self.assertEqual(f.read(), b'abcdefghi')
        c.after_tpc_finish(p64(3), temp_storage)

        # self.assertEqual(dict(c.delta_after0), {2: 3, 1: 3})

    def test_send_queue_small(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        temp_storage = TemporaryStorage()
        temp_storage.store_temp(2, b'abc')
        temp_storage.store_temp(3, b'def')
        tid = p64(55)
        c.after_tpc_finish(tid, temp_storage)
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
        temp_storage = TemporaryStorage()
        temp_storage.store_temp(2, b'abc')
        temp_storage.store_temp(3, b'def' * 100)
        tid = p64(55)
        c.after_tpc_finish(tid, temp_storage)
        self.assertEqual(data, {
            'myprefix:state:55:2': tid + b'abc',
            'myprefix:state:55:3': tid + (b'def' * 100),
            })

    def test_send_queue_none(self):
        from relstorage.tests.fakecache import data
        c = self._makeOne()
        temp_storage = TemporaryStorage()
        tid = p64(55)
        c.after_tpc_finish(tid, temp_storage)
        self.assertEqual(data, {})

    def __not_called(self):
        self.fail("Should not be called")

    def test_instances_share_polling_state(self):
        child = self._makeOne()
        self.assertEqual(1, len(self._instances))
        master = self._instances[0]
        self.assertIs(master.polling_state, child.polling_state)

        # This shouldn't actually happen...
        grandchild = child.new_instance()
        self.assertIs(master.polling_state, grandchild.polling_state)
        self.assertTrue(grandchild.polling_state)

        # releasing drops the master
        grandchild.release()
        self.assertFalse(grandchild.polling_state)
        # Doesn't affect anything else.
        self.assertIs(master.polling_state, child.polling_state)
