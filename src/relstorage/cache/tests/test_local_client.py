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
from functools import partial

from relstorage.tests import TestCase

from relstorage.cache.mapping import SizedLRUMapping
from relstorage.cache.lru_sqlite import SqlMapping

from . import LocalClient
from . import MockOptions
from . import list_lrukeys as list_lrukeys_
from . import list_lrufreq as list_lrufreq_
from .test_memcache_client import AbstractStateCacheTests


class MockOptionsWithSQL(MockOptions):
    cache_local_storage = SqlMapping

class MockOptionsWithMapping(MockOptions):
    cache_local_storage = SizedLRUMapping


class LocalClientStrKeysValuesGenerationalTests(TestCase):
    """
    Depend on having an IGenerationalLRUCache as the backing storage.
    """

    class LocalClientStrKeysValues(LocalClient):
        # Make the cache accept and return str keys and values,
        # for ease of dealing with size limits,
        # bust mostly for compatibility with old tests, before LocalClient
        # implemented IStateCache

        key_weight = len

        def value_weight(self, value):
            return len(value[0] if value[0] else b'')

        def __setitem__(self, key, val):
            LocalClient.__setitem__(self, key, (val, 0))

        def __getitem__(self, key):
            v = self(None, None, None, (key, None))
            if v is not None:
                v = v[0]
            return v

    def getClass(self):
        return self.LocalClientStrKeysValues

    Options = MockOptions

    def _makeOne(self, **kw):
        options = self.Options.from_args(**kw)
        inst = self.getClass()(options, 'pfx')
        inst.restore()
        return inst

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


class LocalClientOIDTests(AbstractStateCacheTests):
    # Uses true oid/int keys and state/tid values.

    Options = MockOptionsWithMapping

    tid = 34567
    key_tid = 33333
    oid = 12345

    key = (oid, key_tid)
    value = (b'statebytes', tid)

    missing_key = (-1, -1)

    def getClass(self):
        return LocalClient

    def test_bucket_kind(self):
        c = self._makeOne()
        bucket = c._bucket0
        self.assertIsInstance(bucket, self.Options.cache_local_storage)

    def test_items(self):
        c = self._makeOne()
        self.assertEqual(list(c.items()), [])

        c[self.key] = self.value
        self.assertEqual(list(c.items()), [(self.key, self.value)])

    def test_values(self):
        c = self._makeOne()
        self.assertEqual(list(c.values()), [])

        c[self.key] = self.value
        self.assertEqual(list(c.values()), [self.value])

    def test_setitem_min_allowed_missing(self):
        c = self._makeOne()

        c[self.key] = self.value
        self.assertIsEmpty(c._min_allowed_writeback)

    def test_setitem_min_allowed_greater(self):
        c = self._makeOne()
        min_allowed = self.tid * 2
        c._min_allowed_writeback[self.oid] = min_allowed
        c[self.key] = self.value
        self.assertEqual(c._min_allowed_writeback[self.oid], min_allowed)

    def test_setitem_min_allowed_less(self):
        c = self._makeOne()
        min_allowed = 1
        c._min_allowed_writeback[self.oid] = min_allowed
        c[self.key] = self.value
        self.assertEqual(c._min_allowed_writeback[self.oid], self.tid)

    def test_cache_corruption_on_save(self):
        c = self._makeOne(cache_local_dir=':memory:')
        c[self.key] = self.value
        # Different key, same real tid, different state. Uh-oh!
        c[(self.oid, 1)] = (b'bad bytes', self.tid)

        self.assertEqual(c.save(close_async=False), 0)
        # And the cache was flushed.
        self.assertIsNone(c[self.key])

    def test_delete_stale_objects_on_save(self):
        from relstorage.cache.local_database import Database
        from relstorage.cache.persistence import sqlite_connect
        c = self._makeOne(cache_local_dir=":memory:")

        conn = sqlite_connect(c.options, 'ignored-pfx', close_async=False)
        self.addCleanup(conn.close)
        db = Database.from_connection(conn)
        db.store_temp([(0, 0, b'state', 0)])
        db.move_from_temp()
        self.assertEqual(dict(db.oid_to_tid), {0: 0})
        conn.commit()
        # Pretend we loaded this from the db
        c[(0, 0)] = (b'state', 0)
        c._min_allowed_writeback[0] = 0

        # Pass a newer version through
        c[(0, 1)] = (b'state', 1)
        self.assertEqual(c._min_allowed_writeback[0], 1)
        # Evict it so we don't have it to write.
        del c._bucket0[(0, 1)]

        # But it gets removed based on having seen it and knowing
        # it's there.
        c.write_to_sqlite(conn)
        self.assertEmpty(db.oid_to_tid)

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
        c[self.key] = self.value
        self.assertEqual(c[self.key], self.value)
        self.assertEqual(c[self.missing_key], None)

    def test_set_and_get_string_uncompressed(self):
        c = self._makeOne(cache_local_compression='none')
        c[self.key] = self.value
        self.assertEqual(c[self.key], self.value)
        self.assertEqual(c[self.missing_key], None)

    def test_set_and_get_object_too_large(self):
        c = self._makeOne(cache_local_compression='none')
        c[self.key] = (b'abcdefgh' * 10000, 1)
        self.assertEqual(c[self.key], None)

    def test_set_with_zero_space(self):
        c = self._makeOne(cache_local_mb=0)
        self.assertEqual(c.limit, 0)
        self.assertEqual(c._value_limit, 16384)
        c[self.key] = self.value
        c[self.missing_key] = (b'', 1)
        self.assertEqual(c[self.key], None)
        self.assertEqual(c[self.missing_key], None)

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
        c.restore()
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

        key = (0, 0)
        val = (b'abc', 0)
        c[key] = val
        c.__getitem__(key) # Increment the count so it gets saved
        self.assertEqual(c[key], val)

        self.assertTrue(c.save(close_async=False))

        cache_files = get_cache_files()
        self.assertEqual(len(cache_files), len_initial_cache_files)
        self.assertTrue(cache_files[0].startswith('relstorage-cache-'), cache_files)

        # Loading it works
        c2 = self._makeOne(cache_local_dir=temp_dir)
        c2.restore()
        self.assertEqual(c2[key], val)
        cache_files = get_cache_files()
        self.assertEqual(len_initial_cache_files, len(cache_files))

        # Add a new key and saving updates the existing file.
        key2 = (1, 1)
        val2 = (b'def', 0)
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
        c3.restore()
        self.assertEqual(c3[key], val)
        self.assertIsNone(c3[key2])
        self.assertEqual(c3[(1, 0)], val2)

        # If we corrupt the file, it is silently ignored and removed
        for f in new_cache_files:
            with open(os.path.join(temp_dir, f), 'wb') as f:
                f.write(b'Nope!')

        c3 = self._makeOne(cache_local_dir=temp_dir)
        c3.restore()
        self.assertEqual(c3[key], None)
        cache_files = get_cache_files()
        self.assertEqual(len_initial_cache_files, len(cache_files))

        c3.remove_invalid_persistent_oids([0])

        # At no point did we spawn extra threads
        self.assertEqual(1, threading.active_count())


class LocalClientSqlOIDTests(LocalClientOIDTests):

    Options = MockOptionsWithSQL
