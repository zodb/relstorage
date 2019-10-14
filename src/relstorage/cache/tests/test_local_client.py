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

from . import LocalClient
from . import MockOptions
from . import list_lrukeys as list_lrukeys_
from . import list_lrufreq as list_lrufreq_
from .test_memcache_client import AbstractStateCacheTests


class LocalClientStrKeysValuesGenerationalTests(TestCase):
    """
    Depend on having an IGenerationalLRUCache as the backing storage.
    """

    class LocalClientStrKeysValues(LocalClient):
        # Make the cache accept and return str keys and values,
        # for ease of dealing with size limits,
        # bust mostly for compatibility with old tests, before LocalClient
        # implemented IStateCache

        def __setitem__(self, key, val):
            oid = int(key[0])
            tid = int(key[1])
            LocalClient.__setitem__(self, (oid, tid), (val, tid))

        def __getitem__(self, key):
            v = LocalClient.__getitem__(self, (int(key[0]), int(key[1])))
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

        # The initial weight is the size of the cache object and the generations;
        # on a 64-bit platform, I get 224
        initial_weight = c._cache.weight
        c['55'] = b''
        # The weight of an empty entry is the overhead. On a64-bit platform
        # I get 96
        entry_weight = c._cache[5].weight

        # This limit will result in
        # eden and probation of 5, protected of 40. This means that eden
        # and probation each can hold one item, while protected can hold 4,
        # so our max size will be 60
        c.limit = (entry_weight + 10) * 5 + 1
        setattr(c, '_cache', True) # Avoid type inference warnings
        c.flush_all()

        list_lrukeys = partial(list_lrukeys_, c._cache)
        list_lrufreq = partial(list_lrufreq_, c._cache)
        def all_lrukeys():
            return {k: list_lrukeys(k) for k in ('eden', 'probation', 'protected')}
        def expected_weight(entry_count, python_weight):
            return python_weight + (entry_count * entry_weight) + initial_weight

        k = None

        for i in range(5):
            # add 10 bytes
            k = '%d%d' % (i, i)
            # This will go to eden, replacing any value that was there
            # into probation.
            c[k] = b'0123456789'


        # While we have the room, we initially put items into the protected
        # space when they graduate from eden.
        self.assertEqual(
            all_lrukeys(),
            {'eden': [4], 'probation': [], 'protected': [3, 2, 1, 0]}
        )
        self.assertEqual(c._cache.weight, expected_weight(5, 50))

        c['55'] = b'0123456789'

        # Right now, we're one entry over size, because we put k5
        # in eden, which dropped k4 to probation; since probation was empty, we
        # allowed it to stay there
        self.assertEqual(list_lrukeys('eden'), [5])
        self.assertEqual(list_lrukeys('probation'), [4])
        self.assertEqual(list_lrukeys('protected'), [3, 2, 1, 0])
        self.assertEqual(c._cache.weight, expected_weight(6, 60))

        v = c['22']
        self.assertEqual(v, b'0123456789')
        self.assertEqual(c._cache.weight, expected_weight(6, 60))

        del c[(1, 1)]
        c['11'] = b'b'
        self.assertEqual(list_lrukeys('eden'), [1])
        self.assertEqual(list_lrukeys('probation'), [5])
        self.assertEqual(list_lrukeys('protected'), [2, 3, 0])

        self.assertEqual(c._cache.weight, expected_weight(5, 41))

        for i in range(4):
            # add 10 bytes
            c[(-i, i)] = b'0123456789'
            # Notice that we're not promoting these through the layers. So
            # when we're done, we'll wind up with one key each in
            # eden and probation, and all the K keys in protected (since
            # they have been promoted)


        # x0 and x1 started in eden and got promoted to the probation ring,
        # from whence they were ejected because of never being accessed.
        # k2 was allowed to remain because it'd been accessed
        # more often
        self.assertEqual(list_lrukeys('eden'), [-3])
        self.assertEqual(list_lrukeys('probation'), [-2])
        self.assertEqual(list_lrukeys('protected'), [0, 2, 3])
        self.assertEqual(c._cache.weight, expected_weight(5, 50))

        #pprint.pprint(c._cache.stats())
        self.assertEqual(c[(-1, 0)], None)
        self.assertEqual(c[(-1, 1)], None)
        self.assertEqual(c[(-2, 2)], b'0123456789')
        self.assertEqual(c[(-3, 3)], b'0123456789')
        self.assertEqual(c[(2, 2)], b'0123456789')

        # Note that this last set of checks perturbed protected and probation;
        # We lost a key
        #pprint.pprint(c._cache.stats())
        self.assertEqual(list_lrukeys('eden'), [-3])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), [2, -2, 0, 3])


        self.assertEqual(c['00'], b'0123456789')
        self.assertEqual(c['00'], b'0123456789') # One more to increase its freq count
        self.assertEqual(c['11'], None)
        self.assertEqual(c['22'], b'0123456789')
        self.assertEqual(c['33'], b'0123456789')
        self.assertEqual(c['44'], None)
        self.assertEqual(c['55'], None)

        # Let's promote from probation, causing places to switch.
        # First, verify our current state after those gets.
        self.assertEqual(list_lrukeys('eden'), [-3])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), [3, 2, 0, -2])
        # Now get and switch
        c.__getitem__((-2, 2))
        self.assertEqual(list_lrukeys('eden'), [-3])
        self.assertEqual(list_lrukeys('probation'), [])
        self.assertEqual(list_lrukeys('protected'), [-2, 3, 2, 0])


        # Confirm frequency counts
        self.assertEqual(list_lrufreq('eden'), [2])
        self.assertEqual(list_lrufreq('probation'), [])
        self.assertEqual(list_lrufreq('protected'), [3, 2, 4, 4])
        # A brand new key is in eden, shifting eden to probation

        c[(100, 0)] = b'0123456789'

        # Now, because we had accessed k0 (probation) more than we'd
        # accessed the last key from eden (x3), that's the one we keep
        self.assertEqual(list_lrukeys('eden'), [100])
        self.assertEqual(list_lrukeys('probation'), [-3])
        self.assertEqual(list_lrukeys('protected'), [-2, 3, 2, 0])

        self.assertEqual(list_lrufreq('eden'), [1])
        self.assertEqual(list_lrufreq('probation'), [2])
        self.assertEqual(list_lrufreq('protected'), [3, 2, 4, 4])

        self.assertEqual(c._cache.weight, expected_weight(6, 60))

        self.assertEqual(c[(-3, 3)], b'0123456789')
        self.assertEqual(list_lrukeys('probation'), [])


class LocalClientOIDTests(AbstractStateCacheTests):
    # Uses true oid/int keys and state/tid values.

    Options = MockOptions

    tid = 34567
    key_tid = tid
    oid = 12345

    key = (oid, key_tid)
    value = (b'statebytes', tid)

    missing_key = (0, 1)

    def getClass(self):
        return LocalClient

    def test_cache_corruption_on_store(self):
        from relstorage.cache.interfaces import CacheConsistencyError
        c = self._makeOne(cache_local_dir=':memory:')
        c[self.key] = self.value
        # Different key, same real tid, different state. Uh-oh!
        with self.assertRaises(CacheConsistencyError):
            c[(self.oid, self.tid)] = (b'bad bytes', self.tid)

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
        c[self.key] = (b'abcdefgh' * 10000, self.key_tid)
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
        self.assertFalse(c.save())

        key = (0, 1)
        val = (b'abc', 1)
        c[key] = val
        c.__getitem__(key) # Increment the count so it gets saved
        self.assertEqual(c[key], val)

        self.assertTrue(c.save())

        cache_files = get_cache_files()
        self.assertEqual(len(cache_files), len_initial_cache_files)
        self.assertTrue(cache_files[0].startswith('relstorage-cache2-'), cache_files)

        # Loading it works
        c2 = self._makeOne(cache_local_dir=temp_dir)
        c2.restore()
        self.assertEqual(c2[key], val)
        cache_files = get_cache_files()
        self.assertEqual(len_initial_cache_files, len(cache_files))

        # Add a new key and saving updates the existing file.
        key2 = (1, 1)
        val2 = (b'def', 1)
        # Non matching keys aren't allowed
        with self.assertRaises(AssertionError):
            c2[(1, 0)] = val2
        c2[key2] = val2
        c2.__getitem__(key2) # increment

        c2.save()
        new_cache_files = get_cache_files()
        # Same file still
        self.assertEqual(cache_files, new_cache_files)

        # And again
        cache_files = new_cache_files
        c2.save()
        new_cache_files = get_cache_files()
        self.assertEqual(cache_files, new_cache_files)

        # Notice, though, that we normalized the tid value
        # on reading.
        c3 = self._makeOne(cache_local_dir=temp_dir)
        c3.restore()
        self.assertEqual(c3[key], val)
        self.assertEqual(c3[key2], val2)

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
