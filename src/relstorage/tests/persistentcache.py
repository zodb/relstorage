##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""
Test mixin for exercising persistent caching features.
"""
import transaction

from persistent.mapping import PersistentMapping
from ZODB.DB import DB
from ZODB.serialize import referencesf
#from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64
from ZODB.utils import z64

from . import TestCase


ROOT_OID = 0
ROOT_KEY = 'myobj'

def find_cache(obj):
    # Pass a connection, a storage, or the cache.
    storage = getattr(obj, '_storage', obj)
    cache = getattr(storage, '_cache', storage)
    return cache

class PersistentCacheStorageTests(TestCase):
    # pylint:disable=abstract-method

    _storage = None

    def make_storage(self, *args, **kwargs):
        raise NotImplementedError

    def __make_storage_pcache(
            self,
            expected_checkpoints=None,
            expected_root_tid=None,
    ):
        """
        Make a storage that reads a persistent cache that
        already exists.
        """
        storage = self.make_storage(zap=False)
        cache = storage._cache
        # It did read checkpoints and TID
        self.assertIsNotNone(cache.checkpoints)
        if expected_checkpoints:
            self.assertEqual(cache.checkpoints, expected_checkpoints)

        if expected_root_tid is not None:
            self.assert_oid_known(ROOT_OID, storage)
            self.assertEqual(cache.delta_after0[0], expected_root_tid)
        else:
            self.assert_oid_not_known(ROOT_OID, storage)
        self.assertIsNotNone(cache.current_tid)
        return storage

    def __make_storage_no_pcache(self):
        storage = self._closing(
            self.make_storage(cache_local_dir=None, zap=False)
        )
        # It didn't read checkpoints or TID
        cache = find_cache(storage)
        self.assertIsNone(cache.checkpoints)
        self.assertIsNone(cache.current_tid)
        return storage

    def assert_oid_not_known(self, oid, storage):
        cache = find_cache(storage)
        self.assertNotIn(oid, cache.delta_after0)
        self.assertNotIn(oid, cache.delta_after1)

    def assert_oid_known(self, oid, storage):
        cache = find_cache(storage)
        self.assertIn(oid, cache.delta_after0)
        return cache.delta_after0[oid]

    def assert_tid_after(self, oid, tid, storage):
        cache = find_cache(storage)
        new_tid = self.assert_oid_known(oid, cache)
        self.assertGreater(new_tid, tid)
        return new_tid

    def assert_oid_current(self, oid, storage, expected_tid=None):
        cache = find_cache(storage)

        tid = self.assert_oid_known(oid, cache)
        self.assertEqual(cache.current_tid, tid)
        if expected_tid is not None:
            self.assertEqual(tid, expected_tid)
        return tid

    def assert_checkpoints(self, storage, expected_checkpoints=None):
        cache = find_cache(storage)
        self.assertIsNotNone(cache.checkpoints)
        if expected_checkpoints:
            self.assertEqual(cache.checkpoints, expected_checkpoints)
        return cache.checkpoints

    def assert_cached(self, oid, tid, storage):
        cache = find_cache(storage)
        cache_data = cache.local_client[(oid, tid)]
        __traceback_info__ = (oid, tid), [k for k in cache.local_client if k[0] == oid]
        self.assertIsNotNone(cache_data)
        return cache_data

    def assert_cached_exact(self, oid, tid, storage):
        data = self.assert_cached(oid, tid, storage)
        self.assertEqual(data[1], tid)
        return data

    def assert_oid_not_cached(self, oid, storage):
        cache = find_cache(storage)
        keys = [k for k in cache.local_client if k[0] == oid]
        __traceback_info__ = (oid, keys)
        self.assertEmpty(keys)


    def __do_sets(self, root, new_data, old_tids, old_data):
        for key, value in new_data.items():
            old_tid = old_tids.get(key)
            old_value = old_data.get(key)

            key_path = key.split('.')
            attr_name = key_path[-1]
            __traceback_info__ = key_path
            base = root
            name = None
            for name in key_path[:-1]:
                base = getattr(base, name)
            oid = bytes8_to_int64(base._p_oid)
            __traceback_info__ = key, oid, old_tid
            if old_tid is not None:
                # Opening the database loaded the root object, so it's
                # now cached with the expected key; it may not actually
                # be at that exact TID, though.
                self.assert_cached(oid, old_tid, root._p_jar)

            if old_value is not None:
                val = getattr(base, attr_name)
                self.assertEqual(val, old_value)

            setattr(base, attr_name, value)
            # Make sure we have something
            old_tids[key] = old_tid

    def __do_check_tids(self, root, old_tids):
        for key, old_tid in old_tids.items():
            key_path = key.split('.')
            base = root
            for name in key_path[:-1]:
                base = getattr(base, name)
            oid = bytes8_to_int64(base._p_oid)

            # We have a saved TID for the root object. If we had an old one,
            # it's now bigger.
            if old_tid is not None:
                self.assert_tid_after(oid, old_tid, root._p_jar)
            else:
                self.assert_oid_current(oid, root._p_jar)

    def __set_keys_in_root_to(self,
                              storage,
                              new_data,
                              old_tids,
                              old_data,
                              pack=False):
        """
        And return the transaction ID and current checkpoints.

        Uses an independent transaction.

        Closes *storage*.
        """
        db1 = self._closing(DB(storage))
        tx = transaction.TransactionManager()
        c1 = db1.open(tx)
        # We've polled and gained checkpoints
        self.assert_checkpoints(c1)

        root = c1.root()
        self.__do_sets(root, new_data, old_tids, old_data)
        tx.commit()

        checkpoints = self.assert_checkpoints(c1)
        self.__do_check_tids(root, old_tids)
        tid_int = bytes8_to_int64(c1._storage.lastTransaction())
        c1.close()
        if pack:
            storage.pack(tid_int, referencesf)
        db1.close()
        return tid_int, checkpoints

    def __set_key_in_root_to(self,
                             storage,
                             value,
                             key=ROOT_KEY,
                             old_tid=None,
                             old_value=None):
        return self.__set_keys_in_root_to(
            storage,
            {key: value},
            {key: old_tid},
            {key: old_value}
        )

    def checkNoConflictWhenChangeMissedByPersistentCacheAfterCP0(self):
        orig_tid, orig_checkpoints = self.__set_key_in_root_to(self._storage, 42)

        # A storage that will not update the persistent cache.
        new_tid, _ = self.__set_key_in_root_to(
            self.__make_storage_no_pcache(),
            420
        )
        self.assertGreater(new_tid, orig_tid)

        # Now a new storage that will read the persistent cache.
        # It has the correct checkpoints, and the root oid is found in
        # delta_after0, where we will poll for it.
        storage3 = self.__make_storage_pcache(
            orig_checkpoints,
            expected_root_tid=orig_tid)

        db3 = self._closing(DB(storage3))
        c3 = db3.open()

        # Polling in the connection, however, caught us up, because the object changed
        # after current_tid.
        self.assert_oid_current(ROOT_OID, c3, new_tid)

        r = c3.root()
        self.assertEqual(r.myobj, 420)
        r.myobj = 180
        transaction.commit()
        c3.close()
        db3.close()

    def _populate_root_and_mapping(self):
        tx1 = transaction.TransactionManager()
        storage1 = self._storage
        db1 = self._closing(DB(storage1))
        c1 = db1.open(tx1)
        root = c1.root()
        root.myobj1 = mapping = PersistentMapping()
        root.myobj = 1
        tx1.commit()
        c1._storage._cache.clear(load_persistent=False)

        c1._storage.poll_invalidations()
        root.myobj = 2
        tx1.commit()
        c1._storage._cache.clear(load_persistent=False)

        c1._storage.poll_invalidations()
        root.myobj = 3
        tx1.commit()
        root_tid = self.assert_oid_known(ROOT_OID, c1)
        c1._storage._cache.clear(load_persistent=False)

        # Now, mutate an object that's not the root
        # so that we get a new transaction after the root was
        # modified. This transaction will be included in
        # a persistent cache.
        c1._storage.poll_invalidations()
        root.myobj1.key = PersistentMapping()
        mapping_oid = mapping._p_oid
        mapping_oid_int = bytes8_to_int64(mapping_oid)
        tx1.commit()
        mapping_tid = self.assert_oid_known(mapping_oid_int, c1)

        self.assert_checkpoints(c1, (root_tid, root_tid))
        self.assert_oid_current(mapping_oid_int, c1)

        # the root is not in a delta
        self.assert_oid_not_known(ROOT_OID, c1)
        # Nor is it in the cache, because the Connection's
        # object cache still had the root and we were never
        # asked.
        self.assert_oid_not_cached(ROOT_OID, c1)
        # So lets get it in the cache with its current TID.
        c1._storage.load(z64)
        self.assert_cached_exact(ROOT_OID, root_tid, c1)

        c1.close()
        return root_tid, mapping_tid, db1

    def checkNoConflictWhenChangeMissedByPersistentCacheBeforeCP1(self):
        _root_tid, _mapping_tid, db = self._populate_root_and_mapping()

        # Make some changes to the root in a storage that will not
        # read or update the persistent cache.
        new_tid, _ = self.__set_key_in_root_to(
            self.__make_storage_no_pcache(),
            420,
            old_tid=None,
        )

        # Now move the persistent checkpoints forward, pushing the
        # last TID for the root object out of the delta ranges.
        db.storage._cache.local_client.store_checkpoints(new_tid, new_tid)
        # Persist
        db.close()

        # Now a new storage that will read the persistent cache
        # The root object, however, was not put into a delta map.
        storage3 = self.__make_storage_pcache(
            expected_checkpoints=(new_tid, new_tid),
            expected_root_tid=None,
        )
        # Nor is it in the cache at any key.
        self.assert_oid_not_cached(ROOT_OID, storage3)

        # We can successfully open and edit the root object.
        self.__set_key_in_root_to(storage3, 180, old_tid=new_tid, old_value=420)

    def checkNoConflictNoChangeInPersistentCacheBeforeCP1(self):
        root_tid, _mapping_tid, db = self._populate_root_and_mapping()

        # Make some changes to the sub-object, and not the root, in a storage that will not
        # read or update the persistent cache.
        new_tid, _ = self.__set_key_in_root_to(
            self.__make_storage_no_pcache(),
            420,
            key='myobj1.key',
        )

        # Now move the persistent checkpoints forward, pushing the
        # last TID for the root object out of the delta ranges.
        db.storage._cache.local_client.store_checkpoints(new_tid, new_tid)
        # persist
        db.close()

        # Now a new storage that will read the persistent cache
        storage = self.__make_storage_pcache(
            # With checkpoints
            expected_checkpoints=(new_tid, new_tid),
            # No root TID in delta after 0
            expected_root_tid=None
        )
        # But it is in the cache for its old key, because we verified it
        # to still be in sync.
        cache_data = self.assert_cached(ROOT_OID, new_tid, storage)
        self.assertEqual(cache_data[1], root_tid)

        self.__set_keys_in_root_to(
            storage,
            {
                'myobj': 180,
                'myobj1.key': 360,
            },
            old_data={
                'myobj': 3,
                'myobj1.key': 420
            },
            old_tids={}
        )

    def checkNoConflictWhenDeletedNotInInPersistentCacheBeforeCP1(self):
        root_tid, _mapping_tid, db = self._populate_root_and_mapping()

        # Now, remove a persistent object. We do this by setting its
        # key to a new persistent object.
        c1 = db.open()
        root = c1.root()
        new_nested_mapping = PersistentMapping()
        root.myobj1.key = new_nested_mapping

        mapping_oid = root.myobj1._p_oid
        mapping_oid_int = bytes8_to_int64(mapping_oid)
        c1.add(new_nested_mapping)
        nested_mapping_oid = new_nested_mapping._p_oid
        nested_mapping_oid_int = bytes8_to_int64(nested_mapping_oid)
        transaction.commit()
        self.assert_oid_current(nested_mapping_oid_int, c1)

        self.assert_checkpoints(c1, (root_tid, root_tid))

        # the root is not in a delta
        self.assert_oid_not_known(ROOT_OID, c1)

        # Though it is in the cache.
        self.assert_cached_exact(ROOT_OID, root_tid, c1)

        # Create a new transaction that deletes an object but
        # that won't update the persistent cache.
        new_tid, _ = self.__set_keys_in_root_to(
            self.__make_storage_no_pcache(),
            {'myobj1.key': None},
            {},
            {},
            pack=True
        )

        # Now move the persistent checkpoints forward, pushing the
        # last TID for the root object out of the delta ranges.
        c1._storage._cache.local_client.store_checkpoints(new_tid, new_tid)
        # Persist
        c1.close()
        db.close()
        del db, c1

        # Now a new storage that will read the persistent cache
        storage = self.__make_storage_pcache(
            expected_checkpoints=(new_tid, new_tid),
        )
        # The deleted object was not put in a delta map
        self.assert_oid_not_known(nested_mapping_oid_int, storage)
        # Nor is it in a cache at the old key
        self.assert_oid_not_cached(nested_mapping_oid_int, storage)

        # Likewise, the parent mapping isn't found anywhere, because it
        # changed
        self.assert_oid_not_known(mapping_oid_int, storage)
        self.assert_oid_not_cached(mapping_oid_int, storage)

        self.__set_keys_in_root_to(
            storage,
            {'myobj': 180, 'myobj1.key': 360},
            {'': root_tid},
            {'myobj': 3, 'myobj1.key': None}
        )
