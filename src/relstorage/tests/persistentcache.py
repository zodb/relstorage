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

def as_int(oid):
    if isinstance(oid, bytes):
        oid = bytes8_to_int64(oid)
    return oid

class PersistentCacheStorageTests(TestCase):
    # pylint:disable=abstract-method

    _storage = None

    def make_storage(self, *args, **kwargs):
        raise NotImplementedError

    def __make_storage_pcache(
            self,
            expected_checkpoints=None,
            expected_root_tid=None,
    ): # pylint:disable=unused-argument
        """
        Make a storage that reads a persistent cache that
        already exists.
        """
        storage = self.make_storage(zap=False)
        cache = storage._cache
        # It did read checkpoints and TID
        # self.assertIsNotNone(cache.checkpoints)
        # if expected_checkpoints:
        #     self.assertEqual(cache.checkpoints, expected_checkpoints)

        if expected_root_tid is not None:
            # Nothing is actually in the index
            self.assert_oid_not_known(ROOT_OID, storage)
            # because everything is frozen.
            _state, tid = cache.local_client[(ROOT_OID, None)]
            self.assertEqual(tid, expected_root_tid)
        else:
            self.assert_oid_not_known(ROOT_OID, storage)
        self.assertIsNotNone(cache.polling_state.object_index.maximum_highest_visible_tid)
        return storage

    def __make_storage_no_pcache(self):
        storage = self._closing(
            self.make_storage(cache_local_dir=None, zap=False)
        )
        # It didn't read checkpoints or TID
        cache = find_cache(storage)
        # self.assertIsNone(cache.checkpoints)
        self.assertIsNone(cache.current_tid)
        return storage

    def assert_oid_not_known(self, oid, storage):
        cache = find_cache(storage)
        self.assertNotIn(oid, cache.object_index or ())

    def assert_exact_tid_in_oid_index(self, oid, storage, expected_tid=None):
        oid = as_int(oid)
        expected_tid = as_int(expected_tid)
        cache = find_cache(storage)
        index = cache.object_index or cache.polling_state.object_index or ()
        __traceback_info__ = index.keys()
        self.assertIn(oid, index)
        tid = index[oid]
        if expected_tid:
            self.assertEqual(tid, expected_tid)
        return tid

    assert_oid_known = assert_exact_tid_in_oid_index

    def assert_tid_after(self, oid, tid, storage):
        cache = find_cache(storage)
        new_tid = self.assert_oid_known(oid, cache)
        self.assertGreater(new_tid, tid)
        return new_tid

    def assert_oid_current(self, oid, storage, expected_tid=None):
        """
        Check that *oid* is in the object index; if *expected_tid* is given,
        make sure it matches that value.
        """
        cache = find_cache(storage)

        tid = self.assert_oid_known(oid, cache)
        self.assertEqual(cache.current_tid, tid,
                         "cache tid is not OID tid (expected_tid: %s)" % (expected_tid,))
        if expected_tid is not None:
            self.assertEqual(tid, expected_tid)
        return tid

    # def assert_checkpoints(self, storage, expected_checkpoints=None):
    #     cache = find_cache(storage)
    #     self.assertIsNotNone(cache.checkpoints)
    #     if expected_checkpoints:
    #         self.assertEqual(cache.checkpoints, expected_checkpoints)
    #     return cache.checkpoints

    def assert_cached(self, oid, tid, storage, msg=None):
        oid = as_int(oid)
        tid = as_int(tid)
        cache = find_cache(storage)
        cache_data = cache.local_client[(oid, tid)]
        __traceback_info__ = (
            (oid, tid),
            cache.object_index,
            cache.polling_state.object_index,
            [k
             for k in cache.local_client
             if k[0] == oid]
        )
        self.assertIsNotNone(cache_data,
                             msg or "Unable to find cache for (%s, %s)" % (oid, tid))
        return cache_data

    def assert_cached_exact(self, oid, tid, storage, msg=None):
        data = self.assert_cached(oid, tid, storage, msg)
        self.assertEqual(data[1], as_int(tid), msg)
        return data

    def assert_oid_not_cached(self, oid, storage, msg=None):
        oid = as_int(oid)
        cache = find_cache(storage)
        keys = [k for k in cache.local_client if k[0] == oid]
        __traceback_info__ = (oid, keys)
        self.assertEmpty(keys, msg)


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
        Set the values for *new_data* in the root object of the storage.

        And return the transaction ID of when we mad the change,
        and the transaction ID of the last time the root changed.

        Uses an independent transaction.

        *old_tids* is a map from the keys in *new_data* to an expected TID
        that should be cached. *old_value* is the same for the expected
        current values on the root.

        Closes *storage*.
        """
        db1 = self._closing(DB(storage))
        tx = transaction.TransactionManager()
        c1 = db1.open(tx)
        # We've polled and gained checkpoints
        # self.assert_checkpoints(c1)

        root = c1.root()
        self.__do_sets(root, new_data, old_tids, old_data)
        tx.commit()

        self.__do_check_tids(root, old_tids)
        tid_int = bytes8_to_int64(c1._storage.lastTransaction())
        self.assertEqual(c1._storage._cache.current_tid, tid_int)
        c1.close()
        if pack:
            storage.pack(tid_int, referencesf)
        db1.close()
        return tid_int, bytes8_to_int64(root._p_serial)

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
        # It has the correct checkpoints,
        storage3 = self.__make_storage_pcache(
            orig_checkpoints)

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
        """
        Creates the following structure in ``self._storage``::

            root.myobj1 = PersistentMapping()
            root.myobj1.key = PersistentMapping()
            root.myobj = 3

        Does this over several transactions. Returns
        the tid of the last time the root changed, and the tid
        of ``root.myobj1``, which is later than the root TID and which
        is current, and the database opened on the storage.
        """
        tx1 = transaction.TransactionManager()
        storage1 = self._storage
        db1 = self._closing(DB(storage1))
        c1 = db1.open(tx1)
        root = c1.root
        root().myobj1 = root.myobj1 = mapping = PersistentMapping()
        root().myobj = root.myobj = 1
        tx1.commit()
        c1._storage._cache.clear(load_persistent=False)

        c1._storage.poll_invalidations()
        root().myobj = root.myobj = 2
        tx1.commit()
        c1._storage._cache.clear(load_persistent=False)

        c1._storage.poll_invalidations()
        root().myobj = root.myobj = 3
        tx1.commit()
        root_tid = self.assert_oid_known(ROOT_OID, c1)
        c1._storage._cache.clear(load_persistent=False)

        # Now, mutate an object that's not the root
        # so that we get a new transaction after the root was
        # modified. This transaction will be included in
        # a persistent cache.
        c1._storage.poll_invalidations()
        root().myobj1.key = root.myobj1.key = PersistentMapping()
        mapping_oid = mapping._p_oid
        mapping_oid_int = bytes8_to_int64(mapping_oid)
        tx1.commit()
        mapping_tid = self.assert_oid_known(mapping_oid_int, c1)

        # self.assert_checkpoints(c1, (root_tid, root_tid))
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

        # Persist
        db.close()

        # Now a new storage that will read the persistent cache
        storage3 = self.__make_storage_pcache(
            expected_checkpoints=(new_tid, new_tid),
            expected_root_tid=None,
        )
        # The root object is not cached, it was rejected.
        self.assert_oid_not_cached(ROOT_OID, storage3)

        # We can successfully open and edit the root object.
        self.__set_key_in_root_to(storage3, 180, old_tid=new_tid, old_value=420)

    def checkNoConflictNoChangeInPersistentCacheBeforeCP1(self):
        root_tid, _mapping_tid, db = self._populate_root_and_mapping()

        # Make some changes to the sub-object, and not the root, in a storage that will not
        # read or update the persistent cache.
        new_tid, root_tid = self.__set_key_in_root_to(
            self.__make_storage_no_pcache(),
            420,
            key='myobj1.key',
        )

        # persist
        db.close()

        # Now a new storage that will read the persistent cache
        storage = self.__make_storage_pcache(
            # With checkpoints
            expected_checkpoints=(new_tid, new_tid),
            # No root TID in delta after 0
            expected_root_tid=None
        )
        # But it is in the cache for its correct key, because we verified it
        # to still be in sync.
        cache_data = self.assert_cached(ROOT_OID, root_tid, storage)
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

        # self.assert_checkpoints(c1, (root_tid, root_tid))

        # the root is not in a delta
        # self.assert_oid_not_known(ROOT_OID, c1)

        # Though it is in the cache.
        self.assert_cached_exact(ROOT_OID, root_tid, c1)

        # Create a new transaction that derefs an object but
        # that won't update the persistent cache.
        new_tid, _ = self.__set_keys_in_root_to(
            self.__make_storage_no_pcache(),
            {'myobj1.key': None},
            {},
            {},
            pack=True
        )

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
        # Though it remains cached.
        # self.assert_oid_not_cached(nested_mapping_oid_int, storage)

        # Likewise, the parent mapping isn't found anywhere, because it
        # changed
        self.assert_oid_not_known(mapping_oid_int, storage)
        # self.assert_oid_not_cached(mapping_oid_int, storage)

        self.__set_keys_in_root_to(
            storage,
            {'myobj': 180, 'myobj1.key': 360},
            {'': root_tid},
            {'myobj': 3, 'myobj1.key': None}
        )


    def checkNoConflictWhenOverlappedModificationNotInCache(self):
        # A storage writes a current state of an object O1 at TID1
        # to persistent cache.
        # Meanwhile, an unconnected storage writes O1
        # to the database at TID2.
        # A third storage polls, and sees the change for O1, but has
        # never had O1 in its cache. It writes the persistent cache.

        # When we open and read the persistent cache, we don't use the old
        # state for O1.

        # First storage caches this state.
        root_tid, _mapping_tid, db = self._populate_root_and_mapping()
        conn = db.open()
        nested_tid = conn.root.myobj1.key._p_serial
        nested_oid = conn.root.myobj1.key._p_oid
        conn.close()
        # Persist.
        db.close()

        # Second storage, disconnected, mutates.
        # Third storage is around to observe this.
        storage2 = self.__make_storage_no_pcache()
        txm2 = transaction.TransactionManager(True)
        db2 = DB(storage2)

        storage3 = self.__make_storage_pcache()
        find_cache(storage3).clear(load_persistent=False)
        storage3.poll_invalidations()

        conn2 = db2.open(txm2)
        txm2.begin()
        conn2.root.myobj1.key['foo'] = 'bar'

        # A fourth storage is opened right now, loading the old state.
        storage_with_old_state = self.__make_storage_pcache()

        txm2.commit()
        _, new_nested_tid = conn2._storage.load(nested_oid)
        self.assertNotEqual(nested_tid, new_nested_tid)
        conn2.close()
        db2.close()


        # Storage 3 observes the change, but has never had that object cached.
        storage3.poll_invalidations()
        self.assert_exact_tid_in_oid_index(nested_oid, storage3, new_nested_tid)
        self.assert_oid_not_cached(nested_oid, storage3)
        # Persist. Make it have a hit first so that it actually does want to save.
        storage3.load(z64)
        self.assertEqual(1, find_cache(storage3).save())
        storage3.close()

        storage4 = self.__make_storage_pcache()
        # When storage3 saved, it removed that data from the cache, so
        # storage4 doesn't have it.
        self.assert_oid_not_cached(
            nested_oid, storage4,
            "OID %s should not be cached except with TID %s" % (
                bytes8_to_int64(nested_oid),
                bytes8_to_int64(new_nested_tid)
            ))
        self.assert_cached(ROOT_OID, root_tid, storage4)

        def _check_nested(storage):
            db4 = DB(storage)
            txm4 = transaction.TransactionManager(True)
            conn4 = db4.open(txm4)
            txm4.begin()
            self.assertEqual(
                conn4.root.myobj1.key['foo'],
                'bar'
            )
            conn4.close()
            db4.close()
        _check_nested(storage4)

        # The one opened in between has it, but won't use it.
        self.assert_cached_exact(nested_oid, nested_tid, storage_with_old_state)

        # However, when it polls, it won't use the old state.
        _check_nested(storage_with_old_state)

    def checkNoConflictWhenOverlappedModificationNotInCache2(self):
        # pylint:disable=too-many-statements
        # Like checkNoConflictWhenOverlappedModificationNotInCache,
        # but the storage that does the invalidation polls
        # several times and loses the invalidation data from its object index.

        # First storage caches this state.
        root_tid, _mapping_tid, db = self._populate_root_and_mapping()
        conn = db.open()
        nested_tid = conn.root.myobj1.key._p_serial
        nested_oid = conn.root.myobj1.key._p_oid
        conn.close()
        # Persist.
        db.close()

        # Second storage, disconnected, mutates.
        # Third storage is around to observe this.
        storage2 = self.__make_storage_no_pcache()
        txm2 = transaction.TransactionManager(True)
        db2 = DB(storage2)

        storage3 = self.__make_storage_pcache()
        find_cache(storage3).clear(load_persistent=False)
        storage3.poll_invalidations()

        conn2 = db2.open(txm2)
        txm2.begin()
        conn2.root.myobj1.key['foo'] = 'bar'

        # A fourth storage is opened right now, loading the old state.
        storage_with_old_state = self.__make_storage_pcache()

        txm2.commit()
        _, new_nested_tid = conn2._storage.load(nested_oid)
        self.assertNotEqual(nested_tid, new_nested_tid)
        conn2.close()
        db2.close()


        # Storage 3 observes the change, but has never had that object cached.
        storage3.poll_invalidations()
        self.assert_exact_tid_in_oid_index(nested_oid, storage3, new_nested_tid)
        self.assert_oid_not_cached(nested_oid, storage3)

        # Storage 3 makes some changes of its own and polls forward, losing
        # the old object index.
        def _make_change(storage):
            db = DB(storage)
            tx = transaction.TransactionManager(True)
            conn = db.open(tx)
            tx.begin()
            conn.root.another_obj = 42
            tx.commit()
            tid = conn.root()._p_serial
            tx.begin()
            tx.commit()
            conn.close()
            return tid, db

        root_tid, db = _make_change(storage3)
        storage3.poll_invalidations()
        # We still have this data in our map because the DB object and its open
        # connection still have that around.
        self.assert_exact_tid_in_oid_index(nested_oid, storage3, new_nested_tid)
        self.assert_oid_not_cached(nested_oid, storage3)


        # Persist. Make it have a hit first so that it actually does want to save.
        storage3.load(z64)
        self.assertEqual(1, find_cache(storage3).save())
        # We detached everything and vacuumed in order to save. So
        # we've lost the tid data, and couldn't invalidate it based on
        # the object_index. So unless we do validation based on
        # something else, we'd still save and use incorrect data.
        self.assert_oid_not_known(nested_oid, storage3)
        storage3.close()

        storage4 = self.__make_storage_pcache()
        # We still figured out to drop this data, though.
        self.assert_oid_not_cached(
            nested_oid, storage4,
            "OID %s should not be cached except with TID %s" % (
                bytes8_to_int64(nested_oid),
                bytes8_to_int64(new_nested_tid)
            ))
        self.assert_cached(ROOT_OID, root_tid, storage4)

        def _check_nested(storage):
            db4 = DB(storage)
            txm4 = transaction.TransactionManager(True)
            conn4 = db4.open(txm4)
            txm4.begin()
            self.assertEqual(
                conn4.root.myobj1.key['foo'],
                'bar'
            )
            conn4.close()
            db4.close()
        _check_nested(storage4)

        # The one opened in between has it, but won't use it.
        self.assert_cached_exact(nested_oid, nested_tid, storage_with_old_state)

        # However, when it polls, it won't use the old state.
        _check_nested(storage_with_old_state)
