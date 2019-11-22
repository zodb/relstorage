# -*- coding: utf-8; -*-
##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""A foundation for RelStorage tests"""
from __future__ import absolute_import
from __future__ import print_function

# pylint:disable=too-many-ancestors,abstract-method,too-many-public-methods,too-many-lines
# pylint:disable=too-many-statements,too-many-locals
import contextlib
import functools
import os
import random
import shutil
import tempfile
import time
import threading
import unittest

import transaction
from persistent import Persistent
from persistent.mapping import PersistentMapping
from zc.zlibstorage import ZlibStorage

import ZODB.tests.util

from ZODB.Connection import TransactionMetaData
from ZODB.DB import DB
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ReadConflictError
from ZODB.POSException import ReadOnlyError
from ZODB.serialize import referencesf
from ZODB.utils import z64
from ZODB.utils import u64 as bytes8_to_int64

from ZODB.tests import BasicStorage
from ZODB.tests import ConflictResolution
from ZODB.tests import MTStorage
from ZODB.tests import PackableStorage
from ZODB.tests import PersistentStorage
from ZODB.tests import ReadOnlyStorage
from ZODB.tests import StorageTestBase
from ZODB.tests import Synchronization
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.tests.StorageTestBase import zodb_unpickle
from ZODB.tests.MinPO import MinPO

from . import fakecache
from . import util
from . import mock
from . import TestCase
from . import StorageCreatingMixin
from . import skipIfNoConcurrentWriters
from .persistentcache import PersistentCacheStorageTests
from .locking import TestLocking
from .test_zodbconvert import FSZODBConvertTests

class RelStorageTestBase(StorageCreatingMixin,
                         TestCase,
                         StorageTestBase.StorageTestBase):

    base_dbname = None # Override
    keep_history = None  # Override
    _storage_created = None

    def _close(self):
        # Override from StorageTestBase.

        # Try to avoid creating one through our _storage property.
        if '_storage' in self.__dict__:
            storage = self._storage
        else:
            storage = self._storage_created
        self._storage = None

        if storage is not None:
            storage.close()
            storage.cleanup()

    def make_storage_to_cache(self):
        return self.make_storage()

    def get_storage(self):
        # Create a storage with default options
        # if it has not been created already.
        storage = self._storage_created
        if storage is None:
            storage = self.make_storage_to_cache()
            self._storage_created = storage
        return storage

    def set_storage(self, storage):
        self._storage_created = storage

    _storage = property(
        lambda self: self.get_storage(),
        lambda self, nv: self.set_storage(nv)
    )

    def open(self, read_only=False, **kwargs):
        # This is used by a few ZODB tests that close and reopen the storage.
        storage = self._storage
        if storage is not None:
            self._storage = None
            storage.close()
            storage.cleanup()
        self._storage = storage = self.make_storage(
            read_only=read_only, zap=False, **kwargs)
        return storage


class StorageClientThread(MTStorage.StorageClientThread):
    # MTStorage assumes that the storage object is thread safe.
    # This doesn't make any sense for an MVCC Storage like RelStorage;
    # don't try to use a single instance in multiple threads.
    #
    # This patch makes it respect that.

    def __init__(self, storage, *args, **kwargs):
        storage = storage.new_instance()
        super(StorageClientThread, self).__init__(storage, *args, **kwargs)

    def runtest(self):
        try:
            super(StorageClientThread, self).runtest()
        finally:
            self.storage.release()
            self.storage = None


class ExtStorageClientThread(StorageClientThread, MTStorage.ExtStorageClientThread):
    "Same as above."

class ThreadWrapper(object):

    def __init__(self, storage):
        self.__storage = storage
        # We can't use an RLock, which verifies that the thread that
        # acquired is the one that releases; check_tid_ordering_w_commit
        # deliberately spreads these actions across threads (for same reason).
        self.__commit_lock = threading.Lock()
        rl = self.__read_lock = threading.Lock()
        self.__txn = None

        def make_locked(name):
            meth = getattr(storage, name)
            @functools.wraps(meth)
            def func(*args, **kwargs):
                with rl:
                    return meth(*args, **kwargs)
            return func

        for name in (
                'loadBefore',
                'load',
                'store',
                'getTid',
                'lastTransaction',
        ):
            setattr(self, name, make_locked(name))

    def __getattr__(self, name):
        return getattr(self.__storage, name)

    def tpc_begin(self, txn):
        self.__commit_lock.acquire()
        self.__read_lock.acquire()
        assert not self.__txn
        self.__txn = txn
        self.__read_lock.release()
        return self.__storage.tpc_begin(txn)

    def tpc_finish(self, txn, callback=None):
        self.__read_lock.acquire()
        assert txn is self.__txn
        try:
            return self.__storage.tpc_finish(txn, callback)
        finally:
            self.__txn = None
            self.__commit_lock.release()
            self.__read_lock.release()

    def tpc_abort(self, txn):
        self.__read_lock.acquire()
        assert txn is self.__txn, (txn, self.__txn)
        try:
            return self.__storage.tpc_abort(txn)
        finally:
            self.__txn = None
            self.__commit_lock.release()
            self.__read_lock.release()

class UsesThreadsOnASingleStorageMixin(object):
    # These tests attempt to use threads on a single storage object.
    # That doesn't make sense with MVCC, where every instance is its
    # own connection and doesn't need to do any locking. This mixin makes
    # those tests use a special storage that locks.

    @contextlib.contextmanager
    def __thread_safe_wrapper(self):
        orig_storage = self._storage
        wrapped = self._storage = ThreadWrapper(orig_storage)
        try:
            yield
        finally:
            if self._storage is wrapped:
                self._storage = orig_storage

    def __generic_wrapped_test(self, meth_name):
        meth = getattr(
            super(UsesThreadsOnASingleStorageMixin, self),
            meth_name)
        try:
            with self.__thread_safe_wrapper():
                meth()
        finally:
            self._storage.zap_all(slow=True)

    def make_func(name): # pylint:disable=no-self-argument
        return lambda self: self.__generic_wrapped_test(name)

    for bad_test in (
            'check_checkCurrentSerialInTransaction',
            # This one stores a b'y' (invalid pickle) into the
            # database as the root object, so if we don't get zapped
            # afterwards, we can't open the database.
            'check_tid_ordering_w_commit',
    ):
        locals()[bad_test] = make_func(bad_test)

    del make_func
    del bad_test


class GenericRelStorageTests(
        UsesThreadsOnASingleStorageMixin,
        RelStorageTestBase,
        PersistentCacheStorageTests,
        TestLocking,
        BasicStorage.BasicStorage,
        PackableStorage.PackableStorage,
        Synchronization.SynchronizedStorage,
        ConflictResolution.ConflictResolvingStorage,
        PersistentStorage.PersistentStorage,
        MTStorage.MTStorage,
        ReadOnlyStorage.ReadOnlyStorage,
    ):

    def setUp(self):
        # ZODB.tests.util.TestCase likes to change directories
        # It tries to change back in tearDown(), but if there's an error,
        # we may not get to tearDown. addCleanup() always runs, though.
        # do that as the very last thing that happens (except for subclasses, they
        # could add things first)
        self.addCleanup(os.chdir, os.getcwd())

        super(GenericRelStorageTests, self).setUp()
        # PackableStorage is particularly bad about leaving things
        # dangling. For example, if the ClientThread runs into
        # problems, it doesn't close its connection, which can leave
        # locks dangling until GC happens and break other threads and even
        # other tests.
        #
        # Patch around that. Be sure to only close a given connection once,
        # though.
        _closing = self._closing
        def db_factory(storage, *args, **kwargs):
            db = _closing(DB(storage, *args, **kwargs))
            db_open = db.open
            def o(transaction_manager=None, at=None, before=None):
                conn = db_open(transaction_manager=transaction_manager,
                               at=at,
                               before=before)

                _closing(conn)
                if transaction_manager is not None:
                    # If we're using an independent transaction, abort it *before*
                    # attempting to close the connection; that means it must be registered
                    # after the connection.
                    self.addCleanup(transaction_manager.abort)
                return conn
            db.open = o
            return db
        PackableStorage.DB = db_factory

        self.addCleanup(setattr, MTStorage,
                        'StorageClientThread', MTStorage.StorageClientThread)
        MTStorage.StorageClientThread = StorageClientThread

        self.addCleanup(setattr, MTStorage,
                        'ExtStorageClientThread', MTStorage.ExtStorageClientThread)
        MTStorage.ExtStorageClientThread = ExtStorageClientThread

    def tearDown(self):
        PackableStorage.DB = DB
        super(GenericRelStorageTests, self).tearDown()

    def _make_readonly(self):
        # checkWriteMethods in ReadOnlyStorage assumes that
        # the object has an undo() method, even though that's only
        # required if it's IStorageUndoable, aka history-preserving.
        super(GenericRelStorageTests, self)._make_readonly()
        storage = self._storage
        if not hasattr(storage, 'undo'):
            def undo(*args, **kwargs):
                raise ReadOnlyError
            storage.undo = undo # pylint:disable=attribute-defined-outside-init
        return storage

    def checkCurrentObjectTidsRoot(self):
        # Get the root object in place
        db = self._closing(DB(self._storage))
        conn = self._closing(db.open())

        storage = conn._storage
        cursor = storage._load_connection.cursor
        oid_to_tid = storage._adapter.mover.current_object_tids(cursor, [0])
        self.assertEqual(1, len(oid_to_tid))
        self.assertIn(0, oid_to_tid)

        # Ask for many, many objects that don't exist.
        # Force the implementation to loop if that's what it does internally.
        oid_to_tid = storage._adapter.mover.current_object_tids(cursor, range(0, 3523))
        self.assertEqual(1, len(oid_to_tid))
        self.assertIn(0, oid_to_tid)

        # No matching oids.
        oid_to_tid = storage._adapter.mover.current_object_tids(cursor, range(1, 3523))
        self.assertEqual(0, len(oid_to_tid))

        conn.close()
        db.close()

    def checkLen(self):
        # Override the version from BasicStorage because we
        # actually do guarantee to keep track of the counts,
        # within certain limits.

        # len(storage) reports the number of objects.
        # check it is zero when empty
        self.assertEqual(len(self._storage), 0)
        # check it is correct when the storage contains two object.
        # len may also be zero, for storages that do not keep track
        # of this number
        self._dostore(data=PersistentMapping())
        self._dostore(data=PersistentMapping())
        self._storage._adapter.stats.large_database_change()
        self.assertEqual(len(self._storage), 2)

    def checkDropAndPrepare(self):
        # Under PyPy, this test either takes a very long time (PyMySQL)
        # or hangs (psycopg2cffi) longer than I want to wait (10+ minutes).
        # This suggests there's a lock on a particular table (the eighth table we drop)
        # which in turn suggests that there are connections still open and leaked!
        # Running a manual GC seems to fix it. It's hard to reproduce manually because
        # it seems to depend on a particular set of tests being run.
        import gc
        gc.collect()
        gc.collect()

        self._storage._adapter.schema.drop_all()
        self._storage._adapter.schema.prepare()

    def checkCrossConnectionInvalidation(self):
        # Verify connections see updated state at txn boundaries
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['myobj'] = 'yes'
            c2 = db.open()
            r2 = c2.root()
            self.assertNotIn('myobj', r2)

            storage = c1._storage
            t = transaction.Transaction()
            t.description = u'invalidation test'
            c1.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(storage._transaction)
            storage.tpc_finish(storage._transaction)

            self.assertNotIn('myobj', r2)
            c2.sync()
            self.assertIn('myobj', r2)
            self.assertEqual(r2['myobj'], 'yes')
        finally:
            db.close()

    def checkCrossConnectionIsolation(self):
        # Verify MVCC isolates connections
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = PersistentMapping()
            r1['gamma'] = PersistentMapping()
            transaction.commit()

            # Open a second connection but don't load root['alpha'] yet
            c2 = db.open()
            r2 = c2.root()

            r1['alpha']['beta'] = 'yes'

            storage = c1._storage
            t = transaction.Transaction()
            t.description = u'isolation test 1'
            c1.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(storage._transaction)
            storage.tpc_finish(storage._transaction)

            # The second connection will now load root['alpha'], but due to
            # MVCC, it should continue to see the old state.
            self.assertIsNone(r2['alpha']._p_changed)  # A ghost
            self.assertFalse(r2['alpha'])
            self.assertEqual(r2['alpha']._p_changed, 0)

            # make root['alpha'] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assertIsNone(r2['alpha']._p_changed)  # A ghost
            self.assertTrue(r2['alpha'])
            self.assertEqual(r2['alpha']._p_changed, 0)
            self.assertEqual(r2['alpha']['beta'], 'yes')

            # Repeat the test with root['gamma']
            r1['gamma']['delta'] = 'yes'

            storage = c1._storage
            t = transaction.Transaction()
            t.description = u'isolation test 2'
            c1.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(storage._transaction)
            storage.tpc_finish(storage._transaction)

            # The second connection will now load root[3], but due to MVCC,
            # it should continue to see the old state.
            self.assertIsNone(r2['gamma']._p_changed)  # A ghost
            self.assertFalse(r2['gamma'])
            self.assertEqual(r2['gamma']._p_changed, 0)

            # make root[3] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assertIsNone(r2['gamma']._p_changed)  # A ghost
            self.assertTrue(r2['gamma'])
            self.assertEqual(r2['gamma']._p_changed, 0)
            self.assertEqual(r2['gamma']['delta'], 'yes')
        finally:
            db.close()

    def checkResolveConflictBetweenConnections(self, clear_cache=False):
        # Verify that conflict resolution works between storage instances
        # bound to connections.
        obj = ConflictResolution.PCounter()
        obj.inc()
        # Establish a polling state; dostoreNP won't.
        self._storage.poll_invalidations()
        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))
        self._storage.poll_invalidations()
        # These will both poll and get the state for (oid, revid1)
        # cached at that location, where it will be found during conflict
        # resolution.
        storage1 = self._storage.new_instance()
        storage1.load(oid, '')

        storage2 = self._storage.new_instance()
        storage2.load(oid, '')
        # Remember that the cache stats are shared between instances.
        # The first had to fetch it, the second can use it.
        __traceback_info__ = storage1._cache.stats()
        self.assertEqual(storage1._cache.stats()['hits'], 1)
        storage1._cache.reset_stats()
        if clear_cache:
            storage1._cache.clear(load_persistent=False)
        self.assertEqual(storage1._cache.stats()['hits'], 0)

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        root_storage = self._storage
        try:

            def noConflict(*_args, **_kwargs):
                self.fail("Should be no conflict.")
            storage1.tryToResolveConflict = noConflict
            self._storage = storage1
            _revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

            # This one had no conflicts and did no cache work
            self.assertEqual(storage1._cache.stats()['hits'], 0)
            self.assertEqual(storage1._cache.stats()['misses'], 0)

            # This will conflict; we will prefetch everything through the cache,
            # or database, and not the storage's loadSerial.
            def noLoadSerial(*_args, **_kwargs):
                self.fail("loadSerial on the storage should never be called")
            storage2.loadSerial = noLoadSerial
            self._storage = storage2
            _revid3 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

            # We don't actually update cache stats at all, however,
            # despite the prefetching.
            cache_stats = storage1._cache.stats()
            __traceback_info__ = cache_stats, clear_cache

            self.assertEqual(cache_stats['misses'], 0)
            self.assertEqual(cache_stats['hits'], 0)

            data, _serialno = self._storage.load(oid, '')
            inst = zodb_unpickle(data)
            self.assertEqual(inst._value, 5)
        finally:
            storage1.close()
            storage2.close()
            self._storage = root_storage

    def checkResolveConflictBetweenConnectionsNoCache(self):
        # If we clear the cache, we can still loadSerial()
        self.checkResolveConflictBetweenConnections(clear_cache=True)

    def check16KObject(self):
        # Store 16 * 1024 bytes in an object, then retrieve it
        data = b'a 16 byte string' * 1024
        oid = self._storage.new_oid()
        self._dostoreNP(oid, data=data)
        got, _ = self._storage.load(oid, '')
        self.assertIsInstance(got, bytes)
        self.assertEqual(got, data)
        self.assertEqual(len(got), len(data))

    def check16MObject(self):
        # Store 16 * 1024 * 1024 bytes in an object, then retrieve it
        data = b'a 16 byte string' * (1024 * 1024)
        oid = self._storage.new_oid()
        self._dostoreNP(oid, data=data)
        got, _serialno = self._storage.load(oid, '')
        self.assertEqual(len(got), len(data))
        self.assertEqual(got, data)

    def check99X1900Objects(self):
        # Store 99 objects each with 1900 bytes.  This is intended
        # to exercise possible buffer overfilling that the batching
        # code might cause.
        data = b'0123456789012345678' * 100
        t = transaction.Transaction()
        self._storage.tpc_begin(t)
        oids = []
        for _ in range(99):
            oid = self._storage.new_oid()
            self._storage.store(oid, b'\0'*8, data, '', t)
            oids.append(oid)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        for oid in oids:
            got, _serialno = self._storage.load(oid, '')
            self.assertEqual(len(got), len(data))
            self.assertEqual(got, data)

    def checkPreventOIDOverlap(self):
        # Store an object with a particular OID, then verify that
        # OID is not reused.
        data = b'mydata'
        oid1 = b'\0' * 7 + b'\x0f'
        self._dostoreNP(oid1, data=data)
        oid2 = self._storage.new_oid()
        oid1_int = bytes8_to_int64(oid1)
        oid2_int = bytes8_to_int64(oid2)
        self.assertGreater(
            oid2_int, oid1_int,
            'old OID %r (%d) should be less than new OID %r (%d)'
            % (oid1, oid1_int, oid2, oid2_int))

    def checkNoDuplicateOIDsManyThreads(self):
        # Many threads in many storages can allocate OIDs with
        # no duplicates or overlaps.
        # https://github.com/zodb/relstorage/issues/283
        from itertools import combinations

        thread_count = 11
        oids_per_segment = 578
        segment_count = 3
        total_expected_oids = oids_per_segment * segment_count
        oids_by_thread = [list() for _ in range(thread_count)]

        def allocate_oids(thread_storage, thread_num):
            try:
                store_conn = thread_storage._store_connection
                allocator = thread_storage._oids
                my_oids = oids_by_thread[thread_num]
                for _ in range(segment_count):
                    my_oids.extend(
                        bytes8_to_int64(thread_storage.new_oid())
                        for _ in range(oids_per_segment)
                    )
                    # Periodically call set_min_oid, like the storage does,
                    # to check for interference.
                    allocator.set_min_oid(my_oids[-1])
                    store_conn.commit()
            finally:
                thread_storage.release()

        threads = [threading.Thread(target=allocate_oids,
                                    args=(self._storage.new_instance(), i))
                   for i in range(thread_count)]
        for t in threads:
            t.start()

        for t in threads:
            t.join(99)

        # They all have the desired length, and each one has no duplicates.
        self.assertEqual(
            [len(s) for s in oids_by_thread],
            [total_expected_oids for _ in range(thread_count)]
        )
        self.assertEqual(
            [len(s) for s in oids_by_thread],
            [len(set(s)) for s in oids_by_thread]
        )

        # They are all disjoint
        for a, b in combinations(oids_by_thread, 2):
            __traceback_info__ = a, b
            a = set(a)
            b = set(b)
            self.assertTrue(a.isdisjoint(b))

        # They are all monotonically increasing.
        for s in oids_by_thread:
            self.assertEqual(
                s,
                sorted(s)
            )

    def checkUseCache(self):
        # Store an object, cache it, then retrieve it from the cache
        self._storage = self.make_storage(
            cache_servers='x:1 y:2',
            cache_module_name=fakecache.__name__,
            cache_prefix='zzz',
        )

        fakecache.data.clear()
        db = DB(self._storage)
        try:
            c1 = db.open()
            self.assertEqual(
                c1._storage._cache.cache.g.client.servers,
                ['x:1', 'y:2'])
            r1 = c1.root()
            # The root state and checkpoints should now be cached.
            # A commit count *might* be cached depending on the ZODB version.
            # (Checkpoints are stored in the cache for the sake of tests/monitoring,
            # but aren't read.)
            # self.assertIn('zzz:checkpoints', fakecache.data)
            # self.assertIsNotNone(db.storage._cache.polling_state.checkpoints)
            self.assertEqual(sorted(fakecache.data.keys())[-1][:10],
                             'zzz:state:')
            r1['alpha'] = PersistentMapping()
            transaction.commit()
            cp_count = 1
            if self.keep_history:
                item_count = 2
            else:
                # The previous root state was automatically invalidated
                # XXX: We go back and forth on that.
                item_count = 2
            item_count += cp_count
            self.assertEqual(len(fakecache.data), item_count)

            oid = r1['alpha']._p_oid
            c1._storage.load(oid, '')
            # Came out of the cache, nothing new
            self.assertEqual(len(fakecache.data), item_count)

            # make a change
            r1['beta'] = 0
            transaction.commit()
            # Once again, history free automatically invalidated.
            # XXX: Depending on my mood.
            item_count += 1
            self.assertEqual(len(fakecache.data), item_count)

            c1._storage.load(oid, '')

            # try to load an object that doesn't exist
            self.assertRaises(KeyError, c1._storage.load, b'bad.oid.', '')
        finally:
            db.close()

    def checkMultipleStores(self):
        # Verify a connection can commit multiple transactions
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = 1
            transaction.commit()
            r1['alpha'] = 2
            transaction.commit()
        finally:
            db.close()

    def checkLongTransactionDescription(self):
        # Don't trip over long transaction descriptions
        db = DB(self._storage)
        try:
            c = db.open()
            r = c.root()
            r['key'] = 1
            transaction.get().note(u'A long description. ' * 1000)
            transaction.commit()
        finally:
            db.close()

    def checkAutoReconnect(self):
        # Verify auto-reconnect
        db = self._closing(DB(self._storage))

        c1 = db.open()
        r = c1.root()
        r['alpha'] = 1
        transaction.commit()
        c1.close()

        # Going behind its back.
        c1._storage._load_connection.connection.close()
        c1._storage._store_connection.connection.close()
        # ZODB5 implicitly calls sync
        # immediately when a connection is opened;
        # fake that here for older releases.
        c2 = db.open()
        self.assertIs(c2, c1)
        c2.sync()
        r = c2.root()
        self.assertEqual(r['alpha'], 1)
        r['beta'] = PersistentMapping()
        c2.add(r['beta'])
        transaction.commit()
        c2.close()

        del c1
        del c2

    def checkAutoReconnectOnSync(self):
        # Verify auto-reconnect.
        db = self._closing(DB(self._storage))

        c1 = db.open()
        r = c1.root()

        c1._storage._load_connection.connection.close()
        c1._storage.sync(True)
        # ZODB5 calls sync when a connection is opened. Our monkey
        # patch on a Connection makes sure that works in earlier
        # versions, but we don't have that patch on ZODB5. So test
        # the storage directly. NOTE: The load connection must be open.
        # to trigger the actual sync.

        r = c1.root()
        r['alpha'] = 1
        transaction.commit()
        c1.close()

        c1._storage._load_connection.connection.close()
        c1._storage._store_connection.connection.close()

        c2 = db.open()
        self.assertIs(c2, c1)

        r = c2.root()
        self.assertEqual(r['alpha'], 1)
        r['beta'] = PersistentMapping()
        c2.add(r['beta'])
        transaction.commit()
        c2.close()

        del c1
        del c2

    def checkCachePolling(self):
        storage2 = self.make_storage(zap=False)

        db = DB(self._storage)
        db2 = DB(storage2)
        try:
            # Set up the database.
            tm1 = transaction.TransactionManager()
            c1 = db.open(transaction_manager=tm1)
            r1 = c1.root()
            r1['obj'] = obj1 = PersistentMapping({'change': 0})
            tm1.commit()

            # Load and change the object in an independent connection.
            tm2 = transaction.TransactionManager()
            c2 = db2.open(transaction_manager=tm2)
            r2 = c2.root()
            r2['obj']['change'] = 1
            tm2.commit()
            # Now c2 has delta_after0.
            # self.assertEqual(len(c2._storage._cache.delta_after0), 2)
            c2.close()

            # Change the object in the original connection.
            c1.sync()
            obj1['change'] = 2
            tm1.commit()

            # Close the database connection to c2.
            c2._storage._load_connection.drop()
            self.assertFalse(c2._storage._load_connection)
            # Make the database connection to c2 reopen without polling.
            c2._storage.load(b'\0' * 8, '')
            self.assertTrue(c2._storage._load_connection)

            # Open a connection, which should be the same connection
            # as c2.
            c3 = db2.open(transaction_manager=tm2)
            self.assertTrue(c3 is c2)
            # self.assertEqual(len(c2._storage._cache.delta_after0), 2)

            # Clear the caches (but not delta_after*)
            c3._resetCache()
            c3._storage._cache.cache.flush_all()

            obj3 = c3.root()['obj']
            # Should have loaded the new object.
            self.assertEqual(obj3['change'], 2)

        finally:
            db.close()
            db2.close()

    def checkDoubleCommitter(self):
        # Verify we can store an object that gets committed twice in
        # a single transaction.
        db = DB(self._storage)
        try:
            conn = db.open()
            try:
                conn.root()['dc'] = DoubleCommitter()
                transaction.commit()
                conn2 = db.open()
                self.assertEqual(conn2.root()['dc'].new_attribute, 1)
                conn2.close()
            finally:
                transaction.abort()
                conn.close()
        finally:
            db.close()

    def checkHistoryWithExtension(self):
        # Verify the history method works with transactions that have
        # extended info.
        db = DB(self._storage)
        try:
            conn = db.open()
            try:
                conn.root()['pi'] = 3.14
                transaction.get().setExtendedInfo("digits", 3)
                transaction.commit()
                history = self._storage.history(conn.root()._p_oid)
                self.assertEqual(len(history), 1)
                if self.keep_history:
                    self.assertEqual(history[0]['digits'], 3)
            finally:
                conn.close()
        finally:
            db.close()

    def checkPackBatchLockNoWait(self):
        # Holding the commit lock doesn't interfere with packing.
        #
        # TODO: But what about row locking? Let's add a test
        # that begins a commit and locks some rows and then packs.
        self._storage = self.make_storage(pack_batch_timeout=0)

        adapter = self._storage._adapter
        test_conn, test_cursor = adapter.connmanager.open_for_store()

        db = self._closing(DB(self._storage))
        try:
            # add some data to be packed
            c = self._closing(db.open())
            r = c.root()
            r['alpha'] = PersistentMapping()
            transaction.commit()
            del r['alpha']
            transaction.commit()

            # Pack, with a commit lock held
            now = packtime = time.time()
            while packtime <= now:
                packtime = time.time()
            adapter.locker.hold_commit_lock(test_cursor)
            self._storage.pack(packtime, referencesf)
            adapter.locker.release_commit_lock(test_cursor)
        finally:
            db.close()
            adapter.connmanager.close(test_conn, test_cursor)

    def checkPackKeepNewObjects(self):
        # Packing should not remove objects created or modified after
        # the pack time, even if they are unreferenced.
        db = DB(self._storage)
        try:
            # add some data to be packed
            c = db.open()
            extra1 = PersistentMapping()
            c.add(extra1)
            extra2 = PersistentMapping()
            c.add(extra2)
            transaction.commit()

            # Choose the pack time to be that last committed transaction.
            packtime = c._storage.lastTransactionInt()

            extra2.foo = 'bar'
            extra3 = PersistentMapping()
            c.add(extra3)
            transaction.commit()

            self.assertGreater(c._storage.lastTransactionInt(), packtime)

            self._storage.pack(packtime, referencesf)

            # extra1 should have been garbage collected
            self.assertRaises(KeyError,
                              self._storage.load, extra1._p_oid, '')
            # extra2 and extra3 should both still exist
            self._storage.load(extra2._p_oid, '')
            self._storage.load(extra3._p_oid, '')
        finally:
            db.close()

    def checkPackBrokenPickle(self):
        # Verify the pack stops with the right exception if it encounters
        # a broken pickle.
        # Under Python 2, with zodbpickle, there may be a difference depending
        # on whether the accelerated implementation is in use. Also ,the pure-python
        # version on PyPy can raise IndexError
        from zodbpickle.pickle import UnpicklingError as pUnpickErr
        unpick_errs = (pUnpickErr, IndexError)
        try:
            from zodbpickle.fastpickle import UnpicklingError as fUnpickErr
        except ImportError:
            pass
        else:
            unpick_errs += (fUnpickErr,)


        self._dostoreNP(self._storage.new_oid(), data=b'brokenpickle')
        self.assertRaises(unpick_errs, self._storage.pack,
                          time.time() + 10000, referencesf)

    def checkBackwardTimeTravelWithoutRevertWhenStale(self):
        # If revert_when_stale is false (the default), when the database
        # connection is stale (such as through failover to an
        # asynchronous slave that is not fully up to date), the poller
        # should notice that backward time travel has occurred and
        # raise a ReadConflictError.
        self._storage = self.make_storage(revert_when_stale=False)
        db = DB(self._storage)
        try:
            c = db.open()
            c._storage._adapter.poller.transactions_may_go_backwards = True
            r = c.root()
            r['alpha'] = PersistentMapping()
            transaction.commit()

            # To simulate failover to an out of date async slave, take
            # a snapshot of the database at this point, change some
            # object, then restore the database to its earlier state.

            d = tempfile.mkdtemp()
            try:
                # Snapshot the database.
                fs = FileStorage(os.path.join(d, 'Data.fs'))
                fs.copyTransactionsFrom(c._storage)

                # Change data in it.
                r['beta'] = PersistentMapping()
                transaction.commit()
                self.assertTrue('beta' in r)

                # Revert the data.
                # We must use a separate, unrelated storage object to do this,
                # because our storage object is smart enough to notice that the data
                # has been zapped and revert caches for all connections and
                # ZODB objects when we invoke this API.
                storage_2 = self.make_storage(zap=False)
                storage_2.zap_all(reset_oid=False, slow=True)
                storage_2.copyTransactionsFrom(fs)
                storage_2.close()
                del storage_2
                fs.close()
                del fs
            finally:
                shutil.rmtree(d)

            # Sync, which will call poll_invalidations().
            c.sync()

            # Try to load an object, which should cause ReadConflictError.
            r._p_deactivate()
            with self.assertRaises(ReadConflictError):
                r.__getitem__('beta')

        finally:
            db.close()

    def checkBackwardTimeTravelWithRevertWhenStale(self):
        # If revert_when_stale is true, when the database
        # connection is stale (such as through failover to an
        # asynchronous slave that is not fully up to date), the poller
        # should notice that backward time travel has occurred and
        # invalidate all objects that have changed in the interval.
        self._storage = self.make_storage(revert_when_stale=True)

        db = DB(self._storage)
        try:
            transaction.begin()
            c = db.open()
            r = c.root()
            r['alpha'] = PersistentMapping()
            transaction.commit()

            # To simulate failover to an out of date async slave, take
            # a snapshot of the database at this point, change some
            # object, then restore the database to its earlier state.

            d = tempfile.mkdtemp()
            try:
                transaction.begin()
                fs = FileStorage(os.path.join(d, 'Data.fs'))
                fs.copyTransactionsFrom(c._storage)

                r['beta'] = PersistentMapping()
                transaction.commit()
                self.assertTrue('beta' in r)

                c._storage.zap_all(reset_oid=False, slow=True)
                c._storage.copyTransactionsFrom(fs)

                fs.close()
            finally:
                shutil.rmtree(d)

            # r should still be in the cache.
            self.assertTrue('beta' in r)

            # Now sync, which will call poll_invalidations().
            c.sync()

            # r should have been invalidated
            self.assertEqual(r._p_changed, None)

            # r should be reverted to its earlier state.
            self.assertFalse('beta' in r)

        finally:
            db.close()

    @util.skipOnAppveyor("Random failures")
    # https://ci.appveyor.com/project/jamadden/relstorage/build/1.0.75/job/32uu4xdp5mubqma8
    def checkBTreesLengthStress(self):
        # BTrees.Length objects are unusual Persistent objects: they
        # have a conflict resolution algorithm that cannot fail, so if
        # we do get a failure it's due to a problem with us.
        # Unfortunately, tryResolveConflict hides all underlying exceptions
        # so we have to enable logging to see them.
        from relstorage.adapters.interfaces import UnableToAcquireLockError
        from ZODB.ConflictResolution import logger as CRLogger
        from BTrees.Length import Length
        import BTrees
        from six import reraise

        def log_err(*args, **kwargs): # pylint:disable=unused-argument
            import sys
            reraise(*sys.exc_info())

        CRLogger.debug = log_err
        CRLogger.exception = log_err

        updates_per_thread = 50
        thread_count = 4
        lock_errors = []

        self.maxDiff = None
        db = DB(self._storage)
        try:
            c = db.open()
            try:
                root = c.root()
                root['length'] = Length()
                # XXX: Eww! On MySQL, if we try to take a shared lock on
                # OID 0, and a write lock on OID 1, we fail with a deadlock
                # error. It seems that taking the shared lock on 0 also takes a shared
                # lock on 1 --- somehow. Because they're adjacent to each other?
                # I don't know. We have to add some space between them to be sure
                # that doesn't happen. On MySQL 5.7, just 10 extra items was enough.
                # On MySQL 8, we had to add more.
                for i in range(50):
                    root[i] = BTrees.OOBTree.BTree()
                transaction.commit()
            except:
                transaction.abort()
                raise
            finally:
                c.close()

            def updater():
                for _ in range(updates_per_thread):
                    thread_c = db.open()
                    __traceback_info__ = thread_c._storage
                    try:
                        # Perform readCurrent on an object not being modified.
                        # This adds stress to databases that use separate types of locking
                        # for modified and current objects. It was used to discover
                        # bugs in gevent+MySQL and plain MySQLdb against both 5.7 and 8.
                        root = thread_c.root()
                        root._p_activate() # unghost; only non-ghosts can readCurrent
                        root._p_jar.readCurrent(root)
                        root['length'].change(1)
                        time.sleep(random.random() * 0.05)
                        try:
                            transaction.commit()
                        except UnableToAcquireLockError as e:
                            lock_errors.append((type(e), str(e)))
                            transaction.abort()
                            raise
                    finally:
                        thread_c.close()

            threads = []
            for _ in range(thread_count):
                t = threading.Thread(target=updater)
                threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join(120)

            self.assertEqual(lock_errors, [])

            c = db.open()
            try:
                self.assertEqual(c.root()['length'](),
                                 updates_per_thread * thread_count)
            finally:
                transaction.abort()
                c.close()

        finally:
            db.close()
            del CRLogger.debug
            del CRLogger.exception


    def checkAfterCompletion(self):
        # The after completion method, which can only be called
        # outside of 2-phase commit is otherise equivalent to calling
        # tpc_abort.
        from ZODB.interfaces import IMVCCAfterCompletionStorage
        self._storage = self.make_storage(revert_when_stale=False)

        with mock.patch.object(self._storage._load_connection,
                               'rollback_quietly') as rb:
            self._storage.afterCompletion()
            rb.assert_called_with()

        self.assertTrue(
            IMVCCAfterCompletionStorage.providedBy(self._storage))

    def checkConfigureViaZConfig(self):
        replica_fn = None
        replica_conf = ''
        if util.DEFAULT_DATABASE_SERVER_HOST == util.STANDARD_DATABASE_SERVER_HOST:
            replica_fn = self.get_adapter_zconfig_replica_conf()
            if replica_fn:
                replica_conf = 'replica-conf ' + self.get_adapter_zconfig_replica_conf()

        conf = u"""
        %import relstorage
        <zodb main>
            <relstorage>
            name xyz
            read-only false
            keep-history {KEEP_HISTORY}
            {REPLICA_CONF}
            blob-dir .
            blob-cache-size-check-external true
            blob-cache-size 100MB
            blob-chunk-size 10MB
            cache-local-dir-read-count 12
            cache-local-dir-write-max-size 10MB
            {ADAPTER}
            </relstorage>
        </zodb>
        """.format(
            KEEP_HISTORY='true' if self.keep_history else 'false',
            REPLICA_CONF=replica_conf,
            ADAPTER=self.get_adapter_zconfig()
        )

        __traceback_info__ = conf

        schema_xml = u"""
        <schema>
        <import package="ZODB"/>
        <section type="ZODB.database" name="main" attribute="database"/>
        </schema>
        """
        import ZConfig
        from io import StringIO
        from ZODB.interfaces import IBlobStorageRestoreable
        from relstorage.adapters.interfaces import IRelStorageAdapter
        from relstorage.blobhelper.interfaces import ICachedBlobHelper
        from hamcrest import assert_that
        from nti.testing.matchers import validly_provides
        schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
        config, _ = ZConfig.loadConfigFile(schema, StringIO(conf))

        db = config.database.open()
        try:
            storage = db.storage
            assert_that(storage, validly_provides(IBlobStorageRestoreable))
            self.assertEqual(storage.isReadOnly(), False)
            self.assertEqual(storage.getName(), "xyz")
            assert_that(storage.blobhelper, validly_provides(ICachedBlobHelper))
            self.assertIn('_External', str(storage.blobhelper.cache_checker))

            adapter = storage._adapter
            self.assertIsInstance(adapter, self.get_adapter_class())
            assert_that(adapter, validly_provides(IRelStorageAdapter))
            self.verify_adapter_from_zconfig(adapter)
            self.assertEqual(adapter.keep_history, self.keep_history)
            if replica_fn:
                self.assertEqual(
                    adapter.connmanager.replica_selector.replica_conf,
                    replica_fn)
            self.assertEqual(storage._options.blob_chunk_size, 10485760)
        finally:
            db.close()

    def checkGeventSwitchesOnOpen(self):
        # We make some queries when we open; if the driver is gevent
        # capable, that should switch.
        driver = self._storage._adapter.driver
        if not driver.gevent_cooperative():
            raise unittest.SkipTest("Driver %s not gevent capable" % (driver,))

        from gevent.util import assert_switches
        with assert_switches():
            self.open()

    #####
    # Prefetch Tests
    #####

    def checkPrefetch(self):
        db = DB(self._storage)
        conn = db.open()

        mapping = conn.root()['key'] = PersistentMapping()

        transaction.commit()

        item_count = 3

        # The new state for the root invalidated the old state,
        # and since there is no other connection that might be using it,
        # we drop it from the cache.
        item_count = 2
        self.assertEqual(item_count, len(self._storage._cache))
        tid = bytes8_to_int64(mapping._p_serial)
        d = self._storage._cache.local_client._cache
        self.assertEqual(d[0].max_tid, tid)
        self.assertEqual(d[1].max_tid, tid)
        self._storage._cache.clear()

        self.assertEmpty(self._storage._cache)

        conn.prefetch(z64, mapping)

        self.assertEqual(2, len(self._storage._cache))

        # second time is a no-op
        conn.prefetch(z64, mapping)
        self.assertEqual(2, len(self._storage._cache))

    ######
    # Parallel Commit Tests
    ######

    @skipIfNoConcurrentWriters
    def checkCanVoteAndCommitWhileOtherStorageVotes(self):
        storage1 = self._closing(self._storage.new_instance())
        storage2 = self._closing(self._storage.new_instance())

        # Bring them both into tpc_vote phase. Before parallel commit,
        # this would have blocked as the first storage took the commit lock
        # in tpc_vote.
        txs = {}
        for storage in (storage1, storage2):
            data = zodb_pickle(MinPO(str(storage)))
            t = TransactionMetaData()
            txs[storage] = t

            storage.tpc_begin(t)
            oid = storage.new_oid()
            storage.store(oid, None, data, '', t)
            storage.tpc_vote(t)

        # The order we choose to finish is the order of the returned
        # tids.
        tid1 = storage2.tpc_finish(txs[storage2])
        tid2 = storage1.tpc_finish(txs[storage1])

        self.assertGreater(tid2, tid1)

        storage1.close()
        storage2.close()

    def checkCanLoadObjectStateWhileBeingModified(self):
        # Get us an object in the database
        storage1 = self._closing(self._storage.new_instance())
        data = zodb_pickle(MinPO(str(storage1)))
        t = TransactionMetaData()
        storage1.tpc_begin(t)
        oid = storage1.new_oid()

        storage1.store(oid, None, data, '', t)
        storage1.tpc_vote(t)
        initial_tid = storage1.tpc_finish(t)

        storage1.release()
        del storage1

        self._storage._cache.clear(load_persistent=False)

        storage1 = self._closing(self._storage.new_instance())

        # Get a completely independent storage, not sharing a cache
        storage2 = self._closing(self.make_storage(zap=False))

        # First storage attempts to modify the oid.
        t = TransactionMetaData()
        storage1.tpc_begin(t)
        storage1.store(oid, initial_tid, data, '', t)
        # And locks the row.
        storage1.tpc_vote(t)

        # storage2 would like to read the old row.
        loaded_data, loaded_tid = storage2.load(oid)

        self.assertEqual(loaded_data, data)
        self.assertEqual(loaded_tid, initial_tid)

        # Commit can now happen.
        tid2 = storage1.tpc_finish(t)
        self.assertGreater(tid2, initial_tid)

        storage1.close()
        storage2.close()

class AbstractRSZodbConvertTests(StorageCreatingMixin,
                                 FSZODBConvertTests,
                                 # This one isn't cooperative in
                                 # setUp(), so it needs to be last.
                                 ZODB.tests.util.TestCase):
    keep_history = True
    filestorage_name = 'source'
    relstorage_name = 'destination'
    filestorage_file = None

    def setUp(self):
        super(AbstractRSZodbConvertTests, self).setUp()
        cfg = """
        %%import relstorage
        %%import zc.zlibstorage
        <zlibstorage %s>
        <filestorage>
            path %s
        </filestorage>
        </zlibstorage>
        <zlibstorage %s>
        <relstorage>
            %s
            cache-prefix %s
            cache-local-dir %s
        </relstorage>
        </zlibstorage>
        """ % (
            self.filestorage_name,
            self.filestorage_file,
            self.relstorage_name,
            self.get_adapter_zconfig(),
            self.relstorage_name,
            os.path.abspath('.'),
        )
        self._write_cfg(cfg)

        self.make_storage(zap=True).close()

    def _wrap_storage(self, storage):
        return self._closing(ZlibStorage(storage))

    def _create_dest_storage(self):
        return self._wrap_storage(super(AbstractRSZodbConvertTests, self)._create_dest_storage())

    def _create_src_storage(self):
        return self._wrap_storage(super(AbstractRSZodbConvertTests, self)._create_src_storage())

    def test_new_instance_still_zlib(self):
        storage = self._closing(self.make_storage())
        new_storage = self._closing(storage.new_instance())
        self.assertIsInstance(new_storage,
                              ZlibStorage)

        self.assertIn('_crs_untransform_record_data', storage.base.__dict__)
        self.assertIn('_crs_transform_record_data', storage.base.__dict__)

        self.assertIn('_crs_untransform_record_data', new_storage.base.__dict__)
        self.assertIn('_crs_transform_record_data', new_storage.base.__dict__)

class AbstractRSDestZodbConvertTests(AbstractRSZodbConvertTests):

    zap_supported_by_dest = True

    @property
    def filestorage_file(self):
        return self.srcfile

    def _create_dest_storage(self):
        return self._closing(self.make_storage(cache_prefix=self.relstorage_name, zap=False))

class AbstractRSSrcZodbConvertTests(AbstractRSZodbConvertTests):

    filestorage_name = 'destination'
    relstorage_name = 'source'

    @property
    def filestorage_file(self):
        return self.destfile

    def _create_src_storage(self):
        return self._closing(self.make_storage(cache_prefix=self.relstorage_name, zap=False))

class AbstractIDBOptionsTest(unittest.TestCase):

    db_options = None

    def test_db_options_compliance(self):
        from hamcrest import assert_that
        from nti.testing.matchers import validly_provides

        from relstorage.adapters.interfaces import IDBDriverOptions
        from relstorage.adapters.interfaces import IDBDriverFactory
        __traceback_info__ = self.db_options
        assert_that(self.db_options, validly_provides(IDBDriverOptions))

        for factory in self.db_options.known_driver_factories():
            assert_that(factory, validly_provides(IDBDriverFactory))

class AbstractIDBDriverTest(unittest.TestCase):

    driver = None

    def test_db_driver_compliance(self):
        from hamcrest import assert_that
        from nti.testing.matchers import validly_provides

        from relstorage.adapters.interfaces import IDBDriver
        __traceback_info__ = self.driver
        assert_that(self.driver, validly_provides(IDBDriver))


class DoubleCommitter(Persistent):
    """A crazy persistent class that changes self in __getstate__"""
    def __getstate__(self):
        if not hasattr(self, 'new_attribute'):
            self.new_attribute = 1 # pylint:disable=attribute-defined-outside-init
        return Persistent.__getstate__(self)


def _close_and_clean_storage(storage):
    try:
        storage.close()
        storage.cleanup()
    except Exception: # pylint:disable=broad-except
        pass


class AbstractToFileStorage(RelStorageTestBase):
    # Subclass this and set:
    # - keep_history = True; and
    # - A base class of UndoableRecoveryStorage
    #
    # or
    # - keep_history = False; and
    # A base class of BasicRecoveryStorage

    # We rely on being placed in a temporary directory by a super
    # class that will be cleaned up by tearDown().

    def setUp(self):
        super(AbstractToFileStorage, self).setUp()
        # Use the abspath so that even if we close it after
        # we've returned to our original directory (e.g.,
        # close is run as part of addCleanup(), which happens after
        # tearDown) we don't write index files into the original directory.
        self._dst_path = os.path.abspath(self.rs_temp_prefix + 'Dest.fs')
        self.__dst = None

    @property
    def _dst(self):
        if self.__dst is None:
            self.__dst = FileStorage(self._dst_path, create=True)
            # On Windows, though, this could be too late: We can't remove
            # files that are still open, and zope.testing.setupstack
            # was asked to remove the temp dir as part of tearing itself down;
            # cleanups run after tearDown runs (which is when the setupstack runs.)
            self.addCleanup(_close_and_clean_storage, self.__dst)
        return self.__dst

    def tearDown(self):
        if hasattr(self.__dst, 'close'):
            _close_and_clean_storage(self.__dst)
        self.__dst = 42 # Not none so we don't try to create.
        super(AbstractToFileStorage, self).tearDown()

    def new_dest(self):
        return self._closing(FileStorage(self._dst_path))


class AbstractFromFileStorage(RelStorageTestBase):
    # As for AbstractToFileStorage

    def setUp(self):
        super(AbstractFromFileStorage, self).setUp()
        self._src_path = os.path.abspath(self.rs_temp_prefix + 'Source.fs')
        self.__dst = None

    def make_storage_to_cache(self):
        return FileStorage(self._src_path, create=True)

    @property
    def _dst(self):
        if self.__dst is None:
            self.__dst = self.make_storage()
            self.addCleanup(_close_and_clean_storage, self.__dst)
        return self.__dst

    def tearDown(self):
        if hasattr(self.__dst, 'close'):
            _close_and_clean_storage(self.__dst)

        self.__dst = 42 # Not none so we don't try to create.
        super(AbstractFromFileStorage, self).tearDown()

    def new_dest(self):
        return self._dst
