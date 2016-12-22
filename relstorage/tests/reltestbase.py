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
# pylint:disable=too-many-ancestors,abstract-method,too-many-public-methods
from ZODB.DB import DB
from ZODB.POSException import ReadConflictError
from ZODB.serialize import referencesf
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
from zc.zlibstorage import ZlibStorage
from persistent import Persistent
from persistent.mapping import PersistentMapping
from relstorage.tests import fakecache
import random
import time
import transaction

from . import util
from relstorage.options import Options
from relstorage.storage import RelStorage

class StorageCreatingMixin(object):

    keep_history = None # Override

    def make_adapter(self, options):
        # abstract method
        raise NotImplementedError()

    def _wrap_storage(self, storage):
        return storage

    def make_storage(self, zap=True, **kw):
        if ('cache_servers' not in kw
                and 'cache_module_name' not in kw
                and kw.get('share_local_cache', True)):
            if util.CACHE_SERVERS and util.CACHE_MODULE_NAME:
                kw['cache_servers'] = util.CACHE_SERVERS
                kw['cache_module_name'] = util.CACHE_MODULE_NAME
                kw['cache_prefix'] = type(self).__name__ + self._testMethodName

        options = Options(keep_history=self.keep_history, **kw)
        adapter = self.make_adapter(options)
        storage = RelStorage(adapter, options=options)
        storage._batcher_row_limit = 1
        if zap:
            # XXX: Some ZODB tests, possibly check4ExtStorageThread and
            # check7StorageThreads don't close storages when done with them?
            # This leads to connections remaining open with locks on PyPy, so on PostgreSQL
            # we can't TRUNCATE tables and have to go the slow route.
            storage.zap_all(slow=True)
        return self._wrap_storage(storage)


class RelStorageTestBase(StorageCreatingMixin,
                         StorageTestBase.StorageTestBase):

    keep_history = None  # Override
    _storage_created = None

    def setUp(self):
        pass
        # Note that we're deliberately NOT calling super's setup.
        # It does stuff on disk, etc, that's not necessary for us
        # and just slows us down by ~10%.
        #super(RelStorageTestBase, self).setUp()

    def tearDown(self):
        transaction.abort()
        # XXX: This could create one! Do subclasses override self._storage?
        storage = self._storage
        if storage is not None:
            self._storage = None
            storage.close()
            storage.cleanup()
        # See comments in setUp.
        #super(RelStorageTestBase, self).tearDown()

    def get_storage(self):
        # Create a storage with default options
        # if it has not been created already.
        storage = self._storage_created
        if storage is None:
            storage = self.make_storage()
            self._storage_created = storage
        return storage

    def set_storage(self, storage):
        self._storage_created = storage

    _storage = property(get_storage, set_storage)

    def open(self, read_only=False):
        # This is used by a few ZODB tests that close and reopen the storage.
        storage = self._storage
        if storage is not None:
            self._storage = None
            storage.close()
            storage.cleanup()
        self._storage = storage = self.make_storage(
            read_only=read_only, zap=False)
        return storage


class GenericRelStorageTests(
        RelStorageTestBase,
        BasicStorage.BasicStorage,
        PackableStorage.PackableStorage,
        Synchronization.SynchronizedStorage,
        ConflictResolution.ConflictResolvingStorage,
        PersistentStorage.PersistentStorage,
        MTStorage.MTStorage,
        ReadOnlyStorage.ReadOnlyStorage,
    ):

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

    def checkResolveConflictBetweenConnections(self):
        # Verify that conflict resolution works between storage instances
        # bound to connections.
        obj = ConflictResolution.PCounter()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        storage1 = self._storage.new_instance()
        storage1.load(oid, '')
        storage2 = self._storage.new_instance()
        storage2.load(oid, '')

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        root_storage = self._storage
        try:
            self._storage = storage1
            _revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
            self._storage = storage2
            _revid3 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

            data, _serialno = self._storage.load(oid, '')
            inst = zodb_unpickle(data)
            self.assertEqual(inst._value, 5)
        finally:
            storage1.close()
            storage2.close()
            self._storage = root_storage

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
        self.assertTrue(oid1 < oid2, 'old OID %r should be less than new OID %r'
                        % (oid1, oid2))

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
                c1._storage._cache.clients_global_first[0].servers,
                ['x:1', 'y:2'])
            r1 = c1.root()
            # The root state and checkpoints should now be cached.
            # A commit count *might* be cached depending on the ZODB version.
            self.assertTrue('zzz:checkpoints' in fakecache.data)
            self.assertEqual(sorted(fakecache.data.keys())[-1][:10],
                             'zzz:state:')
            r1['alpha'] = PersistentMapping()
            transaction.commit()
            self.assertEqual(len(fakecache.data), 4)

            oid = r1['alpha']._p_oid
            c1._storage.load(oid, '')
            # another state should now be cached
            self.assertEqual(len(fakecache.data), 4)

            # make a change
            r1['beta'] = 0
            transaction.commit()
            self.assertEqual(len(fakecache.data), 5)

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
        db = DB(self._storage)
        try:
            c1 = db.open()
            r = c1.root()
            r['alpha'] = 1
            transaction.commit()
            c1.close()

            c1._storage._load_conn.close()
            c1._storage._store_conn.close()
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
        finally:
            db.close()

    def checkAutoReconnectOnSync(self):
        # Verify auto-reconnect.
        db = DB(self._storage)
        try:
            c1 = db.open()
            r = c1.root()

            c1._storage._load_conn.close()
            c1._storage.sync()
            # ZODB5 calls sync when a connection is opened. Our monkey
            # patch on a Connection makes sure that works in earlier
            # versions, but we don't have that patch on ZODB5. So test
            # the storage directly. NOTE: The load connection must be open.
            # to trigger the actual sync.

            r = c1.root()
            r['alpha'] = 1
            transaction.commit()
            c1.close()

            c1._storage._load_conn.close()
            c1._storage._store_conn.close()

            c2 = db.open()
            self.assertIs(c2, c1)

            r = c2.root()
            self.assertEqual(r['alpha'], 1)
            r['beta'] = PersistentMapping()
            c2.add(r['beta'])
            transaction.commit()
            c2.close()
        finally:
            db.close()

    def checkCachePolling(self):
        # NOTE: This test still sets poll_interval, a deprecated
        # option that does nothing. We keep it around to verify that
        # this scenario still works either way.
        self._storage = self.make_storage(
            poll_interval=3600, share_local_cache=False)

        db = DB(self._storage)
        try:
            # Set up the database.
            tm1 = transaction.TransactionManager()
            c1 = db.open(transaction_manager=tm1)
            r1 = c1.root()
            r1['obj'] = obj1 = PersistentMapping({'change': 0})
            tm1.commit()

            # Load and change the object in an independent connection.
            tm2 = transaction.TransactionManager()
            c2 = db.open(transaction_manager=tm2)
            r2 = c2.root()
            r2['obj']['change'] = 1
            tm2.commit()
            # Now c2 has delta_after0.
            self.assertEqual(len(c2._storage._cache.delta_after0), 1)
            c2.close()

            # Change the object in the original connection.
            c1.sync()
            obj1['change'] = 2
            tm1.commit()

            # Close the database connection to c2.
            c2._storage._drop_load_connection()

            # Make the database connection to c2 reopen without polling.
            c2._storage.load(b'\0' * 8, '')
            self.assertTrue(c2._storage._load_transaction_open)

            # Open a connection, which should be the same connection
            # as c2.
            c3 = db.open(transaction_manager=tm2)
            self.assertTrue(c3 is c2)
            self.assertEqual(len(c2._storage._cache.delta_after0), 1)

            # Clear the caches (but not delta_after*)
            c3._resetCache()
            for client in c3._storage._cache.clients_local_first:
                client.flush_all()

            obj3 = c3.root()['obj']
            # Should have loaded the new object.
            self.assertEqual(obj3['change'], 2)

        finally:
            db.close()

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
        # Exercise the code in the pack algorithm that attempts to get the
        # commit lock but will sleep if the lock is busy.
        self._storage = self.make_storage(pack_batch_timeout=0)

        adapter = self._storage._adapter
        test_conn, test_cursor = adapter.connmanager.open()

        slept = []
        def sim_sleep(seconds):
            slept.append(seconds)
            adapter.locker.release_commit_lock(test_cursor)
            test_conn.rollback()
            adapter.connmanager.close(test_conn, test_cursor)

        db = DB(self._storage)
        try:
            # add some data to be packed
            c = db.open()
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
            self._storage.pack(packtime, referencesf, sleep=sim_sleep)

            self.assertTrue(len(slept) > 0)
        finally:
            db.close()

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

            # Choose the pack time
            now = packtime = time.time()
            while packtime <= now:
                time.sleep(0.1)
                packtime = time.time()
            while packtime == time.time():
                time.sleep(0.1)

            extra2.foo = 'bar'
            extra3 = PersistentMapping()
            c.add(extra3)
            transaction.commit()

            self._storage.pack(packtime, referencesf)

            # extra1 should have been garbage collected
            self.assertRaises(KeyError,
                              self._storage.load, extra1._p_oid, '')
            # extra2 and extra3 should both still exist
            self._storage.load(extra2._p_oid, '')
            self._storage.load(extra3._p_oid, '')
        finally:
            db.close()

    def checkPackWhileReferringObjectChanges(self):
        # Packing should not remove objects referenced by an
        # object that changes during packing.
        db = DB(self._storage)
        try:
            # add some data to be packed
            c = db.open()
            root = c.root()
            child = PersistentMapping()
            root['child'] = child
            transaction.commit()
            expect_oids = [child._p_oid]

            def inject_changes():
                # Change the database just after the list of objects
                # to analyze has been determined.
                child2 = PersistentMapping()
                root['child2'] = child2
                transaction.commit()
                expect_oids.append(child2._p_oid)

            adapter = self._storage._adapter
            adapter.packundo.on_filling_object_refs = inject_changes
            packtime = time.time()
            self._storage.pack(packtime, referencesf)

            # "The on_filling_object_refs hook should have been called once")
            self.assertEqual(len(expect_oids), 2, expect_oids)

            # Both children should still exist.
            self._storage.load(expect_oids[0], '')
            self._storage.load(expect_oids[1], '')
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

        import os
        import shutil
        import tempfile
        from ZODB.FileStorage import FileStorage
        db = DB(self._storage)
        try:
            c = db.open()
            r = c.root()
            r['alpha'] = PersistentMapping()
            transaction.commit()

            # To simulate failover to an out of date async slave, take
            # a snapshot of the database at this point, change some
            # object, then restore the database to its earlier state.

            d = tempfile.mkdtemp()
            try:
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

            # Sync, which will call poll_invalidations().
            c.sync()

            # Try to load an object, which should cause ReadConflictError.
            r._p_deactivate()
            self.assertRaises(ReadConflictError, lambda: r['beta'])

        finally:
            db.close()

    def checkBackwardTimeTravelWithRevertWhenStale(self):
        # If revert_when_stale is true, when the database
        # connection is stale (such as through failover to an
        # asynchronous slave that is not fully up to date), the poller
        # should notice that backward time travel has occurred and
        # invalidate all objects that have changed in the interval.
        self._storage = self.make_storage(revert_when_stale=True)

        import os
        import shutil
        import tempfile
        from ZODB.FileStorage import FileStorage
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

    def checkBTreesLengthStress(self):
        # BTrees.Length objects are unusual Persistent objects: they
        # set _p_independent and they frequently invoke conflict
        # resolution. Run a stress test on them.
        updates_per_thread = 50
        thread_count = 4

        from BTrees.Length import Length
        db = DB(self._storage)
        try:
            c = db.open()
            try:
                c.root()['length'] = Length()
                transaction.commit()
            finally:
                c.close()

            def updater():
                thread_db = DB(self._storage)
                for _ in range(updates_per_thread):
                    thread_c = thread_db.open()
                    try:
                        thread_c.root()['length'].change(1)
                        time.sleep(random.random() * 0.05)
                        transaction.commit()
                    finally:
                        thread_c.close()

            import threading
            threads = []
            for _ in range(thread_count):
                t = threading.Thread(target=updater)
                threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join(120)

            c = db.open()
            try:
                self.assertEqual(c.root()['length'](),
                                 updates_per_thread * thread_count)
            finally:
                transaction.abort()
                c.close()

        finally:
            db.close()

from .test_zodbconvert import FSZODBConvertTests

class AbstractRSZodbConvertTests(StorageCreatingMixin,
                                 FSZODBConvertTests):
    keep_history = True
    filestorage_name = 'source'
    relstorage_name = 'destination'
    filestorage_file = None

    def _relstorage_contents(self):
        raise NotImplementedError()

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
        </relstorage>
        </zlibstorage>
        """ % (self.filestorage_name, self.filestorage_file,
               self.relstorage_name, self._relstorage_contents())
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
        return self._closing(self.make_storage(zap=False))

class AbstractRSSrcZodbConvertTests(AbstractRSZodbConvertTests):

    filestorage_name = 'destination'
    relstorage_name = 'source'

    @property
    def filestorage_file(self):
        return self.destfile

    def _create_src_storage(self):
        return self._closing(self.make_storage(zap=False))

class DoubleCommitter(Persistent):
    """A crazy persistent class that changes self in __getstate__"""
    def __getstate__(self):
        if not hasattr(self, 'new_attribute'):
            self.new_attribute = 1 # pylint:disable=attribute-defined-outside-init
        return Persistent.__getstate__(self)
