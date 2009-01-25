##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""A foundation for relstorage adapter tests"""

import itertools
import time
from relstorage.relstorage import RelStorage

from ZODB.DB import DB
from ZODB.utils import p64
from ZODB.FileStorage import FileStorage
from persistent.mapping import PersistentMapping
import transaction

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     IteratorStorage, RevisionStorage, PersistentStorage, \
     MTStorage, ReadOnlyStorage, RecoveryStorage

from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle
from ZODB.serialize import referencesf


class BaseRelStorageTests(StorageTestBase.StorageTestBase):

    def make_adapter(self):
        # abstract method
        raise NotImplementedError

    def open(self, **kwargs):
        adapter = self.make_adapter()
        self._storage = RelStorage(adapter, **kwargs)

    def setUp(self):
        self.open(create=1)
        self._storage.zap_all()

    def tearDown(self):
        self._storage.close()
        self._storage.cleanup()


class RelStorageTests(
    BaseRelStorageTests,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    PackableStorage.PackableStorage,
    PackableStorage.PackableUndoStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    HistoryStorage.HistoryStorage,
    IteratorStorage.IteratorStorage,
    IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage
    ):

    def checkCrossConnectionInvalidation(self):
        # Verify connections see updated state at txn boundaries
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['myobj'] = 'yes'
            c2 = db.open()
            r2 = c2.root()
            self.assert_('myobj' not in r2)

            storage = c1._storage
            t = transaction.Transaction()
            t.description = 'invalidation test'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            self.assert_('myobj' not in r2)
            c2.sync()
            self.assert_('myobj' in r2)
            self.assert_(r2['myobj'] == 'yes')
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
            t.description = 'isolation test 1'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            # The second connection will now load root['alpha'], but due to
            # MVCC, it should continue to see the old state.
            self.assert_(r2['alpha']._p_changed is None)  # A ghost
            self.assert_(not r2['alpha'])
            self.assert_(r2['alpha']._p_changed == 0)

            # make root['alpha'] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assert_(r2['alpha']._p_changed is None)  # A ghost
            self.assert_(r2['alpha'])
            self.assert_(r2['alpha']._p_changed == 0)
            self.assert_(r2['alpha']['beta'] == 'yes')

            # Repeat the test with root['gamma']
            r1['gamma']['delta'] = 'yes'

            storage = c1._storage
            t = transaction.Transaction()
            t.description = 'isolation test 2'
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            # The second connection will now load root[3], but due to MVCC,
            # it should continue to see the old state.
            self.assert_(r2['gamma']._p_changed is None)  # A ghost
            self.assert_(not r2['gamma'])
            self.assert_(r2['gamma']._p_changed == 0)

            # make root[3] visible to the second connection
            c2.sync()

            # Now it should be in sync
            self.assert_(r2['gamma']._p_changed is None)  # A ghost
            self.assert_(r2['gamma'])
            self.assert_(r2['gamma']._p_changed == 0)
            self.assert_(r2['gamma']['delta'] == 'yes')
        finally:
            db.close()

    def checkResolveConflictBetweenConnections(self):
        # Verify that conflict resolution works between storage instances
        # bound to connections.
        obj = ConflictResolution.PCounter()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        storage1 = self._storage.bind_connection(None)
        storage1.load(oid, '')
        storage2 = self._storage.bind_connection(None)
        storage2.load(oid, '')

        obj.inc()
        obj.inc()
        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.
        root_storage = self._storage
        try:
            self._storage = storage1
            revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
            self._storage = storage2
            revid3 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

            data, serialno = self._storage.load(oid, '')
            inst = zodb_unpickle(data)
            self.assertEqual(inst._value, 5)
        finally:
            self._storage = root_storage

    def check16KObject(self):
        # Store 16 * 1024 bytes in an object, then retrieve it
        data = 'a 16 byte string' * 1024
        oid = self._storage.new_oid()
        self._dostoreNP(oid, data=data)
        got, serialno = self._storage.load(oid, '')
        self.assertEqual(len(got), len(data))
        self.assertEqual(got, data)

    def checkPreventOIDOverlap(self):
        # Store an object with a particular OID, then verify that
        # OID is not reused.
        data = 'mydata'
        oid1 = '\0' * 7 + '\x0f'
        self._dostoreNP(oid1, data=data)
        oid2 = self._storage.new_oid()
        self.assert_(oid1 < oid2, 'old OID %r should be less than new OID %r'
            % (oid1, oid2))

    def check16MObject(self):
        # Store 16 * 1024 * 1024 bytes in an object, then retrieve it
        data = 'a 16 byte string' * (1024 * 1024)
        oid = self._storage.new_oid()
        self._dostoreNP(oid, data=data)
        got, serialno = self._storage.load(oid, '')
        self.assertEqual(len(got), len(data))
        self.assertEqual(got, data)

    def checkLoadFromCache(self):
        # Store an object, cache it, then retrieve it from the cache
        self._storage._options.cache_servers = 'x:1 y:2'
        self._storage._options.cache_module_name = 'relstorage.tests.fakecache'

        db = DB(self._storage)
        try:
            c1 = db.open()
            cache = c1._storage._cache_client
            self.assertEqual(cache.servers, ['x:1', 'y:2'])
            self.assertEqual(len(cache.data), 0)
            r1 = c1.root()
            self.assertEqual(len(cache.data), 2)
            r1['alpha'] = PersistentMapping()
            transaction.commit()
            oid = r1['alpha']._p_oid

            self.assertEqual(len(cache.data), 2)
            got, serialno = c1._storage.load(oid, '')
            self.assertEqual(len(cache.data), 4)
            # load the object from the cache
            got, serialno = c1._storage.load(oid, '')
            # try to load an object that doesn't exist
            self.assertRaises(KeyError, c1._storage.load, 'bad.oid.', '')
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

            c2 = db.open()
            self.assert_(c2 is c1)
            r = c2.root()
            self.assertEqual(r['alpha'], 1)
            r['beta'] = 2
            transaction.commit()
            c2.close()
        finally:
            db.close()

    def checkPollInterval(self):
        # Verify the poll_interval parameter causes RelStorage to
        # delay invalidation polling.
        self._storage._options.poll_interval = 3600
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = 1
            transaction.commit()

            c2 = db.open()
            r2 = c2.root()
            self.assertEqual(r2['alpha'], 1)

            r1['alpha'] = 2
            # commit c1 without triggering c2.afterCompletion().
            storage = c1._storage
            t = transaction.Transaction()
            storage.tpc_begin(t)
            c1.commit(t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            # flush invalidations to c2, but the poll timer has not
            # yet expired, so the change to r2 should not be seen yet.
            self.assertTrue(c2._storage._poll_at > 0)
            c2._flush_invalidations()
            r2 = c2.root()
            self.assertEqual(r2['alpha'], 1)

            # expire the poll timer and verify c2 sees the change
            c2._storage._poll_at -= 3601
            c2._flush_invalidations()
            r2 = c2.root()
            self.assertEqual(r2['alpha'], 2)

            transaction.abort()
            c2.close()
            c1.close()

        finally:
            db.close()


    def checkTransactionalUndoIterator(self):
        # this test overrides the broken version in TransactionalUndoStorage.

        s = self._storage

        BATCHES = 4
        OBJECTS = 4

        orig = []
        for i in range(BATCHES):
            t = transaction.Transaction()
            tid = p64(i + 1)
            s.tpc_begin(t, tid)
            txn_orig = []
            for j in range(OBJECTS):
                oid = s.new_oid()
                obj = MinPO(i * OBJECTS + j)
                revid = s.store(oid, None, zodb_pickle(obj), '', t)
                txn_orig.append((tid, oid, revid))
            serials = s.tpc_vote(t)
            if not serials:
                orig.extend(txn_orig)
            else:
                # The storage provided revision IDs after the vote
                serials = dict(serials)
                for tid, oid, revid in txn_orig:
                    self.assertEqual(revid, None)
                    orig.append((tid, oid, serials[oid]))
            s.tpc_finish(t)

        i = 0
        for tid, oid, revid in orig:
            self._dostore(oid, revid=revid, data=MinPO(revid),
                          description="update %s" % i)

        # Undo the OBJECTS transactions that modified objects created
        # in the ith original transaction.

        def undo(i):
            info = s.undoInfo()
            t = transaction.Transaction()
            s.tpc_begin(t)
            base = i * OBJECTS + i
            for j in range(OBJECTS):
                tid = info[base + j]['id']
                s.undo(tid, t)
            s.tpc_vote(t)
            s.tpc_finish(t)

        for i in range(BATCHES):
            undo(i)

        # There are now (2 + OBJECTS) * BATCHES transactions:
        #     BATCHES original transactions, followed by
        #     OBJECTS * BATCHES modifications, followed by
        #     BATCHES undos

        iter = s.iterator()
        offset = 0

        eq = self.assertEqual

        for i in range(BATCHES):
            txn = iter[offset]
            offset += 1

            tid = p64(i + 1)
            eq(txn.tid, tid)

            L1 = [(rec.oid, rec.tid, rec.data_txn) for rec in txn]
            L2 = [(oid, revid, None) for _tid, oid, revid in orig
                  if _tid == tid]

            eq(L1, L2)

        for i in range(BATCHES * OBJECTS):
            txn = iter[offset]
            offset += 1
            eq(len([rec for rec in txn if rec.data_txn is None]), 1)

        for i in range(BATCHES):
            txn = iter[offset]
            offset += 1

            # The undos are performed in reverse order.
            otid = p64(BATCHES - i)
            L1 = [rec.oid for rec in txn]
            L2 = [oid for _tid, oid, revid in orig if _tid == otid]
            L1.sort()
            L2.sort()
            eq(L1, L2)

        self.assertRaises(IndexError, iter.__getitem__, offset)

    def checkNonASCIITransactionMetadata(self):
        # Verify the database stores and retrieves non-ASCII text
        # in transaction metadata.
        ugly_string = ''.join(chr(c) for c in range(256))

        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = 1
            transaction.get().setUser(ugly_string)
            transaction.commit()
            r1['alpha'] = 2
            transaction.get().note(ugly_string)
            transaction.commit()

            info = self._storage.undoInfo()
            self.assertEqual(info[0]['description'], ugly_string)
            self.assertEqual(info[1]['user_name'], '/ ' + ugly_string)
        finally:
            db.close()

    def checkPackGC(self, gc_enabled=True):
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['alpha'] = PersistentMapping()
            transaction.commit()

            oid = r1['alpha']._p_oid
            r1['alpha'] = None
            transaction.commit()

            # The object should still exist
            self._storage.load(oid, '')

            # Pack
            now = packtime = time.time()
            while packtime <= now:
                packtime = time.time()
            self._storage.pack(packtime, referencesf)

            if gc_enabled:
                # The object should now be gone
                self.assertRaises(KeyError, self._storage.load, oid, '')
            else:
                # The object should still exist
                self._storage.load(oid, '')
        finally:
            db.close()

    def checkPackGCDisabled(self):
        self._storage._options.pack_gc = False
        self.checkPackGC(gc_enabled=False)

    def checkPackOldUnreferenced(self):
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['A'] = PersistentMapping()
            B = PersistentMapping()
            r1['A']['B'] = B
            transaction.get().note('add A then add B to A')
            transaction.commit()

            del r1['A']['B']
            transaction.get().note('remove B from A')
            transaction.commit()

            r1['A']['C'] = ''
            transaction.get().note('add C to A')
            transaction.commit()

            now = packtime = time.time()
            while packtime <= now:
                packtime = time.time()
            self._storage.pack(packtime, referencesf)

            # B should be gone, since nothing refers to it.
            self.assertRaises(KeyError, self._storage.load, B._p_oid, '')

        finally:
            db.close()
        


class IteratorDeepCompareUnordered:
    # Like IteratorDeepCompare, but compensates for OID order
    # differences in transactions.
    def compare(self, storage1, storage2):
        eq = self.assertEqual
        iter1 = storage1.iterator()
        iter2 = storage2.iterator()
        for txn1, txn2 in itertools.izip(iter1, iter2):
            eq(txn1.tid,         txn2.tid)
            eq(txn1.status,      txn2.status)
            eq(txn1.user,        txn2.user)
            eq(txn1.description, txn2.description)

            missing = object()
            e1 = getattr(txn1, 'extension', missing)
            if e1 is missing:
                # old attribute name
                e1 = txn1._extension
            e2 = getattr(txn2, 'extension', missing)
            if e2 is missing:
                # old attribute name
                e2 = txn2._extension
            eq(e1, e2)

            recs1 = [(r.oid, r) for r in txn1]
            recs1.sort()
            recs2 = [(r.oid, r) for r in txn2]
            recs2.sort()
            eq(len(recs1), len(recs2))
            for (oid1, rec1), (oid2, rec2) in zip(recs1, recs2):
                eq(rec1.oid,     rec2.oid)
                eq(rec1.tid,  rec2.tid)
                eq(rec1.version, rec2.version)
                eq(rec1.data,    rec2.data)
        # Make sure ther are no more records left in txn1 and txn2, meaning
        # they were the same length
        self.assertRaises(IndexError, iter1.next)
        self.assertRaises(IndexError, iter2.next)
        iter1.close()
        iter2.close()



class RecoveryStorageSubset(IteratorDeepCompareUnordered):
    # The subset of RecoveryStorage tests that do not rely on version
    # support.
    pass

for name, attr in RecoveryStorage.RecoveryStorage.__dict__.items():
    if 'check' in name and 'Version' not in name:
        setattr(RecoveryStorageSubset, name, attr)


class ToFileStorage(BaseRelStorageTests, RecoveryStorageSubset):
    def setUp(self):
        self.open(create=1)
        self._storage.zap_all()
        self._dst = FileStorage("Dest.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return FileStorage('Dest.fs')


class FromFileStorage(BaseRelStorageTests, RecoveryStorageSubset):
    def setUp(self):
        self.open(create=1)
        self._storage.zap_all()
        self._dst = self._storage
        self._storage = FileStorage("Source.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return self._dst
