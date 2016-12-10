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
"""A foundation for history-preserving RelStorage tests"""

from persistent.mapping import PersistentMapping
from relstorage.tests.RecoveryStorage import UndoableRecoveryStorage
from relstorage.tests.reltestbase import GenericRelStorageTests
from relstorage.tests.reltestbase import RelStorageTestBase

from ZODB.DB import DB
from ZODB.FileStorage import FileStorage
from ZODB.serialize import referencesf
from ZODB.tests import HistoryStorage
from ZODB.tests import IteratorStorage
from ZODB.tests import PackableStorage
from ZODB.tests import RevisionStorage
from ZODB.tests import TransactionalUndoStorage
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.utils import p64
import time
import transaction
import unittest


class HistoryPreservingRelStorageTests(GenericRelStorageTests,
                                       TransactionalUndoStorage.TransactionalUndoStorage,
                                       IteratorStorage.IteratorStorage,
                                       IteratorStorage.ExtendedIteratorStorage,
                                       RevisionStorage.RevisionStorage,
                                       PackableStorage.PackableUndoStorage,
                                       HistoryStorage.HistoryStorage):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals
    keep_history = True

    def checkUndoMultipleConflictResolution(self, *_args, **_kwargs):
        # 4.2.3 and above add this. it's an exotic feature according to jimfulton.
        raise unittest.SkipTest("conflict-resolving undo not supported")

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
            for j in range(OBJECTS):
                oid = s.new_oid()
                obj = MinPO(i * OBJECTS + j)
                s.store(oid, None, zodb_pickle(obj), '', t)
                orig.append((tid, oid))
            s.tpc_vote(t)
            s.tpc_finish(t)

        orig = [(tid, oid, s.getTid(oid)) for tid, oid in orig]

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
        if isinstance(ugly_string, bytes):
            # Always text. Use latin 1 because it can decode any arbitrary
            # bytes.
            ugly_string = ugly_string.decode('latin-1') # pylint:disable=redefined-variable-type

        # The storage layer is defined to take bytes (implicitly in
        # older ZODB releases, explicitly in ZODB 5.something), but historically
        # it can accept either text or bytes. However, it always returns bytes
        check_string = ugly_string.encode("utf-8")

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
            self.assertEqual(info[0]['description'], check_string)
            self.assertEqual(info[1]['user_name'], b'/ ' + check_string)
        finally:
            db.close()

    def checkPackGC(self, expect_object_deleted=True, close=True):
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
            self._storage.sync()

            if expect_object_deleted:
                # The object should now be gone
                self.assertRaises(KeyError, self._storage.load, oid, '')
            else:
                # The object should still exist
                self._storage.load(oid, '')
        finally:
            if close:
                db.close()
        return oid

    def checkPackGCDisabled(self):
        self._storage = self.make_storage(pack_gc=False)
        self.checkPackGC(expect_object_deleted=False)

    def checkPackGCPrePackOnly(self):
        self._storage = self.make_storage(pack_prepack_only=True)
        self.checkPackGC(expect_object_deleted=False)

    def checkPackGCReusePrePackData(self):
        self._storage = self.make_storage(pack_prepack_only=True)
        oid = self.checkPackGC(expect_object_deleted=False, close=False)
        # We now have pre-pack analysis data
        self._storage._options.pack_prepack_only = False
        self._storage.pack(0, referencesf, skip_prepack=True)
        # The object should now be gone
        self.assertRaises(KeyError, self._storage.load, oid, '')
        self._storage.close()

    def checkPackOldUnreferenced(self):
        db = DB(self._storage)
        try:
            c1 = db.open()
            r1 = c1.root()
            r1['A'] = PersistentMapping()
            B = PersistentMapping()
            r1['A']['B'] = B
            transaction.get().note(u'add A then add B to A')
            transaction.commit()

            del r1['A']['B']
            transaction.get().note(u'remove B from A')
            transaction.commit()

            r1['A']['C'] = ''
            transaction.get().note(u'add C to A')
            transaction.commit()

            now = packtime = time.time()
            while packtime <= now:
                packtime = time.time()
            self._storage.pack(packtime, referencesf)

            # B should be gone, since nothing refers to it.
            self.assertRaises(KeyError, self._storage.load, B._p_oid, '')

        finally:
            db.close()

    def checkHistoricalConnection(self):
        import datetime
        import persistent
        import ZODB.POSException
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()

        root['first'] = persistent.mapping.PersistentMapping(count=0)
        transaction.commit()

        time.sleep(.02)
        now = datetime.datetime.utcnow()
        time.sleep(.02)

        root['second'] = persistent.mapping.PersistentMapping()
        root['first']['count'] += 1
        transaction.commit()

        transaction1 = transaction.TransactionManager()

        historical_conn = db.open(transaction_manager=transaction1, at=now)

        eq = self.assertEqual

        # regular connection sees present:

        eq(sorted(conn.root().keys()), ['first', 'second'])
        eq(conn.root()['first']['count'], 1)

        # historical connection sees past:

        eq(sorted(historical_conn.root().keys()), ['first'])
        eq(historical_conn.root()['first']['count'], 0)

        # Can't change history:

        historical_conn.root()['first']['count'] += 1
        eq(historical_conn.root()['first']['count'], 1)
        self.assertRaises(ZODB.POSException.ReadOnlyHistoryError,
                          transaction1.commit)
        transaction1.abort()
        eq(historical_conn.root()['first']['count'], 0)

        historical_conn.close()
        conn.close()
        db.close()

    def checkImplementsExternalGC(self):
        import ZODB.interfaces
        self.assertFalse(ZODB.interfaces.IExternalGC.providedBy(self._storage))
        self.assertRaises(AttributeError, self._storage.deleteObject)

class HistoryPreservingToFileStorage(RelStorageTestBase,
                                     UndoableRecoveryStorage):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals
    keep_history = True

    def setUp(self):
        self._storage = self.make_storage()
        self._dst = FileStorage("Dest.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return FileStorage('Dest.fs')


class HistoryPreservingFromFileStorage(RelStorageTestBase,
                                       UndoableRecoveryStorage):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals
    keep_history = True

    def setUp(self):
        self._dst = self.make_storage()
        self._storage = FileStorage("Source.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return self._dst
