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
import unittest

import transaction
from persistent.mapping import PersistentMapping

from ZODB.Connection import TransactionMetaData
from ZODB.DB import DB
from ZODB.serialize import referencesf
from ZODB.POSException import POSKeyError
from ZODB.tests import HistoryStorage
from ZODB.tests import IteratorStorage
from ZODB.tests import PackableStorage
from ZODB.tests import RevisionStorage
from ZODB.tests import TransactionalUndoStorage
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
# This class is sadly not cooperative with its superclass,
# so we need to explicitly place it at the back of the MRO.
from ZODB.tests.util import TestCase as ZODBTestCase

from ZODB.utils import p64

from relstorage.storage import bytes8_to_int64
from relstorage.tests.RecoveryStorage import UndoableRecoveryStorage
from relstorage.tests.reltestbase import GenericRelStorageTests
from relstorage.tests.reltestbase import AbstractFromFileStorage
from relstorage.tests.reltestbase import AbstractToFileStorage

# Prior to https://github.com/zopefoundation/ZODB/pull/281, ZODB's
# tests had some previous transaction ids as native strings, not
# bytes. Correct that.
RevisionStorage.ZERO = b'\0' * 8
TransactionalUndoStorage.ZERO = b'\0' * 8

class HistoryPreservingRelStorageTests(GenericRelStorageTests,
                                       TransactionalUndoStorage.TransactionalUndoStorage,
                                       IteratorStorage.IteratorStorage,
                                       IteratorStorage.ExtendedIteratorStorage,
                                       RevisionStorage.RevisionStorage,
                                       PackableStorage.PackableUndoStorage,
                                       HistoryStorage.HistoryStorage,
                                       ZODBTestCase):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals,too-many-public-methods
    keep_history = True

    def checkUndoMultipleConflictResolution(self, *_args, **_kwargs):
        # pylint:disable=arguments-differ
        # 4.2.3 and above add this. it's an exotic feature according to jimfulton.
        raise unittest.SkipTest("conflict-resolving undo not supported")

    def checkIterationIntraTransaction(self):
        # XXX: This test overrides the broken version from
        # IteratorStorage; prior to
        # https://github.com/zopefoundation/ZODB/pull/281 it passed a
        # native str, not bytes, as the previous tid.
        oid = self._storage.new_oid()
        t = TransactionMetaData()
        data = zodb_pickle(MinPO(0))
        try:
            self._storage.tpc_begin(t)
            self._storage.store(oid, RevisionStorage.ZERO, data, '', t)
            self._storage.tpc_vote(t)
            # Don't do tpc_finish yet
            it = self._storage.iterator()
            for x in it:
                self.assertIsNotNone(x)
        finally:
            self._storage.tpc_finish(t)

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

        # pylint:disable=unnecessary-comprehension
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
            ugly_string = ugly_string.decode('latin-1')

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
            __traceback_info__ = info
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
            self._storage.pack(self._storage.lastTransactionInt(), referencesf)
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
            A_B = PersistentMapping()
            r1['A']['B'] = A_B
            transaction.get().note(u'add A then add B to A')
            transaction.commit()

            del r1['A']['B']
            transaction.get().note(u'remove B from A')
            transaction.commit()

            r1['A']['C'] = ''
            transaction.get().note(u'add C (non-persistent) to A')
            transaction.commit()

            packtime = c1._storage.lastTransactionInt()
            self._storage.pack(packtime, referencesf)

            # B should be gone, since nothing refers to it.
            with self.assertRaises(KeyError):
                __traceback_info__ = bytes8_to_int64(A_B._p_oid)
                self._storage.load(A_B._p_oid)

        finally:
            db.close()

    def checkHistoricalConnection(self):
        import persistent
        import ZODB.POSException
        db = DB(self._storage)
        conn = db.open()
        root = conn.root()

        root['first'] = persistent.mapping.PersistentMapping(count=0)
        transaction.commit()

        time_of_first_transaction = conn._storage.lastTransaction()

        root['second'] = persistent.mapping.PersistentMapping()
        root['first']['count'] += 1
        transaction.commit()

        transaction1 = transaction.TransactionManager(explicit=True)

        historical_conn = db.open(transaction_manager=transaction1, at=time_of_first_transaction)

        eq = self.assertEqual

        # regular connection sees present:

        eq(sorted(conn.root().keys()), ['first', 'second'])
        eq(conn.root()['first']['count'], 1)

        # historical connection sees past:
        transaction1.begin()
        eq(sorted(historical_conn.root().keys()), ['first'])
        eq(historical_conn.root()['first']['count'], 0)

        # Can't change history:

        historical_conn.root()['first']['count'] += 1
        eq(historical_conn.root()['first']['count'], 1)
        self.assertRaises(ZODB.POSException.ReadOnlyHistoryError,
                          transaction1.commit)
        transaction1.abort()
        eq(historical_conn.root()['first']['count'], 0)

        # Making a change in the present
        root['third'] = 3
        transaction.commit()

        # Is also not reflected in the past, even after explicit sync,
        transaction1.begin()
        eq(sorted(historical_conn.root().keys()), ['first'])
        eq(historical_conn.root()['first']['count'], 0)
        # Since we cannot change anything, we cannot join a transaction either.
        # The afterCompletion call is never invoked.
        historical_conn._storage._storage.afterCompletion = lambda: self.fail("Not called")
        transaction1.commit()


        historical_conn.close()
        conn.close()
        db.close()

    def checkMigrateTransactionEmpty(self):
        # The transaction.empty column gets renamed in 'prepare'
        adapter = self._storage._adapter
        schema = adapter.schema
        # First, we have to flip it back to the old name, since we installed
        # with the name we want.
        stmt = schema._rename_transaction_empty_stmt
        # ALTER TABLE transaction RENAME empty TO is_empty
        # or
        # ALTER TABLE transaction CHANGE empty is_empty
        stmt = stmt.replace('is_empty', 'FOOBAR')
        stmt = stmt.replace('empty', 'is_empty')
        stmt = stmt.replace("FOOBAR", 'empty')

        test_conn, test_cursor = adapter.connmanager.open()
        try:

            try:
                test_cursor.execute(stmt)
            except Exception as e:
                # XXX: This should be more strict. We really just
                # want to catch the db-api specific ProgrammingError,
                # and only on MySQL 8.0+. But we don't have a good way to do that.
                raise unittest.SkipTest(str(e))

            self.assertTrue(schema._needs_transaction_empty_update(test_cursor))

            schema.update_schema(test_cursor, None)
            self.assertFalse(schema._needs_transaction_empty_update(test_cursor))
        finally:
            adapter.connmanager.close(test_conn, test_cursor)

    def __setup_checkImplementsIExternalGC(self):
        from zope.interface.verify import verifyObject
        import ZODB.interfaces
        verifyObject(ZODB.interfaces.IExternalGC, self._storage)

        # Now do it.
        # We need to create a few different revisions of an object
        # so that we can selectively remove old versions and check that
        # when we remove the final version, the whole thing goes away.
        db = self._closing(ZODB.DB(self._storage))
        conn = self._closing(db.open())
        root = conn.root()
        root['key'] = PersistentMapping()
        transaction.commit()

        for i in range(5):
            tx = transaction.begin()
            tx.description = u'Revision %s' % i
            root['key']['item'] = i
            transaction.commit()

        obj_oid = root['key']._p_oid

        return db, conn, obj_oid

    def checkImplementsIExternalGC(self):
        db, conn, obj_oid = self.__setup_checkImplementsIExternalGC()

        storage = conn._storage
        history = storage.history(obj_oid, size=100)
        self.assertEqual(6, len(history))
        latest_tid = history[0]['tid']
        # We can delete the latest TID for the OID, and the whole
        # object goes away on a pack.
        t = TransactionMetaData()
        storage.tpc_begin(t)
        count = storage.deleteObject(obj_oid, latest_tid, t)
        self.assertEqual(count, 1)
        # Doing it again will do nothing because it's already
        # gone.
        count = storage.deleteObject(obj_oid, latest_tid, t)
        invalidations = storage.tpc_vote(t)
        storage.tpc_finish(t)

        # It is detected as invalidated during voting
        self.assertEqual([obj_oid], list(invalidations))

        # And not at the next poll.
        invalidations = storage.poll_invalidations()
        self.assertEqual([], list(invalidations))

        # Getting the most recent fails.
        with self.assertRaises(POSKeyError):
            storage.load(obj_oid)

        # But we can load a state before then.
        state = storage.loadSerial(obj_oid, history[1]['tid'])
        self.assertEqual(len(state), history[1]['size'])

        # Length hasn't changed
        storage._adapter.stats.large_database_change()
        self.assertEqual(len(storage), 2)

        # History records it as a deletion with size 0
        history_after = storage.history(obj_oid)
        self.assertEqual(0, history_after[0]['size'])

        # Now if we proceed to pack it, *without* doing a GC...
        from relstorage.storage.pack import Pack
        options = storage._options.copy(pack_gc=False)
        self.assertFalse(options.pack_gc)
        packer = Pack(options, storage._adapter, storage.blobhelper, storage._cache)
        self.assertFalse(packer.options.pack_gc)
        packer.pack(storage.lastTransactionInt(), referencesf)

        # ... and bring the storage into the current view...
        storage.sync()

        # ...then the object is gone in all revisions...
        with self.assertRaises(POSKeyError):
            storage.load(obj_oid)

        for history_item in history:
            tid = history_item['tid']
            with self.assertRaises(POSKeyError):
                storage.loadSerial(obj_oid, tid)

        # ...and the size is smaller.
        self.assertEqual(len(storage), 1)
        conn.close()
        db.close()

    ###
    # Fixes for tests that assume the current clock
    # and the TID clock are the same.
    ###

    def __tid_clock_needs_care(self):
        adapter = self._storage._adapter
        return getattr(adapter, 'RS_TEST_TXN_PACK_NEEDS_SLEEP', False)

    def __maybe_ignore_monotonic(self, cls, method_name):
        if not self.__tid_clock_needs_care():
            return getattr(super(HistoryPreservingRelStorageTests, self), method_name)()

        # Override one from RevisionStorage to go back to actually sleeping,
        # since our TID clock is external now.
        unbound = getattr(cls, method_name)
        if hasattr(unbound, 'im_func'):
            # We're on python 2. There's no __wrapped__ to give us access to the raw
            # function, we have to dig it out by hand. Our only option is the closure.
            function_wrapper = unbound.im_func
            unbound = function_wrapper.__closure__[0].cell_contents
        else:
            unbound = unbound.__wrapped__ # pylint:disable=no-member

        bound = lambda: unbound(self)
        self.__never_snoozing(bound)

    def __never_snoozing(self, method):
        def never_snooze():
            return

        before_snooze = RevisionStorage.snooze
        assert PackableStorage.snooze == RevisionStorage.snooze
        RevisionStorage.snooze = never_snooze
        PackableStorage.snooze = never_snooze
        try:
            return method()
        finally:
            RevisionStorage.snooze = before_snooze
            PackableStorage.snooze = before_snooze

    def checkLoadBefore(self):
        # Most of the time this works, but sometimes it fails an internal assertion,
        # most commonly seen on AppVeyor.
        # https://ci.appveyor.com/project/jamadden/relstorage/builds/26243441/job/p24ocr2ir6wpvg3v#L1087
        raise unittest.SkipTest("Assumes it can control timestamps")

    def checkLoadBeforeOld(self):
        self.__maybe_ignore_monotonic(RevisionStorage.RevisionStorage,
                                      'checkLoadBeforeOld')

    def checkSimpleHistory(self):
        if not self.__tid_clock_needs_care():
            return super(HistoryPreservingRelStorageTests, self).checkSimpleHistory()
        # This assumes that the `time` value in the storage.history()
        # for an object always increases, even though there are 8-byte TID values
        # that, while themselves increasing, round down to equal floating point
        # time values. For example, these two values are in the proper sequence:
        #   b'\x03\xd1K\xc6-\xf33!' == 275084366792831777,
        # and
        #   b'\x03\xd1K\xc6-\xf33"' == 275084366792831778,
        # But both have
        #   TimeStamp(tid).timeTime() == 1563997810.769531.
        #
        # This test tries to do something about that (delaying between transactions to let
        # time.time() move forward), but only on Windows, and it turns out even if we
        # let it sleep it doesn't actually help things 100% of the time (99% yes, but not
        # 100%), so we took that out. See below.

        # Even wheen sleeping, we've seen a very small number of apparent genuine failures
        # (Python 3.5 on Travis), that of reporting
        #
        #      HistoryStorage.py", line 54, in _checkHistory
        #        self.assertLess(a, b)
        #    AssertionError:  1564151039.9015017 not less than 1564150979.908543
        #
        # Which is true, it isn't. In fact, there's a difference of
        # 59.99s between the two stamps (all the errors have shown that same amount).
        #
        # - 1564151039.9015017 -> '2019-07-26 14:23:59.901502' -> 275095335361341917
        # - 1564150979.9085430 -> '2019-07-26 14:22:59.908543' -> 275095331066878668
        #
        # And
        # - a -> \x03\xd1U\xbf\xff\x94i\xdd    -> [3, 209, 85, 191, 255, 148, 105, 221]
        # - b -> \x03\xd1U\xbe\xff\x9c\x1a\xcc -> [3, 209, 85, 190, 255, 156,  26, 204]
        #
        # Which makes very little sense:
        #
        # - The DB query orders by TID, DESC;
        # - We iterate those in that same order.
        #
        # But the very first comparison is against the local value of
        # time.time(); time.time() isn't guaranteed to be monotonic
        # increasing and the docs specifically say it can move
        # backwards, so perhaps it did?
        #
        # I was able to catch an instance of this locally running
        # ``ztest --layer My -t checkSimpleHistory --repeat 15`` and
        # indeed, the failure was against the local time.time() value
        # on the first comparison.
        #
        # The local time was              1564152719.998203
        # The DB TID (in the future) was  275095459921661457 -> '\x03\xd1U\xdc\xff\xf4\x0e\x11' ->
        #                                 1564152779.9890637
        # Note the DB TID is one of those ambiguous ones, plugging the unix timestamp
        # back into the process we get:
        #
        #   '\x03\xd1U\xdc\xff\xf4\x0e\x00' -> 275095459921661440
        #
        # The previous TID in the database was
        #   '\x03\xd1U\xdc\xff\xecD\xaa'    -> 275095459921151146
        #
        # Which is a much greater difference than would be needed (i.e., we didn't have to add 1 to
        # get to the new tid).
        #
        # So the only conclusion can be that we just can't compare
        # local time directly to database time.
        #
        # We fix this by disabling 'assertLess'; the test contains
        # calls to assertEqual() on the TIDs, which makes much more
        # sense anyway, so we don't lose much.

        self.assertLess = lambda *args: None

        try:
            super(HistoryPreservingRelStorageTests, self).checkSimpleHistory()
        finally:
            del self.assertLess

    def _run_with_storage_packing_at_packtime(self,
                                              methname,
                                              find_packtime):
        # Ignore the pack timestamp given. Execute `find_packtime(storage)`
        # instead and use that.
        meth = getattr(super(HistoryPreservingRelStorageTests, self), methname)
        if not self.__tid_clock_needs_care():
            return meth()

        orig_pack = self._storage.pack
        def pack(_ignored_packtime, ref_getter):
            packtime = find_packtime(self._storage)
            orig_pack(packtime, ref_getter)
        self._storage.pack = pack

        try:
            return self.__never_snoozing(meth)
        finally:
            del self._storage.pack
            orig_pack = None

    def _run_packing_to_latest(self, methname):
        # Ignore the pack timestamp given. The test is trying to pack to "now", but
        # because of how fast we commit, that doesn't always work out correctly.
        # Instead, use the actual most recent tid.
        return self._run_with_storage_packing_at_packtime(
            methname,
            find_packtime=lambda s: s.lastTransactionInt())

    def checkPackJustOldRevisions(self):
        self._run_packing_to_latest('checkPackJustOldRevisions')

    def checkPackAllRevisions(self):
        self._run_packing_to_latest('checkPackAllRevisions')

    def checkPackOnlyOneObject(self):
        self._run_packing_to_latest('checkPackOnlyOneObject')

    def checkPackUndoLog(self):
        packafter = []
        orig_dostore = self._dostoreNP
        def dostorenp(*args, **kwargs):
            result = orig_dostore(*args, **kwargs)
            if not packafter:
                packafter.append(self._storage.lastTransactionInt())
            return result
        self._dostoreNP = dostorenp

        def find_packtime(_storage):
            assert len(packafter) == 1, packafter
            return packafter[0]

        try:
            self._run_with_storage_packing_at_packtime('checkPackUndoLog',
                                                       find_packtime)
        finally:
            del self._dostoreNP



    def __pack_after_nth_commit_on_nth_child_instance(self, meth_name,
                                                      commit_number, child_number=0):
        # Use child_number = 0 to specify self._storage.
        # Currently only support that or the first child
        assert child_number in (0, 1)

        commit_tids = []
        def watching_tpc_finish(inst, orig_finish, *args, **kwargs):
            tid = orig_finish(*args, **kwargs)
            commit_tids.append(tid)
            if len(commit_tids) == commit_number:
                del inst.tpc_finish
            return tid

        s = self._storage
        if child_number == 1:
            orig_new_instance = s.new_instance
            def new_instance():
                inst = orig_new_instance()
                orig_finish = inst.tpc_finish
                inst.tpc_finish = lambda *args, **kw: watching_tpc_finish(inst, orig_finish,
                                                                          *args, **kw)
                return inst
            s.new_instance = new_instance
        else:
            orig_finish = s.tpc_finish
            s.tpc_finish = lambda *args, **kw: watching_tpc_finish(s, orig_finish, *args, **kw)

        def find_packtime(_storage):
            assert len(commit_tids) == commit_number
            return bytes8_to_int64(commit_tids[commit_number - 1])

        try:
            self._run_with_storage_packing_at_packtime(meth_name, find_packtime)
        finally:
            # Break cycles
            s.new_instance = None
            s = None

    def __pack_after_first_commit_on_first_child_instance(self, meth_name):
        return self.__pack_after_nth_commit_on_nth_child_instance(
            meth_name,
            1,
            1
        )

    def checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot(self):
        self.__pack_after_first_commit_on_first_child_instance(
            'checkTransactionalUndoAfterPackWithObjectUnlinkFromRoot'
        )

    def checkTransactionalUndoAfterPack(self):
        self.__pack_after_nth_commit_on_nth_child_instance(
            'checkTransactionalUndoAfterPack',
            2,
            0
        )

    def checkPackUnlinkedFromRoot(self):
        self.__pack_after_first_commit_on_first_child_instance('checkPackUnlinkedFromRoot')

class HistoryPreservingToFileStorage(AbstractToFileStorage,
                                     UndoableRecoveryStorage,
                                     ZODBTestCase):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals
    keep_history = True


class HistoryPreservingFromFileStorage(AbstractFromFileStorage,
                                       UndoableRecoveryStorage,
                                       ZODBTestCase):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals
    keep_history = True
