##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""More recovery and iterator tests."""

# This is copied from ZODB.tests.RecoveryStorage and expanded to fit
# history-free storages.
# pylint:disable=no-member,too-many-locals

import itertools
import time

import transaction
import ZODB.POSException
from ZODB import DB
from ZODB.Connection import TransactionMetaData as Transaction
from ZODB.blob import is_blob_record
from ZODB.serialize import referencesf
from ZODB.tests.StorageTestBase import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle

from relstorage.interfaces import IRelStorage
from relstorage._compat import PY3
from relstorage._compat import iteritems

if not PY3:
    zip = itertools.izip

class IteratorDeepCompare(object):

    def compare(self, src, dest):
        # override this for storages that truncate on restore (because
        # they do not store history).
        self.compare_exact(src, dest)

    def compare_exact(self, storage1, storage2):
        """Confirm that storage1 and storage2 contain equivalent data"""
        eq = self.assertEqual
        missing = object()
        # len(storage) is only required to be approximate and for "informational purposes."
        # So we cannot use it as a hard check.
        # self.assertEqual(len(storage1), len(storage2))
        iter1 = self._closing(storage1.iterator())
        iter2 = self._closing(storage2.iterator())
        for txn1, txn2 in zip(iter1, iter2):
            eq(txn1.tid, txn2.tid)
            eq(txn1.status, txn2.status)
            eq(txn1.user, txn2.user)
            eq(txn1.description, txn2.description)

            # b/w compat on the 'extension' attribute
            e1 = getattr(txn1, 'extension', missing)
            if e1 is missing:
                # old attribute name
                e1 = txn1._extension
            e2 = getattr(txn2, 'extension', missing)
            if e2 is missing:
                # old attribute name
                e2 = txn2._extension
            eq(e1, e2)

            # compare the objects in the transaction, but disregard
            # the order of the objects and any duplicated records
            # since those are not important.
            recs1 = {r.oid: r for r in txn1}
            recs2 = {r.oid: r for r in txn1}
            eq(len(recs1), len(recs2))
            recs1 = sorted(iteritems(recs1))
            recs2 = sorted(iteritems(recs2))
            recs2.sort()
            for (_oid1, rec1), (_oid2, rec2) in zip(recs1, recs2):
                eq(rec1.oid, rec2.oid)
                eq(rec1.tid, rec2.tid)
                eq(rec1.data, rec2.data)
                if is_blob_record(rec1.data):
                    try:
                        fn1 = storage1.loadBlob(rec1.oid, rec1.tid)
                    except ZODB.POSException.POSKeyError:
                        self.assertRaises(
                            ZODB.POSException.POSKeyError,
                            storage2.loadBlob, rec1.oid, rec1.tid)
                    else:
                        fn2 = storage2.loadBlob(rec1.oid, rec1.tid)
                        self.assertNotEqual(fn1, fn2)
                        with open(fn1, 'rb') as f1, open(fn2, 'rb') as f2:
                            eq(f1.read(), f2.read())

        # Make sure there are no more records left in txn1 and txn2, meaning
        # they were the same length
        try:
            next(iter1)
        except (IndexError, StopIteration):
            pass
        else:
            self.fail("storage1 has more records")

        try:
            __traceback_info__ = next(iter2)
        except (IndexError, StopIteration):
            pass
        else:
            self.fail("storage2 has more records")

        iter1.close()
        iter2.close()

    def compare_truncated(self, src, dest):
        """Confirm that dest is a truncated copy of src.

        The copy process should have dropped all old revisions of objects
        in src.  Also note that the dest does not retain transaction
        metadata.
        """

        src_objects = {}  # {oid: (tid, data, blob or None)}
        for txn in src.iterator():
            for rec in txn:
                if is_blob_record(rec.data):
                    try:
                        fn = src.loadBlob(rec.oid, rec.tid)
                    except ZODB.POSException.POSKeyError:
                        blob = None
                    else:
                        blob = open(fn, 'rb').read()
                else:
                    blob = None
                src_objects[rec.oid] = (rec.tid, rec.data, blob)

        unchecked = set(src_objects)
        for txn in dest.iterator():
            for rec in txn:
                if is_blob_record(rec.data):
                    try:
                        fn = dest.loadBlob(rec.oid, rec.tid)
                    except ZODB.POSException.POSKeyError:
                        blob = None
                    else:
                        blob = open(fn, 'rb').read()
                else:
                    blob = None
                dst_object = (rec.tid, rec.data, blob)
                src_object = src_objects[rec.oid]
                self.assertEqual(src_object, dst_object)
                unchecked.remove(rec.oid)

        self.assertEqual(len(unchecked), 0)


class BasicRecoveryStorage(IteratorDeepCompare):

    # Requires a setUp() that creates a self._dst destination storage
    def checkSimpleRecovery(self):
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=11)
        revid = self._dostore(oid, revid=revid, data=12)
        revid = self._dostore(oid, revid=revid, data=13)
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)

    def checkPackWithGCOnDestinationAfterRestore(self):
        raises = self.assertRaises
        closing = self._closing
        __traceback_info__ = self._storage, self._dst
        db = closing(DB(self._storage))
        conn = closing(db.open())
        root = conn.root()
        root.obj = obj1 = MinPO(1)
        txn = transaction.get()
        txn.note(u'root -> obj')
        txn.commit()
        root.obj.obj = obj2 = MinPO(2)
        txn = transaction.get()
        txn.note(u'root -> obj -> obj')
        txn.commit()
        del root.obj
        txn = transaction.get()
        txn.note(u'root -X->')
        txn.commit()

        storage_last_tid = conn._storage.lastTransaction()
        self.assertEqual(storage_last_tid, root._p_serial)

        # Now copy the transactions to the destination
        self._dst.copyTransactionsFrom(self._storage)
        self.assertEqual(self._dst.lastTransaction(), storage_last_tid)
        # If the source storage is a history-free storage, all
        # of the transactions are now marked as packed in the
        # destination storage.  To trigger a pack, we have to
        # add another transaction to the destination that is
        # not packed.
        db2 = closing(DB(self._dst))
        tx_manager = transaction.TransactionManager(explicit=True)
        conn2 = closing(db2.open(tx_manager))
        txn = tx_manager.begin()
        root2 = conn2.root()
        root2.extra = 0
        txn.note(u'root.extra = 0')
        txn.commit()

        dest_last_tid = conn2._storage.lastTransaction()
        self.assertGreater(dest_last_tid, storage_last_tid)
        self.assertEqual(dest_last_tid, root2._p_serial)

        # Now pack the destination.
        from ZODB.utils import u64 as bytes8_to_int64
        if IRelStorage.providedBy(self._dst):
            packtime = bytes8_to_int64(storage_last_tid)
        else:
            from persistent.timestamp import TimeStamp
            packtime = TimeStamp(dest_last_tid).timeTime() + 2
        self._dst.pack(packtime, referencesf)
        # And check to see that the root object exists, but not the other
        # objects.
        __traceback_info__ += (packtime,)
        _data, _serial = self._dst.load(root._p_oid, '')
        raises(KeyError, self._dst.load, obj1._p_oid, '')
        raises(KeyError, self._dst.load, obj2._p_oid, '')

    def checkRestoreAfterDoubleCommit(self):
        oid = self._storage.new_oid()
        revid = b'\0' * 8
        data1 = zodb_pickle(MinPO(11))
        data2 = zodb_pickle(MinPO(12))
        # Begin the transaction
        t = Transaction()
        try:
            self._storage.tpc_begin(t)
            # Store an object
            self._storage.store(oid, revid, data1, '', t)
            # Store it again
            self._storage.store(oid, revid, data2, '', t)
            # Finish the transaction
            self._storage.tpc_vote(t)
            self._storage.tpc_finish(t)
        except:
            self._storage.tpc_abort(t)
            raise
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)


class UndoableRecoveryStorage(BasicRecoveryStorage):
    """These tests require the source storage to be undoable"""

    def checkRestoreAcrossPack(self):
        closing = self._closing
        db = closing(DB(self._storage))
        c = closing(db.open())
        r = c.root()
        r["obj1"] = MinPO(1)
        transaction.commit()
        r["obj2"] = MinPO(1)
        transaction.commit()

        self._dst.copyTransactionsFrom(self._storage)
        self._dst.pack(time.time(), referencesf)

        self._undo(self._storage.undoInfo()[0]['id'])

        # copy the final transaction manually.  even though there
        # was a pack, the restore() ought to succeed.
        it = closing(self._storage.iterator())
        # Get the last transaction and its record iterator. Record iterators
        # can't be accessed out-of-order, so we need to do this in a bit
        # complicated way:
        final = None
        for final in it:
            records = list(final)

        self._dst.tpc_begin(final, final.tid, final.status)
        for r in records:
            self._dst.restore(r.oid, r.tid, r.data, '', r.data_txn,
                              final)
        self._dst.tpc_vote(final)
        self._dst.tpc_finish(final)


    def checkRestoreWithMultipleObjectsInUndoRedo(self):
        # pylint:disable=too-many-statements
        from ZODB.FileStorage import FileStorage

        # Undo creates backpointers in (at least) FileStorage.  ZODB 3.2.1
        # FileStorage._data_find() had an off-by-8 error, neglecting to
        # account for the size of the backpointer when searching a
        # transaction with multiple data records.  The results were
        # unpredictable.  For example, it could raise a Python exception
        # due to passing a negative offset to file.seek(), or could
        # claim that a transaction didn't have data for an oid despite
        # that it actually did.
        #
        # The former failure mode was seen in real life, in a ZRS secondary
        # doing recovery.  On my box today, the second failure mode is
        # what happens in this test (with an unpatched _data_find, of
        # course).  Note that the error can only "bite" if more than one
        # data record is in a transaction, and the oid we're looking for
        # follows at least one data record with a backpointer.
        #
        # Unfortunately, _data_find() is a low-level implementation detail,
        # and this test does some horrid white-box abuse to test it.

        is_filestorage = isinstance(self._storage, FileStorage)
        closing = self._closing
        db = closing(DB(self._storage))
        c = closing(db.open())
        r = c.root()

        # Create some objects.
        r["obj1"] = MinPO(1)
        r["obj2"] = MinPO(1)
        transaction.commit()

        # Add x attributes to them.
        r["obj1"].x = 'x1'
        r["obj2"].x = 'x2'
        transaction.commit()

        c2 = closing(db.open())

        r = c2.root()
        self.assertEqual(r["obj1"].x, 'x1')
        self.assertEqual(r["obj2"].x, 'x2')

        # Dirty tricks.
        if is_filestorage:
            obj1_oid = r["obj1"]._p_oid
            obj2_oid = r["obj2"]._p_oid
            # This will be the offset of the next transaction, which
            # will contain two backpointers.
            pos = self._storage.getSize()

        # Undo the attribute creation.
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        _oids = self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        r = closing(db.open()).root()
        self.assertRaises(AttributeError, getattr, r["obj1"], 'x')
        self.assertRaises(AttributeError, getattr, r["obj2"], 'x')

        if is_filestorage:
            # _data_find should find data records for both objects in that
            # transaction.  Without the patch, the second assert failed
            # (it claimed it couldn't find a data record for obj2) on my
            # box, but other failure modes were possible.
            self.assertTrue(self._storage._data_find(pos, obj1_oid, '') > 0)
            self.assertTrue(self._storage._data_find(pos, obj2_oid, '') > 0)

            # The offset of the next ("redo") transaction.
            pos = self._storage.getSize()

        # Undo the undo (restore the attributes).
        info = self._storage.undoInfo()
        tid = info[0]['id']
        t = Transaction()
        self._storage.tpc_begin(t)
        _oids = self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)

        c3 = closing(db.open())
        r = c3.root()
        self.assertEqual(r["obj1"].x, 'x1')
        self.assertEqual(r["obj2"].x, 'x2')

        if is_filestorage:
            # Again _data_find should find both objects in this txn, and
            # again the second assert failed on my box.
            self.assertTrue(self._storage._data_find(pos, obj1_oid, '') > 0)
            self.assertTrue(self._storage._data_find(pos, obj2_oid, '') > 0)

        # Indirectly provoke .restore().  .restore in turn indirectly
        # provokes _data_find too, but not usefully for the purposes of
        # the specific bug this test aims at:  copyTransactionsFrom() uses
        # storage iterators that chase backpointers themselves, and
        # return the data they point at instead.  The result is that
        # _data_find didn't actually see anything dangerous in this
        # part of the test.
        self._dst.copyTransactionsFrom(self._storage)
        self.compare(self._storage, self._dst)
