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
"""A foundation for history-free RelStorage tests"""

from relstorage.tests.RecoveryStorage import BasicRecoveryStorage
from relstorage.tests.RecoveryStorage import UndoableRecoveryStorage
from relstorage.tests.reltestbase import GenericRelStorageTests
from relstorage.tests.reltestbase import RelStorageTestBase

from ZODB.FileStorage import FileStorage
from ZODB.serialize import referencesf
from ZODB.tests.ConflictResolution import PCounter
from ZODB.tests.PackableStorage import dumps
from ZODB.tests.PackableStorage import pdumps
from ZODB.tests.PackableStorage import Root
from ZODB.tests.PackableStorage import ZERO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.tests.StorageTestBase import zodb_unpickle

import time


class HistoryFreeRelStorageTests(GenericRelStorageTests):
    # pylint:disable=too-many-ancestors,abstract-method,too-many-locals,too-many-statements

    keep_history = False

    # This overrides certain tests so they work with a storage that
    # collects garbage but does not retain old versions.

    def checkPackAllRevisions(self):
        from relstorage._compat import loads
        self._initroot()
        eq = self.assertEqual
        raises = self.assertRaises
        # Create a `persistent' object
        obj = self._newobj()
        oid = obj.getoid()
        obj.value = 1
        # Commit three different revisions
        revid1 = self._dostoreNP(oid, data=pdumps(obj))
        obj.value = 2
        revid2 = self._dostoreNP(oid, revid=revid1, data=pdumps(obj))
        obj.value = 3
        revid3 = self._dostoreNP(oid, revid=revid2, data=pdumps(obj))
        # Now make sure only the latest revision can be extracted
        raises(KeyError, self._storage.loadSerial, oid, revid1)
        raises(KeyError, self._storage.loadSerial, oid, revid2)
        data = self._storage.loadSerial(oid, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 3)
        # Now pack all transactions; need to sleep a second to make
        # sure that the pack time is greater than the last commit time.
        now = packtime = time.time()
        while packtime <= now:
            packtime = time.time()
        self._storage.pack(packtime, referencesf)
        self._storage.sync()
        # All revisions of the object should be gone, since there is no
        # reference from the root object to this object.
        raises(KeyError, self._storage.loadSerial, oid, revid1)
        raises(KeyError, self._storage.loadSerial, oid, revid2)
        raises(KeyError, self._storage.loadSerial, oid, revid3)
        raises(KeyError, self._storage.load, oid, '')

    def checkPackJustOldRevisions(self):
        eq = self.assertEqual
        raises = self.assertRaises
        loads = self._makeloader()
        # Create a root object.  This can't be an instance of Object,
        # otherwise the pickling machinery will serialize it as a persistent
        # id and not as an object that contains references (persistent ids) to
        # other objects.
        root = Root()
        # Create a persistent object, with some initial state
        obj = self._newobj()
        oid = obj.getoid()
        # Link the root object to the persistent object, in order to keep the
        # persistent object alive.  Store the root object.
        root.obj = obj
        root.value = 0
        revid0 = self._dostoreNP(ZERO, data=dumps(root))
        # Make sure the root can be retrieved
        data, revid = self._storage.load(ZERO, '')
        eq(revid, revid0)
        eq(loads(data).value, 0)
        # Commit three different revisions of the other object
        obj.value = 1
        revid1 = self._dostoreNP(oid, data=pdumps(obj))
        obj.value = 2
        revid2 = self._dostoreNP(oid, revid=revid1, data=pdumps(obj))
        obj.value = 3
        revid3 = self._dostoreNP(oid, revid=revid2, data=pdumps(obj))
        # Now make sure only the latest revision can be extracted
        raises(KeyError, self._storage.loadSerial, oid, revid1)
        raises(KeyError, self._storage.loadSerial, oid, revid2)
        data = self._storage.loadSerial(oid, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 3)
        # Now pack.  The object should stay alive because it's pointed
        # to by the root.
        now = packtime = time.time()
        while packtime <= now:
            packtime = time.time()
        self._storage.pack(packtime, referencesf)
        # Make sure the revisions are gone, but that object zero and revision
        # 3 are still there and correct
        data, revid = self._storage.load(ZERO, '')
        eq(revid, revid0)
        eq(loads(data).value, 0)
        raises(KeyError, self._storage.loadSerial, oid, revid1)
        raises(KeyError, self._storage.loadSerial, oid, revid2)
        data = self._storage.loadSerial(oid, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 3)
        data, revid = self._storage.load(oid, '')
        eq(revid, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 3)

    def checkPackOnlyOneObject(self):
        eq = self.assertEqual
        raises = self.assertRaises
        loads = self._makeloader()
        # Create a root object.  This can't be an instance of Object,
        # otherwise the pickling machinery will serialize it as a persistent
        # id and not as an object that contains references (persistent ids) to
        # other objects.
        root = Root()
        # Create a persistent object, with some initial state
        obj1 = self._newobj()
        oid1 = obj1.getoid()
        # Create another persistent object, with some initial state.
        obj2 = self._newobj()
        oid2 = obj2.getoid()
        # Link the root object to the persistent objects, in order to keep
        # them alive.  Store the root object.
        root.obj1 = obj1
        root.obj2 = obj2
        root.value = 0
        revid0 = self._dostoreNP(ZERO, data=dumps(root))
        # Make sure the root can be retrieved
        data, revid = self._storage.load(ZERO, '')
        eq(revid, revid0)
        eq(loads(data).value, 0)
        # Commit three different revisions of the first object
        obj1.value = 1
        revid1 = self._dostoreNP(oid1, data=pdumps(obj1))
        obj1.value = 2
        revid2 = self._dostoreNP(oid1, revid=revid1, data=pdumps(obj1))
        obj1.value = 3
        revid3 = self._dostoreNP(oid1, revid=revid2, data=pdumps(obj1))
        # Now make sure only the latest revision can be extracted
        raises(KeyError, self._storage.loadSerial, oid1, revid1)
        raises(KeyError, self._storage.loadSerial, oid1, revid2)
        data = self._storage.loadSerial(oid1, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid1)
        eq(pobj.value, 3)
        # Now commit a revision of the second object
        obj2.value = 11
        revid4 = self._dostoreNP(oid2, data=pdumps(obj2))
        # And make sure the revision can be extracted
        data = self._storage.loadSerial(oid2, revid4)
        pobj = loads(data)
        eq(pobj.getoid(), oid2)
        eq(pobj.value, 11)
        # Now pack just revisions 1 and 2 of object1.  Object1's current
        # revision should stay alive because it's pointed to by the root, as
        # should Object2's current revision.
        now = packtime = time.time()
        while packtime <= now:
            packtime = time.time()
        self._storage.pack(packtime, referencesf)
        # Make sure the revisions are gone, but that object zero, object2, and
        # revision 3 of object1 are still there and correct.
        data, revid = self._storage.load(ZERO, '')
        eq(revid, revid0)
        eq(loads(data).value, 0)
        raises(KeyError, self._storage.loadSerial, oid1, revid1)
        raises(KeyError, self._storage.loadSerial, oid1, revid2)
        data = self._storage.loadSerial(oid1, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid1)
        eq(pobj.value, 3)
        data, revid = self._storage.load(oid1, '')
        eq(revid, revid3)
        pobj = loads(data)
        eq(pobj.getoid(), oid1)
        eq(pobj.value, 3)
        data, revid = self._storage.load(oid2, '')
        eq(revid, revid4)
        eq(loads(data).value, 11)
        data = self._storage.loadSerial(oid2, revid4)
        pobj = loads(data)
        eq(pobj.getoid(), oid2)
        eq(pobj.value, 11)

    def checkRSResolve(self):
        # ZODB.tests.ConflictResolution.ConflictResolvingStorage has a checkResolve
        # with a different signature (as of 4.4.0) that we were unintentionally(?)
        # shadowing, hence the weird name.
        obj = PCounter()
        obj.inc()

        oid = self._storage.new_oid()

        revid1 = self._dostoreNP(oid, data=zodb_pickle(obj))

        obj.inc()
        obj.inc()

        # The effect of committing two transactions with the same
        # pickle is to commit two different transactions relative to
        # revid1 that add two to _value.

        # open s1
        s1 = self._storage.new_instance()
        # start a load transaction in s1
        s1.poll_invalidations()

        # commit a change
        _revid2 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))

        # commit a conflicting change using s1
        main_storage = self._storage
        self._storage = s1
        try:
            # we can resolve this conflict because s1 has an open
            # transaction that can read the old state of the object.
            _revid3 = self._dostoreNP(oid, revid=revid1, data=zodb_pickle(obj))
            s1.release()
        finally:
            self._storage = main_storage

        data, _serialno = self._storage.load(oid, '')
        inst = zodb_unpickle(data)
        self.assertEqual(inst._value, 5)


    def checkImplementsExternalGC(self):
        from zope.interface.verify import verifyObject
        import ZODB.interfaces
        verifyObject(ZODB.interfaces.IExternalGC, self._storage)

        # Now do it.
        from ZODB.utils import z64
        import transaction
        db = ZODB.DB(self._storage) # create the root
        conn = db.open()
        storage = conn._storage
        _state, tid = storage.load(z64)

        # copied from zc.zodbdgc
        t = transaction.begin()
        storage.tpc_begin(t)
        try:
            count = storage.deleteObject(z64, tid, t)
            self.assertEqual(count, 1)
            # Doing it again will do nothing because it's already
            # gone.
            count = storage.deleteObject(z64, tid, t)
            self.assertEqual(count, 0)
        finally:
            transaction.abort()
            conn.close()
            db.close()


class HistoryFreeToFileStorage(RelStorageTestBase,
                               BasicRecoveryStorage):
    # pylint:disable=abstract-method,too-many-ancestors
    keep_history = False

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


class HistoryFreeFromFileStorage(RelStorageTestBase,
                                 UndoableRecoveryStorage):
    # pylint:disable=abstract-method,too-many-ancestors
    keep_history = False

    def setUp(self):
        self._dst = self._storage
        self._storage = FileStorage("Source.fs", create=True)

    def tearDown(self):
        self._storage.close()
        self._dst.close()
        self._storage.cleanup()
        self._dst.cleanup()

    def new_dest(self):
        return self._dst

    def compare(self, src, dest):
        # The dest storage has a truncated copy of dest, so
        # use compare_truncated() instead of compare_exact().
        self.compare_truncated(src, dest)
