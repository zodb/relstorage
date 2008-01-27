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

import unittest
from relstorage.relstorage import RelStorage

from ZODB.DB import DB
from persistent.mapping import PersistentMapping
import transaction

from ZODB.tests import StorageTestBase, BasicStorage, \
     TransactionalUndoStorage, PackableStorage, \
     Synchronization, ConflictResolution, HistoryStorage, \
     RevisionStorage, PersistentStorage, \
     MTStorage, ReadOnlyStorage

from ZODB.tests.StorageTestBase import zodb_unpickle, zodb_pickle


class BaseRelStorageTests(StorageTestBase.StorageTestBase):

    def make_adapter(self):
        # abstract method
        raise NotImplementedError

    def open(self, **kwargs):
        adapter = self.make_adapter()
        self._storage = RelStorage(adapter, **kwargs)

    def setUp(self):
        self.open(create=1)
        self._storage._zap()

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

    def check16MObject(self):
        # Store 16 * 1024 * 1024 bytes in an object, then retrieve it
        data = 'a 16 byte string' * (1024 * 1024)
        oid = self._storage.new_oid()
        self._dostoreNP(oid, data=data)
        got, serialno = self._storage.load(oid, '')
        self.assertEqual(len(got), len(data))
        self.assertEqual(got, data)
