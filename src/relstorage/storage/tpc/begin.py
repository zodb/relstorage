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
"""The core of RelStorage, a ZODB storage for relational databases.

Stores pickles in the database.
"""
from __future__ import absolute_import
from __future__ import print_function

from ZODB.POSException import ReadConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._compat import base64_decodebytes
from relstorage._compat import dumps
from relstorage._compat import OID_TID_MAP_TYPE
from relstorage._util import to_utf8

from . import AbstractTPCState
from .vote import DatabaseLockedForTid
from .vote import HistoryFree as HFVoteFactory
from .vote import HistoryPreserving as HPVoteFactory

class _BadFactory(object):
    "Marker."

    def enter(self):
        raise NotImplementedError

class AbstractBegin(AbstractTPCState):
    """
    The phase we enter after ``tpc_begin`` has been called.
    """
    __slots__ = (
        # (user, description, extension) from the transaction.
        # byte objects.
        'ude',
        # max_stored_oid is the highest OID stored by the current
        # transaction
        'max_stored_oid',
        # required_tids: {oid_int: tid_int}; confirms that certain objects
        # have not changed at commit. May be a BTree
        'required_tids',

        # The factory we call to produce a voting state. Must return
        # an object with an enter() method.
        'tpc_vote_factory',
    )

    _DEFAULT_TPC_VOTE_FACTORY = _BadFactory

    def __init__(self, storage, transaction):
        super(AbstractBegin, self).__init__(storage, transaction)
        self.ude = None
        self.max_stored_oid = 0
        self.required_tids = ()
        self.tpc_vote_factory = self._DEFAULT_TPC_VOTE_FACTORY

        storage._lock.acquire()
        try:
            storage._lock.release()
            storage._commit_lock.acquire()
            storage._lock.acquire()
            storage.transaction = transaction

            user = to_utf8(transaction.user)
            desc = to_utf8(transaction.description)
            ext = transaction.extension

            if ext:
                ext = dumps(ext, 1)
            else:
                ext = b""
            self.ude = user, desc, ext

            storage._restart_store()
            storage._cache.tpc_begin()
        finally:
            storage._lock.release()

        storage.blobhelper.begin()

    def tpc_vote(self, transaction):
        with self.storage._lock:
            if transaction is not self.transaction:
                raise StorageTransactionError(
                    "tpc_vote called with wrong transaction")

            next_phase = self.tpc_vote_factory(self)
            next_phase.enter()
            return next_phase

    def store(self, oid, previous_tid, data, transaction):
        # Called by Connection.commit(), after tpc_begin has been called.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        #adapter = self._adapter
        cache = self.storage._cache
        cursor = self.storage._store_cursor
        assert cursor is not None
        oid_int = bytes8_to_int64(oid)
        if previous_tid:
            # previous_tid is the tid of the state that the
            # object was loaded from.

            # XXX PY3: ZODB.tests.IteratorStorage passes a str (non-bytes) value for oid
            prev_tid_int = bytes8_to_int64(
                previous_tid
                if isinstance(previous_tid, bytes)
                else previous_tid.encode('ascii')
            )
        else:
            prev_tid_int = 0

        with self.storage._lock:
            self.max_stored_oid = max(self.max_stored_oid, oid_int)
            # Save the data locally in a temporary place. Later, closer to commit time,
            # we'll send it all over at once. This lets us do things like use
            # COPY in postgres.
            cache.store_temp(oid_int, data, prev_tid_int)
            return None

    def checkCurrentSerialInTransaction(self, oid, required_tid, transaction):
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        _, committed_tid = self.storage.load(oid)
        if committed_tid != required_tid:
            raise ReadConflictError(
                oid=oid, serials=(committed_tid, required_tid))

        required_tid_int = bytes8_to_int64(required_tid)
        oid_int = bytes8_to_int64(oid)

        # If this transaction already specified a different serial for
        # this oid, the transaction conflicts with itself.
        required_tids = self.required_tids
        if not required_tids:
            required_tids = self.required_tids = OID_TID_MAP_TYPE()

        previous_serial_int = required_tids.get(oid_int, required_tid_int)
        if previous_serial_int != required_tid_int:
            raise ReadConflictError(
                oid=oid,
                serials=(int64_to_8bytes(previous_serial_int),
                         required_tid))
        required_tids[oid_int] = required_tid_int

class HistoryFree(AbstractBegin):

    __slots__ = ()

    _DEFAULT_TPC_VOTE_FACTORY = HFVoteFactory

    def deleteObject(self, oid, oldserial, transaction):
        # This method is only expected to be called from zc.zodbdgc
        # currently, or from ZODB/tests/IExternalGC.test

        # This is called in a phase of two-phase-commit (tpc).
        # This means we have a transaction, and that we are holding
        # the commit lock as well as the regular lock.
        # RelStorage native pack uses a separate pack lock, but
        # unfortunately there's no way to not hold the commit lock;
        # however, the transactions are very short.
        if transaction is not self.transaction: # pragma: no cover
            raise StorageTransactionError(self, transaction)

        # We don't worry about anything in self._cache because
        # by definition we are deleting objects that were
        # not reachable and so shouldn't be in the cache (or if they
        # were, we'll never ask for them anyway)

        # We delegate the actual operation to the adapter's packundo,
        # just like native pack
        cursor = self.storage._store_cursor
        assert cursor is not None
        # When this is done, we get a tpc_vote,
        # and a tpc_finish.
        # The interface doesn't specify a return value, so for testing
        # we return the count of rows deleted (should be 1 if successful)
        return self.storage._adapter.packundo.deleteObject(cursor, oid, oldserial)

class HistoryPreserving(AbstractBegin):

    __slots__ = (
        # Stored in their 8 byte form
        'undone_oids',

        # If we use undo(), we have to allocate a TID, which means
        # we have to lock the database. Not cool.
        'committing_tid_lock',
    )

    _DEFAULT_TPC_VOTE_FACTORY = HPVoteFactory

    def __init__(self, storage, transaction):
        AbstractBegin.__init__(self, storage, transaction)
        self.undone_oids = ()
        self.committing_tid_lock = None

    def undo(self, transaction_id, transaction):
        # Typically if this is called, the store/restore methods will *not* be
        # called, but there's not a strict guarantee about that.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        # Unlike most places, transaction_id is the base 64 encoding
        # of an 8 byte tid

        undo_tid = base64_decodebytes(transaction_id + b'\n') # pylint:disable=deprecated-method
        assert len(undo_tid) == 8
        undo_tid_int = bytes8_to_int64(undo_tid)

        with self.storage._lock:
            adapter = self.storage._adapter
            cursor = self.storage._store_cursor
            assert cursor is not None

            adapter.locker.hold_pack_lock(cursor)
            try:
                adapter.packundo.verify_undoable(cursor, undo_tid_int)
                if self.committing_tid_lock is None:
                    # Note that _prepare_tid acquires the commit lock.
                    # The commit lock must be acquired after the pack lock
                    # because the database adapters also acquire in that
                    # order during packing.
                    adapter = self.storage._adapter
                    cursor = self.storage._store_cursor
                    tid_lock = DatabaseLockedForTid.lock_database_for_next_tid(
                        cursor, adapter, self.ude)
                    self.committing_tid_lock = tid_lock

                self_tid_int = self.committing_tid_lock.tid_int
                copied = adapter.packundo.undo(
                    cursor, undo_tid_int, self_tid_int)
                oids = [int64_to_8bytes(oid_int) for oid_int, _ in copied]

                # Update the current object pointers immediately, so that
                # subsequent undo operations within this transaction will see
                # the new current objects.
                adapter.mover.update_current(cursor, self_tid_int)

                self.storage.blobhelper.copy_undone(copied,
                                                    self.committing_tid_lock.tid)

                if not self.undone_oids:
                    self.undone_oids = set()
                self.undone_oids.update(oids)
            finally:
                adapter.locker.release_pack_lock(cursor)
