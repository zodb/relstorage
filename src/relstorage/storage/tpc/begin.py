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
from ZODB.utils import z64 as NO_PREV_TID


from relstorage._compat import base64_decodebytes
from relstorage._util import metricmethod_sampled
from relstorage._compat import OID_TID_MAP_TYPE
from relstorage._util import to_utf8

from . import AbstractTPCStateDatabaseAvailable
from .vote import DatabaseLockedForTid
from .vote import HistoryFree as HFVoteFactory
from .vote import HistoryPreserving as HPVoteFactory
from .vote import HistoryPreservingDeleteOnly as HPDeleteOnlyVoteFactory


class TransactionConflictsWithItselfError(ReadConflictError):
    """
    The transaction asked for two different OIDs for the same object.
    """

class _BadFactory(object):

    def enter(self, storage):
        raise NotImplementedError

class AbstractBegin(AbstractTPCStateDatabaseAvailable):
    """
    The phase we enter after ``tpc_begin`` has been called.
    """
    __slots__ = (
        # (user, description, extension_bytes) from the transaction.
        # byte objects.
        'ude',
        # The location of the temporary data.
        'temp_storage',

        # required_tids: {oid_int: tid_int}; confirms that certain objects
        # have not changed at commit. May be a BTree
        'required_tids',

        # The factory we call to produce a voting state. Must return
        # an object with an enter() method.
        'tpc_vote_factory',

        # OIDs of things we have deleted or undone.
        # Stored in their 8 byte form
        'invalidated_oids',

    )

    _DEFAULT_TPC_VOTE_FACTORY = _BadFactory # type: Callable[..., AbstractTPCState]

    def __init__(self, shared_state):
        super(AbstractBegin, self).__init__(shared_state)
        self.invalidated_oids = ()
        # We'll replace this later with the right type when it's needed.
        self.required_tids = {} # type: Dict[int, int]
        self.tpc_vote_factory = self._DEFAULT_TPC_VOTE_FACTORY # type: ignore

        user = to_utf8(self.transaction.user)
        desc = to_utf8(self.transaction.description)
        ext = self.transaction.extension_bytes
        self.ude = user, desc, ext

    def tpc_vote(self, storage, transaction):
        if transaction is not self.transaction:
            raise StorageTransactionError(
                "tpc_vote called with wrong transaction")

        next_phase = self.tpc_vote_factory(self)
        next_phase.enter(storage)
        return next_phase

    def store(self, oid, previous_tid, data, transaction):
        """
        This method should take no globally visible commit locks.
        """
        # Called by Connection.commit(), after tpc_begin has been called.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        oid_int = bytes8_to_int64(oid)
        if previous_tid and previous_tid != NO_PREV_TID:
            # previous_tid is the tid of the state that the
            # object was loaded from. cPersistent objects return a brand
            # new bytes object each time even if it's all zeros; Python implementation
            # returns a private constant. It would be nice if they all returned a public
            # interned constant so we could compare with `is`.
            prev_tid_int = bytes8_to_int64(previous_tid)
        else:
            prev_tid_int = 0

        # Save the data locally in a temporary place. Later, closer to commit time,
        # we'll send it all over at once. This lets us do things like use
        # COPY in postgres.
        self.shared_state.temp_storage.store_temp(oid_int, data, prev_tid_int)

    @metricmethod_sampled
    def checkCurrentSerialInTransaction(self, oid, required_tid, transaction):
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        required_tid_int = bytes8_to_int64(required_tid)
        oid_int = bytes8_to_int64(oid)

        # If this transaction already specified a different serial for
        # this oid, the transaction conflicts with itself.
        required_tids = self.required_tids
        if not required_tids:
            required_tids = self.required_tids = OID_TID_MAP_TYPE()

        previous_serial_int = required_tids.get(oid_int, required_tid_int)
        if previous_serial_int != required_tid_int:
            raise TransactionConflictsWithItselfError(
                oid=oid,
                serials=(int64_to_8bytes(previous_serial_int),
                         required_tid))
        required_tids[oid_int] = required_tid_int

        # Previously, we used Loader.getTid() (a wrapper around
        # Loader.load()) to check that what's visible in the database
        # load connection is what they asked for.
        #
        # But this shouldn't actually matter: This object came from
        # the load connection in the first place. This is probably a
        # cache hit. If it doesn't match, then we failed to notice a
        # change and invalidate the object. This is called from
        # Connection.commit(), which happens after tpc_begin() and
        # just before tpc_vote(), so there's no real reason not to
        # just defer it a bit until we do it in bulk for vote.

    def _invalidated_oids(self, *oid_bytes):
        if not self.invalidated_oids:
            self.invalidated_oids = set()
        self.invalidated_oids.update(oid_bytes)


    def deleteObject(self, oid, oldserial, transaction):
        """
        This method operates directly against the ``object_state`` table;
        as such, it immediately takes out locks on that table.

        This method is only expected to be called when performing
        ``IExternalGC`` operations (e.g., from zc.zodbdgc
        or from ZODB/tests/IExternalGC.test).

        In history-free mode, deleting objects does not allocate
        a new tid (well, it allocates it, but there's no place to store
        it). In history preserving mode, it will wind up allocating a tid
        to store the empty transaction (only previous states were undone)

        TODO: This needs a better, staged implementation. I think it is
           highly likely to deadlock now if anything happened to be reading
           those rows.
        XXX: If we have blobs in a non-shared disk location, this does not
           remove them.
        """
        if transaction is not self.transaction: # pragma: no cover
            raise StorageTransactionError(self, transaction)

        # We shouldn't have to worry about anything in self._cache
        # because by definition we are deleting objects that were not
        # reachable and so shouldn't be in the cache (or if they were,
        # we'll never ask for them anyway). Most likely, this is running
        # in a separate process anyway, not used for regular storage (
        # an instance of multi-zodb-gc). However, in case it is in a regular
        # process, and in case we do have other transactions that could theoretically
        # see this state, and to relieve memory pressure on local/global caches,
        # we do go ahead and invalidate a cached entry.
        # TODO: We need a distinct name for invalidate, so we can differentiate
        # between why we're doing it. Did we write a newer version? Did we
        # delete a specific verison? Etc.
        oid_int = bytes8_to_int64(oid)
        tid_int = bytes8_to_int64(oldserial)
        self.shared_state.cache.remove_cached_data(oid_int, tid_int)

        # We delegate the actual operation to the adapter's packundo,
        # just like native pack
        cursor = self.shared_state.store_connection.cursor
        # When this is done, we get a tpc_vote,
        # and a tpc_finish.
        # The interface doesn't specify a return value, so for testing
        # we return the count of rows deleted (should be 1 if successful)
        deleted = self.shared_state.adapter.packundo.deleteObject(cursor, oid, oldserial)
        self._invalidated_oids(oid)
        return deleted


class HistoryFree(AbstractBegin):

    __slots__ = ()

    _DEFAULT_TPC_VOTE_FACTORY = HFVoteFactory


class HistoryPreserving(AbstractBegin):

    __slots__ = (
        # If we use undo() or deleteObject(), we have to allocate a TID, which means
        # we have to lock the database. Not cool.
        'committing_tid_lock',
    )

    _DEFAULT_TPC_VOTE_FACTORY = HPVoteFactory

    def __init__(self, *args):
        AbstractBegin.__init__(self, *args)
        self.committing_tid_lock = None

    def _obtain_commit_lock(self, cursor):
        if self.committing_tid_lock is None:
            # Note that _prepare_tid acquires the commit lock.
            # The commit lock must be acquired after the pack lock
            # because the database adapters also acquire in that
            # order during packing.
            tid_lock = DatabaseLockedForTid.lock_database_for_next_tid(
                cursor, self.shared_state.adapter, self.ude)
            self.committing_tid_lock = tid_lock

    def deleteObject(self, oid, oldserial, transaction):
        # XXX: Maybe we don't need the commit lock at all? Since
        # theoretically these are unreachable? Our custom
        # vote stage just removes this transaction anyway; maybe it
        # can skip the committing.
        self._obtain_commit_lock(self.shared_state.store_connection.cursor)
        # A transaction that deletes objects can *only* delete objects.
        # That way we don't need to store an entry in the transaction table
        # (and add extra bloat to the DB; that kind of defeats the point of
        # deleting objects).
        self.tpc_vote_factory = HPDeleteOnlyVoteFactory
        return super(HistoryPreserving, self).deleteObject(oid, oldserial, transaction)

    def undo(self, transaction_id, transaction):
        """
        This method temporarily holds the pack lock, releasing it when
        done, and it also holds the commit lock, keeping it held for
        the next phase.

        Returns an iterable of ``(oid_int, tid_int)`` pairs giving the
        items that were restored and are now current. All of those oids that
        had any data stored for ``transaction_id`` are now invalid.
        """
        # Typically if this is called, the store/restore methods will *not* be
        # called, but there's not a strict guarantee about that.
        if transaction is not self.transaction:
            raise StorageTransactionError(self, transaction)

        # Unlike most places, transaction_id is the base 64 encoding
        # of an 8 byte tid

        undo_tid = base64_decodebytes(transaction_id + b'\n') # pylint:disable=deprecated-method
        assert len(undo_tid) == 8
        undo_tid_int = bytes8_to_int64(undo_tid)

        adapter = self.shared_state.adapter
        cursor = self.shared_state.store_connection.cursor
        assert cursor is not None

        adapter.locker.hold_pack_lock(cursor)
        try:
            adapter.packundo.verify_undoable(cursor, undo_tid_int)
            self._obtain_commit_lock(cursor)

            self_tid_int = self.committing_tid_lock.tid_int
            copied = adapter.packundo.undo(
                cursor, undo_tid_int, self_tid_int)

            # Invalidate all cached data for these oids. We have a
            # brand new transaction ID that's greater than any they
            # had before. In history-preserving mode, there could
            # still be other valid versions. See notes in packundo:
            # In theory we could be undoing a transaction several generations in the
            # past where the object had multiple intermediate states, but in practice
            # we're probably just undoing the latest state. Still, play it
            # a bit safer.
            oid_ints = [oid_int for oid_int, _ in copied]
            self.shared_state.cache.remove_all_cached_data_for_oids(oid_ints)

            # Update the current object pointers immediately, so that
            # subsequent undo operations within this transaction will see
            # the new current objects.
            adapter.mover.update_current(cursor, self_tid_int)

            self.shared_state.blobhelper.copy_undone(copied,
                                                     self.committing_tid_lock.tid)

            oids = [int64_to_8bytes(oid_int) for oid_int in oid_ints]
            self._invalidated_oids(*oids)

            return copied
        finally:
            adapter.locker.release_pack_lock(cursor)
