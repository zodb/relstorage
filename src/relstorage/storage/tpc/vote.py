##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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
"""
States related to TPC voting.

The implementation of locking the database for commit, flushing
temporary objects to the database, and moving them to their final locations,
live here.
"""
from __future__ import absolute_import
from __future__ import print_function

import time

from persistent.timestamp import TimeStamp
from ZODB.POSException import ConflictError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64


from . import AbstractTPCState
from .finish import Finish

logger = __import__('logging').getLogger(__name__)

class DatabaseLockedForTid(object):

    @classmethod
    def lock_database_for_next_tid(cls, cursor, adapter, ude):
        # TODO: Stop doing this here; go to row-level locking.

        adapter.locker.hold_commit_lock(cursor, ensure_current=True)
        user, desc, ext = ude

        # Choose a transaction ID.
        # Base the transaction ID on the current time,
        # but ensure that the tid of this transaction
        # is greater than any existing tid.
        # TODO: Stop allocating this here. Defer until tpc_finish.
        last_tid = adapter.txncontrol.get_tid(cursor)
        now = time.time()
        stamp = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
        stamp = stamp.laterThan(TimeStamp(int64_to_8bytes(last_tid)))
        tid = stamp.raw()

        tid_int = bytes8_to_int64(tid)
        adapter.txncontrol.add_transaction(cursor, tid_int, user, desc, ext)
        return cls(tid, tid_int, adapter)

    @classmethod
    def lock_database_for_given_tid(cls, tid, tid_is_packed,
                                    cursor, adapter, ude):
        adapter.locker.hold_commit_lock(cursor, ensure_current=True)
        tid_int = bytes8_to_int64(tid)
        user, desc, ext = ude
        adapter.txncontrol.add_transaction(
            cursor, tid_int, user, desc, ext, tid_is_packed)
        return cls(tid, tid_int, adapter)

    __slots__ = (
        'tid',
        'tid_int',
        'release_commit_lock',
    )

    def __init__(self, tid, tid_int, adapter):
        self.tid = tid
        self.tid_int = tid_int
        self.release_commit_lock = adapter.locker.release_commit_lock


class AbstractVote(AbstractTPCState):
    """
    The state we're in following ``tpc_vote``.

    Unlike the begin states, you *must* explicitly call :meth:`enter`
    on this object after it is constructed.

    """

    __slots__ = (
        # (user, description, extension) from the transaction.
        'ude',
        # max_stored_oid is the highest OID stored by the current
        # transaction
        'max_stored_oid',
        # required_tids: {oid_int: tid_int}; confirms that certain objects
        # have not changed at commit. May be a BTree
        'required_tids',
        # The DatabaseLockedForTid object
        'committing_tid_lock',
        # {oid_bytes}: Things that get changed as part of the vote process
        # and thus need to be invalidated.
        'invalidated_oids',
    )

    def __init__(self, begin_state, committing_tid_lock=None):
        # If committing_tid is passed to this method, it means
        # the database has already been locked and the TID is
        # locked in.
        super(AbstractVote, self).__init__(begin_state.storage, begin_state.transaction)

        self.required_tids = begin_state.required_tids or {}
        self.max_stored_oid = begin_state.max_stored_oid
        self.ude = begin_state.ude
        self.committing_tid_lock = committing_tid_lock
        self.invalidated_oids = set()

    def enter(self):
        resolved_in_vote = self.__vote()
        self.invalidated_oids.update(resolved_in_vote)

    def _flush_temps_to_db(self, cursor):
        mover = self.storage._adapter.mover
        mover.store_temps(cursor, self.storage._cache.temp_objects)

    def __vote(self):
        """
        Prepare the transaction for final commit.

        Takes the exclusive database commit lock.
        """
        # This method initiates a two-phase commit process,
        # saving the name of the prepared transaction in self._prepared_txn.

        # It is assumed that self._lock.acquire was called before this
        # method was called.
        cursor = self.storage._store_cursor
        assert cursor is not None
        adapter = self.storage._adapter
        locker = adapter.locker
        mover = adapter.mover

        # execute all remaining batch store operations
        self._flush_temps_to_db(cursor)

        # Reserve all OIDs used by this transaction
        if self.max_stored_oid > self.storage._max_new_oid:
            adapter.oidallocator.set_min_oid(
                cursor, self.max_stored_oid + 1)


        # XXX: When we stop allocating the TID early, we need to move
        # 'move_from_temp' (hf and hp) and 'update_current' (hp only)
        # later, down to tpc_finish. With everything safely locked,
        # that shouldn't fail. But we still need to check serials and
        # resolve conflicts here. When we do that, we need to lock all
        # the rows involved for update, in order (to avoid deadlocks),
        # including the prev_tid rows (hp only) and current committed
        # rows (both) for everything that conflicts.
        #
        # The above is now partway done.

        # Check the things registered by Connection.readCurrent(),
        # while simeoutaneously taking out update locks on both those rows,
        # and the rows we might conflict with or will be replacing.
        oid_ints = self.required_tids.keys()

        # TODO: make this (the locking query?) more useful so we can
        # do fewer overall queries. right now the typical call
        # sequence will take three queries: This one, the one to get
        # current, and the one to detect conflicts.
        locker.lock_current_objects(cursor, oid_ints)

        current = mover.current_object_tids(cursor, oid_ints)
        for oid_int, expect_tid_int in self.required_tids.items():
            actual_tid_int = current.get(oid_int, 0)
            if actual_tid_int != expect_tid_int:
                raise ReadConflictError(
                    oid=int64_to_8bytes(oid_int),
                    serials=(int64_to_8bytes(actual_tid_int),
                             int64_to_8bytes(expect_tid_int)))

        invalidated_oids = self.__check_and_resolve_conflicts()


        if self.storage.blobhelper is not None and self.storage.blobhelper.shared_blob_dir:
            self.__lock_and_move('vote')

        # New storage protocol
        return invalidated_oids

    def __check_and_resolve_conflicts(self):
        """
        Either raises an `ConflictError`, or successfully resolves
        all conflicts.

        Returns a set of byte OIDs for objects modified in this transaction
        but which were then updated by conflict resolution and so must
        be invalidated.

        All the rows needed for detecting conflicts should be locked against
        concurrent changes.
        """
        cursor = self.storage._store_cursor
        adapter = self.storage._adapter
        cache = self.storage._cache
        tryToResolveConflict = self.storage.tryToResolveConflict

        # Detect conflicting changes.
        # Try to resolve the conflicts.
        invalidated = set()  # a set of OIDs
        # In the past, we didn't load all conflicts from the DB at once,
        # just one at a time. This was because we also fetched the state data
        # from the DB, and it could be large. But now we use the state we have in
        # our local temp cache, so memory concerns are gone.
        conflicts = adapter.mover.detect_conflict(cursor)
        if conflicts:
            logger.debug("Attempting to resolve %d conflicts", len(conflicts))

        for conflict in conflicts:
            oid_int, committed_tid_int, tid_this_txn_saw_int = conflict
            state_from_this_txn = cache.read_temp(oid_int)
            oid = int64_to_8bytes(oid_int)
            prev_tid = int64_to_8bytes(committed_tid_int)
            serial = int64_to_8bytes(tid_this_txn_saw_int)

            resolved_state = tryToResolveConflict(oid, prev_tid, serial, state_from_this_txn)
            if resolved_state is None:
                # unresolvable; kill the whole transaction
                raise ConflictError(
                    oid=oid,
                    serials=(prev_tid, serial),
                    data=state_from_this_txn
                )

            # resolved
            state_from_this_txn = resolved_state
            # TODO: Make this use the bulk methods so we can use COPY.
            adapter.mover.replace_temp(
                cursor, oid_int, committed_tid_int, state_from_this_txn)
            invalidated.add(oid)
            cache.store_temp(oid_int, state_from_this_txn)

        return invalidated

    def __finish_store(self, committing_tid_int):
        """
        Move stored objects from the temporary table to final storage.

        Returns a sequence of OIDs that were resolved to be received
        by Connection._handle_serial().
        """
        # Move the new states into the permanent table
        if self.storage.blobhelper is not None:
            txn_has_blobs = self.storage.blobhelper.txn_has_blobs
        else:
            txn_has_blobs = False

        cursor = self.storage._store_cursor
        adapter = self.storage._adapter

        try:
            adapter.mover.move_from_temp(cursor, committing_tid_int, txn_has_blobs)
        except:
            # This could be bad, we're probably already in ``tpc_finish``.

            # XXX: On MySQL, a deadlock here only rolled back that one
            # *statement*, whereas on PostgreSQL it aborted the entire
            # transaction. On MySQL, this means we continue to hold
            # the locks we already took out, and our temporary tables
            # remain valid; thus we should be able to try again to
            # commit (after short sleep).

            # Deadlocks here have been observed during concurrent
            # heavy packing in HP databases (``checkPackLotsWhileWriting``,
            # https://travis-ci.org/zodb/relstorage/jobs/555024285#L476),
            # though it's not entirely clear why.
            # For debugging, lets print what everything is doing.
            import traceback; traceback.print_exc()
            import gevent.util
            gevent.util.print_run_info()
            raise


    def __choose_tid_and_lock(self):
        """
        Choose a tid for the current transaction, and exclusively lock
        the database commit lock.

        This should be done as late in the commit as possible, since
        it must hold an exclusive commit lock.
        """
        adapter = self.storage._adapter
        cursor = self.storage._store_cursor
        # TODO: Stop doing this here; go to row-level locking.
        lock = DatabaseLockedForTid.lock_database_for_next_tid(cursor, adapter, self.ude)
        self.committing_tid_lock = lock
        return lock

    def __lock_and_move(self, method='finish'):
        # Here's where we take the global commit lock, and
        # allocate the next available transaction id, storing it
        # into history-preserving DBs. But if someone passed us
        # a TID (``restore``), then it must already be in the DB, and the lock must
        # already be held.
        #
        # If we've prepared the transaction, then the TID must be in the
        # db, the lock must be held, and we must have finished all of our
        # storage actions. This is only expected to be the case when we have
        # a shared blob dir.
        if self.prepared_txn:
            # Already done.
            assert self.committing_tid_lock
            return

        if not self.committing_tid_lock:
            self.__choose_tid_and_lock()
        committing_tid_int = self.committing_tid_lock.tid_int
        self.__finish_store(committing_tid_int)

        if self.storage.blobhelper is not None:
            meth = getattr(self.storage.blobhelper, method)
            meth(self.committing_tid_lock.tid)
        cursor = self.storage._store_cursor
        self.storage._adapter.mover.update_current(cursor, committing_tid_int)
        conn = self.storage._store_conn
        self.prepared_txn = self.storage._adapter.txncontrol.commit_phase1(
            conn, cursor, committing_tid_int)

    def tpc_finish(self, transaction, f=None):
        with self.storage._lock:
            if transaction is not self.transaction:
                raise StorageTransactionError(
                    "tpc_finish called with wrong transaction")
            # Handle the finishing. We cannot/must not fail now.
            self.__lock_and_move()
            assert self.committing_tid_lock is not None, self
            try:
                try:
                    if f is not None:
                        f(self.committing_tid_lock.tid)
                    next_phase = Finish(self)
                    return next_phase, self.committing_tid_lock.tid
                finally:
                    self._clear_temp()
            finally:
                self.storage._commit_lock.release()


class HistoryFree(AbstractVote):
    pass


class HistoryPreserving(AbstractVote):

    def __init__(self, begin_state):
         # Using undo() requires a new TID, so if we had already begun
        # a transaction by locking the database and allocating a TID,
        # we must preserve that.
        super(HistoryPreserving, self).__init__(begin_state,
                                                begin_state.committing_tid_lock)
        # Anything that we've undone is also invalidated.
        self.invalidated_oids.update(begin_state.undone_oids)
