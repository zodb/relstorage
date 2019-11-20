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

from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.POSException import ConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._util import log_timed
from relstorage._util import log_timed_only_self
from relstorage._util import do_log_duration_info
from relstorage._util import TRACE
from ..interfaces import VoteReadConflictError

from . import LOCK_EARLY
from . import AbstractTPCState
from .finish import Finish


logger = __import__('logging').getLogger(__name__)
perf_logger = logger.getChild('timing')

class DatabaseLockedForTid(object):

    @classmethod
    def lock_database_for_next_tid(cls, cursor, adapter, ude):
        # We're midway between the state of a database-wide lock
        # and consistent row-level locking. The lock here is now
        # a row-level artificial lock on COMMIT_ROW_LOCK, and we then
        # read TRANSACTION (or OBJECT_STATE in HF).
        # TODO: Continue working to remove the need for the artificial
        # lock.
        user, desc, ext = ude
        tid_int = adapter.lock_database_and_choose_next_tid(
            cursor,
            user,
            desc,
            ext
        )

        tid = int64_to_8bytes(tid_int)
        return cls(tid, tid_int, adapter)

    @classmethod
    def lock_database_for_given_tid(cls, tid, tid_is_packed,
                                    cursor, adapter, ude):
        adapter.locker.hold_commit_lock(cursor, ensure_current=(tid,))
        tid_int = bytes8_to_int64(tid)
        user, desc, ext = ude
        adapter.txncontrol.add_transaction(
            cursor, tid_int, user, desc, ext, tid_is_packed)
        return cls(tid, tid_int, adapter)

    __slots__ = (
        'tid',
        'tid_int',
        'release_commit_lock',
        'local_allocation_time',
    )

    def __init__(self, tid, tid_int, adapter):
        self.tid = tid
        self.tid_int = tid_int
        self.release_commit_lock = adapter.locker.release_commit_lock
        self.local_allocation_time = time.time()

    def __repr__(self):
        return "<%s tid_int=%d created=%s>" %(
            self.__class__.__name__,
            self.tid_int,
            self.local_allocation_time
        )

class AbstractVote(AbstractTPCState):
    """
    The state we're in following ``tpc_vote``.

    Unlike the begin states, you *must* explicitly call :meth:`enter`
    on this object after it is constructed.

    """

    __slots__ = (
        # (user, description, extension) from the transaction. byte objects.
        'ude',
        # The TemporaryStorage.
        'temp_storage',
        # required_tids: {oid_int: tid_int}; confirms that certain objects
        # have not changed at commit. May be a BTree
        'required_tids',
        # The DatabaseLockedForTid object
        'committing_tid_lock',
        # {oid_bytes}: Things that get changed as part of the vote process
        # and thus need to be invalidated.
        'invalidated_oids',
        # How many conflicts there were to resolve. None if we're not there yet.
        'count_conflicts',
        # The timestamp we gained control after locking, and then the
        # timestamp we completed voting. If it takes "too long" to get
        # around to finishing, we'll log a warning.
        'lock_and_vote_times'
    )

    def __init__(self, begin_state, committing_tid_lock=None):
        # type: (AbstractBegin, DatabaseLockedForTid) -> None

        # If committing_tid is passed to this method, it means the
        # database has already been locked and the TID is locked in.
        # This is (only!) done when we're restoring transactions.
        super(AbstractVote, self).__init__(begin_state, begin_state.transaction)

        self.required_tids = begin_state.required_tids or {} # type: Dict[int, int]
        self.temp_storage = begin_state.temp_storage # type: .temporary_storage.TemporaryStorage
        self.ude = begin_state.ude
        self.committing_tid_lock = committing_tid_lock # type: Optional[DatabaseLockedForTid]
        self.count_conflicts = None
        self.lock_and_vote_times = [None, None]

        # Anything that we've undone or deleted is also invalidated.
        self.invalidated_oids = begin_state.invalidated_oids or set() # type: Set[bytes]

    def _clear_temp(self):
        # Clear all attributes used for transaction commit.
        self.temp_storage.close()

    def _tpc_state_extra_repr_info(self):
        return {
            'share_lock_count': len(self.required_tids),
            'conflict_count': self.count_conflicts,
            'invalidated_count': len(self.invalidated_oids),
        }

    def enter(self, storage):
        resolved_in_vote_oid_ints = self._vote(storage)
        self.invalidated_oids.update({int64_to_8bytes(i) for i in resolved_in_vote_oid_ints})
        self.lock_and_vote_times[1] = time.time()

    @log_timed
    def _flush_temps_to_db(self, cursor):
        if self.temp_storage:
            # Don't bother if we're empty.
            self.adapter.mover.store_temps(cursor, self.temp_storage)

    def __enter_critical_phase_until_transaction_end(self):
        self.load_connection.enter_critical_phase_until_transaction_end()
        self.store_connection.enter_critical_phase_until_transaction_end()


    def _vote(self, storage):
        """
        Prepare the transaction for final commit.

        Locks (only!) the rows that will be updated or were marked as
        explicit dependencies through
        `checkCurrentSerialInTransaction`, and then verfies those
        dependencies and resolves conflicts.

        If we're using a shared blob dir, we then take out the commit
        lock, in order to move blobs into final position. (TODO: That
        might not be fully necessary, so long as we can properly roll
        back blobs.) Otherwise, this only locks the rows we will impact.
        """
        # It is assumed that self._lock.acquire was called before this
        # method was called.
        cursor = self.store_connection.cursor
        __traceback_info__ = self.store_connection, cursor
        assert cursor is not None
        adapter = self.adapter

        # execute all remaining batch store operations.
        # This exists as an extension point.
        self._flush_temps_to_db(cursor)

        # Reserve all OIDs used by this transaction.
        # In a typical transaction, this is our first write to the database
        # that's not temporary.

        # In the common case where we allocated OIDs for new objects,
        # the most recent OID we allocated will match the
        # ``max_stored_oid```. In the also-common case where we've
        # *never* allocated objects and we're just updating
        # pre-existing objects, every OID we see will be newer than
        # what we allocated (0, we never allocated). So it is the
        # responsibility of the ``relstorage.storage.oid.OIDs`` object
        # to keep track of seen OIDs across transacations, keeping
        # track of the maximum that it's seen, and only actually
        # sending a request down to the database when it's a genuinely
        # higher OID.
        #
        # The other time this can come up is ``copyTransactionsFrom()`` or ``restore()``:
        # those OIDs originated elsewhere and have no relation to our database sequence. That's
        # ok, the same logic applies.
        #
        # In this way, we minimize trips to the DB. We could be a
        # *little* bit smarter and track whether one of those APIs was
        # used, or whether we're updating existing objects and avoid a
        # bit more overhead, but benchmarking suggests that it's not
        # worth it in common cases.
        storage._oids.set_min_oid(self.temp_storage.max_stored_oid)

        # Lock objects being modified and those registered with
        # readCurrent(). This could raise ReadConflictError or locking
        # errors. See ``IRelStorageAdapter`` for details.
        conflicts = adapter.lock_objects_and_detect_conflicts(cursor, self.required_tids)
        self.lock_and_vote_times[0] = time.time()
        # Ok, we have now taken database locks: exclusive for each old
        # object we are updating and shared for each we wanted to
        # read-current. From now through ``tpc_finish`` we should do
        # our best to avoid anything that introduces unpredictable,
        # arbitrary latencies or delays. This is especially the case
        # if we're gevent monkey-patched: switching greenlets could
        # take an arbitrary amount of time to get back to *this*
        # greenlet so we can finish the job and release our locks.
        # That means we should limit opportunities that involve IO
        # (database queries) or even taking internal locks that may be
        # contended. We ask both our load and store connections to
        # boost their priority until commit so that any queries we do
        # make get serviced ASAP (some gevent drivers can actually
        # guarantee this), but only right before we're about to do something
        # that could potentially be unbounded or allow switching.
        invalidated_oid_ints = self.__check_and_resolve_conflicts(storage, conflicts)

        blobs_must_be_moved_now = False
        blobhelper = self.blobhelper
        committing_tid_bytes = None
        if self.committing_tid_lock:
            # We've already picked a TID. Must have called undo().
            committing_tid_bytes = self.committing_tid_lock.tid

        try:
            blobhelper.vote(committing_tid_bytes)
        except StorageTransactionError:
            # If this raises an STE, it must be a shared (non-db)
            # blobhelper, and the TID must not be locked.
            assert committing_tid_bytes is None
            blobs_must_be_moved_now = True

        if blobs_must_be_moved_now or LOCK_EARLY:
            logger.log(TRACE, "Locking early (for blobs? %s)", blobs_must_be_moved_now)
            # It is crucial to do this only after locking the current
            # object rows in order to prevent deadlock. (The same order as a regular
            # transaction, just slightly sooner.)
            self._lock_and_move(vote_only=True)

        # New storage protocol
        return invalidated_oid_ints

    @log_timed_only_self
    def __check_and_resolve_conflicts(self, storage, conflicts):
        """
        Either raises an `ConflictError`, or successfully resolves
        all conflicts.

        Returns a set of int OIDs for objects modified in this transaction
        but which were then updated by conflict resolution and so must
        be invalidated.

        All the rows needed for detecting conflicts should be locked against
        concurrent changes.

        :param conflicts: A sequence of information needed for detecting
           and resolving conflicts:
           ``(oid_int, committed_tid_int, tid_this_txn_saw_int, committed_state)``.
           If ``tid_this_txn_saw_int`` is None, it was a read-current check,
           and unless the ``committed_tid_int`` matches the expected value,
           a conflict error is raised.
        """
        # pylint:disable=too-many-locals
        invalidated_oid_ints = set()
        if not conflicts:
            return invalidated_oid_ints

        self.count_conflicts = count_conflicts = len(conflicts)

        # In the past, we didn't load all conflicts from the DB at
        # once, just one at a time. This was because we also fetched
        # the new state data from the DB, and it could be large (if
        # lots of conflicts). But now we use the state we have in our
        # local temp cache for the new state, so we don't need to
        # fetch it, meaning this result will be small...
        #
        # ...almost. The resolution process needs three pickles: the
        # one we tried to save, the one we're based off of, and the
        # one currently committed. Remember we have locked objects at
        # this point, so we need to finish ASAP to not block other
        # transactions; in gevent, we need to also avoid giving up
        # control to the event loop for arbitrary periods of time too
        # as it could take a long time to get back to us.

        # - The one we tried to save (the new one) is passed as a
        # parameter. We read this from our local storage, which is
        # probably in memory and thus fast.
        #
        # - The one currently committed can optionally be passed, and
        # if not, loadSerial() is used to get it. It seems somewhat
        # unlikely that it's not in the local pickle cache, so we
        # probably benefit from returning it in the conflict query.
        #
        # - The one we were based off of is always loaded with
        # loadSerial(). We *possibly* have the one we're based off of
        # already in our storage cache, but there's no guarantee. So
        # it's best to prefetch all these things in order to limit the
        # number of database round-trips and the opportunity to block
        # for arbitrary periods of time.
        logger.debug("Attempting to resolve %d conflicts", count_conflicts)

        required_tids = self.required_tids
        old_states_to_prefetch = []
        actual_conflicts = []
        # First, go through and distinguish read-current conflicts from
        # state conflicts (if the adapter didn't do that already).
        for conflict in conflicts:
            oid_int, committed_tid_int, tid_this_txn_saw_int, _ = conflict
            if tid_this_txn_saw_int is not None:
                # An actual conflict. We need the state.
                actual_conflicts.append(conflict)
                old_states_to_prefetch.append((oid_int, tid_this_txn_saw_int))
            else:
                # A readCurrent entry. Did it conflict?
                # Note that some database adapters (MySQL) may have already raised a
                # UnableToLockRowsToReadCurrentError indicating a conflict. That's a type
                # of ReadConflictError like this.
                expect_tid_int = required_tids[oid_int]
                if committed_tid_int != expect_tid_int:
                    raise VoteReadConflictError(
                        oid=int64_to_8bytes(oid_int),
                        serials=(int64_to_8bytes(committed_tid_int),
                                 int64_to_8bytes(expect_tid_int)))

        if not actual_conflicts:
            # Nothing to prefetch or resolve. No need to go critical,
            # we have no other opportunities to switch.
            return invalidated_oid_ints

        # We're probably going to need to make a database query. Elevate our
        # priority and regain control ASAP.
        self.__enter_critical_phase_until_transaction_end()

        old_states_and_tids = self.cache.prefetch_for_conflicts(
            self.load_connection.cursor,
            old_states_to_prefetch
        )

        tryToResolveConflict = _CachedConflictResolver(
            storage, old_states_and_tids
        ).tryToResolveConflict

        adapter = self.adapter
        read_temp = self.temp_storage.read_temp
        store_temp = self.temp_storage.store_temp

        # The conflicts can be very large binary strings, no need to include
        # them in traceback info. (Plus they could be sensitive.)
        __traceback_info__ = count_conflicts, invalidated_oid_ints

        for conflict in actual_conflicts:
            # Match the names of the arguments used
            oid_int, committed_tid_int, tid_this_txn_saw_int, committedData = conflict

            oid = int64_to_8bytes(oid_int)
            committedSerial = int64_to_8bytes(committed_tid_int)
            oldSerial = int64_to_8bytes(tid_this_txn_saw_int)
            newpickle = read_temp(oid_int)

            # Because we're using the _CachedConflictResolver, we can only loadSerial()
            # one state: the ``oldSerial`` state. Therefore the committedData *must* be
            # given.

            resolved_state = tryToResolveConflict(oid, committedSerial, oldSerial,
                                                  newpickle, committedData)

            if resolved_state is None:
                # unresolvable; kill the whole transaction
                raise ConflictError(
                    oid=oid,
                    serials=(oldSerial, committedSerial),
                    data=newpickle,
                )

            # resolved
            invalidated_oid_ints.add(oid_int)
            store_temp(oid_int, resolved_state, committed_tid_int)

        # We resolved some conflicts, so we need to send them over to the database.
        adapter.mover.replace_temps(
            self.store_connection.cursor,
            self.temp_storage.iter_for_oids(invalidated_oid_ints)
        )

        return invalidated_oid_ints

    @log_timed
    def _lock_and_move(self, vote_only=False):
        # Here's where we take the global commit lock, and
        # allocate the next available transaction id, storing it
        # into history-preserving DBs. But if someone passed us
        # a TID (``restore`` or ``undo``), then it must already be in the DB, and the lock must
        # already be held.
        #
        # If we've prepared the transaction, then the TID must be in the
        # db, the lock must be held, and we must have finished all of our
        # storage actions. This is only expected to be the case when we have
        # a shared blob dir.
        #
        # Returns True if we also committed to the database.
        if self.prepared_txn:
            # Already done; *should* have been vote_only.
            assert self.committing_tid_lock, (self.prepared_txn, self.committing_tid_lock)
            return False

        kwargs = {
            'commit': True
        }
        if self.committing_tid_lock:
            kwargs['committing_tid_int'] = self.committing_tid_lock.tid_int
        if vote_only:
            # Must be voting.
            blob_meth = self.blobhelper.vote
            kwargs['after_selecting_tid'] = lambda tid_int: blob_meth(int64_to_8bytes(tid_int))
            kwargs['commit'] = False

        if vote_only or self.adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE:
            # If we're going to have to make two trips to the database, one to lock it and get a
            # tid and then one to commit and release locks, either because we're
            # just voting right now, not committing, or because the database doesn't
            # support doing that in a single operation, we need to go critical and
            # regain control ASAP so we can complete the operation.
            self.__enter_critical_phase_until_transaction_end()
        committing_tid_int, prepared_txn = self.adapter.lock_database_and_move(
            self.store_connection,
            self.blobhelper,
            self.ude,
            **kwargs
        )

        self.prepared_txn = prepared_txn
        committing_tid_lock = self.committing_tid_lock
        assert committing_tid_lock is None or committing_tid_int == committing_tid_lock.tid_int, (
            committing_tid_int, committing_tid_lock)

        log_msg = "Database lock and tid already allocated: %s"
        if committing_tid_lock is None:
            self.committing_tid_lock = DatabaseLockedForTid(
                int64_to_8bytes(committing_tid_int),
                committing_tid_int,
                self.adapter
            )
            log_msg = "Adapter locked database and allocated tid: %s"

        logger.log(TRACE, log_msg, self.committing_tid_lock)

        return kwargs['commit']

    @log_timed
    def tpc_finish(self, transaction, f=None):
        if transaction is not self.transaction:
            raise StorageTransactionError(
                "tpc_finish called with wrong transaction")
        try:
            finish_entry = time.time()
            # Handle the finishing. We cannot/must not fail now.
            # TODO: Move most of this into the Finish class/module.
            did_commit = self._lock_and_move()
            if did_commit:
                locks_released = time.time()
            assert self.committing_tid_lock is not None, self


            # The IStorage docs say that f() "must be called while the
            # storage transaction lock is held." We don't really have a
            # "storage transaction lock", just the global database lock,
            # that we want to drop as quickly as possible, so it would be
            # nice to drop the commit lock and then call f(). This
            # probably doesn't really matter, though, as ZODB.Connection
            # doesn't use f().
            #
            # If we called `lock_and_move` for the first time in this
            # method, then the adapter will have been asked to go ahead
            # and commit, releasing any locks it can (some adapters do,
            # some don't). So we may or may not have a database lock at
            # this point.
            assert not self.blobhelper.NEEDS_DB_LOCK_TO_FINISH
            try:
                self.blobhelper.finish(self.committing_tid_lock.tid)
            except (IOError, OSError):
                # If something failed to move, that's not really a problem:
                # if we did any moving now, we're just a cache.
                logger.exception(
                    "Failed to update blob-cache; ignoring (will re-download)"
                )


            if f is not None:
                f(self.committing_tid_lock.tid)
            next_phase = Finish(self, not did_commit)
            if not did_commit:
                locks_released = time.time()

            locked_duration = locks_released - self.lock_and_vote_times[0]
            between_vote_and_finish = finish_entry - self.lock_and_vote_times[1]
            do_log_duration_info(
                "Objects were locked by %s for %.3fs",
                AbstractVote.tpc_finish.__wrapped__, # pylint:disable=no-member
                self, None,
                locked_duration,
                perf_logger
            )
            do_log_duration_info(
                "Time between vote exiting and %s entering was %.3fs",
                AbstractVote.tpc_finish.__wrapped__, # pylint:disable=no-member
                self, None,
                between_vote_and_finish,
                perf_logger
            )

            return next_phase, self.committing_tid_lock.tid
        finally:
            self._clear_temp()


class HistoryFree(AbstractVote):
    __slots__ = ()


class HistoryPreserving(AbstractVote):
    __slots__ = ()

    def __init__(self, begin_state):
        # Using undo() requires a new TID, so if we had already begun
        # a transaction by locking the database and allocating a TID,
        # we must preserve that.
        super(HistoryPreserving, self).__init__(begin_state,
                                                begin_state.committing_tid_lock)


class HistoryPreservingDeleteOnly(HistoryPreserving):
    __slots__ = ()

    def _vote(self, storage):
        if self.temp_storage and self.temp_storage.stored_oids:
            raise StorageTransactionError("Cannot store and delete at the same time.")
        # We only get here if we've deleted objects, meaning we hold their row locks.
        # We only delete objects once we hold the commit lock.
        assert self.committing_tid_lock
        # Holding the commit lock put an entry in the transaction table,
        # but we don't want to bump the TID or store that data.
        self.adapter.txncontrol.delete_transaction(
            self.store_connection.cursor,
            self.committing_tid_lock.tid_int
        )
        self.lock_and_vote_times[0] = time.time()
        return ()

    def _lock_and_move(self, vote_only=False):
        # We don't do the final commit,
        # we just prepare.
        self.prepared_txn = self.adapter.txncontrol.commit_phase1(
            self.store_connection,
            self.committing_tid_lock.tid_int
        )
        return False


class _CachedConflictResolver(ConflictResolvingStorage):

    def __init__(self, storage, old_states_and_tids):
        self._old_states_and_tids = old_states_and_tids
        self._crs_transform_record_data = storage._crs_transform_record_data
        self._crs_untransform_record_data = storage._crs_untransform_record_data

    def loadSerial(self, oid, serial):
        state, tid = self._old_states_and_tids[bytes8_to_int64(oid)]
        assert bytes8_to_int64(serial) == tid
        return state
