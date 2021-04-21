# -*- coding: utf-8 -*-
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
Test mixin dealing with different locking scenarios.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading
import time

from functools import partial
from functools import update_wrapper

import transaction

from ZODB.DB import DB
from ZODB.Connection import TransactionMetaData

from ZODB.tests.MinPO import MinPO

from relstorage.storage.interfaces import VoteReadConflictError

from .util import RUNNING_ON_CI
from . import TestCase
from . import skipIfNoConcurrentWriters

def WithAndWithoutInterleaving(func):
    # Expands a test case into two tests, for those that can run
    # both with the stored procs and without it.
    def _interleaved(self):
        adapter = self._storage._adapter
        if not adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE:
            adapter.force_lock_objects_and_detect_conflicts_interleavable = True

        func(self)
        if 'force_lock_objects_and_detect_conflicts_interleavable' in adapter.__dict__:
            del adapter.force_lock_objects_and_detect_conflicts_interleavable

    def _stored_proc(self):
        adapter = self._storage._adapter
        if adapter.DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE:
            # No stored proc version.
            return
        func(self)

    def test(self):
        # Sadly using self.subTest()
        # causes zope-testrunner to lose the exception reports.
        _stored_proc(self)
        _interleaved(self)

    return update_wrapper(test, func)


class TestLocking(TestCase):
    # pylint:disable=abstract-method

    _storage = None

    def make_storage(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def __tiny_commit_time(self):
        # Use a very small commit lock timeout.
        if self._storage._adapter.schema.database_type == 'postgresql':
            return 0.1
        return 1

    def __store_two_for_read_current_error(self, release_extra_storage=False):
        db = self._closing(DB(self._storage, pool_size=1))
        conn = db.open()
        root = conn.root()
        root['object1'] = MinPO('object1')
        root['object2'] = MinPO('object2')
        transaction.commit()

        obj1_oid = root['object1']._p_oid
        obj2_oid = root['object2']._p_oid
        obj1_tid = root['object1']._p_serial
        assert obj1_tid == root['object2']._p_serial

        conn.close()
        # We can't close the DB, that will close the storage that we
        # still need. But we can release its storage, since we'll never use
        # this again.
        if release_extra_storage:
            conn._normal_storage.release()
        return obj1_oid, obj2_oid, obj1_tid, db

    def __read_current_and_lock(self, storage, read_current_oid, lock_oid, tid,
                                begin=True, tx=None):
        tx = tx if tx is not None else TransactionMetaData()
        if begin:
            storage.tpc_begin(tx)
        if read_current_oid is not None:
            storage.checkCurrentSerialInTransaction(read_current_oid, tid, tx)
        storage.store(lock_oid, tid, b'bad pickle2', '', tx)
        storage.tpc_vote(tx)
        return tx

    def __do_check_error_with_conflicting_concurrent_read_current(
            self,
            exception_in_b,
            commit_lock_timeout=None,
            storageA=None,
            storageB=None,
            identical_pattern_a_b=False,
            copy_interleave=('A', 'B'),
            abort=True
    ):
        # pylint:disable=too-many-locals
        root_adapter = self._storage._adapter
        if commit_lock_timeout:
            root_adapter.locker.commit_lock_timeout = commit_lock_timeout
            self._storage._options.commit_lock_timeout = commit_lock_timeout

        if storageA is None:
            storageA = self._closing(self._storage.new_instance())
        if storageB is None:
            storageB = self._closing(self._storage.new_instance())

        should_ileave = root_adapter.force_lock_objects_and_detect_conflicts_interleavable
        if 'A' in copy_interleave:
            storageA._adapter.force_lock_objects_and_detect_conflicts_interleavable = should_ileave
        if 'B' in copy_interleave:
            storageB._adapter.force_lock_objects_and_detect_conflicts_interleavable = should_ileave

        # First, store the two objects in an accessible location.
        obj1_oid, obj2_oid, tid, _db = self.__store_two_for_read_current_error(
            release_extra_storage=True)
        # XXX: We'd like to close the DB here, but the best we can do is to release the
        # extra storage it made.


        # Now transaction A readCurrent 1 and modify 2
        # up through the vote phase
        txa = self.__read_current_and_lock(storageA, obj1_oid, obj2_oid, tid)

        if not identical_pattern_a_b:
            # Second transaction does exactly the opposite, and blocks,
            # raising an exception (usually)
            lock_b = partial(
                self.__read_current_and_lock,
                storageB,
                obj2_oid,
                obj1_oid,
                tid
            )
        else:
            lock_b = partial(
                self.__read_current_and_lock,
                storageB,
                obj1_oid,
                obj2_oid,
                tid
            )

        txb = None
        before = time.time()
        if exception_in_b:
            with self.assertRaises(exception_in_b):
                lock_b()
        else:
            txb = lock_b()
        after = time.time()
        duration_blocking = after - before

        if abort:
            storageA.tpc_abort(txa)
            storageB.tpc_abort(txb)

        return duration_blocking

    @WithAndWithoutInterleaving
    def checkTL_ConflictingReadCurrent(self):
        # Given two objects 1 and 2, if transaction A does readCurrent(1)
        # and modifies 2 up through the voting phase, and then transaction
        # B does precisely the same thing,
        # we get an error after ``commit_lock_timeout``  and not a deadlock.
        from relstorage.adapters.interfaces import UnableToLockRowsToModifyError
        from relstorage.adapters.interfaces import UnableToLockRowsToReadCurrentError

        commit_lock_timeout = self.__tiny_commit_time
        duration_blocking = self.__do_check_error_with_conflicting_concurrent_read_current(
            (UnableToLockRowsToModifyError
             # Oracle has no share locks so we can't distinguish.
             if not self.__is_oracle()
             else UnableToLockRowsToReadCurrentError),
            commit_lock_timeout=commit_lock_timeout,
            identical_pattern_a_b=True
        )
        multiplier = 3
        if RUNNING_ON_CI:
            multiplier = 4.2
        self.assertLessEqual(duration_blocking, commit_lock_timeout * multiplier)

    @WithAndWithoutInterleaving
    def checkTL_ReadCurrentConflict_DoesNotTakeExclusiveLocks(self):
        # Proves that if we try to check an old serial that has already moved on,
        # we don't try taking exclusive locks at all.
        from relstorage.adapters.interfaces import UnableToLockRowsToModifyError
        obj1_oid, obj2_oid, tid, db = self.__store_two_for_read_current_error()
        assert obj1_oid, obj1_oid
        assert obj2_oid, obj2_oid
        assert tid, tid

        root_adapter = self._storage._adapter
        commit_lock_timeout = self.__tiny_commit_time
        root_adapter.locker.commit_lock_timeout = commit_lock_timeout
        self._storage._options.commit_lock_timeout = commit_lock_timeout

        storageA = self._closing(self._storage.new_instance())
        storageC = self._closing(self._storage.new_instance())

        # The Begin phase actually calls into the database when readCurrent()
        # is called to verify the tid. So we actually need to do that much of it now.
        storageB = self._closing(self._storage.new_instance())
        txb = TransactionMetaData()
        storageB.tpc_begin(txb) # Capture our current tid now
        storageB.checkCurrentSerialInTransaction(obj2_oid, tid, txb)

        should_ileave = root_adapter.force_lock_objects_and_detect_conflicts_interleavable
        for s in storageA, storageB, storageC:
            s._adapter.force_lock_objects_and_detect_conflicts_interleavable = should_ileave

        # Walk through a full transaction in A so that the tid changes.
        conn = db.open()
        root = conn.root()
        root['object1'].some_attr = 1
        root['object2'].some_attr = 1
        transaction.commit()
        new_tid = root['object1']._p_serial
        conn.close()
        self.assertGreater(new_tid, tid)

        # Tx A takes exclusive lock on object1
        txa = self.__read_current_and_lock(storageA, None, obj1_oid, new_tid)

        # Prove that it's locked. (This is slow on MySQL 5.7)
        with self.assertRaises(UnableToLockRowsToModifyError):
            self.__read_current_and_lock(storageC, None, obj1_oid, new_tid)

        # Now, try to readCurrent of object2 in the old tid, and take an exclusive lock
        # on obj1. We should immediately get a read current error and not conflict with the
        # exclusive lock.
        with self.assertRaisesRegex(VoteReadConflictError, "serial this txn started"):
            self.__read_current_and_lock(storageB, obj2_oid, obj1_oid, tid, begin=False, tx=txb)

        # Which is still held because we cannot lock it.
        with self.assertRaises(UnableToLockRowsToModifyError):
            self.__read_current_and_lock(storageC, None, obj1_oid, new_tid)

        storageA.tpc_abort(txa)

    @skipIfNoConcurrentWriters
    @WithAndWithoutInterleaving
    def checkTL_OverlappedReadCurrent_SharedLocksFirst(self):
        # Starting with two objects 1 and 2, if transaction A modifies 1 and
        # does readCurrent of 2, up through the voting phase, and transaction B does
        # exactly the opposite, transaction B is immediately killed with a read conflict
        # error. (We use the same two objects instead of a new object in transaction B to prove
        # shared locks are taken first.)
        from relstorage.adapters.interfaces import UnableToLockRowsToReadCurrentError
        commit_lock_timeout = self.__tiny_commit_time
        duration_blocking = self.__do_check_error_with_conflicting_concurrent_read_current(
            UnableToLockRowsToReadCurrentError,
            commit_lock_timeout=commit_lock_timeout,
        )
        # The NOWAIT lock should be very quick to fire.
        assert self._storage._adapter.locker.supports_row_lock_nowait is not None
        if self._storage._adapter.locker.supports_row_lock_nowait:
            multiplier = 1.5
            if RUNNING_ON_CI:
                # Give some extra time on CI since the resources are shared
                # and often extremely limited.
                multiplier = 2.1
            self.assertLessEqual(duration_blocking, commit_lock_timeout * multiplier)
        else:
            # Sigh. Old MySQL. Very slow. This takes around 4.5s to run both iterations.
            self.assertLessEqual(duration_blocking, commit_lock_timeout * 2.5)


    def __lock_rows_being_modified_only(self, storage, cursor,
                                        _current_oids, _share_blocks,
                                        _after_share=lambda: None):
        # A monkey-patch for Locker.lock_current_objects to only take the exclusive
        # locks.
        storage._adapter.locker._lock_rows_being_modified(cursor)

    def __assert_small_blocking_duration(self, storage, duration_blocking):
        # Even though we just went with the default commit_lock_timeout,
        # which is large...
        self.assertGreaterEqual(storage._options.commit_lock_timeout, 10)
        # ...the lock violation happened very quickly...
        # except on Oracle, which takes closer to the actual timeout , for some reason...
        # possibly because we don't have share locks?
        duration = 3
        if self.__is_oracle():
            duration = 12

        self.assertLessEqual(duration_blocking, duration)

    @skipIfNoConcurrentWriters
    def checkTL_InterleavedConflictingReadCurrent(self):
        # Similar to
        # ``checkTL_ConflictingReadCurrent``
        # except that we pause the process immediately after txA takes
        # its exclusive locks to let txB take *its* exclusive locks.
        # Then txB can go for the shared locks, which will block if
        # we're not in ``NOWAIT`` mode for shared locks. This tests
        # that we don't block, we report a retryable error. Note that
        # we don't adjust the commit_lock_timeout; it doesn't apply.
        #
        # If we're using a stored procedure, this test will break
        # because we won't be able to force the interleaving, so we make it
        # use the old version.

        from relstorage.adapters.interfaces import UnableToLockRowsToReadCurrentError
        storageA = self._closing(self._storage.new_instance())
        # This turns off stored procs and lets us control that phase.
        storageA._adapter.force_lock_objects_and_detect_conflicts_interleavable = True

        storageA._adapter.locker.lock_current_objects = partial(
            self.__lock_rows_being_modified_only,
            storageA)

        duration_blocking = self.__do_check_error_with_conflicting_concurrent_read_current(
            UnableToLockRowsToReadCurrentError,
            storageA=storageA,
            copy_interleave=()
        )

        self.__assert_small_blocking_duration(storageA, duration_blocking)

    @skipIfNoConcurrentWriters
    def checkTL_InterleavedConflictingReadCurrentDeadlock(self):
        # pylint:disable=too-many-statements
        # Like
        # ``checkTL_InterleavedConflictingReadCurrent``
        # except that we interleave both txA and txB: txA takes modify
        # lock, txB takes modify lock, txA attempts shared lock, txB
        # attempts shared lock. This results in a database deadlock, which is reported as
        # a retryable error.
        #
        # We have to use a thread to do the shared locks because it blocks.
        from relstorage.adapters.interfaces import UnableToLockRowsToReadCurrentError
        from relstorage.storage.tpc.vote import AbstractVote as VotePhase
        from relstorage.storage.tpc import NotInTransaction

        store_conn_pool = self._storage._store_connection_pool
        self.assertEqual(store_conn_pool.instance_count, 1)

        storageA = self._closing(self._storage.new_instance())
        storageB = self._closing(self._storage.new_instance())
        storageA.last_error = storageB.last_error = None

        self.assertEqual(store_conn_pool.instance_count, 3)

        storageA._adapter.locker.lock_current_objects = partial(
            self.__lock_rows_being_modified_only,
            storageA)
        storageB._adapter.locker.lock_current_objects = partial(
            self.__lock_rows_being_modified_only,
            storageB)

        # This turns off stored procs for locking.
        storageA._adapter.force_lock_readCurrent_for_share_blocking = True
        storageB._adapter.force_lock_readCurrent_for_share_blocking = True

        # This won't actually block, we haven't conflicted yet.
        self.__do_check_error_with_conflicting_concurrent_read_current(
            None,
            storageA=storageA,
            storageB=storageB,
            abort=False
        )
        self.assertEqual(store_conn_pool.instance_count, 3)

        self.assertIsInstance(storageA._tpc_phase, VotePhase)
        self.assertIsInstance(storageB._tpc_phase, VotePhase)

        cond = threading.Condition()
        cond.acquire()
        def lock_shared(storage, notify=True):
            cursor = storage._tpc_phase.shared_state.store_connection.cursor
            read_current_oids = storage._tpc_phase.required_tids.keys()
            if notify:
                cond.acquire(5)
                cond.notifyAll()
                cond.release()

            try:
                storage._adapter.locker._lock_readCurrent_oids_for_share(
                    cursor,
                    read_current_oids,
                    shared_locks_block=True
                )
            except UnableToLockRowsToReadCurrentError as ex:
                storage.last_error = ex
            finally:
                if notify:
                    cond.acquire(5)
                    cond.notifyAll()
                    cond.release()


        thread_locking_a = threading.Thread(
            target=lock_shared,
            args=(storageA,),
            name="thread_locking_a",
        )
        thread_locking_a.start()

        # wait for the background thread to get ready to lock.
        cond.acquire(5)
        cond.wait(5)
        cond.release()

        begin = time.time()

        lock_shared(storageB, notify=False)

        # Wait for background thread to finish.
        cond.acquire(5)
        cond.wait(5)
        cond.release()

        end = time.time()
        duration_blocking = end - begin

        # Now, one or the other storage got killed by the deadlock
        # detector, but NOT both. Which one depends on the database.
        # PostgreSQL likes to kill the foreground thread (storageB),
        # MySQL likes to kill the background thread (storageA)...
        # And Oracle likes to kill them both! Argh!
        self.assertTrue(storageA.last_error or storageB.last_error,
                        (storageA.last_error, storageB.last_error))
        if not self.__is_oracle():
            self.assertFalse(storageA.last_error and storageB.last_error,
                             (storageA.last_error, storageB.last_error))

        last_error = storageA.last_error or storageB.last_error

        self.assertIn('deadlock', str(last_error).lower())

        self.__assert_small_blocking_duration(storageA, duration_blocking)
        self.__assert_small_blocking_duration(storageB, duration_blocking)

        storageA.tpc_abort(storageA._tpc_phase.transaction)
        storageB.tpc_abort(storageB._tpc_phase.transaction)

        self.assertIsInstance(storageA._tpc_phase, NotInTransaction)
        self.assertIsInstance(storageB._tpc_phase, NotInTransaction)

        self.assertEqual(store_conn_pool.instance_count, 3)
        storageA.release()
        storageB.release()

        self.assertLessEqual(store_conn_pool.instance_count, 1)
        self.assertLessEqual(store_conn_pool.pooled_connection_count, 1)

    def __is_oracle(self):
        # This is an anti-pattern. we really should subclass the tests
        # in our testoracle.py file.
        return self._storage._adapter.schema.database_type == 'oracle'
