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
Base class for ``IRelStorageAdapter``.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from persistent.timestamp import TimeStamp

from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage.storage.interfaces import VoteReadConflictError
from .._compat import metricmethod_sampled
from .._util import timestamp_at_unixtime
from .._util import TRACE
from ..options import Options

from .drivers import _select_driver
from .interfaces import UnableToLockRowsToModifyError
from .interfaces import UnableToLockRowsToReadCurrentError

logger = __import__('logging').getLogger(__name__)

class AbstractAdapter(object):

    keep_history = None # type: bool
    options = None # type: Options
    driver_options = None # type: IDBDriverOptions
    locker = None # type: ILocker
    txncontrol = None # type: ITransactionControl
    mover = None # type: IObjectMover
    connmanager = None # type: IConnectionManager
    oidallocator = None # type: IOIDAllocator

    def __init__(self, options=None):
        if options is None:
            options = Options()
        self.options = options
        self.keep_history = options.keep_history

        self.driver = driver = self._select_driver()
        self._binary = driver.Binary

        self._create()

        self.connmanager.add_on_store_opened(self.mover.on_store_opened)
        self.connmanager.add_on_load_opened(self.mover.on_load_opened)
        self.connmanager.add_on_store_opened(self.locker.on_store_opened)

    def _create(self):
        raise NotImplementedError

    def release(self):
        if self.oidallocator is not None:
            self.oidallocator.release()
            self.oidallocator = None

    def close(self):
        if self.oidallocator is not None:
            self.oidallocator.close()
            self.oidallocator = None

    def _select_driver(self, options=None):
        return _select_driver(
            options or self.options or Options(),
            self.driver_options
        )

    def __repr__(self):
        return "<%s.%s at 0x%x keep_history=%s driver=%s>" % (
            self.__class__.__module__,
            self.__class__.__name__,
            id(self),
            self.keep_history,
            self.driver,
        )

    @metricmethod_sampled
    def lock_database_and_choose_next_tid(self, cursor,
                                          username,
                                          description,
                                          extension):
        self.locker.hold_commit_lock(cursor, ensure_current=True)

        # Choose a transaction ID.
        #
        # Base the transaction ID on the current time, but ensure that
        # the tid of this transaction is greater than any existing
        # tid.
        last_tid = self.txncontrol.get_tid(cursor)
        now = time.time()
        stamp = timestamp_at_unixtime(now)
        stamp = stamp.laterThan(TimeStamp(int64_to_8bytes(last_tid)))
        tid = stamp.raw()

        tid_int = bytes8_to_int64(tid)
        self.txncontrol.add_transaction(cursor, tid_int, username, description, extension)
        logger.log(TRACE, "Picked next tid locally: %s", tid_int)
        return tid_int

    @metricmethod_sampled
    def lock_database_and_move(self,
                               store_connection,
                               blobhelper,
                               ude,
                               commit=True,
                               committing_tid_int=None,
                               after_selecting_tid=lambda tid: None):
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

        cursor = store_connection.cursor
        if committing_tid_int is None:
            committing_tid_int = self.lock_database_and_choose_next_tid(
                cursor,
                *ude
            )

        # Move the new states into the permanent table
        # TODO: Figure out how to do as much as possible of this before holding
        # the commit lock. For example, use a dummy TID that we later replace.
        # (This has FK issues in HP dbs).
        txn_has_blobs = blobhelper.txn_has_blobs

        self.mover.move_from_temp(cursor, committing_tid_int, txn_has_blobs)

        after_selecting_tid(committing_tid_int)

        self.mover.update_current(cursor, committing_tid_int)
        prepared_txn_id = self.txncontrol.commit_phase1(
            store_connection, committing_tid_int)

        if commit:
            self.txncontrol.commit_phase2(store_connection, prepared_txn_id)

        return committing_tid_int, prepared_txn_id

    DEFAULT_LOCK_OBJECTS_AND_DETECT_CONFLICTS_INTERLEAVABLE = True
    WRITING_REQUIRES_EXCLUSIVE_LOCK = False

    # Hooks for unit tests.
    force_lock_objects_and_detect_conflicts_interleavable = False
    force_lock_readCurrent_for_share_blocking = False


    @metricmethod_sampled
    def lock_objects_and_detect_conflicts(self, cursor, read_current_oids):
        if (
                self.force_lock_readCurrent_for_share_blocking
                or self.force_lock_objects_and_detect_conflicts_interleavable
        ):
            # Delegate to the individual statements that can control lock timeouts,
            # or that allow a controlling test to carefully interleave operations to simulate
            # various concurrency situations.
            return self._composed_lock_objects_and_detect_conflicts(cursor,
                                                                    read_current_oids)
        begin = time.time()
        try:
            return self._best_lock_objects_and_detect_conflicts(cursor, read_current_oids)
        except self.locker.lock_exceptions:
            # Heuristic to guess. If the stored proc or stored proc runner can do better,
            # they should.
            elapsed = time.time() - begin
            kind = UnableToLockRowsToModifyError
            if read_current_oids and elapsed < self.locker.commit_lock_timeout:
                kind = UnableToLockRowsToReadCurrentError

            self.locker.reraise_commit_lock_error(
                cursor,
                self._describe_best_lock_objects_and_detect_conflicts(),
                kind
            )

    def _composed_lock_objects_and_detect_conflicts(self, cursor, read_current_oids):
        read_current_oid_ints = read_current_oids.keys()

        def after_lock_share():
            current = self.mover.current_object_tids(cursor, read_current_oid_ints)
            # We go ahead and compare the readCurrent TIDs here, so
            # that we don't have to make the call to detect conflicts
            # or even lock rows if there are readCurrent violations.
            for oid_int, expect_tid_int in read_current_oids.items():
                actual_tid_int = current.get(oid_int, 0)
                if actual_tid_int != expect_tid_int:
                    raise VoteReadConflictError(
                        oid=int64_to_8bytes(oid_int),
                        serials=(int64_to_8bytes(actual_tid_int),
                                 int64_to_8bytes(expect_tid_int)))

        self.locker.lock_current_objects(
            cursor, read_current_oid_ints,
            self.force_lock_readCurrent_for_share_blocking,
            after_lock_share)

        conflicts = self.mover.detect_conflict(cursor)
        return conflicts

    #: Subclasses that have the ability to implement
    #: :meth:`lock_objects_and_detect_conflicts` in a single database
    #: call, or otherwise do better than our
    #: :meth:`_composed_lock_objects_and_detect_conflicts` implementation,
    #: should override this method. It *must* return a materialized list
    #: of conflicts that supports len() and iterating multiple times.
    _best_lock_objects_and_detect_conflicts = _composed_lock_objects_and_detect_conflicts

    def _describe_best_lock_objects_and_detect_conflicts(self):
        return '<unknown>'
