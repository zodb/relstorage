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
TPC protocol state management.

The various states in which a storage instance can find itself during
two-phase commit are complicated. This package presents a set of
objects that encapsulate various possibilities. In this way we can
test independent states...independently, and the state transitions are
explicit.

"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import os

from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError

log = logging.getLogger("relstorage")

# Set the RELSTORAGE_ABORT_EARLY environment variable when debugging
# a failure revealed by the ZODB test suite.  The test suite often fails
# to call tpc_abort in the event of an error, leading to deadlocks.
# This variable causes RelStorage to abort failed transactions
# early rather than wait for an explicit abort.
abort_early = os.environ.get('RELSTORAGE_ABORT_EARLY')

class AbstractTPCState(object):

    # The transaction
    transaction = None
    prepared_txn = None

    aborted = False
    storage = None

    # - store
    # - restore/restoreBlob
    # - deleteObject
    # - undo

    # should raise ReadOnlyError if the storage is read only.

    # - tpc_vote should raise StorageTransactionError

    # Because entering tpc_begin wasn't allowed if the storage was
    # read only, this needs to happen in the "not in transaction"
    # state.

    def tpc_finish(self, transaction, f=None):
        # For the sake of some ZODB tests, we need to implement this everywhere,
        # even if it's not actually usable, and the first thing it needs to
        # do is check the transaction.
        if transaction is not self.transaction:
            raise StorageTransactionError('tpc_finish called with wrong transaction')
        raise NotImplementedError("tpc_finish not allowed in this state.")

    def tpc_abort(self, transaction):
        if transaction is not self.transaction:
            return self

        # the lock is held here
        try:
            try:
                self.storage._rollback_load_connection()
                if self.storage._store_cursor is not None:
                    self.storage._adapter.txncontrol.abort(
                        self.storage._store_conn,
                        self.storage._store_cursor,
                        self.prepared_txn)
                    self.storage._adapter.locker.release_commit_lock(self.storage._store_cursor)
                if self.storage.blobhelper is not None:
                    self.storage.blobhelper.abort()
            finally:
                self._clear_temp()
        finally:
            self.aborted = True
            self.storage._commit_lock.release()
        return NotInTransaction(self.storage)

    def _clear_temp(self):
        # Clear all attributes used for transaction commit.
        # It is assumed that self._lock.acquire was called before this
        # method was called.
        self.storage._cache.clear_temp()
        blobhelper = self.storage.blobhelper
        if blobhelper is not None:
            blobhelper.clear_temp()

class NotInTransaction(AbstractTPCState):
    # The default state, when the storage is not attached to a
    # transaction.

    __slots__ = ('is_read_only',)

    def __init__(self, storage):
        self.is_read_only = storage._is_read_only

    def tpc_abort(self, *args, **kwargs): # pylint:disable=arguments-differ,unused-argument
        # Nothing to do
        return self

    def _no_transaction(self, *args, **kwargs):
        raise StorageTransactionError("No transaction in progress")

    tpc_finish = tpc_vote = _no_transaction

    def store(self, *_args, **_kwargs):
        if self.is_read_only:
            raise ReadOnlyError()
        self._no_transaction()

    restore = deleteObject = undo = restoreBlob = store

    # This object appears to be false.
    def __bool__(self):
        return False
    __nonzero__ = __bool__
