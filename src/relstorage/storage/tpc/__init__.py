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

#: Set the ``RELSTORAGE_ABORT_EARLY`` environment variable when debugging
#: a failure revealed by the ZODB test suite.  The test suite often fails
#: to call tpc_abort in the event of an error, leading to deadlocks.
#: This variable causes RelStorage to abort failed transactions
#: early rather than wait for an explicit abort.
#:
#: TODO: Now that we lock rows explicitly, maybe this should now be the default?
ABORT_EARLY = os.environ.get('RELSTORAGE_ABORT_EARLY')

#: Set the ``RELSTORAGE_LOCK_EARLY`` environment variable if you
#: experience deadlocks or failures to commit (``tpc_finish``). This
#: will cause the commit lock to be taken as part of ``tpc_vote``
#: (similar to RelStorage 2.x) instead of deferring it until
#: ``tpc_finish``.
#:
#: If this is necessary, this is probably a bug in RelStorage; please report
#: it.
LOCK_EARLY = os.environ.get('RELSTORAGE_LOCK_EARLY')

class AbstractTPCState(object):

    __slots__ = (
        'storage',
        'transaction',
        'prepared_txn',
    )

    # - store
    # - restore/restoreBlob
    # - deleteObject
    # - undo

    # should raise ReadOnlyError if the storage is read only.

    # - tpc_vote should raise StorageTransactionError

    # Because entering tpc_begin wasn't allowed if the storage was
    # read only, this needs to happen in the "not in transaction"
    # state.

    def __init__(self, storage, transaction=None):
        self.storage = storage         # XXX: This introduces a cycle.
        self.transaction = transaction
        self.prepared_txn = None

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

        try:
            try:
                self.storage._rollback_load_connection()
                if self.storage._store_cursor is not None:
                    self.storage._adapter.txncontrol.abort(
                        self.storage._store_conn,
                        self.storage._store_cursor,
                        self.prepared_txn)
                    self.storage._adapter.locker.release_commit_lock(self.storage._store_cursor)
                self.storage.blobhelper.abort()
            finally:
                self._clear_temp()
        finally:
            self.storage._commit_lock.release()
        return NotInTransaction(self.storage)

    def _clear_temp(self):
        # Clear all attributes used for transaction commit.
        # It is assumed that self._lock.acquire was called before this
        # method was called.
        self.storage._cache.clear_temp()

    def tpc_begin(self, transaction):
        if transaction is self.transaction:
            raise StorageTransactionError("Duplicate tpc_begin calls for same transaction.")
        # XXX: Shouldn't we tpc_abort() first (well, not that exactly, because
        # the transaction won't match, but logically)? The original storage
        # code didn't do that, but it seems like it should.
        return self.storage._tpc_begin_factory(self.storage, transaction)

    def no_longer_stale(self):
        return self

    def stale(self, e):
        return Stale(self, e)

class NotInTransaction(AbstractTPCState):
    # The default state, when the storage is not attached to a
    # transaction.

    def tpc_abort(self, *args, **kwargs): # pylint:disable=arguments-differ,unused-argument
        # Nothing to do
        return self

    def _no_transaction(self, *args, **kwargs):
        raise StorageTransactionError("No transaction in progress")

    tpc_finish = tpc_vote = _no_transaction

    def store(self, *_args, **_kwargs):
        if self.storage._is_read_only:
            raise ReadOnlyError()
        self._no_transaction()

    restore = deleteObject = undo = restoreBlob = store

    def tpc_begin(self, transaction):
        if self.storage._is_read_only:
            raise ReadOnlyError()
        return super(NotInTransaction, self).tpc_begin(transaction)

    # This object appears to be false.
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class Stale(AbstractTPCState):
    """
    An error that lets us know we are stale
    was encountered.

    Just about all accesses to this object result in
    re-raising that error.
    """

    def __init__(self, previous_state, stale_error):
        super(Stale, self).__init__(None, None)
        self.previous_state = previous_state
        self.stale_error = stale_error

    def _stale(self, *args, **kwargs):
        raise self.stale_error

    store = restore = checkCurrentSerialInTransaction = _stale
    undo = deleteObject = _stale
    tpc_begin = _stale

    def no_longer_stale(self):
        return self.previous_state

    def stale(self, e):
        return self
