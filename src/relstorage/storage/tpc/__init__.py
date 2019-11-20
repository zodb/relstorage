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

from transaction.interfaces import NoTransaction
from transaction._transaction import rm_key
from transaction import get as get_thread_local_transaction

from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError

log = logging.getLogger("relstorage")

#: Set the ``RELSTORAGE_LOCK_EARLY`` environment variable if you
#: experience deadlocks or failures to commit (``tpc_finish``). This
#: will cause the commit lock to be taken as part of ``tpc_vote``
#: (similar to RelStorage 2.x) instead of deferring it until
#: ``tpc_finish``.
#:
#: If this is necessary, this is probably a bug in RelStorage; please report
#: it.
LOCK_EARLY = os.environ.get('RELSTORAGE_LOCK_EARLY')

class _StorageFacade(object):
    # makes a storage look like a previous state for construction
    # purposes

    transaction = None
    prepared_txn = None

    def __init__(self, storage):
        self.adapter = storage._adapter
        self.load_connection = storage._load_connection
        self.store_connection = storage._store_connection
        self.blobhelper = storage.blobhelper
        self.cache = storage._cache
        self.read_only = storage._is_read_only

class AbstractTPCState(object):

    __slots__ = (
        'adapter',
        'load_connection',
        'store_connection',
        'transaction',
        'prepared_txn',
        'blobhelper',
        'cache',
        'read_only',
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

    @classmethod
    def from_storage(cls, storage):
        return cls(_StorageFacade(storage), None)

    def __init__(self, previous_state, transaction=None):
        if 0: # pylint:disable=using-constant-test
            # This block (elided from the bytecode)
            # is for pylint static analysis
            self.adapter = self.load_connection = self.store_connection = self.transaction = None
            self.prepared_txn = self.blobhelper = None
            self.cache = None # type: relstorage.cache.storage_cache.StorageCache
            self.read_only = False
        for attr in AbstractTPCState.__slots__:
            val = getattr(previous_state, attr)
            setattr(self, attr, val)

        self.transaction = transaction

    def __repr__(self):
        result = "<%s at 0x%x blobhelper=%r stored_count=%s %s" % (
            type(self).__name__,
            id(self),
            self.blobhelper,
            len(getattr(self, 'temp_storage', ()) or ()),
            self._tpc_state_transaction_data(),
        )

        extra = self._tpc_state_extra_repr_info()
        for k, v in extra.items():
            result += ' %s=%r' % (k, v)
        result += '>'
        return result

    def _tpc_state_extra_repr_info(self):
        return {}

    def _tpc_state_transaction_data(self):
        # Grovels around in the transaction object and tries to find interesting
        # things to include.

        # The ZODB Connection passes us an internal TransactionMetaData
        # object; the real transaction object stores a reference to that in its data,
        # keyed off the connection.
        # We may or may not be able to get the real transaction using transaction.get(),
        # depending on if we are using the global (thread local) transaction manager or not.
        try:
            global_tx = get_thread_local_transaction()
        except NoTransaction:
            # It's in explicit mode and we're not using it.
            return "<no global transaction> tx=%r" % (self.transaction,)

        tx_data = getattr(global_tx, '_data', None)
        if not tx_data:
            # No data stored on the transaction (or the implementation changed!)
            return "<no transaction data> tx=%r" % (self.transaction,)

        for v in tx_data.values():
            if v is self.transaction:
                # Yes, we found the metadata that ZODB uses, so we are
                # joined to this transaction.
                break
        else:
            return "<no transaction meta %r> tx=%r" % (tx_data, self.transaction,)

        resources = sorted(global_tx._resources, key=rm_key)
        return "transaction=%r resources=%r" % (global_tx, resources)


    def tpc_finish(self, transaction, f=None):
        # For the sake of some ZODB tests, we need to implement this everywhere,
        # even if it's not actually usable, and the first thing it needs to
        # do is check the transaction.
        if transaction is not self.transaction:
            raise StorageTransactionError('tpc_finish called with wrong transaction')
        raise NotImplementedError("tpc_finish not allowed in this state.")

    def tpc_abort(self, transaction, force=False):
        if not force:
            if transaction is not self.transaction:
                return self

        try:
            # Drop locks first.
            if self.store_connection:
                # It's possible that this connection/cursor was
                # already closed if an error happened (which would
                # release the locks). Don't try to re-open it.
                self.adapter.locker.release_commit_lock(self.store_connection.cursor)
            self.adapter.txncontrol.abort(
                self.store_connection,
                self.prepared_txn)

            if force:
                self.load_connection.drop()
                self.store_connection.drop()
            else:
                self.load_connection.rollback_quietly()
            self.blobhelper.abort()
        finally:
            self._clear_temp()
        return NotInTransaction(self)

    def _clear_temp(self):
        """
        Clear all attributes used for transaction commit.
        Subclasses should override. Called on tpc_abort; subclasses
        should call on other exit states.
        """

    def tpc_begin(self, transaction, begin_factory):
        if transaction is self.transaction:
            raise StorageTransactionError("Duplicate tpc_begin calls for same transaction.")
        # XXX: Shouldn't we tpc_abort() first (well, not that exactly, because
        # the transaction won't match, but logically)? The original storage
        # code didn't do that, but it seems like it should.
        return begin_factory(self, transaction)

    def no_longer_stale(self):
        return self

    def stale(self, e):
        return Stale(self, e)


class NotInTransaction(AbstractTPCState):
    # The default state, when the storage is not attached to a
    # transaction.

    __slots__ = ()

    def __init__(self, previous_state, transaction=None):
        super(NotInTransaction, self).__init__(previous_state)
        # Reset some things that need to go away.
        self.prepared_txn = None

    def tpc_abort(self, *args, **kwargs): # pylint:disable=arguments-differ,unused-argument
        # Nothing to do
        return self

    def _no_transaction(self, *args, **kwargs):
        raise StorageTransactionError("No transaction in progress")

    tpc_finish = tpc_vote = _no_transaction

    def store(self, *_args, **_kwargs):
        if self.read_only:
            raise ReadOnlyError()
        self._no_transaction()

    restore = deleteObject = undo = restoreBlob = store

    def tpc_begin(self, transaction, begin_factory):
        if self.read_only:
            raise ReadOnlyError()
        return super(NotInTransaction, self).tpc_begin(transaction, begin_factory)

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
        super(Stale, self).__init__(previous_state, None)
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
