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

from zope.interface import implementer

from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError

from ..interfaces import ITPCStateNotInTransaction
from ..._util import Lazy

from .temporary_storage import TemporaryStorage

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

class SharedTPCState(object):
    """
    Contains attributes marking resources that *might* be used during the commit
    process. If any of them are, then the `abort` method takes care of cleaning them up.

    Accessing a resource implicitly begins it, if needed.
    """

    prepared_txn = None
    transaction = None
    not_in_transaction_state = None
    read_only = False # Or we wouldn't allocate this object.

    def __init__(self, initial_state, storage, transaction):
        self.initial_state = initial_state
        self._storage = storage
        self.transaction = transaction

    @Lazy
    def store_connection(self):
        return self._storage._store_connection_pool.borrow()

    @Lazy
    def load_connection(self):
        return self._storage._load_connection

    @Lazy
    def blobhelper(self):
        blobhelper = self._storage.blobhelper
        blobhelper.begin()
        return blobhelper

    @Lazy
    def cache(self):
        return self._storage._cache

    @Lazy
    def adapter(self):
        return self._storage._adapter

    @Lazy
    def temp_storage(self):
        return TemporaryStorage()

    def has_temp_data(self):
        return 'temp_storage' in self.__dict__ and self.temp_storage

    def abort(self, force=False):
        # pylint:disable=no-member,using-constant-test,too-many-branches
        try:
           # Drop locks first.
            if 'store_connection' in self.__dict__:
                store_connection = self.store_connection
                try:
                    if store_connection:
                        # It's possible that this connection/cursor was
                        # already closed if an error happened (which would
                        # release the locks). Don't try to re-open it.
                        self.adapter.locker.release_commit_lock(store_connection.cursor)

                    # Though, this might re-open it.
                    self.adapter.txncontrol.abort(
                        store_connection,
                        self.prepared_txn)

                    if force:
                        store_connection.drop()
                finally:
                    self._storage._store_connection_pool.replace(store_connection)

            if 'load_connection' in self.__dict__:
                if force:
                    self.load_connection.drop()
                else:
                    self.load_connection.rollback_quietly()

            if 'blobhelper' in self.__dict__:
                self.blobhelper.abort()

            if 'temp_storage' in self.__dict__:
                self.temp_storage.close()
        finally:
            for attr in 'store_connection', 'load_connection', 'blobhelper', 'temp_storage':
                self.__dict__.pop(attr, None)


    def release(self):
        # TODO: Release the store_connection back to the pool.
        # pylint:disable=no-member
        if 'store_connection' in self.__dict__:
            self._storage._store_connection_pool.replace(self.store_connection)
            del self.store_connection

        if 'temp_storage' in self.__dict__:
            self.temp_storage.close()
            del self.temp_storage



class AbstractTPCStateDatabaseAvailable(object):

    __slots__ = (
        'shared_state',
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

    def __init__(self, shared_state):
        self.shared_state = shared_state # type: SharedTPCState

    @property
    def transaction(self):
        return self.shared_state.transaction

    def __repr__(self):
        result = "<%s at 0x%x stored_count=%s %s" % (
            type(self).__name__,
            id(self),
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

    def tpc_finish(self, storage, transaction, f=None): # pylint:disable=unused-argument
        # For the sake of some ZODB tests, we need to implement this everywhere,
        # even if it's not actually usable, and the first thing it needs to
        # do is check the transaction.
        if transaction is not self.transaction:
            raise StorageTransactionError('tpc_finish called with wrong transaction')
        raise NotImplementedError("tpc_finish not allowed in this state.")

    def tpc_begin(self, _storage, transaction):
        # Ditto as for tpc_finish
        raise StorageTransactionError('tpc_begin not allowed in this state', type(self))

    def tpc_abort(self, transaction, force=False):
        if not force:
            if transaction is not self.transaction:
                return self


        self.shared_state.abort(force)
        return self.shared_state.initial_state

    def no_longer_stale(self):
        return self

    def stale(self, e):
        return Stale(self, e)

    def close(self):
        if self.shared_state is not None:
            self.tpc_abort(None, True)
            self.shared_state = None


@implementer(ITPCStateNotInTransaction)
class NotInTransaction(object):
    # The default state, when the storage is not attached to a
    # transaction.

    __slots__ = (
        'last_committed_tid_int',
        'read_only',
        'begin_factory',
    )

    transaction = None

    def __init__(self, begin_factory, read_only, committed_tid_int=0):
        self.begin_factory = begin_factory
        self.read_only = read_only
        self.last_committed_tid_int = committed_tid_int

    def tpc_abort(self, *args, **kwargs): # pylint:disable=arguments-differ,unused-argument,signature-differs
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

    def tpc_begin(self,  storage, transaction): # XXX: Signature needs to change.
        if self.read_only:
            raise ReadOnlyError()
        if transaction is self.transaction: # Also handles None.
            raise StorageTransactionError("Duplicate tpc_begin calls for same transaction.")
        state = SharedTPCState(self, storage, transaction)
        try:
            return self.begin_factory(state)
        except:
            state.abort()
            raise

    # def no_longer_stale(self):
    #     return self

    # def stale(self, e):
    #     return Stale(self, e)


    # This object appears to be false.
    def __bool__(self):
        return False
    __nonzero__ = __bool__

    def close(self):
        pass

@implementer(ITPCStateNotInTransaction)
class Stale(object):
    """
    An error that lets us know we are stale
    was encountered.

    Just about all accesses to this object result in
    re-raising that error.
    """

    def __init__(self, previous_state, stale_error):
        self.previous_state = previous_state
        self.stale_error = stale_error

    def _stale(self, *args, **kwargs):
        raise self.stale_error

    store = restore = checkCurrentSerialInTransaction = _stale
    undo = deleteObject = _stale
    tpc_begin = _stale

    def no_longer_stale(self):
        return self.previous_state

    def stale(self, _e):
        return self
