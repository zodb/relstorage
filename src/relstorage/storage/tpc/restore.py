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
Implements restoring from one database to another.

"""
from __future__ import absolute_import
from __future__ import print_function

from ZODB.POSException import StorageTransactionError

from ZODB.utils import u64 as bytes8_to_int64

from .vote import DatabaseLockedForTid
from .vote import HistoryFree as HFVoteFactory
from .vote import HistoryPreserving as HPVoteFactory


class Restore(object):
    """
    A type of begin state that wraps another begin state and adds the methods needed to
    restore or commit to a particular tid.
    """

    # batcher: An object that accumulates store operations
    # so they can be executed in batch (to minimize latency).
    batcher = None

    # _batcher_row_limit: The number of rows to queue before
    # calling the database.
    batcher_row_limit = 100

    # Methods from the underlying begin implementation we need to
    # expose.
    _COPY_ATTRS = (
        'store',
        'checkCurrentSerialInTransaction',
        'deletObject',
        'undo',
        'tpc_vote',
        'tpc_abort',
        'no_longer_stale',
    )

    def __init__(self, begin_state, committing_tid, status):
        # type: (AbstractBegin, DatabaseLockedForTid, str) -> None
        # This is an extension we use for copyTransactionsFrom;
        # it is not part of the IStorage API.
        assert committing_tid is not None
        self.wrapping = begin_state # type: AbstractTPCState

        # hold the commit lock and add the transaction now.
        # This will prevent anyone else from modifying rows
        # other than this transaction. We currently avoid the temp tables,
        # though, so if we do multiple things in a restore transaction,
        # we could still wind up with locking issues (I think?)
        adapter = begin_state.adapter
        cursor = begin_state.store_connection.cursor
        packed = (status == 'p')
        try:
            committing_tid_lock = DatabaseLockedForTid.lock_database_for_given_tid(
                committing_tid, packed,
                cursor, adapter, begin_state.ude
            )
        except:
            begin_state.store_connection.drop()
            raise

        # This is now only used for restore()
        self.batcher = batcher = adapter.mover.make_batcher(
            cursor,
            self.batcher_row_limit)

        for name in self._COPY_ATTRS:
            try:
                meth = getattr(begin_state, name)
            except AttributeError:
                continue
            else:
                setattr(self, name, meth)

        # Arrange for voting to store our batch too, since
        # the mover is unaware of it.
        factory = begin_state.tpc_vote_factory
        assert factory is HFVoteFactory or factory is HPVoteFactory
        def tpc_vote_factory(state,
                             _f=_HFVoteFactory if factory is HFVoteFactory else _HPVoteFactory,
                             _c=committing_tid_lock,
                             _b=batcher):
            return _f(state, _c, _b)
        begin_state.tpc_vote_factory = tpc_vote_factory
        begin_state.temp_storage = _TempStorageWrapper(begin_state.temp_storage)

    def restore(self, oid, this_tid, data, prev_txn, transaction):
        # Similar to store() (see comments in FileStorage.restore for
        # some differences), but used for importing transactions.
        # Note that *data* can be None.
        # The *prev_txn* "backpointer" optimization/hint is ignored.
        #
        # pylint:disable=unused-argument
        state = self.wrapping
        if transaction is not state.transaction:
            raise StorageTransactionError(self, transaction)

        adapter = state.adapter
        cursor = state.store_connection.cursor
        assert cursor is not None
        oid_int = bytes8_to_int64(oid)
        tid_int = bytes8_to_int64(this_tid)

        # Save the `data`.  Note that `data` can be None.
        # Note also that this doesn't go through the cache.
        state.temp_storage.max_restored_oid = max(state.temp_storage.max_restored_oid,
                                                  oid_int)
        # TODO: Make it go through the cache, or at least the same
        # sort of queing thing, so that we can do a bulk COPY.
        # The way we do it now complicates restoreBlob() and it complicates voting.
        adapter.mover.restore(
            cursor, self.batcher, oid_int, tid_int, data)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, txn):
        self.restore(oid, serial, data, prev_txn, txn)
        state = self.wrapping
        # Restoring the entry for the blob MAY have used the batcher, and
        # we're going to want to foreign-key off of that data when
        # we add blob chunks (since we skip the temp tables).
        # Ideally, we wouldn't need to flush the batcher here
        # (we'd prefer having DEFERRABLE INITIALLY DEFERRED FK
        # constraints, but as-of 8.0 MySQL doesn't support that.)
        self.batcher.flush()
        cursor = state.store_connection.cursor
        state.blobhelper.restoreBlob(cursor, oid, serial, blobfilename)

class _TempStorageWrapper(object):

    __slots__ = (
        'temp_storage',
        'max_restored_oid',
    )

    def __init__(self, temp_storage):
        self.temp_storage = temp_storage
        self.max_restored_oid = 0

    @property
    def max_stored_oid(self):
        return max(self.temp_storage.max_stored_oid, self.max_restored_oid)

    def __len__(self):
        return len(self.temp_storage)

    def __iter__(self):
        return iter(self.temp_storage)

    def __getattr__(self, name):
        return getattr(self.temp_storage, name)

class _VoteFactoryMixin(object):
    __slots__ = ()

    def __init__(self, state, committing_tid_lock, batcher):
        # type: (Restore, Optional[DatabaseLockedForTid], Any) -> None
        super(_VoteFactoryMixin, self).__init__(state)
        # pylint:disable=assigning-non-slot
        self.committing_tid_lock = committing_tid_lock
        self.batcher = batcher

    def _flush_temps_to_db(self, cursor):
        super(_VoteFactoryMixin, self)._flush_temps_to_db(cursor)
        self.batcher.flush()


class _HFVoteFactory(_VoteFactoryMixin, HFVoteFactory):
    __slots__ = ('batcher',)


class _HPVoteFactory(_VoteFactoryMixin, HPVoteFactory):
    __slots__ = ('batcher',)
