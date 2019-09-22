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
Transaction iterator support.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ZODB.BaseStorage import DataRecord
from ZODB.BaseStorage import TransactionRecord
from ZODB.interfaces import StorageStopIteration
from ZODB.utils import p64 as int64_to_8bytes
from ZODB.utils import u64 as bytes8_to_int64

from relstorage._compat import loads
from relstorage.adapters.connections import LoadConnection

logger = __import__('logging').getLogger(__name__)


class _TransactionIterator(object):
    """
    Iterate over the transactions in a RelStorage instance.
    """

    __slots__ = (
        '_adapter',
        '_cursor',
        '_closed',
        '_transactions',
        '_index',
    )

    def __init__(self, adapter, load_connection, start, stop):
        self._adapter = adapter
        self._cursor = load_connection.cursor
        self._closed = False

        if start is not None:
            start_int = bytes8_to_int64(start)
        else:
            start_int = 1
        if stop is not None:
            stop_int = bytes8_to_int64(stop)
        else:
            stop_int = None

        # _transactions: [(tid, username, description, extension, packed)]
        with load_connection.server_side_cursor() as cursor:
            self._transactions = adapter.dbiter.iter_transactions_range(
                cursor, start_int, stop_int)
        self._index = 0

    def close(self):
        self._closed = True
        self._cursor = None
        self._transactions = ()

    def __del__(self):
        # belt-and-suspenders, effective on CPython
        # TODO: Issue a warning if we get here without being closed.
        self.close()

    def iterator(self):
        return self

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._transactions)

    def __getitem__(self, n):
        self._index = n
        return next(self)

    def next(self):
        if self._index >= len(self._transactions):
            self.close() # Don't leak our connection
            raise StorageStopIteration()
        if self._closed:
            raise IOError("TransactionIterator already closed")
        params = self._transactions[self._index]
        res = RelStorageTransactionRecord(
            self,
            params.tid_int,
            params.username,
            params.description,
            params.extension,
            params.packed
        )
        self._index += 1
        return res

    __next__ = next


class HistoryPreservingTransactionIterator(_TransactionIterator):
    """
    Opens a distinct load connection to read transactions.

    Closes this connection when it is closed.
    """
    __slots__ = (
        '_conn',
    )

    def __init__(self, adapter, start, stop):
        self._conn = load_connection = LoadConnection(adapter.connmanager)
        super(HistoryPreservingTransactionIterator, self).__init__(
            adapter, load_connection, start, stop)

    def close(self):
        try:
            if self._conn is not None:
                self._conn.drop()
        finally:
            self._conn = None
            super(HistoryPreservingTransactionIterator, self).close()

class HistoryFreeTransactionIterator(_TransactionIterator):
    """
    Uses the given load connection cursor. Does not close the
    connection or cursor. This way multiple iterators can be
    consistent with each other.

    If we do not do this, then multiple iterators may be inconsistent
    and see different subsets of data.

    Suppose an iterator is opened from a storage, and slightly later a
    second iterator is also opened. The two iterators may produce very
    different data, and attempting to compare them or treat the later
    one as a strict superset of the former will break in a
    history-free storage.

    For both history-free and history-preserving databases, the second
    iterator can include transactions not in the first one.

    In a history-preserving database, absent a pack, that's fine: the
    second iterator will be a strict superset of the first one.

    In a history-free database, however, when an object is changed, it is
    effectively *deleted* from its old transaction; its new state is
    only visible as a record in its new transaction.

    That's also fine: the second iterator will include that new state
    (eventually).

    Where it's not fine is when the `stop` parameter is used. **HF
    RelStorage is only fully consistent as-of the most recent
    committed transaction visible to any given connection.**

    Suppose the first iterator is opened and returns transactions 1,
    2, 3, 4, 5, where 5 is now the highest visible transaction at the
    time it is opened. In transaction 5, the state of object A
    references object C.

    Open a second iterator, which lets the view of the database move
    forward. This time, there's a new transaction, 6: the state of A
    has changed, but still contains a reference to C. If you pass
    `stop=5`, though, you won't see *any* state for A because it's
    only found in transaction 6. If A was the only reference to object
    C, object C now appears to be garbage.

    This comes up in ``zc.zodbdgc``, which does exactly that sequence by
    default, or if you pass ``--days <non-zero>``. If you pass ``--days
    0``, it only uses one iterator, but it still passes an arbitrary
    ``stop`` parameter, making that unsafe as well (it tries to use the
    current time as the value to stop at, in order to view all
    committed transactions consistently as-of this date, but that's
    insufficient and there could be committed transactions more recent
    than that; consider clock differences among machines).

    This causes data loss.

    To avoid this, pass in the storage's load cursor consistently. Only if
    the storage has been `sync()` or committed a transaction would the
    two differ.
    """
    __slots__ = ()


class RelStorageTransactionRecord(TransactionRecord):

    def __init__(self, trans_iter, tid_int, user, desc, ext, packed):
        self._trans_iter = trans_iter
        self._tid_int = tid_int
        tid = int64_to_8bytes(tid_int)
        status = 'p' if packed else ' '
        user = user or b''
        description = desc or b''
        if ext:
            extension = loads(ext)
        else:
            extension = {}

        TransactionRecord.__init__(self, tid, status, user, description, extension)

    def __iter__(self):
        return RecordIterator(self)

    def __repr__(self):
        return '<%s at %x tid=%d status=%r user=%r description=%r>' % (
            self.__class__.__name__,
            id(self),
            self._tid_int,
            self.status,
            self.user,
            self.description
        )

class RecordIterator(object):
    """Iterate over the objects in a transaction."""

    __slots__ = (
        'tid',
        '_records',
        '_index',
    )

    def __init__(self, record):
        # type: (RelStorageTransactionRecord) -> None

        cursor = record._trans_iter._cursor
        adapter = record._trans_iter._adapter
        tid_int = record._tid_int
        self.tid = record.tid
        self._records = list(adapter.dbiter.iter_objects(cursor, tid_int))
        self._index = 0

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._records)

    def __getitem__(self, n):
        self._index = n
        return next(self)

    def next(self):
        if self._index >= len(self._records):
            raise StorageStopIteration()
        params = self._records[self._index]
        res = Record(self.tid, *params)
        self._index += 1
        return res

    __next__ = next


class Record(DataRecord):
    """An object state in a transaction"""

    def __init__(self, tid, oid_int, data):
        DataRecord.__init__(self, int64_to_8bytes(oid_int), tid, data, None)
