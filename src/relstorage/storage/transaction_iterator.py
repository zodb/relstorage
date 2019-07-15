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

logger = __import__('logging').getLogger(__name__)


class TransactionIterator(object):
    """Iterate over the transactions in a RelStorage instance."""

    __slots__ = (
        '_adapter',
        '_conn',
        '_cursor',
        '_closed',
        '_transactions',
        '_index',
    )

    def __init__(self, adapter, start, stop):
        self._adapter = adapter
        self._conn, self._cursor = self._adapter.connmanager.open_for_load()
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
        self._transactions = list(adapter.dbiter.iter_transactions_range(
            self._cursor, start_int, stop_int))
        self._index = 0

    def close(self):
        if self._closed:
            return
        self._adapter.connmanager.close(self._conn, self._cursor)
        self._closed = True
        self._conn = None
        self._cursor = None

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
        res = RelStorageTransactionRecord(self, *params)
        self._index += 1
        return res

    __next__ = next


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
