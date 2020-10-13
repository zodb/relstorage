##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
from __future__ import absolute_import
from __future__ import print_function

from collections import namedtuple

from zope.interface import implementer

from ZODB.utils import p64 as int64_to_8bytes

from relstorage._compat import TidList
from relstorage._util import Lazy

from ._util import DatabaseHelpersMixin
from .interfaces import IDatabaseIterator
from .schema import Schema
from .sql import it

class DatabaseIterator(DatabaseHelpersMixin):
    """
    Abstract base class for database iteration.
    """

    def __init__(self, database_driver):
        """
        :param database_driver: Necessary to bind queries correctly.
        """
        self.driver = database_driver

    @Lazy
    def _as_state(self):
        return self.driver.binary_column_as_state_type

    @Lazy
    def _as_bytes(self):
        return self.driver.binary_column_as_bytes

    _iter_objects_query = Schema.object_state.select(
        it.c.zoid,
        it.c.state
    ).where(
        it.c.tid == it.bindparam('tid')
    ).order_by(
        it.c.zoid
    )

    def iter_objects(self, cursor, tid):
        """Iterate over object states in a transaction.

        Yields ``(oid, state)`` for each object in the transaction.
        """
        self._iter_objects_query.execute(cursor, {'tid': tid})
        as_state = self._as_state
        for oid, state in cursor:
            state = as_state(state) # pylint:disable=too-many-function-args
            yield oid, state

    _iter_current_records = Schema.all_current_object_state.select(
        it.c.zoid,
        it.c.tid,
        it.c.state
    ).where(
        it.c.zoid >= it.bindparam('start_oid')
    ).order_by(
        it.c.zoid
    )

    def iter_current_records(self, cursor, start_oid_int=0):
        """
        Cause the *cursor* (which should be a server-side cursor)
        to execute a query that will iterate over
        ``(oid_int, tid_int, state_bytes)`` values for all the current objects.

        Each current object is returned only once, at the transaction most recently
        committed for it.

        Objects are iterated or order of their OID for compatibility with FileStorage.

        Returns a generator.
        """
        self._iter_current_records.execute(cursor, {'start_oid': start_oid_int})
        i_b = int64_to_8bytes
        s = self._as_state
        for oid_int, tid_int, state_bytes in cursor:
            yield i_b(oid_int), i_b(tid_int), s(state_bytes) # pylint:disable=too-many-function-args


class _HistoryPreservingTransactionRecord(namedtuple(
        '_HistoryPreservingTransactionRecord',
        ('tid_int', 'username', 'description', 'extension', 'packed')
)):
    __slots__ = ()

    @property
    def pickle_size(self):
        return self.packed


@implementer(IDatabaseIterator)
class HistoryPreservingDatabaseIterator(DatabaseIterator):

    keep_history = True

    def _transaction_iterator(self, cursor):
        """
        Iterate over a list of transactions returned from the database.

        Each row is ``(tid, username, description, extension, X)``
        """
        # Iterating the cursor itself in a generator is not safe if
        # the cursor doesn't actually buffer all the rows *anyway*. If
        # we break from the iterating loop before exhausting all the
        # rows, a subsequent query or close operation can lead to
        # things like MySQL Connector/Python raising
        # InternalError(unread results)
        # Because we have it all in memory anyway, there's not much point in
        # making this a generator.

        # Although the transaction interface for username and description are
        # defined as strings, this layer works with bytes. The ZODB layer
        # does the conversion.
        as_bytes = self._as_bytes
        # pylint:disable=too-many-function-args
        return [
            _HistoryPreservingTransactionRecord(
                tid,
                as_bytes(username),
                as_bytes(description),
                as_bytes(ext),
                packed
            )
            for (tid, username, description, ext, packed)
            in cursor
        ]

    _iter_transactions_query = Schema.transaction.select(
        it.c.tid, it.c.username, it.c.description, it.c.extension, 0
    ).where(
        it.c.packed == False # pylint:disable=singleton-comparison
    ).and_(
        it.c.tid != 0
    ).order_by(
        it.c.tid, 'DESC'
    )

    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """
        self._iter_transactions_query.execute(cursor)
        return self._transaction_iterator(cursor)

    _iter_transactions_range_query = Schema.transaction.select(
        it.c.tid,
        it.c.username,
        it.c.description,
        it.c.extension,
        it.c.packed,
    ).where(
        it.c.tid >= it.bindparam('min_tid')
    ).and_(
        it.c.tid <= it.bindparam('max_tid')
    ).order_by(
        it.c.tid
    )

    def iter_transactions_range(self, cursor, start=None, stop=None):
        """
        See `IDatabaseIterator`.
        """
        params = {
            'min_tid': start if start else 0,
            'max_tid': stop if stop else self.MAX_TID
        }
        self._iter_transactions_range_query.execute(cursor, params)
        return self._transaction_iterator(cursor)

    _object_exists_query = Schema.current_object.select(
        1
    ).where(
        it.c.zoid == it.bindparam('oid')
    )

    _object_history_query = Schema.transaction.natural_join(
        Schema.object_state
    ).select(
        it.c.tid, it.c.username, it.c.description, it.c.extension,
        Schema.object_state.c.state_size
    ).where(
        it.c.zoid == it.bindparam("oid")
    ).and_(
        it.c.packed == False # pylint:disable=singleton-comparison
    ).order_by(
        it.c.tid, "DESC"
    )

    def iter_object_history(self, cursor, oid):
        """
        See `IDatabaseIterator`
        Raises KeyError if the object does not exist.
        """
        params = {'oid': oid}
        self._object_exists_query.execute(cursor, params)
        if not cursor.fetchall():
            raise KeyError(oid)

        self._object_history_query.execute(cursor, params)
        return self._transaction_iterator(cursor)

class _HistoryFreeTransactionRecord(object):
    __slots__ = ('tid_int',)

    username = b''
    description = b''
    extension = b''
    packed = True

    def __init__(self, tid):
        self.tid_int = tid


class _HistoryFreeObjectHistoryRecord(_HistoryFreeTransactionRecord):
    __slots__ = ('pickle_size',)

    def __init__(self, tid, size):
        _HistoryFreeTransactionRecord.__init__(self, tid)
        self.pickle_size = size


class _HistoryFreeTransactionRange(object):
    # By storing just the int, and materializing the records on demand, we
    # save substantial amounts of memory. For example, 18MM records on PyPy
    # went from about 3.5GB to about 0.5GB
    __slots__ = (
        'tid_ints',
    )

    def __init__(self, tid_ints):
        self.tid_ints = tid_ints

    def __len__(self):
        return len(self.tid_ints)

    def __getitem__(self, ix):
        return _HistoryFreeTransactionRecord(self.tid_ints[ix])

@implementer(IDatabaseIterator)
class HistoryFreeDatabaseIterator(DatabaseIterator):

    keep_history = False

    def iter_transactions(self, cursor):
        """
        This always returns an empty iterable.
        """
        # pylint:disable=unused-argument
        return ()

    _iter_transactions_range_query = Schema.object_state.select(
        it.c.tid,
    ).where(
        it.c.tid >= it.bindparam('min_tid')
    ).and_(
        it.c.tid <= it.bindparam('max_tid')
    ).order_by(
        it.c.tid
    ).distinct()

    def iter_transactions_range(self, cursor, start=None, stop=None):
        """
        See `IDatabaseIterator`.
        """
        params = {
            'min_tid': start if start else 0,
            'max_tid': stop if stop else self.MAX_TID
        }
        self._iter_transactions_range_query.execute(cursor, params)
        return _HistoryFreeTransactionRange(TidList((tid for (tid,) in cursor)))

    _iter_object_history_query = Schema.object_state.select(
        it.c.tid, it.c.state_size
    ).where(
        it.c.zoid == it.bindparam('oid')
    )

    def iter_object_history(self, cursor, oid):
        """
        See `IDatabaseIterator`

        Yields a single row.
        """
        self._iter_object_history_query.execute(cursor, {'oid': oid})
        rows = cursor.fetchall()
        if not rows:
            raise KeyError(oid)
        assert len(rows) == 1
        tid, size = rows[0]
        return [_HistoryFreeObjectHistoryRecord(tid, size)]
