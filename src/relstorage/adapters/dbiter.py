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

from zope.interface import implementer

from relstorage._compat import MAX_TID

from .interfaces import IDatabaseIterator
from .schema import Schema
from .sql import it

class DatabaseIterator(object):
    """
    Abstract base class for database iteration.
    """

    def __init__(self, database_driver):
        """
        :param database_driver: Necessary to bind queries correctly.
        """
        self.driver = database_driver

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
        for oid, state in cursor:
            state = self.driver.binary_column_as_state_type(state)
            yield oid, state


@implementer(IDatabaseIterator)
class HistoryPreservingDatabaseIterator(DatabaseIterator):

    keep_history = True

    def _transaction_iterator(self, cursor):
        """
        Iterate over a list of transactions returned from the database.

        Each row begins with ``(tid, username, description, extension)``
        and may have other columns.
        """
        # Iterating the cursor itself in a generator is not safe if
        # the cursor doesn't actually buffer all the rows *anyway*. If
        # we break from the iterating loop before exhausting all the
        # rows, a subsequent query or close operation can lead to
        # things like MySQL Connector/Python raising
        # InternalError(unread results)
        rows = cursor.fetchall()
        for row in rows:
            tid, username, description, ext = row[:4]
            # Although the transaction interface for username and description are
            # defined as strings, this layer works with bytes. PY3.
            username = self.driver.binary_column_as_bytes(username)
            description = self.driver.binary_column_as_bytes(description)
            ext = self.driver.binary_column_as_bytes(ext)

            yield (tid, username, description, ext) + tuple(row[4:])

    _iter_transactions_query = Schema.transaction.select(
        it.c.tid, it.c.username, it.c.description, it.c.extension
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
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """
        params = {
            'min_tid': start if start else 0,
            'max_tid': stop if stop else MAX_TID
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
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        params = {'oid': oid}
        self._object_exists_query.execute(cursor, params)
        if not cursor.fetchall():
            raise KeyError(oid)

        self._object_history_query.execute(cursor, params)
        return self._transaction_iterator(cursor)


@implementer(IDatabaseIterator)
class HistoryFreeDatabaseIterator(DatabaseIterator):

    keep_history = False

    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields ``(tid, username, description, extension)`` for each transaction.

        This always returns an empty iterable.
        """
        # pylint:disable=unused-argument
        return []

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
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields ``(tid, username, description, extension, packed)``
        for each transaction.
        """
        params = {
            'min_tid': start if start else 0,
            'max_tid': stop if stop else MAX_TID
        }
        self._iter_transactions_range_query.execute(cursor, params)
        return ((tid, b'', b'', b'', True) for (tid,) in cursor)

    _iter_object_history_query = Schema.object_state.select(
        it.c.tid, it.c.state_size
    ).where(
        it.c.zoid == it.bindparam('oid')
    )

    def iter_object_history(self, cursor, oid):
        """
        Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields a single row,
        ``(tid, username, description, extension, pickle_size)``
        """
        self._iter_object_history_query.execute(cursor, {'oid': oid})
        rows = cursor.fetchall()
        if not rows:
            raise KeyError(oid)
        assert len(rows) == 1
        return [(tid, '', '', b'', size) for (tid, size) in rows]
