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

from base64 import decodestring
from relstorage.adapters.interfaces import IDatabaseIterator
from zope.interface import implements


class DatabaseIterator(object):
    """Abstract base class for database iteration.
    """

    def __init__(self, database_type, runner):
        self.use_base64 = (database_type == 'postgresql')
        self.runner = runner

    def iter_objects(self, cursor, tid):
        """Iterate over object states in a transaction.

        Yields (oid, prev_tid, state) for each object state.
        """
        if self.use_base64:
            stmt = """
            SELECT zoid, encode(state, 'base64')
            FROM object_state
            WHERE tid = %(tid)s
            ORDER BY zoid
            """
        else:
            stmt = """
            SELECT zoid, state
            FROM object_state
            WHERE tid = %(tid)s
            ORDER BY zoid
            """
        self.runner.run_script_stmt(cursor, stmt, {'tid': tid})
        for oid, state in cursor:
            if hasattr(state, 'read'):
                # Oracle
                state = state.read()
            if state is not None and self.use_base64:
                state = decodestring(state)
            yield oid, state


class HistoryPreservingDatabaseIterator(DatabaseIterator):
    implements(IDatabaseIterator)

    def _transaction_iterator(self, cursor):
        """Iterate over a list of transactions returned from the database.

        Each row begins with (tid, username, description, extension)
        and may have other columns.
        """
        use_base64 = self.use_base64
        for row in cursor:
            tid, username, description, ext = row[:4]
            if username is None:
                username = ''
            else:
                username = str(username)
                if use_base64:
                    username = decodestring(username)
            if description is None:
                description = ''
            else:
                description = str(description)
                if use_base64:
                    description = decodestring(description)
            if ext is None:
                ext = ''
            else:
                ext = str(ext)
                if use_base64:
                    ext = decodestring(ext)
            yield (tid, username, description, ext) + tuple(row[4:])


    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """
        if self.use_base64:
            stmt = """
            SELECT tid, encode(username, 'base64'),
                encode(description, 'base64'), encode(extension, 'base64')
            FROM transaction
            WHERE packed = %(FALSE)s
                AND tid != 0
            ORDER BY tid DESC
            """
        else:
            stmt = """
            SELECT tid, username, description, extension
            FROM transaction
            WHERE packed = %(FALSE)s
                AND tid != 0
            ORDER BY tid DESC
            """
        self.runner.run_script_stmt(cursor, stmt)
        return self._transaction_iterator(cursor)


    def iter_transactions_range(self, cursor, start=None, stop=None):
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """
        if self.use_base64:
            stmt = """
            SELECT tid, encode(username, 'base64'),
                encode(description, 'base64'), encode(extension, 'base64'),
                CASE WHEN packed = %(TRUE)s THEN 1 ELSE 0 END
            FROM transaction
            WHERE tid >= 0
            """
        else:
            stmt = """
            SELECT tid, username, description, extension,
                CASE WHEN packed = %(TRUE)s THEN 1 ELSE 0 END
            FROM transaction
            WHERE tid >= 0
            """
        if start is not None:
            stmt += " AND tid >= %(min_tid)s"
        if stop is not None:
            stmt += " AND tid <= %(max_tid)s"
        stmt += " ORDER BY tid"
        self.runner.run_script_stmt(cursor, stmt,
            {'min_tid': start, 'max_tid': stop})
        return self._transaction_iterator(cursor)


    def iter_object_history(self, cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        stmt = """
        SELECT 1 FROM current_object WHERE zoid = %(oid)s
        """
        self.runner.run_script_stmt(cursor, stmt, {'oid': oid})
        if not cursor.fetchall():
            raise KeyError(oid)

        if self.use_base64:
            stmt = """
            SELECT tid, encode(username, 'base64'),
                encode(description, 'base64'), encode(extension, 'base64'),
                state_size
            """
        else:
            stmt = """
            SELECT tid, username, description, extension, state_size
            """
        stmt += """
        FROM transaction
            JOIN object_state USING (tid)
        WHERE zoid = %(oid)s
            AND packed = %(FALSE)s
        ORDER BY tid DESC
        """
        self.runner.run_script_stmt(cursor, stmt, {'oid': oid})
        return self._transaction_iterator(cursor)


class HistoryFreeDatabaseIterator(DatabaseIterator):
    implements(IDatabaseIterator)

    def iter_transactions(self, cursor):
        """Iterate over the transaction log, newest first.

        Skips packed transactions.
        Yields (tid, username, description, extension) for each transaction.
        """
        return []

    def iter_transactions_range(self, cursor, start=None, stop=None):
        """Iterate over the transactions in the given range, oldest first.

        Includes packed transactions.
        Yields (tid, username, description, extension, packed)
        for each transaction.
        """
        stmt = """
        SELECT DISTINCT tid
        FROM object_state
        WHERE tid > 0
        """
        if start is not None:
            stmt += " AND tid >= %(min_tid)s"
        if stop is not None:
            stmt += " AND tid <= %(max_tid)s"
        stmt += " ORDER BY tid"
        self.runner.run_script_stmt(cursor, stmt,
            {'min_tid': start, 'max_tid': stop})
        return ((tid, '', '', '', True) for (tid,) in cursor)

    def iter_object_history(self, cursor, oid):
        """Iterate over an object's history.

        Raises KeyError if the object does not exist.
        Yields (tid, username, description, extension, pickle_size)
        for each modification.
        """
        stmt = """
        SELECT tid, state_size
        FROM object_state
        WHERE zoid = %(oid)s
        """
        self.runner.run_script_stmt(cursor, stmt, {'oid': oid})
        return ((tid, '', '', '', size) for (tid, size) in cursor)
