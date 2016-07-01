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
"""TransactionControl implementations"""

from relstorage.adapters.interfaces import ITransactionControl
from zope.interface import implementer
import logging

log = logging.getLogger(__name__)


class TransactionControl(object):
    """Abstract base class"""

    def commit_phase1(self, conn, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        return '-'

    def commit_phase2(self, conn, cursor, txn):
        """Final transaction commit.

        txn is the name returned by commit_phase1.
        """
        conn.commit()

    def abort(self, conn, cursor, txn=None):
        """Abort the commit.  If txn is not None, phase 1 is also aborted."""
        conn.rollback()


@implementer(ITransactionControl)
class PostgreSQLTransactionControl(TransactionControl):

    def __init__(self, keep_history, driver):
        self.keep_history = keep_history
        self._Binary = driver.Binary

    def get_tid(self, cursor):
        """Returns the most recent tid."""
        if self.keep_history:
            stmt = """
            SELECT tid
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
        else:
            stmt = """
            SELECT tid
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # nothing has been stored yet
                return 0

        assert cursor.rowcount == 1
        return cursor.fetchone()[0]

    def add_transaction(self, cursor, tid, username, description, extension,
                        packed=False):
        """Add a transaction."""
        if self.keep_history:
            stmt = """
            INSERT INTO transaction
                (tid, packed, username, description, extension)
            VALUES (%s, %s,
                %s, %s,
                %s)
            """
            Binary = self._Binary
            cursor.execute(stmt, (tid, packed,
                                  Binary(username), Binary(description),
                                  Binary(extension)))


@implementer(ITransactionControl)
class MySQLTransactionControl(TransactionControl):

    def __init__(self, keep_history, Binary):
        self.keep_history = keep_history
        self.Binary = Binary

    def get_tid(self, cursor):
        """Returns the most recent tid."""
        if self.keep_history:
            stmt = """
            SELECT tid
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
        else:
            stmt = """
            SELECT tid
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # nothing has been stored yet
                return 0

        assert cursor.rowcount == 1
        return cursor.fetchone()[0]

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        if self.keep_history:
            stmt = """
            INSERT INTO transaction
                (tid, packed, username, description, extension)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(stmt, (
                tid, packed, self.Binary(username),
                self.Binary(description), self.Binary(extension)))


@implementer(ITransactionControl)
class OracleTransactionControl(TransactionControl):

    def __init__(self, keep_history, Binary, twophase):
        self.keep_history = keep_history
        self.Binary = Binary
        self.twophase = twophase

    def commit_phase1(self, conn, cursor, tid):
        """Begin a commit.  Returns the transaction name.

        The transaction name must not be None.

        This method should guarantee that commit_phase2() will succeed,
        meaning that if commit_phase2() would raise any error, the error
        should be raised in commit_phase1() instead.
        """
        if self.twophase:
            conn.prepare()
        return '-'

    def get_tid(self, cursor):
        """Returns the most recent tid.
        """
        if self.keep_history:
            stmt = """
            SELECT MAX(tid)
            FROM transaction
            """
            cursor.execute(stmt)
            rows = list(cursor)
        else:
            stmt = """
            SELECT MAX(tid)
            FROM object_state
            """
            cursor.execute(stmt)
            rows = list(cursor)
            if not rows:
                # nothing has been stored yet
                return 0

        assert len(rows) == 1
        tid = rows[0][0]
        if tid is None:
            tid = 0
        return tid

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        if self.keep_history:
            stmt = """
            INSERT INTO transaction
                (tid, packed, username, description, extension)
            VALUES (:1, :2, :3, :4, :5)
            """
            max_desc_len = 2000
            if len(description) > max_desc_len:
                log.warning('Trimming description of transaction %s '
                    'to %d characters', tid, max_desc_len)
                description = description[:max_desc_len]
            cursor.execute(stmt, (
                tid, packed and 'Y' or 'N', self.Binary(username),
                self.Binary(description), self.Binary(extension)))
