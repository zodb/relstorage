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

from base64 import encodestring
from relstorage.adapters.interfaces import ITransactionControl
from zope.interface import implements
import logging
import re
import time

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


class PostgreSQLTransactionControl(TransactionControl):
    implements(ITransactionControl)

    def __init__(self, keep_history):
        self.keep_history = keep_history

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        if self.keep_history:
            stmt = """
            SELECT tid, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
        else:
            stmt = """
            SELECT tid, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # nothing has been stored yet
                stmt = "SELECT 0, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)"
                cursor.execute(stmt)

        assert cursor.rowcount == 1
        return cursor.fetchone()

    def add_transaction(self, cursor, tid, username, description, extension,
            packed=False):
        """Add a transaction."""
        if self.keep_history:
            stmt = """
            INSERT INTO transaction
                (tid, packed, username, description, extension)
            VALUES (%s, %s,
                decode(%s, 'base64'), decode(%s, 'base64'),
                decode(%s, 'base64'))
            """
            cursor.execute(stmt, (tid, packed,
                encodestring(username), encodestring(description),
                encodestring(extension)))


class MySQLTransactionControl(TransactionControl):
    implements(ITransactionControl)

    def __init__(self, keep_history, Binary):
        self.keep_history = keep_history
        self.Binary = Binary

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        # Lock in share mode to ensure the data being read is up to date.
        if self.keep_history:
            stmt = """
            SELECT tid, UNIX_TIMESTAMP()
            FROM transaction
            ORDER BY tid DESC
            LIMIT 1
            LOCK IN SHARE MODE
            """
            cursor.execute(stmt)
        else:
            stmt = """
            SELECT tid, UNIX_TIMESTAMP()
            FROM object_state
            ORDER BY tid DESC
            LIMIT 1
            LOCK IN SHARE MODE
            """
            cursor.execute(stmt)
            if not cursor.rowcount:
                # nothing has been stored yet
                stmt = "SELECT 0, UNIX_TIMESTAMP()"
                cursor.execute(stmt)

        assert cursor.rowcount == 1
        tid, timestamp = cursor.fetchone()
        # MySQL does not provide timestamps with more than one second
        # precision.  To provide more precision, if the system time is
        # within one minute of the MySQL time, use the system time instead.
        now = time.time()
        if abs(now - timestamp) <= 60.0:
            timestamp = now
        return tid, timestamp

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


class OracleTransactionControl(TransactionControl):
    implements(ITransactionControl)

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

    def _parse_dsinterval(self, s):
        """Convert an Oracle dsinterval (as a string) to a float."""
        mo = re.match(r'([+-]\d+) (\d+):(\d+):([0-9.]+)', s)
        if not mo:
            raise ValueError(s)
        day, hour, min, sec = [float(v) for v in mo.groups()]
        return day * 86400 + hour * 3600 + min * 60 + sec

    def get_tid_and_time(self, cursor):
        """Returns the most recent tid and the current database time.

        The database time is the number of seconds since the epoch.
        """
        if self.keep_history:
            stmt = """
            SELECT MAX(tid),
                TO_CHAR(TO_DSINTERVAL(SYSTIMESTAMP - TO_TIMESTAMP_TZ(
                '1970-01-01 00:00:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')))
            FROM transaction
            """
            cursor.execute(stmt)
            rows = list(cursor)
        else:
            stmt = """
            SELECT MAX(tid),
                TO_CHAR(TO_DSINTERVAL(SYSTIMESTAMP - TO_TIMESTAMP_TZ(
                '1970-01-01 00:00:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')))
            FROM object_state
            """
            cursor.execute(stmt)
            rows = list(cursor)
            if not rows:
                # nothing has been stored yet
                stmt = """
                SELECT 0,
                TO_CHAR(TO_DSINTERVAL(SYSTIMESTAMP - TO_TIMESTAMP_TZ(
                '1970-01-01 00:00:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')))
                FROM DUAL
                """
                cursor.execute(stmt)
                rows = list(cursor)

        assert len(rows) == 1
        tid, now = rows[0]
        if tid is None:
            tid = 0
        return tid, self._parse_dsinterval(now)

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

