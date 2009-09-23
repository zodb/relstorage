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
"""Locker implementations.
"""

from relstorage.adapters.interfaces import ILocker
from ZODB.POSException import StorageError
from zope.interface import implements
import re

commit_lock_timeout = 30


class Locker(object):

    def __init__(self, database_errors):
        self.database_errors = database_errors


class PostgreSQLLocker(Locker):
    implements(ILocker)

    def hold_commit_lock(self, cursor, ensure_current=False):
        if ensure_current:
            # Hold commit_lock to prevent concurrent commits
            # (for as short a time as possible).
            # Lock transaction and current_object in share mode to ensure
            # conflict detection has the most current data.
            cursor.execute("""
            LOCK TABLE commit_lock IN EXCLUSIVE MODE;
            LOCK TABLE transaction IN SHARE MODE;
            LOCK TABLE current_object IN SHARE MODE
            """)
        else:
            cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")

    def release_commit_lock(self, cursor):
        # no action needed
        pass

    def _pg_version(self, cursor):
        """Return the (major, minor) version of PostgreSQL"""
        cursor.execute("SELECT version()")
        v = cursor.fetchone()[0]
        m = re.search(r"([0-9]+)[.]([0-9]+)", v)
        if m is None:
            raise AssertionError("Unable to detect PostgreSQL version: " + v)
        else:
            return int(m.group(1)), int(m.group(2))

    def _pg_has_advisory_locks(self, cursor):
        """Return true if this version of PostgreSQL supports advisory locks"""
        return self._pg_version(cursor) >= (8, 2)

    def create_pack_lock(self, cursor):
        if not self._pg_has_advisory_locks(cursor):
            cursor.execute("CREATE TABLE pack_lock ()")

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_try_advisory_lock(1)")
            locked = cursor.fetchone()[0]
            if not locked:
                raise StorageError('A pack or undo operation is in progress')
        else:
            # b/w compat
            try:
                cursor.execute("LOCK pack_lock IN EXCLUSIVE MODE NOWAIT")
            except self.database_errors:  # psycopg2.DatabaseError:
                raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_advisory_unlock(1)")
        # else no action needed since the lock will be released at txn commit


class MySQLLocker(Locker):
    implements(ILocker)

    def hold_commit_lock(self, cursor, ensure_current=False):
        cursor.execute("SELECT GET_LOCK(CONCAT(DATABASE(), '.commit'), %s)",
            (commit_lock_timeout,))
        locked = cursor.fetchone()[0]
        if not locked:
            raise StorageError("Unable to acquire commit lock")

    def release_commit_lock(self, cursor):
        cursor.execute("SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.commit'))")

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.pack'), 0)"
        cursor.execute(stmt)
        res = cursor.fetchone()[0]
        if not res:
            raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        stmt = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.pack'))"
        cursor.execute(stmt)


class OracleLocker(Locker):
    implements(ILocker)

    def hold_commit_lock(self, cursor, ensure_current=False):
        # Hold commit_lock to prevent concurrent commits
        # (for as short a time as possible).
        cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")
        if ensure_current:
            # Lock transaction and current_object in share mode to ensure
            # conflict detection has the most current data.
            cursor.execute("LOCK TABLE transaction IN SHARE MODE")
            cursor.execute("LOCK TABLE current_object IN SHARE MODE")

    def release_commit_lock(self, cursor):
        # no action needed
        pass

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = """
        LOCK TABLE pack_lock IN EXCLUSIVE MODE NOWAIT
        """
        try:
            cursor.execute(stmt)
        except self.database_errors:  # cx_Oracle.DatabaseError:
            raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        # No action needed
        pass
