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

from ZODB.POSException import StorageError
from perfmetrics import metricmethod
from relstorage.adapters.interfaces import ILocker
from zope.interface import implements


class Locker(object):

    def __init__(self, options, lock_exceptions):
        self.keep_history = options.keep_history
        self.commit_lock_timeout = options.commit_lock_timeout
        self.commit_lock_id = options.commit_lock_id
        self.lock_exceptions = lock_exceptions


class PostgreSQLLocker(Locker):
    implements(ILocker)

    def __init__(self, options, lock_exceptions, version_detector):
        super(PostgreSQLLocker, self).__init__(
            options=options, lock_exceptions=lock_exceptions)
        self.version_detector = version_detector

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        try:
            if ensure_current:
                # Hold commit_lock to prevent concurrent commits
                # (for as short a time as possible).
                # Lock transaction and current_object in share mode to ensure
                # conflict detection has the most current data.
                if self.keep_history:
                    stmt = """
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE transaction IN SHARE MODE;
                    LOCK TABLE current_object IN SHARE MODE
                    """ % (nowait and ' NOWAIT' or '',)
                else:
                    stmt = """
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE object_state IN SHARE MODE
                    """ % (nowait and ' NOWAIT' or '',)
                cursor.execute(stmt)
            else:
                cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE%s" %
                    (nowait and ' NOWAIT' or '',))
        except self.lock_exceptions:
            if nowait:
                return False
            raise StorageError('Acquiring a commit lock failed')
        return True

    def release_commit_lock(self, cursor):
        # no action needed
        pass

    def _pg_has_advisory_locks(self, cursor):
        """Return true if this version of PostgreSQL supports advisory locks"""
        return self.version_detector.get_version(cursor) >= (8, 2)

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
            except self.lock_exceptions:  # psycopg2.DatabaseError:
                raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        if self._pg_has_advisory_locks(cursor):
            cursor.execute("SELECT pg_advisory_unlock(1)")
        # else no action needed since the lock will be released at txn commit


class MySQLLocker(Locker):
    implements(ILocker)

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        timeout = not nowait and self.commit_lock_timeout or 0
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.commit'), %s)"
        cursor.execute(stmt, (timeout,))
        locked = cursor.fetchone()[0]
        if nowait and locked in (0, 1):
            return bool(locked)
        if not locked:
            raise StorageError("Unable to acquire commit lock")

    def release_commit_lock(self, cursor):
        stmt = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.commit'))"
        cursor.execute(stmt)

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

    def __init__(self, options, lock_exceptions, inputsize_NUMBER):
        super(OracleLocker, self).__init__(
            options=options, lock_exceptions=lock_exceptions)
        self.inputsize_NUMBER = inputsize_NUMBER

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # Hold commit_lock to prevent concurrent commits
        # (for as short a time as possible).
        timeout = not nowait and self.commit_lock_timeout or 0
        status = cursor.callfunc(
            "DBMS_LOCK.REQUEST",
            self.inputsize_NUMBER, (
                self.commit_lock_id,
                6,  # exclusive (X_MODE)
                timeout,
                True,
            ))
        if status != 0:
            if nowait and status == 1:
                return False # Lock failed due to a timeout
            if status >= 1 and status <= 5:
                msg = ('', 'timeout', 'deadlock', 'parameter error',
                    'lock already owned', 'illegal handle')[int(status)]
            else:
                msg = str(status)
            raise StorageError(
                "Unable to acquire commit lock (%s)" % msg)

        # Alternative:
        #cursor.execute("LOCK TABLE commit_lock IN EXCLUSIVE MODE")

        if ensure_current:
            if self.keep_history:
                # Lock transaction and current_object in share mode to ensure
                # conflict detection has the most current data.
                cursor.execute("LOCK TABLE transaction IN SHARE MODE")
                cursor.execute("LOCK TABLE current_object IN SHARE MODE")
            else:
                cursor.execute("LOCK TABLE object_state IN SHARE MODE")
        return True

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
        except self.lock_exceptions:  # cx_Oracle.DatabaseError:
            raise StorageError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        # No action needed
        pass
