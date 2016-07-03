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

from perfmetrics import metricmethod
from relstorage.adapters.interfaces import ILocker
from .interfaces import UnableToAcquireCommitLockError
from .interfaces import UnableToAcquirePackUndoLockError
from zope.interface import implementer


class Locker(object):

    def __init__(self, options, lock_exceptions):
        self.keep_history = options.keep_history
        self.commit_lock_timeout = options.commit_lock_timeout
        self.commit_lock_id = options.commit_lock_id
        self.lock_exceptions = lock_exceptions


@implementer(ILocker)
class PostgreSQLLocker(Locker):

    _checked_lock_timeout = False
    _has_lock_timeout = None

    def __init__(self, options, lock_exceptions, version_detector):
        super(PostgreSQLLocker, self).__init__(
            options=options, lock_exceptions=lock_exceptions)
        self.version_detector = version_detector

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # in PostgreSQL 9.3+, lock_timeout in ms; if nowait, then ignored
        timeout = (not nowait and self.commit_lock_timeout or 0) * 1000  # ms
        timeout_stmt = ''
        if not self._checked_lock_timeout:
            # This won't change, we're only used for a single database.
            self._has_lock_timeout = self._pg_has_lock_timeout(cursor)
            self._checked_lock_timeout = True

        if self._has_lock_timeout:
            timeout_stmt = 'SET lock_timeout = %d; ' % timeout
        try:
            if ensure_current:
                # Hold commit_lock to prevent concurrent commits
                # (for as short a time as possible).
                # Lock transaction and current_object in share mode to ensure
                # conflict detection has the most current data.
                if self.keep_history:
                    stmt = """
                    %s
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE transaction IN SHARE MODE;
                    LOCK TABLE current_object IN SHARE MODE
                    """ % (timeout_stmt, nowait and ' NOWAIT' or '',)
                else:
                    stmt = """
                    %s
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE object_state IN SHARE MODE
                    """ % (timeout_stmt, nowait and ' NOWAIT' or '',)
                for s in stmt.splitlines():
                    if not s.strip():
                        continue
                    cursor.execute(s)
            else:
                stmt = """
                %s
                LOCK TABLE commit_lock IN EXCLUSIVE MODE%s
                """ % (timeout_stmt, ' NOWAIT' if nowait else '')
                for s in stmt.splitlines():
                    if not s.strip():
                        continue
                    cursor.execute(s)

        except self.lock_exceptions:
            if nowait:
                return False
            raise UnableToAcquireCommitLockError('Acquiring a commit lock failed')
        return True

    def release_commit_lock(self, cursor):
        # no action needed
        pass

    def _pg_has_lock_timeout(self, cursor):
        """Returns True if PostgreSQL 9.3+, supporting lock_timeout"""
        return self.version_detector.get_version(cursor) >= (9, 3)

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        cursor.execute("SELECT pg_try_advisory_lock(1)")
        locked = cursor.fetchone()[0]
        if not locked:
            raise UnableToAcquirePackUndoLockError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        cursor.execute("SELECT pg_advisory_unlock(1)")


@implementer(ILocker)
class MySQLLocker(Locker):

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        timeout = not nowait and self.commit_lock_timeout or 0
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.commit'), %s)"
        cursor.execute(stmt, (timeout,))
        locked = cursor.fetchone()[0]
        if nowait and locked in (0, 1):
            return bool(locked)
        if not locked:
            raise UnableToAcquireCommitLockError("Unable to acquire commit lock")

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
            raise UnableToAcquirePackUndoLockError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        stmt = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.pack'))"
        cursor.execute(stmt)


@implementer(ILocker)
class OracleLocker(Locker):

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
            raise UnableToAcquireCommitLockError(
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
            raise UnableToAcquirePackUndoLockError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        # No action needed
        pass
