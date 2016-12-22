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
"""
Locker implementations.
"""

from __future__ import absolute_import

from perfmetrics import metricmethod
from ..locker import AbstractLocker
from ..interfaces import ILocker
from ..interfaces import UnableToAcquireCommitLockError
from ..interfaces import UnableToAcquirePackUndoLockError
from zope.interface import implementer

class CommitLockQueryFailedError(UnableToAcquireCommitLockError):
    pass

@implementer(ILocker)
class MySQLLocker(AbstractLocker):
    """
    MySQL locks.

    Unlike PostgreSQL and Oracle locks which are implicitly released
    at the end of a transaction, MySQL locks **must** be released explicitly.

    .. caution::
       Prior to MySQL 5.7.5, it is not possible to hold more
       than one lock in a single session. Thus, the pack lock must be
       held by a different connection than the commit lock during the
       packing process (otherwise when
       :meth:`relstorage.adapters.packundo.HistoryFreePackUndo.fill_object_refs`
       acquires the commit lock, the pack lock held by :meth:`relstorage.storage.RelStorage.pack`
       would be dropped).

       http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
    """

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        timeout = not nowait and self.commit_lock_timeout or 0
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.commit'), %s)"
        cursor.execute(stmt, (timeout,))
        try:
            locked = cursor.fetchone()[0]
        except TypeError as e: # pragma: no cover
            # This has been observed under certain database drivers and concurrency loads,
            # specifically gevent with umysqldb and high concurrency. It's not clear what the cause
            # is, so lets at least raise a specific message.
            raise CommitLockQueryFailedError("The commit lock query failed: %s" % repr(e))

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
