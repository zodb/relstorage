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

import sys

from perfmetrics import metricmethod
import six
from zope.interface import implementer

from ..interfaces import ILocker
from ..interfaces import UnableToAcquireCommitLockError
from ..interfaces import UnableToAcquirePackUndoLockError
from ..locker import AbstractLocker


@implementer(ILocker)
class PostgreSQLLocker(AbstractLocker):

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # in PostgreSQL 9.3+, lock_timeout in ms; if nowait, then ignored
        timeout = (not nowait and self.commit_lock_timeout or 0) * 1000  # ms
        timeout_stmt = 'SET lock_timeout = %d; ' % timeout
        try:
            if ensure_current:
                # Hold commit_lock to prevent concurrent commits
                # (for as short a time as possible).

                # Lock transaction and current_object in share mode to
                # ensure conflict detection has the most current data.
                # A 'SHARE' lock prevents concurrent writes. If the
                # cursor is in READ COMMITTED mode, that's important
                # because otherwise subsequent reads of the table for
                # conflict resolution could get different (modified)
                # snapshots (XXX: Who would have modified them without
                # taking the commit lock first? taking the commit lock
                # should always preceed that and shouldn't be released
                # until that's done.)
                #
                # OTOH, if we're in a 'REPEATABLE READ' or
                # 'SERIALIZABLE' transaction, we're guaranteed to get
                # a snapshot where the entire set of tables is consistent
                # as-of our first SELECT. Without a lock, modifications could
                # still happen...but again, who would be doing that without
                # taking the commit lock?
                if self.keep_history:
                    stmt = """
                    %s
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE transaction IN SHARE MODE;
                    LOCK TABLE current_object IN SHARE MODE;
                    """ % (timeout_stmt, nowait and ' NOWAIT' or '',)
                else:
                    stmt = """
                    %s
                    LOCK TABLE commit_lock IN EXCLUSIVE MODE%s;
                    LOCK TABLE object_state IN SHARE MODE
                    """ % (timeout_stmt, nowait and ' NOWAIT' or '',)
            else:
                stmt = """
                %s
                LOCK TABLE commit_lock IN EXCLUSIVE MODE%s
                """ % (timeout_stmt, ' NOWAIT' if nowait else '')

            for s in stmt.splitlines():
                if not s.strip():
                    continue
                cursor.execute(s)

        except self.lock_exceptions as e:
            if nowait:
                return False
            six.reraise(
                UnableToAcquireCommitLockError,
                UnableToAcquireCommitLockError('Acquiring a commit lock failed: %s' % (e,)),
                sys.exc_info()[2])
        return True

    def release_commit_lock(self, cursor):
        # no action needed, locks released with transaction.
        pass

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
