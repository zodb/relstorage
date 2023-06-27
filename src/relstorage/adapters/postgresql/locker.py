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

from zope.interface import implementer

from relstorage._util import Lazy
from ..interfaces import ILocker
from ..interfaces import UnableToLockRowsToModifyError
from ..interfaces import UnableToLockRowsToReadCurrentError
from ..interfaces import UnableToAcquirePackUndoLockError
from ..locker import AbstractLocker


@implementer(ILocker)
class PostgreSQLLocker(AbstractLocker):
    """
    On PostgreSQL, for locking individual objects, we use advisory
    locks, not row locks.

    This is because row locks perform disk I/O, which can "bloat" tables;
    this is especially an issue for shared (readCurrent) locks, which perform
    I/O to pages that might not otherwise need it. Using many row locks
    requires a good (auto)vacuuming setup to prevent this bloat from becoming
    a problem.

    Also, advisory locks are much faster than row locks.

    We take advisory locks based on OID. OIDs begin at 0 and increase. To avoid
    conflicting with those, any advisory locks used for "administrative" purposes,
    such as the pack lock, need to use negative numbers.

    Locks include:

    - -1: The commit lock
    - -2: The pack lock
    """

    def _on_store_opened_set_row_lock_timeout(self, cursor, restart=False):
        # This only lasts beyond the current transaction if it
        # commits. If we have a rollback, then our effect is lost.
        # Luckily, right here is a fine time to commit.
        self._set_row_lock_timeout(cursor, self.commit_lock_timeout)
        cursor.connection.commit()

    def _set_row_lock_timeout(self, cursor, timeout):
        # This will rollback if the transaction rolls back,
        # which should restart our store.
        # Maybe for timeout == 0, we should do SET LOCAL, which
        # is guaranteed not to persist?
        # Recall postgresql uses milliseconds, but our configuration is given
        # in integer seconds.
        # For tests, though, we accept floating point numbers
        timeout_ms = int(timeout * 1000.0)
        self.driver.set_lock_timeout(cursor, timeout_ms)


    @Lazy
    def _lock_current_objects_query(self):
        # This is not used in production, only in tests that try to interleave
        # different types of locking.
        get = self._get_current_objects_query[0]
        stmt = get.replace('SELECT zoid', 'SELECT pg_advisory_xact_lock(zoid)')
        stmt += ' ORDER BY zoid'
        return stmt

    def _lock_suffix_for_readCurrent(self, shared_locks_block):
        # This is not used in production, only in tests that try to interleave
        # different types of locking.
        return ''

    def _lock_column_name_for_readCurrent(self, shared_locks_block):
        # This is not used in production, only in tests that try to interleave
        # different types of locking.
        return (
            'pg_advisory_xact_lock_shared(zoid)'
            if shared_locks_block
            else
            'pg_try_advisory_xact_lock_shared(zoid)'
        )

    def _lock_consume_rows_for_readCurrent(self, rows, shared_locks_block):
        # This is not used in production, only in tests that try to interleave
        # different types of locking.

        # If we used the blocking version, it returned void (NULL/None),
        # or raised an error. No need to examine the rows.
        if shared_locks_block:
            return super()._lock_consume_rows_for_readCurrent(
                rows,
                shared_locks_block)

        for got_lock, in rows:
            if not got_lock:
                raise UnableToLockRowsToReadCurrentError
        return None

    def release_commit_lock(self, cursor):
        # no action needed, locks released with transaction.
        pass

    def _modify_commit_lock_kind(self, kind, exc):
        # We can distinguish exclusive locks from shared locks
        # by the error message produced by postgresql. The exclusive lock
        # failures have a full query ('SELECT ... FOR UPDATE'), while
        # the shared locks either have 'RETURN QUERY' in their string
        # (when we couldn't get a lock specifically) or 'readCurrent' in their
        # hint (when we could get a lock, but the object changed)
        exc_str = str(exc).lower()
        if 'for update' in exc_str:
            kind = UnableToLockRowsToModifyError
        elif 'return query' in exc_str or 'readcurrent' in exc_str:
            kind = UnableToLockRowsToReadCurrentError
        return kind

    def _get_commit_lock_debug_info(self, cursor, was_failure=False):
        # When we're called, the transaction is guaranteed to be
        # aborted, so to be able to execute any queries we need to
        # rollback. Unfortunately, that would release the locks that
        # we're holding, which have probably(?) already have been released
        # anyway. The output can be excessive.
        if was_failure:
            cursor.connection.rollback()
            return 'Transaction failed; no lock info available'

        from .util import debug_locks
        debug_locks(cursor)
        return self._rows_as_pretty_string(cursor)

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        cursor.execute("SELECT pg_try_advisory_lock(-2)")
        locked = cursor.fetchone()[0]
        if not locked:
            raise UnableToAcquirePackUndoLockError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        cursor.execute("SELECT pg_advisory_unlock(-2)")
