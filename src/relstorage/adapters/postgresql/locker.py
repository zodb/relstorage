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

from ..interfaces import ILocker
from ..interfaces import UnableToAcquirePackUndoLockError
from ..locker import AbstractLocker


@implementer(ILocker)
class PostgreSQLLocker(AbstractLocker):

    def _on_store_opened_set_row_lock_timeout(self, cursor, restart=False):
        # This only lasts beyond the current transaction if it
        # commits.
        if not restart:
            # We never change our lock timeout setting; the lock_and_choose_tid
            # functions have it as part of their function definition, and
            # we have NOWAIT for when we need a 0 timeout.
            self._set_row_lock_timeout(cursor, self.commit_lock_timeout)

    def _set_row_lock_timeout(self, cursor, timeout):
        # This will rollback if the transaction rolls back,
        # which should restart our store.
        # Maybe for timeout == 0, we should do SET LOCAL, which
        # is guaranteed not to persist?
        # Recall postgresql uses milliseconds, but our configuration is given
        # in seconds.
        self.driver.set_lock_timeout(cursor, timeout * 1000)

    def release_commit_lock(self, cursor):
        # no action needed, locks released with transaction.
        pass

    def _get_commit_lock_debug_info(self, cursor, was_failure=False):
        # When we're called, the transaction is guaranteed to be
        # aborted, so to be able to execute any queries we need to
        # rollback. Unfortunately, that would release the locks that
        # we're holding, which have probably(?) already have been released
        # anyway. The output can be excessive.
        if was_failure:
            cursor.connection.rollback()
            return 'Transaction failed; no lock info available'

        from . import debug_locks
        debug_locks(cursor)
        return self._rows_as_pretty_string(cursor)

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
