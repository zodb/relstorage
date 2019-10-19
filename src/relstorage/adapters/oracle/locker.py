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
class OracleLocker(AbstractLocker):

    def __init__(self, options, driver, inputsize_NUMBER, batcher_factory):
        super(OracleLocker, self).__init__(
            options=options,
            driver=driver,
            batcher_factory=batcher_factory
        )
        self.inputsize_NUMBER = inputsize_NUMBER
        timeout = ' WAIT ' + str(self.commit_lock_timeout)
        self._lock_current_clause = self._lock_current_clause + timeout
        # 'FOR SHARE' isn't supported so all locks are exclusive.
        self._lock_share_clause = self._lock_current_clause
        self._lock_share_clause_nowait = 'FOR UPDATE NOWAIT'

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
