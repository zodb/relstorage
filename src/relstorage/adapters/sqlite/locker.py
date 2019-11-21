# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from zope.interface import implementer

from ..interfaces import ILocker
from ..locker import AbstractLocker


@implementer(ILocker)
class Sqlite3Locker(AbstractLocker):

    supports_row_lock_nowait = False
    _lock_share_clause = '<This should not be used>'
    _lock_share_clause_nowait = '<This should not be used>'
    _lock_current_clause = '<This Should not be used>'

    def _lock_readCurrent_oids_for_share(self, cursor, current_oids, shared_locks_block):
        """
        This is a no-op.

        Once this is called, the parent class will call the after_lock_share callback,
        and the parent adapter will use that to check the recorded TIDs against the
        desired TIDs. Our store connection is still in autocommit mode at this point,
        so it will read the current data.
        """

    _lock_current_objects_query = 'UPDATE commit_row_lock SET tid = tid WHERE tid < 0'

    def _lock_rows_being_modified(self, cursor):
        # We exclusively lock the whole database at this point. There's no way to
        # avoid it. We also switch out of autocommit mode and start a transaction,
        # meaning our store cursor is no longer updating...but since we have an exclusive
        # lock, that's fine. We use a statement that matches no rows, but still takes
        # locks.
        conn = cursor.connection
        # If we pre-allocated the TID due to a restore, we could already be
        # in a tranasction. In which case there's nothing to do (but for safety
        # we still execute the query)
        AbstractLocker._lock_rows_being_modified(self, cursor)
        assert conn.in_transaction or conn.in_transaction is None, repr(conn) # Py2

    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # We may or may not actually have locked rows; if we're doing a restore
        # we allocate the tid first. Likewise, an undo also
        # locks upfront.
        if ensure_current:
            cursor.execute(self._lock_current_objects_query)
            in_transaction = cursor.connection.in_transaction
            assert in_transaction or in_transaction is None # Py2

    def release_commit_lock(self, cursor):
        # no action needed, locks released with transaction.
        pass

    def release_pack_lock(self, cursor):
        pass

    def hold_pack_lock(self, cursor):
        pass
