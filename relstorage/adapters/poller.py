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

from relstorage.adapters.interfaces import IPoller
from zope.interface import implements

class Poller:
    """Database change notification poller"""
    implements(IPoller)

    def __init__(self, poll_query, keep_history, runner):
        self.poll_query = poll_query
        self.keep_history = keep_history
        self.runner = runner

    def poll_invalidations(self, conn, cursor, prev_polled_tid, ignore_tid):
        """Polls for new transactions.

        conn and cursor must have been created previously by open_for_load().
        prev_polled_tid is the tid returned at the last poll, or None
        if this is the first poll.  If ignore_tid is not None, changes
        committed in that transaction will not be included in the list
        of changed OIDs.

        Returns (changed_oids, new_polled_tid).
        """
        # find out the tid of the most recent transaction.
        cursor.execute(self.poll_query)
        new_polled_tid = cursor.fetchone()[0]

        if prev_polled_tid is None:
            # This is the first time the connection has polled.
            return None, new_polled_tid

        if new_polled_tid == prev_polled_tid:
            # No transactions have been committed since prev_polled_tid.
            return (), new_polled_tid

        if self.keep_history:
            stmt = "SELECT 1 FROM transaction WHERE tid = %(tid)s"
        else:
            stmt = "SELECT 1 FROM object_state WHERE tid <= %(tid)s LIMIT 1"
        cursor.execute(intern(stmt % self.runner.script_vars),
            {'tid': prev_polled_tid})
        rows = cursor.fetchall()
        if not rows:
            # Transaction not found; perhaps it has been packed.
            # The connection cache needs to be cleared.
            return None, new_polled_tid

        # Get the list of changed OIDs and return it.
        if ignore_tid is None:
            stmt = """
            SELECT zoid
            FROM current_object
            WHERE tid > %(tid)s
            """
            cursor.execute(intern(stmt % self.runner.script_vars),
                {'tid': prev_polled_tid})
        else:
            stmt = """
            SELECT zoid
            FROM current_object
            WHERE tid > %(tid)s
                AND tid != %(self_tid)s
            """
            cursor.execute(intern(stmt % self.runner.script_vars),
                {'tid': prev_polled_tid, 'self_tid': ignore_tid})
        oids = [oid for (oid,) in cursor]

        return oids, new_polled_tid

