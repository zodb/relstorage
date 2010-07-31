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
import logging
log = logging.getLogger(__name__)

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

        Returns (changes, new_polled_tid), where changes is either
        a list of (oid, tid) that have changed, or None to indicate
        that the changes are too complex to list.  new_polled_tid can be
        0 if there is no data in the database.
        """
        # find out the tid of the most recent transaction.
        cursor.execute(self.poll_query)
        rows = list(cursor)
        if not rows:
            # No data.
            return None, 0
        new_polled_tid = rows[0][0]
        if not new_polled_tid:
            # No data.
            return None, 0

        if prev_polled_tid is None:
            # This is the first time the connection has polled.
            return None, new_polled_tid

        if new_polled_tid == prev_polled_tid:
            # No transactions have been committed since prev_polled_tid.
            return (), new_polled_tid

        if self.keep_history:
            # If the previously polled transaction no longer exists,
            # the cache is too old and needs to be cleared.
            # XXX Do we actually need to detect this condition? I think
            # if we delete this block of code, all the unreachable
            # objects will be garbage collected anyway. So, as a test,
            # there is no equivalent of this block of code for
            # history-free storage. If something goes wrong, then we'll
            # know there's some other edge condition we have to account
            # for.
            stmt = "SELECT 1 FROM transaction WHERE tid = %(tid)s"
            cursor.execute(intern(stmt % self.runner.script_vars),
                {'tid': prev_polled_tid})
            rows = cursor.fetchall()
            if not rows:
                # Transaction not found; perhaps it has been packed.
                # The connection cache needs to be cleared.
                return None, new_polled_tid

        # Get the list of changed OIDs and return it.
        if new_polled_tid > prev_polled_tid:
            if self.keep_history:
                stmt = """
                SELECT zoid, tid
                FROM current_object
                WHERE tid > %(tid)s
                """
            else:
                stmt = """
                SELECT zoid, tid
                FROM object_state
                WHERE tid > %(tid)s
                """
            params = {'tid': prev_polled_tid}
            if ignore_tid is not None:
                stmt += " AND tid != %(self_tid)s"
                params['self_tid'] = ignore_tid
            stmt = intern(stmt % self.runner.script_vars)

            cursor.execute(stmt, params)
            changes = list(cursor)

            return changes, new_polled_tid

        else:
            # We moved backward in time. This can happen after failover
            # to an asynchronous slave that is not fully up to date. If
            # this was not caused by failover, it suggests that
            # transaction IDs are not being created in order, which can
            # lead to consistency violations.
            log.warning(
                "Detected backward time travel (old tid %d, new tid %d). "
                "This is acceptable if it was caused by failover to a "
                "read-only asynchronous slave, but otherwise it may "
                "indicate a problem.",
                prev_polled_tid, new_polled_tid)
            # Although we could handle this situation by looking at the
            # whole cPickleCache and invalidating only certain objects,
            # invalidating the whole cache is simpler.
            return None, new_polled_tid

    def list_changes(self, cursor, after_tid, last_tid):
        """Return the (oid, tid) values changed in a range of transactions.

        The returned iterable must include the latest changes in the range
        after_tid < tid <= last_tid.
        """
        if self.keep_history:
            stmt = """
            SELECT zoid, tid
            FROM current_object
            WHERE tid > %(min_tid)s
                AND tid <= %(max_tid)s
            """
        else:
            stmt = """
            SELECT zoid, tid
            FROM object_state
            WHERE tid > %(min_tid)s
                AND tid <= %(max_tid)s
            """
        params = {'min_tid': after_tid, 'max_tid': last_tid}
        stmt = intern(stmt % self.runner.script_vars)

        cursor.execute(stmt, params)
        return list(cursor)
