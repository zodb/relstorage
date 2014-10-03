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

from ZODB.POSException import ReadConflictError
from relstorage.adapters.interfaces import IPoller
from zope.interface import implements
import logging

log = logging.getLogger(__name__)


class Poller:
    """Database change notification poller"""
    implements(IPoller)

    def __init__(self, poll_query, keep_history, runner, revert_when_stale):
        self.poll_query = poll_query
        self.keep_history = keep_history
        self.runner = runner
        self.revert_when_stale = revert_when_stale

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

        elif new_polled_tid > prev_polled_tid:
            # New transaction(s) have been added.

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
                cursor.execute(
                    intern(stmt % self.runner.script_vars),
                    {'tid': prev_polled_tid})
                rows = cursor.fetchall()
                if not rows:
                    # Transaction not found; perhaps it has been packed.
                    # The connection cache should be cleared.
                    return None, new_polled_tid

            # Get the list of changed OIDs and return it.
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
            changes = cursor.fetchall()

            return changes, new_polled_tid

        else:
            # The database connection is stale. This can happen after
            # reading an asynchronous slave that is not fully up to date.
            # (It may also suggest that transaction IDs are not being created
            # in order, which would be a serious bug leading to consistency
            # violations.)
            if self.revert_when_stale:
                # This client prefers to revert to the old state.
                log.warning(
                    "Reverting to stale transaction ID %d and clearing cache. "
                    "(prev_polled_tid=%d)",
                    new_polled_tid, prev_polled_tid)
                # We have to invalidate the whole cPickleCache, otherwise
                # the cache would be inconsistent with the reverted state.
                return None, new_polled_tid
            else:
                # This client never wants to revert to stale data, so
                # raise ReadConflictError to trigger a retry.
                # We're probably just waiting for async replication
                # to catch up, so retrying could do the trick.
                raise ReadConflictError(
                    "The database connection is stale: new_polled_tid=%d, "
                    "prev_polled_tid=%d." % (new_polled_tid, prev_polled_tid))

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
        return cursor.fetchall()
