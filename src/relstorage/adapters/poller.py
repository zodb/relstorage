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
from __future__ import absolute_import, print_function

import logging

from zope.interface import implementer

from .interfaces import IPoller
from .interfaces import StaleConnectionError

from .schema import Schema
from .sql import func

log = logging.getLogger(__name__)

@implementer(IPoller)
class Poller(object):
    """Database change notification poller"""

    # The zoid is the primary key on both ``current_object`` (history
    # preserving) and ``object_state`` (history free), so these
    # queries are guaranteed to only produce an OID once.

    _poll_inv_query = Schema.all_current_object.select(
        Schema.all_current_object.c.zoid, Schema.all_current_object.c.tid
    ).where(
        Schema.all_current_object.c.tid > Schema.all_current_object.bindparam('tid')
    ).order_by(
        Schema.all_current_object.c.tid, 'DESC'
    ).prepared()

    _poll_newest_tid_query = Schema.all_transaction.select(
        func.max(Schema.all_transaction.c.tid)
    ).prepared()

    def __init__(self, driver, keep_history, runner,
                 revert_when_stale, transactions_may_go_backwards):
        self.driver = driver
        self.keep_history = keep_history
        self.runner = runner
        self.revert_when_stale = revert_when_stale
        self.transactions_may_go_backwards = transactions_may_go_backwards

    def get_current_tid(self, cursor):
        self._poll_newest_tid_query.execute(cursor)
        rows = cursor.fetchall() or ((0,),)
        current_tid, = rows[0]
        return current_tid or 0

    def poll_invalidations(self, conn, cursor, prev_polled_tid):
        """
        See ``IPoller``
        """
        # pylint:disable=unused-argument

        # Some databases, in some isolation modes, only establish a snapshot
        # of a particular table when the table is first accessed in a given transaction.
        # (Looking at you, MySQL on Windows). Thus if we're accessing two tables,
        # as we would in history-preserving mode, we could get slightly different answers:
        # The current_object table might move forward by a transaction while we're accessing the
        # transaction table, leading to the results being inconsistent.
        # For this reason, we only perform a single poll query against the actual object data.
        # We order this to get the newest TID first, and we return a chain iterator
        #
        # Return the cursor: let it be its own iterable. This could be a
        # very large result set. For things that matter, like gevent,
        # consume in batches allowing periodic switches.
        if prev_polled_tid is None:
            # This is the first time the connection has polled.
            # We'd have to list the entire database for the changes,
            # which is clearly no good, so we want to fetch just the newest
            # TID.
            return None, self.get_current_tid(cursor)

        params = {'tid': prev_polled_tid}
        self._poll_inv_query.execute(cursor, params)
        rows = cursor.fetchall()
        if not rows:
            if self.transactions_may_go_backwards:
                # No detectable changes. Perhaps we went backwards? Check that,
                # but only if it's a possibility
                self._poll_newest_tid_query.execute(cursor)
                new_polled_tid = self.get_current_tid(cursor)
            else:
                # Assume we're fully caught up and that transactions cannot
                # go back
                new_polled_tid = prev_polled_tid
        else:
            new_polled_tid = rows[0][1]

        if new_polled_tid == prev_polled_tid:
            # No transactions have been committed since prev_polled_tid.
            return (), new_polled_tid

        if new_polled_tid < prev_polled_tid:
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

            # This client never wants to revert to stale data, so
            # raise ReadConflictError to trigger a retry.
            # We're probably just waiting for async replication
            # to catch up, so retrying could do the trick.
            raise StaleConnectionError.from_prev_and_new_tid(
                prev_polled_tid, new_polled_tid)

        # New transaction(s) have been added.

        # In the past, but only for history-preserving databases, we
        # would check to see if the previously polled transaction no
        # longer exists in the transaction table. If it didn't, we
        # would return ``(None, new_polled_tid)``, in order to clear
        # the Connection cache.
        #
        # However, we ran for yers without an analogous case for
        # history-free databases without problems, on the theory that
        # all the unreachable objects will be garbage collected
        # anyway.
        #
        # Thus we became convinced it was safe to remove the check in
        # history-preserving databases.
        return rows, new_polled_tid
