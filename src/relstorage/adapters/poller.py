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
    _list_changes_range_query = Schema.all_current_object.select(
        Schema.all_current_object.c.zoid, Schema.all_current_object.c.tid
    ).where(
        Schema.all_current_object.c.tid > Schema.all_current_object.bindparam('min_tid')
    ).and_(
        Schema.all_current_object.c.tid <= Schema.all_current_object.bindparam('max_tid')
    ).prepared()

    _poll_inv_query = Schema.all_current_object.select(
        Schema.all_current_object.c.zoid, Schema.all_current_object.c.tid
    ).where(
        Schema.all_current_object.c.tid > Schema.all_current_object.bindparam('tid')
    ).prepared()

    _poll_inv_exc_query = _poll_inv_query.and_(
        Schema.all_current_object.c.tid != Schema.all_current_object.bindparam('self_tid')
    ).prepared()

    poll_query = Schema.all_transaction.select(
        func.max(Schema.all_transaction.c.tid)
    ).prepared()

    def __init__(self, driver, keep_history, runner, revert_when_stale):
        self.driver = driver
        self.keep_history = keep_history
        self.runner = runner
        self.revert_when_stale = revert_when_stale

    def poll_invalidations(self, conn, cursor, prev_polled_tid, ignore_tid):
        """
        Polls for new transactions.

        *conn* and *cursor* must have been created previously by
        ``open_for_load()`` (a snapshot connection). prev_polled_tid
        is the tid returned at the last poll, or None if this is the
        first poll. If ignore_tid is not None, changes committed in
        that transaction will not be included in the list of changed
        OIDs.

        Returns ``(changes, new_polled_tid)``, where *changes* is
        either a list of ``(oid_int, tid_int)`` that have changed, or
        ``None`` to indicate that the changes are too complex to li
        --- this must cause local storage caches to be invalidated..
        *new_polled_tid* can be 0 if there is no data in the database.
        """
        # pylint:disable=unused-argument
        # find out the tid of the most recent transaction.
        self.poll_query.execute(cursor)
        rows = cursor.fetchall()
        if not rows or not rows[0][0]:
            # No data, must be fresh database, without even
            # the root object.
            # Anything we had cached is now definitely invalid.
            return None, 0

        new_polled_tid = rows[0][0]
        if prev_polled_tid is None:
            # This is the first time the connection has polled.
            # We'd have to list the entire database for the changes,
            # which is clearly no good. So we have no information
            # about the state of anything we have cached.
            return None, new_polled_tid

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

        # Get the list of changed OIDs and return it.
        stmt = self._poll_inv_query
        params = {'tid': prev_polled_tid}
        if ignore_tid is not None:
            stmt = self._poll_inv_exc_query
            params['self_tid'] = ignore_tid

        stmt.execute(cursor, params)
        changes = cursor.fetchall()
        return changes, new_polled_tid

    def list_changes(self, cursor, after_tid, last_tid):
        """
        See ``IPoller``.
        """
        params = {'min_tid': after_tid, 'max_tid': last_tid}
        self._list_changes_range_query.execute(cursor, params)
        return cursor.fetchall()
