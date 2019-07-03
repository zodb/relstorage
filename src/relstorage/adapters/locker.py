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

import abc
import sys
import six

from perfmetrics import metricmethod

from relstorage._compat import ABC
from relstorage._util import consume

from ._util import query_property as _query_property
from .interfaces import UnableToAcquireCommitLockError

class AbstractLocker(ABC):

    def __init__(self, options, driver, batcher_factory):
        self.keep_history = options.keep_history
        self.commit_lock_timeout = options.commit_lock_timeout
        self.commit_lock_id = options.commit_lock_id
        self.lock_exceptions = driver.lock_exceptions
        self.illegal_operation_exceptions = driver.illegal_operation_exceptions
        self.make_batcher = batcher_factory

    def on_store_opened(self, cursor, restart=False):
        """
        A callback that must be called when a store connection is
        opened or restarted.
        """
        if not restart:
            self._on_store_opened_set_row_lock_timeout(cursor)

    def _on_store_opened_set_row_lock_timeout(self, cursor, restart=False):
        """
        Install a per-session row lock timeout, if desired.
        """
        # XXX: This applies to all locks, including those that are
        # taken by locking current rows in the tpc_vote phase. Is that
        # what we really want?

    def _set_row_lock_timeout(self, cursor, timeout):
        """
        Install a per-session row lock timeout. This should
        be implemented, it's used to implement nowait if needed.
        """

    _lock_current_clause = 'FOR UPDATE'

    _lock_current_objects_queries = (
        (('zoid',), 'current_object', 'zoid'),
        (('zoid',), 'object_state', 'zoid'),
    )

    _lock_current_objects_query = _query_property('_lock_current_objects')

    def lock_current_objects(self, cursor, current_oids):
        # We need to be sure to take the locks in a deterministic
        # order; the easiest way to do that is to order them by OID.
        # But we have two separate sets of OIDs we need to lock: the
        # ones we're finding the current data for, and the ones that
        # we're going to check for conflicts. The ones we're checking
        # for conflicts are already in the database in `temp_store`
        # (and partly in the storage cache's temporary storage and/or
        # the row batcher); the current oids are only in memory.
        # So we have a few choices: either put the current oids into
        # a database table and do a UNION query with temp_store,
        # or pull the temp_store data into memory, union it with
        # current_oids and issue a single big query.
        #
        # Our strategy could even vary depending on the size of current_oids;
        # in the usual case, it will be small or empty, and an in-database
        # big UNION query is probably workable (in the empty case, we can
        # elide this part altogether)
        cols_to_select, table, filter_column = self._lock_current_objects_query

        # In history free mode, *table* will be `object_state`, which
        # has ZOID as its primary key. In history preserving mode,
        # *table* will be `current_object`, where ZOID is also the primary
        # key (and `object_state` is immutable; that's why we don't need
        # to take any locks there --- conflict resolution will always be able
        # to find the desired state without fear of it changing).

        # If we also include the objects being added,
        # mysql takes out gap locks, and we can deadlock?
        # TODO: Confirm.
        objects_being_updated_stmt = """
        SELECT zoid FROM %s WHERE zoid IN (
            SELECT zoid FROM temp_store
        )
        """ % (table, )

        cursor.execute(objects_being_updated_stmt)

        oids_being_updated = [row[0] for row in cursor]

        oids_to_lock = set(oids_being_updated) | set(current_oids)
        oids_to_lock = sorted(oids_to_lock)

        batcher = self.make_batcher(cursor, row_limit=1000)
        # MySQL 8 allows NOWAIT and SKIP LOCKED; earlier versions do not
        # have that.
        # PostgreSQL allows both.
        # There is no specific timeout here. Instead, the global
        # database "row lock timeout" applies.

        consume(batcher.select_from(
            cols_to_select, table,
            suffix='  %s ' % self._lock_current_clause,
            **{filter_column: oids_to_lock}
        ))

    _commit_lock_queries = (
        # MySQL allows aggregates in the top level to use FOR UPDATE,
        # but PostgreSQL does not.
        # 'SELECT MAX(tid) FROM transaction FOR UPDATE',
        'SELECT tid FROM transaction WHERE tid = (SELECT MAX(tid) FROM TRANSACTION) FOR UPDATE',
        'SELECT tid FROM commit_row_lock FOR UPDATE'
    )

    _commit_lock_query = _query_property('_commit_lock')

    _commit_lock_nowait_queries = (
        _commit_lock_queries[0] + ' NOWAIT',
        _commit_lock_queries[1] + ' NOWAIT',
    )

    _commit_lock_nowait_query = _query_property('_commit_lock_nowait')

    # Set this to a false value if you don't support NOWAIT and we'll
    # instead set the lock timeout to 0.
    _supports_row_lock_nowait = True

    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # pylint:disable=unused-argument
        lock_stmt = self._commit_lock_query
        if nowait:
            if self._supports_row_lock_nowait:
                lock_stmt = self._commit_lock_nowait_query
            else:
                # TODO: Do we need to roll this back for any reason?
                # Probably not since we only use nowait during pack.
                self._set_row_lock_timeout(cursor, 0)
        __traceback_info__ = lock_stmt
        try:
            cursor.execute(lock_stmt)
            cursor.fetchone()
        except self.illegal_operation_exceptions as e:
            # Bug in our code.
            raise
        except self.lock_exceptions as e:
            if nowait:
                return False
            six.reraise(
                UnableToAcquireCommitLockError,
                UnableToAcquireCommitLockError('Acquiring a commit lock failed: %s' % (e,)),
                sys.exc_info()[2])
        return True

    @abc.abstractmethod
    def release_commit_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def hold_pack_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def release_pack_lock(self, cursor):
        raise NotImplementedError()
