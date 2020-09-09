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
from __future__ import print_function

from contextlib import contextmanager

from zope.interface import implementer

from ..interfaces import ILocker
from ..interfaces import UnableToAcquireCommitLockError
from ..interfaces import UnableToAcquirePackUndoLockError
from ..locker import AbstractLocker

class CommitLockQueryFailedError(UnableToAcquireCommitLockError):
    pass

_SET_TIMEOUT_STMT = 'SET SESSION innodb_lock_wait_timeout = %s'
# DEFAULT is a literal, not a param, so cursor.execute(stmt, ('DEFAULT',))
# does not work.
_SET_TIMEOUT_DEFAULT_STMT = _SET_TIMEOUT_STMT % ('DEFAULT',)

@contextmanager
def lock_timeout(cursor, timeout, restore_to=None):
    """
    ContextManager that sets the lock timeout to the given value,
    and returns it to the DEFAULT when done.

    If *timeout* is ``None``, makes no changes to the connection.
    """
    if timeout is not None: # 0 is valid
        # Min value of timeout is 1; a value less than that produces
        # a warning but gets truncated to 1
        timeout = timeout if timeout >= 1 else 1
        cursor.execute(_SET_TIMEOUT_STMT, (timeout,))
        try:
            yield
        finally:
            if restore_to is None:
                cursor.execute(_SET_TIMEOUT_DEFAULT_STMT)
            else:
                cursor.execute(_SET_TIMEOUT_STMT, (restore_to,))
    else:
        yield


@implementer(ILocker)
class MySQLLocker(AbstractLocker):
    """
    MySQL locks.

    .. rubric:: Commit and Object Locks

    Two types of locks are used. The ordinary commit lock and the
    object locks are standard InnoDB row-level locks; this brings the
    benefits of being lightweight and automatically being released if
    the transaction aborts or commits, plus instant deadlock
    detection. Prior to MySQL 8.0, these don't support ``NOWAIT``
    syntax, so we synthesize that by setting the session variable
    `innodb_lock_wait_timeout
    <https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_lock_wait_timeout>`_.

    Note that this lock cannot be against the ``object_state`` or
    ``current_object`` tables: arbitrary rows in those tables may have
    been locked by other transactions, and we risk deadlock.

    Also note that by default, a lock timeout will only rollback the
    current *statement*, not the whole transaction, as in most
    databases (this doesn't apply to ``NOWAIT`` in MySQL 8); to
    release any locks taken earlier, we must explicitly rollback the
    transaction. Fortunately, a lock timeout only rolling back the
    single statement is exactly what we want to implement ``NOWAIT``
    on earlier databases. In contrast, a detected deadlock will
    actually rollback the entire transaction.

    The ``ensure_current`` argument is essentially ignored; the locks
    taken out by ``lock_current_objects`` take care of that.

    .. rubric:: Shared and Exclusive Locks Can Block Each Other On Unrelated Rows

    We use two lock classes for object locks: shared locks for
    readCurrent, and exclusive locks for modified objects.

    MySQL 5.7 and 8 handle this weird, though. If two transactions are
    at any level besides ``SERIALIZABLE``, and one locks the *odd*
    rows ``FOR UPDATE`` the other one blocks trying to lock the *even*
    rows ``FOR UPDATE`` *or* in shared mode, if they happened to use
    queries like ``WHERE (zoid % 2) = 1``. This is surprising. (It's
    not surprising in ``SERIALIZABLE``; MySQL's ``SERIALIZABLE`` is
    quite pessimistic.)

    This is because (quoting
    https://dev.mysql.com/doc/refman/5.7/en/innodb-locks-set.html)
    "``SELECT ... LOCK IN SHARE MODE`` sets shared next-key locks on
    all index records the search encounters." While "``SELECT ... FOR
    UPDATE`` sets an exclusive next-key lock on every record the
    search encounters. However, only an index record lock is required
    for statements that lock rows using a unique index to search for a
    unique row. For index records the search encounters, ``SELECT ...
    FOR UPDATE`` blocks other sessions from doing ``SELECT ... LOCK IN
    SHARE MODE`` or from reading in certain transaction isolation
    levels." The complex ``WHERE`` clause does range queries and
    traversal of the index such that it winds up locking many
    unexpected rows.

    The good news is that the query we actually use for locking,
    ``SELECT zoid FROM ... WHERE zoid in (SELECT zoid from
    temp_store)``, doesn't do a range scan. It first accessess the
    ``temp_store`` table and does a sort into a temporary table using
    the index; then it accesses ``object_state`` or ``current_object``
    using the ``eq_ref`` method and the PRIMARY key index in a nested
    loop (sadly all MySQL joins are nested loops). This locks only the
    actually required rows.

    We should probably add some optimizer hints to make absolutely
    sure of that.

    .. rubric:: Pack Locks

    The second type of lock, an advisory lock, is used for pack locks.
    This lock uses the `GET_LOCK
    <https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_get-lock>`_
    and ``RELEASE_LOCK`` functions. These locks persist for the
    duration of a session, and *must* be explicitly released. They do
    *not* participate in deadlock detection.

    Prior to MySQL 5.7.5, it is not possible to hold more than one
    advisory lock in a single session. In the past we used advisory
    locks for the commit lock, and that meant we had to use multiple
    sessions (connections) to be able to hold both the commit lock and
    the pack lock. Fortunately, that limitation has been lifted: we no
    longer support older versions of MySQL, and we don't need multiple
    advisory locks anyway.
    """

    # The old MySQL 5.7 syntax is the default
    _lock_share_clause = 'LOCK IN SHARE MODE'
    _lock_share_clause_nowait = 'LOCK IN SHARE MODE'

    def __init__(self, options, driver, batcher_factory, version_detector):
        super(MySQLLocker, self).__init__(options, driver, batcher_factory)
        assert self.supports_row_lock_nowait # Set by default in the class.
        self.supports_row_lock_nowait = None
        self.version_detector = version_detector

        # No good preparing this, mysql can't take parameters in EXECUTE,
        # they have to be user variables, which defeats most of the point
        # (Although in this case, because it's a static value, maybe not;
        # it could be set once and re-used.)
        self.set_timeout_stmt = _SET_TIMEOUT_STMT

    def on_store_opened(self, cursor, restart=False):
        super(MySQLLocker, self).on_store_opened(cursor, restart=restart)
        if restart:
            return

        # Setting state in an `on_store_opened` is not good. With pooling, it may never
        # get executed. We work around this in our adapter by being sure to only use one
        # connmanager and one locker instance, shared among all the RelStorage instances.
        if self.supports_row_lock_nowait is None:
            self.supports_row_lock_nowait = self.version_detector.supports_nowait(cursor)

            if self.supports_row_lock_nowait:
                self._lock_share_clause = 'FOR SHARE'
                self._lock_share_clause_nowait = 'FOR SHARE NOWAIT'
            else:
                assert self._lock_readCurrent_oids_for_share
                self._lock_readCurrent_oids_for_share = self.__lock_readCurrent_nowait

    def _on_store_opened_set_row_lock_timeout(self, cursor, restart=False):
        self._set_row_lock_timeout(cursor, self.commit_lock_timeout)

    def _set_row_lock_timeout(self, cursor, timeout):
        # Min value of timeout is 1; a value less than that produces
        # a warning.
        timeout = timeout if timeout >= 1 else 1
        cursor.execute(self.set_timeout_stmt, (timeout,))
        # It's INCREDIBLY important to fetch a row after we execute the SET statement;
        # otherwise, the binary drivers that use libmysqlclient tend to crash,
        # usually with a 'malloc: freeing not allocated data' or 'malloc:
        # corrupted data, written after free?' or something like that.
        cursor.fetchone()


    def __lock_readCurrent_nowait(self, cursor, current_oids, shared_locks_block):
        # For MySQL 5.7, we emulate NOWAIT by setting the lock timeout
        if shared_locks_block:
            return AbstractLocker._lock_readCurrent_oids_for_share(self, cursor, current_oids, True)

        with lock_timeout(cursor, 0, self.commit_lock_timeout):
            return AbstractLocker._lock_readCurrent_oids_for_share(self, cursor, current_oids,
                                                                   False)

    def release_commit_lock(self, cursor):
        "Auto-released by transaction end."

    def _get_commit_lock_debug_info(self, cursor, was_failure=False):
        cursor.execute('SELECT connection_id()')
        conn_id = str(cursor.fetchone()[0])

        try:
            # MySQL 8
            cursor.execute("""
            SELECT *
            FROM performance_schema.events_transactions_current AS parent
                INNER JOIN performance_schema.data_locks AS child
                INNER JOIN performance_schema.data_lock_waits dlw on (child.engine_lock_id
                    = dlw.blocking_engine_lock_id)
            WHERE
            parent.THREAD_ID = child.THREAD_ID
            AND parent.EVENT_ID < child.EVENT_ID
            AND (
            child.EVENT_ID <= parent.END_EVENT_ID
            OR parent.END_EVENT_ID IS NULL
            )""")
            return 'Connection: ' + conn_id + '\n' + self._rows_as_pretty_string(cursor)
        except self.driver.driver_module.Error:
            # MySQL 5, or no permissions
            try:
                cursor.execute("""
                SELECT * from information_schema.innodb_locks l
                INNER JOIN information_schema.INNODB_TRX x ON l.lock_trx_id = x.trx_id
                """)
                rows = self._rows_as_pretty_string(cursor)
            except self.driver.driver_module.Error:
                # MySQL 8, and we had no permissions.
                return 'Connection: ' + conn_id
            return 'Connection: ' + conn_id + '\n' + rows

    def hold_pack_lock(self, cursor):
        """Try to acquire the pack lock.

        Raise an exception if packing or undo is already in progress.
        """
        stmt = "SELECT GET_LOCK(CONCAT(DATABASE(), '.pack'), 0)"
        cursor.execute(stmt)
        res = cursor.fetchone()[0]
        if not res:
            raise UnableToAcquirePackUndoLockError('A pack or undo operation is in progress')

    def release_pack_lock(self, cursor):
        """Release the pack lock."""
        stmt = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.pack'))"
        cursor.execute(stmt)
        rows = cursor.fetchall() # stay in sync
        assert rows
