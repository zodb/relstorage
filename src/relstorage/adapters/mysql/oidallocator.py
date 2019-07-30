# -*- coding: utf-8 -*-
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
IOIDAllocator implementations.
"""

from __future__ import absolute_import

from perfmetrics import metricmethod
from zope.interface import implementer

from ..interfaces import IOIDAllocator
from ..oidallocator import AbstractOIDAllocator
from .locker import lock_timeout

logger = __import__('logging').getLogger(__name__)

@implementer(IOIDAllocator)
class MySQLOIDAllocator(AbstractOIDAllocator):

    # After this many allocated OIDs should the (unlucky) thread that
    # allocated the one evenly divisible by this number attempt to remove
    # old OIDs.
    garbage_collect_interval = 100001

    # How many OIDs to attempt to delete at any one request. Keeping
    # this on the small side relative to the interval limits the
    # chances of deadlocks (by observation).
    garbage_collect_interactive_size = 1000

    # How many OIDs to attempt to delete if we're not allocating, only
    # garbage collecting.
    garbage_collect_batch_size = 3000

    # How long to wait in seconds before timing out when batch deleting locks.
    # Note that we don't do this interactively because our connection
    # already has a custom timeout.
    garbage_collect_batch_timeout = 5

    def __init__(self, driver):
        """
        :param type disconnected_exception: The exception to raise when
           we get an invalid value for ``lastrowid``.
        """
        AbstractOIDAllocator.__init__(self)
        self.driver = driver

    # https://dev.mysql.com/doc/refman/5.7/en/example-auto-increment.html
    # "Updating an existing AUTO_INCREMENT column value in an InnoDB
    # table does not reset the AUTO_INCREMENT sequence as it does for
    # MyISAM and NDB tables."
    #
    # Thus we must do a pure INSERT, not a REPLACE or UPDATE..
    #
    # Alternatively, ALTER TABLE ... AUTO_INCREMENT = X;
    # But: That behaves different by version (persistent on 8, non-persistent
    # on 5.7; that's probably ok.) However, it causes an auto-commit, which
    # is certainly not desired as this is used by a store connection.
    #
    # https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html#innodb-auto-increment-lock-modes
    # In InnoDB, it must be indexed as a leading column.
    #
    # "In all lock modes (0, 1, and 2), if a transaction that
    # generated auto-increment values rolls back, those auto-increment
    # values are 'lost'."
    #
    # "If you delete the row containing the maximum value for an
    # AUTO_INCREMENT column, the value is not reused for a MyISAM or
    # InnoDB table. If you delete all rows in the table with DELETE
    # FROM tbl_name (without a WHERE clause) in autocommit mode, the
    # sequence starts over for all storage engines except InnoDB and
    # MyISAM."
    #
    # Regarding `TRUNCATE TABLE`:
    #
    # "Any AUTO_INCREMENT value is reset to its start value. This is
    # true even for MyISAM and InnoDB, which normally do not reuse
    # sequence values."
    def set_min_oid(self, cursor, oid_int):
        # A simple statement like the following can easily deadlock:
        #
        # INSERT INTO new_oid (zoid)
        # SELECT %s
        # WHERE %s > (SELECT COALESCE(MAX(zoid), 0) FROM new_oid)
        #
        #
        # Session A: new_oid() -> Lock 1
        # Session B: new_oid() -> Lock 2 (row 1 is invisible)
        # Session A: new_oid() -> Lock 3 (row 2 is invisible)
        # Session B: set_min_oid(2) -> Hang waiting for lock
        # Session A: new_oid() -> Lock 4: Deadlock, Session B rolls back.
        #
        # Partly this is because MAX() is local to the current session.
        # We deal with this by using a stored procedure to efficiently make
        # multiple queries.

        n = (oid_int + 15) // 16
        self.driver.callproc_multi_result(cursor, 'set_min_oid(%s)', (n,))

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        # Generate a new auto_incremented ID. This will never conflict
        # with any other session because generated IDs are guaranteed
        # to be unique. However, the DELETE statement may interfere
        # with other sessions and lead to deadlock; this is true even
        # when using the 'DELETE IGNORE'. The INSERT statement takes out an
        # exclusive lock on the PRIMARY KEY index.
        #
        # Session A: INSERTS 2000. -> lock 2000
        # Session B: INSERTS 3000. -> lock 3000
        # Session B: DELETE < 3000 -> wants lock on 2000; locks everything under.
        # Session A: DELETE < 2000 -> hold lock on 2000; wants to lock things under.
        #
        # ORDER BY zoid (ASC or DESC) just runs into more lock issues reported for
        # individual rows.
        #
        # Using the single-row update example provided by
        # https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
        # also runs into deadlocks (naturally).
        #
        # Our solution is to just let rows build up if the delete fails. Eventually
        # a GC, which happens at startup, will occur and hopefully get most of them.
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)

        # This is a DB-API extension. Fortunately, all
        # supported drivers implement it. (In the past we used
        # cursor.connection.insert_id(), which was specific to MySQLdb
        # and PyMySQL.)
        # 'SELECT LAST_INSERT_ID()' is the pure-SQL way to do this.
        n = cursor.lastrowid

        if n % self.garbage_collect_interval == 0:
            self.garbage_collect_oids(cursor, n)
        return self._oid_range_around(n)

    def garbage_collect_oids(self, cursor, max_value=None):
        # Clean out previously generated OIDs.
        batch_size = self.garbage_collect_interactive_size
        batch_timeout = None
        if not max_value:
            batch_size = self.garbage_collect_batch_size
            batch_timeout = self.garbage_collect_batch_timeout
            cursor.execute('SELECT MAX(zoid) FROM new_oid')
            row = cursor.fetchone()
            if row:
                max_value = row[0]
        if not max_value:
            return
        # Delete old OIDs, starting with the oldest.
        stmt = """
        DELETE IGNORE
        FROM new_oid
        WHERE zoid < %s
        ORDER BY zoid ASC
        LIMIT %s
        """
        params = (max_value, batch_size)
        __traceback_info__ = stmt, params
        rowcount = True
        with lock_timeout(cursor, batch_timeout):
            while rowcount:
                try:
                    cursor.execute(stmt, params)
                except Exception: # pylint:disable=broad-except
                    # Luckily, a deadlock only rolls back the previous
                    # statement, not the whole transaction.
                    #
                    # XXX: No, that's not true. A general error, like
                    # a lock timeout, will roll back the previous
                    # statement. A deadlock rolls back the whole
                    # transaction. We're lucky the difference here
                    # doesn't make any difference: we don't actually
                    # write anything to the database temp tables, etc,
                    # until the storage enters commit(), at which
                    # point we shouldn't need to allocate any more new
                    # oids.
                    #
                    # TODO: We'd prefer to only do this for errcode 1213: Deadlock.
                    # MySQLdb raises this as an OperationalError; what do all the other
                    # drivers do?
                    logger.debug("Failed to garbage collect allocated OIDs", exc_info=True)
                    break
                else:
                    rowcount = cursor.rowcount
                    logger.debug("Garbage collected %s old OIDs less than %s", rowcount, max_value)

    def reset_oid(self, cursor):
        cursor.execute("TRUNCATE TABLE new_oid")
