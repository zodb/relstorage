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

from zope.interface import implementer


from ..interfaces import IOIDAllocator
from ..oidallocator import AbstractTableOIDAllocator
from .locker import lock_timeout



@implementer(IOIDAllocator)
class MySQLOIDAllocator(AbstractTableOIDAllocator):

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
    def _set_min_oid_from_range(self, cursor, n):
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
        self.driver.callproc_no_result(cursor, 'set_min_oid(%s)', (n,))

   # Notes on new_oids:

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

    # cursor.lastrowid is used in the superclass; that's a DB-API
    # extension. Fortunately, all supported drivers implement it. (In
    # the past we used cursor.connection.insert_id(), which was
    # specific to MySQLdb and PyMySQL.) 'SELECT LAST_INSERT_ID()' is
    # the pure-SQL way to do this.

    def _timeout(self, cursor, batch_timeout):
        return lock_timeout(cursor, batch_timeout)
