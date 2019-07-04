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


@implementer(IOIDAllocator)
class MySQLOIDAllocator(AbstractOIDAllocator):

    def __init__(self, disconnected_exception):
        """
        :param type disconnected_exception: The exception to raise when
           we get an invalid value for ``lastrowid``.
        """
        AbstractOIDAllocator.__init__(self)
        self.disconnected_exception = disconnected_exception

    # https://dev.mysql.com/doc/refman/5.7/en/example-auto-increment.html
    # "Updating an existing AUTO_INCREMENT column value in an InnoDB
    # table does not reset the AUTO_INCREMENT sequence as it does for
    # MyISAM and NDB tables."
    #
    # Thus we must to a pure INSERT.
    #
    # Alternatively, ALTER TABLE ... AUTO_INCREMENT = X;
    # But: That behaves different by version (persistent on 8, non-persistent
    # on 5.7; that's probably ok.) However, it causes an auto-commit, which
    # may or may not be desired; have to check.
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
    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        n = (oid + 15) // 16
        cursor.execute("REPLACE INTO new_oid VALUES(%s)", (n,))

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)
        # This is a DB-API extension. Fortunately, all
        # supported drivers implement it. (In the past we used
        # cursor.connection.insert_id(), which was specific to MySQLdb)
        n = cursor.lastrowid

        # At least in one setup (gevent/umysqldb/pymysql/mysql 5.5)
        # we have observed cursor.lastrowid to be None.
        if n is None:
            raise self.disconnected_exception("Invalid return for lastrowid")

        if n % 1000 == 0:
            # Clean out previously generated OIDs.
            stmt = "DELETE FROM new_oid WHERE zoid < %s"
            cursor.execute(stmt, (n,))
        return self._oid_range_around(n)
