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
Stats implementations
"""

from __future__ import absolute_import

from ..stats import AbstractStats


class PostgreSQLStats(AbstractStats):

    # Getting the COUNT(*)  of tables can be very expensive
    # due to the need to examine rows to check their MVCC visibility.
    # This method only promises to get the *approximate* size,
    # so we use the tables the optimizer uses to get stats.
    # With the autovacuum daemon running, this shouldn't get *too* far
    # out of date.
    # (https://www.postgresql.org/docs/11/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW)
    _get_object_count_queries = (
        "SELECT reltuples::bigint FROM pg_class WHERE relname = 'current_object'",
        "SELECT reltuples::bigint FROM pg_class WHERE relname = 'object_state'"
    )


    def get_db_size(self):
        """Returns the approximate size of the database in bytes"""
        def get_size(_conn, cursor):
            cursor.execute("SELECT pg_database_size(current_database())")
            return cursor.fetchone()[0]
        return self.connmanager.open_and_call(get_size)

    def large_database_change(self):
        conn, cursor = self.connmanager.open()
        try:
            # VACUUM cannot be run inside a transaction block;
            # ANALYZE can be. Both update pg_class.reltuples.
            # VACUUM needs a read table lock, meaning it can be blocked by writes
            # and vice-versa; ANALYZE doesn't appear to need locks.
            cursor.execute('ANALYZE')
            # Depending on the number of pages this touched, the estimate
            # can be better or worse.
            conn.commit()
        finally:
            self.connmanager.close(conn, cursor)
