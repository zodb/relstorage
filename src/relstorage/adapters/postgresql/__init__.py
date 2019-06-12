##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
"""PostgreSQL adapter for RelStorage."""
from __future__ import absolute_import

from relstorage.adapters.postgresql.adapter import PostgreSQLAdapter
from relstorage.adapters.postgresql.adapter import select_driver

assert PostgreSQLAdapter
assert select_driver


def debug_my_locks(cursor): # pragma: no cover
    conn = cursor.connection
    pid = conn.info.backend_pid
    cursor.execute("""
    SELECT
         a.datname,
         l.relation::regclass,
         l.locktype,
         l.mode,
         l.pid,
         l.virtualxid, l.transactionid,
         l.objid, l.objsubid,
         l.virtualtransaction,
         l.granted
    FROM pg_stat_activity a
    JOIN pg_locks l ON l.pid = a.pid
    WHERE a.pid = %s and l.mode like '%%Exclu%%'
    AND l.locktype <> 'virtualxid'
    ORDER BY a.pid
    """, (pid,))
    rows = cursor.fetchall()
    rows = '\n'.join('\t'.join((str(i) for i in r)) for r in rows)
    header = '*Locks for %s isomode %s\n' % (pid, conn.isolation_level)
    return header + rows
