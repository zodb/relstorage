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
from __future__ import print_function

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


def print_size_report(cursor): # pragma: no cover
    extra_query = ''
    query = """
    SELECT table_name,
        pg_size_pretty(total_bytes) AS total,
        pg_size_pretty(index_bytes) AS INDEX,
        pg_size_pretty(toast_bytes) AS toast,
        pg_size_pretty(table_bytes) AS TABLE
    FROM (
    SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
    SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
          , c.reltuples AS row_estimate
          , pg_total_relation_size(c.oid) AS total_bytes
          , pg_indexes_size(c.oid) AS index_bytes
          , pg_total_relation_size(reltoastrelid) AS toast_bytes
      FROM pg_class c
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE relkind = 'r'
        ) a
    ) a
    WHERE (table_name NOT LIKE 'pg_%' and table_name not like 'abstract_%'
    )
    AND table_schema <> 'pg_catalog' and table_schema <> 'information_schema'
    ORDER BY total_bytes DESC
    """

    cursor.execute(query)
    keys = ['table_name', 'total', 'index', 'toast', 'table']
    rows = [dict(zip(keys, row)) for row in cursor]

    rows.insert(0, {k: k for k in keys})
    print()
    fmt = "| {table_name:35s} | {total:10s} | {index:10s} | {toast:10s} | {table:10s}"
    for row in rows:
        if not extra_query and row['total'] in ('72 kB', '32 kB', '24 kB', '16 kB', '8192 bytes'):
            continue
        print(fmt.format(
            **{k: v if v else '<null>' for k, v in row.items()}
        ))
