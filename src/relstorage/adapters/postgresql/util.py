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
"""PostgreSQL utilities."""
from __future__ import absolute_import
from __future__ import print_function

def backend_pid_for_connection(conn, cursor):
    try:
        pid = conn.info.backend_pid
    except AttributeError:
        # connection.info was added to psycopg2 in 2.8.0
        cursor.execute("SELECT pg_backend_pid()")
        pid = cursor.fetchall()[0][0]
    return pid


def debug_locks(cursor, me_only=False, exclusive_only=False): # pragma: no cover
    if me_only:
        conn = cursor.connection
        pid = backend_pid_for_connection(conn, cursor)
        params = (pid,)
        pid_clause = ' AND a.pid = %s '
    else:
        pid_clause = ''
        params = ()
    excl_clause = ''
    if exclusive_only:
        excl_clause = """ AND l.mode like '%%Exclu%%' """

    stmt = """
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
    WHERE l.locktype <> 'virtualxid'
    {pid_clause}
    {excl_clause}
    ORDER BY a.pid
    """.format(
        pid_clause=pid_clause,
        excl_clause=excl_clause
    )
    cursor.execute(stmt, params)
    return cursor

def debug_my_locks(cursor, exclusive_only=False): # pragma: no cover
    return debug_locks(cursor, me_only=True, exclusive_only=exclusive_only)

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
