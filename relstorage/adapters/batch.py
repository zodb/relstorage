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
"""Batch table row insert/delete support.
"""

from relstorage._compat import iterkeys, iteritems


class RowBatcher(object):
    """
    Generic row batcher.

    Expects '%s' parameters and a tuple for each row.
    """

    row_limit = 100
    size_limit = 1 << 20

    def __init__(self, cursor, row_limit=None):
        self.cursor = cursor
        if row_limit is not None:
            self.row_limit = row_limit
        self.rows_added = 0
        self.size_added = 0
        self.deletes = {}  # {(table, columns_tuple): set([(column_value,)])}
        self.inserts = {}  # {(command, header, row_schema): {rowkey: [row]}}

    def delete_from(self, table, **kw):
        if not kw:
            raise AssertionError("Need at least one column value")
        columns = tuple(sorted(iterkeys(kw)))
        key = (table, columns)
        rows = self.deletes.get(key)
        if rows is None:
            self.deletes[key] = rows = set()
        row = tuple(str(kw[column]) for column in columns)
        rows.add(row)
        self.rows_added += 1
        if self.rows_added >= self.row_limit:
            self.flush()

    def insert_into(self, header, row_schema, row, rowkey, size,
                    command='INSERT'):
        key = (command, header, row_schema)
        rows = self.inserts.get(key)
        if rows is None:
            self.inserts[key] = rows = {}
        rows[rowkey] = row  # note that this may replace a row
        self.rows_added += 1
        self.size_added += size
        if (self.rows_added >= self.row_limit
                or self.size_added >= self.size_limit):
            self.flush()

    def flush(self):
        if self.deletes:
            self._do_deletes()
            self.deletes.clear()
        if self.inserts:
            self._do_inserts()
            self.inserts.clear()
        self.rows_added = 0
        self.size_added = 0

    def _do_deletes(self):
        for (table, columns), rows in sorted(iteritems(self.deletes)):
            rows = list(sorted(rows))
            if len(columns) == 1:
                value_str = ','.join(v for (v,) in rows)
                stmt = "DELETE FROM %s WHERE %s IN (%s)" % (
                    table, columns[0], value_str)
            else:
                lines = []
                for row in rows:
                    line = []
                    for i, column in enumerate(columns):
                        line.append("%s = %s" % (column, row[i]))
                    lines.append(" AND ".join(line))
                stmt = "DELETE FROM %s WHERE %s" % (
                    table, " OR ".join(lines))
            self.cursor.execute(stmt)

    def _do_inserts(self):
        items = sorted(iteritems(self.inserts))
        for (command, header, row_schema), rows in items:
            # Batched inserts
            parts = []
            params = []
            s = "(%s)" % row_schema
            for row in rows.values():
                parts.append(s)
                params.extend(row)

            stmt = "%s INTO %s VALUES\n%s" % (command, header, ',\n'.join(parts))
            self.cursor.execute(stmt, tuple(params))
