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
from collections import defaultdict
import itertools

from relstorage._compat import iteritems


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
        self.rows_deleted = 0
        self.size_added = 0
        self.deletes = defaultdict(set)   # {(table, columns_tuple): set([(column_value,)])}
        self.inserts = defaultdict(dict)  # {(command, header, row_schema, suffix): {rowkey: [row]}}

    def delete_from(self, table, **kw):
        """
        .. caution:: The keyword values must have a valid str representation.
        """
        if not kw:
            raise AssertionError("Need at least one column value")
        columns = tuple(sorted(kw))
        key = (table, columns)
        rows = self.deletes[key]
        # string conversion in done by _do_deletes
        row = tuple(kw[column] for column in columns)
        rows.add(row)
        self.rows_added += 1
        self.rows_deleted += 1
        if self.rows_added >= self.row_limit:
            self.flush()

    def insert_into(self, header, row_schema, row, rowkey, size,
                    command='INSERT', suffix=''):
        key = (command, header, row_schema, suffix)
        rows = self.inserts[key]
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
            # XXX: Stop doing string conversion manually. Let the
            # cursor do it. It may have a non-text protocol for integer
            # objects; it may also have a different representation in text.
            if len(columns) == 1:
                value_str = ','.join(str(v) for (v,) in rows)
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
        # In the case that we have only a single table we're inserting into,
        # and thus common key values, we don't need to sort or iterate.
        # If we have multiple tables, we never want to sort by the rows too,
        # that doesn't make any sense.
        if len(self.inserts) == 1:
            items = [self.inserts.popitem()]
        else:
            items = sorted(iteritems(self.inserts), key=lambda i: i[0])
        for (command, header, row_schema, suffix), rows in items:
            # Batched inserts
            rows = list(rows.values())
            value_template = "(%s)" % row_schema
            values_template = [value_template] * len(rows)
            params = list(itertools.chain.from_iterable(rows))

            stmt = "%s INTO %s VALUES\n%s\n%s" % (
                command, header, ',\n'.join(values_template), suffix)
            # e.g.,
            # INSERT INTO table(c1, c2)
            # VALUES (%s, %s), (%s, %s), (%s, %s)
            # <suffix>
            self.cursor.execute(stmt, params)
